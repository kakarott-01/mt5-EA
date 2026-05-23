//+------------------------------------------------------------------+
//|                                             ApexExecution.mq5    |
//|                                               Arunaditya Lal     |
//|                                        https://www.mql5.com      |
//+------------------------------------------------------------------+
#property copyright "Arunaditya Lal"
#property link      "https://www.mql5.com"
#property version   "4.08"
#property strict

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>

#ifndef TRADE_CONTEXT_BUSY
   #define TRADE_CONTEXT_BUSY 146
#endif
#ifndef ERR_TRADE_CONTEXT_BUSY
   #define ERR_TRADE_CONTEXT_BUSY TRADE_CONTEXT_BUSY
#endif

//+------------------------------------------------------------------+
//| INPUT PARAMETERS                                                 |
//+------------------------------------------------------------------+
input group "=== EXECUTION ==="
input int      NumTrades           = 1;
input double   LotSize             = 0.01;
input double   StopLoss            = 0;
input double   TakeProfit          = 0;
input int      Slippage            = 10;

input group "=== FILTERS ==="
input double   MaxSpread           = 3.0;

input group "=== TRAILING STOP ==="
input double   TrailingStop        = 0;
input double   TrailingStep        = 5;

input group "=== BREAKEVEN ==="
input double   BreakevenTrigger    = 20;
input double   BreakevenBuffer     = 2;

input group "=== RISK MANAGEMENT ==="
input bool     UseRiskPercent      = false;
input double   RiskPercent         = 1.0;
input double   PartialClosePct     = 50.0;

input group "=== EA SETTINGS ==="
input long     MagicBase           = 123456;
input bool     HotkeysEnabled      = true;

input group "=== RECOVERY ==="
input int      ReconnectStableMs     = 500;
input int      RebuildMinIntervalSec = 2;
input int      RebuildBackoffMs      = 500;
input int      RebuildMaxAttempts    = 5;

input group "=== DIAGNOSTICS ==="
input bool     EnableDiagnostics     = true;
input int      DiagnosticsIntervalMs = 5000;
input int      DiagnosticsQueueWarn  = 50;
input int      DiagnosticsStateWarnMs= 3000;

//+------------------------------------------------------------------+
//| OBJECTS                                                          |
//+------------------------------------------------------------------+
CTrade        trade;
CPositionInfo posInfo;

//+------------------------------------------------------------------+
//| STRUCTS                                                          |
//+------------------------------------------------------------------+
struct BatchInfo
  {
   long      magic;
   string    symbol;
   int       direction;
   double    sl;
   double    tp;
   bool      syncing;
   bool      trailing;
   bool      active;
   datetime  created;
   int       requested;
   int       filled;
   int       errors;
   bool      partial;
   int       modifyCount;
   uint      modifyWindowStartMs;
   int       closeCount;
   uint      closeWindowStartMs;
  };

struct PositionState
  {
   ulong     ticket;
   long      magic;
   string    symbol;
   int       direction;
   double    sl;
   double    tp;
   bool      hasSL;
   bool      hasTP;
   datetime  lastUpdate;
  };

struct ModifyLock
  {
   ulong ticket;
   uint  unlockAtMs;
  };

struct ModifyRequest
  {
   ulong   ticket;
   double  sl;
   double  tp;
   string  context;
   uint    rc;
   int     status;
   uint    requestedAtMs;
  };

struct RetryState
  {
   ulong ticket;
   int   attempts;
   uint  lastAttemptMs;
   uint  cooldownMs;
  };

//+------------------------------------------------------------------+
//| EXEC STATE                                                       |
//+------------------------------------------------------------------+
// EXEC_MODIFYING (7): distinguishes modify-queue dispatch from fill loop.
enum EXEC_STATE { EXEC_IDLE=0, EXEC_EXECUTING=1, EXEC_SYNCING=2, EXEC_TRAILING=3,
                  EXEC_CLOSING=4, EXEC_RECOVERING=5, EXEC_REBUILDING=6,
                  EXEC_MODIFYING=7 };

//+------------------------------------------------------------------+
//| GLOBAL STATE                                                     |
//+------------------------------------------------------------------+
BatchInfo     g_Batches[];
PositionState g_PosStates[];
ModifyLock    g_ModifyLocks[];
ModifyRequest g_ModifyQueue[];
RetryState    g_RetryStates[];

bool    g_IsExecuting             = false;
bool    g_WasConnected            = true;
bool    g_AbortFlag               = false;
bool    g_IsDeinitializing        = false;
bool    g_PendingReconnectResync  = false;
bool    g_PendingReconnectRebuild = false;
datetime g_LastRegistryRebuild   = 0;
uint    g_ReconnectDetectedMs    = 0;
int     g_RebuildAttempts        = 0;
uint    g_RebuildNextAllowedMs   = 0;
int     g_ResyncAttempts         = 0;
uint    g_ResyncNextAllowedMs    = 0;

// v4.07: runtime trailing toggle
bool    g_TrailingEnabled        = true;

// Panel interaction state
bool    g_HoverBuy               = false;
bool    g_HoverSell              = false;
bool    g_HoverTrail             = false;
int     g_SelectedBatchIndex     = -1;
int     g_PanelNumTrades         = 0;
double  g_PanelLotSize           = 0;
double  g_PanelSL                = 0;
double  g_PanelTP                = 0;
uint    g_LastTradeActionMs      = 0;
int     g_TradeDelayMs           = 10;

// Integrity check
uint    g_LastIntegrityCheckMs   = 0;

// Exec state
int     g_ExecState        = EXEC_IDLE;
uint    g_ExecStateStartMs = 0;
uint    g_LastTimerMs      = 0;

// Counters
int g_ModifyTotalEnqueued = 0;
int g_ModifySuccessCount  = 0;
int g_ModifyFailCount     = 0;
int g_InFlightModifies    = 0;
int g_InFlightCloses      = 0;

uint g_LastDiagMs       = 0;
int  g_TimerSkipCount   = 0;
int  g_SyncSkipCount    = 0;
int  g_TrailSkipCount   = 0;
int  g_ModQSkipCount    = 0;
int  g_RebuildSkipCount = 0;
int  g_CloseSkipCount   = 0;

// v4.08: Async burst execution dispatcher state
bool   g_PendingExecActive       = false;
int    g_PendingExecTotal        = 0;
int    g_PendingExecDispatched   = 0;
int    g_PendingExecErrors       = 0;
long   g_PendingExecBatchID      = 0;
double g_PendingExecLot          = 0.0;
int    g_PendingExecDir          = 0;
double g_PendingExecSL           = 0.0;
double g_PendingExecTP           = 0.0;
uint   g_PendingExecLastBurstMs  = 0;
bool   g_PendingExecPostAttach   = false;
uint   g_PendingExecPostAttachMs = 0;
double g_PendingExecBatchSLPrice = 0.0;
double g_PendingExecBatchTPPrice = 0.0;

//+------------------------------------------------------------------+
//| CONSTANTS                                                        |
//+------------------------------------------------------------------+
const int TRADE_DELAY_MIN_MS = 10;
const int TRADE_DELAY_MAX_MS = 25;
const int TRADE_RETRY_MAX    = 4;
const int MODIFY_COOLDOWN_MS = 200;
const int RETRY_ESCALATION_FACTOR = 4;

// C-001 fix: EXEC_EXECUTING_TIMEOUT_MS=360s covers worst-case scenarios.
const int EXEC_STATE_TIMEOUT_MS      = 10000;
const int EXEC_EXECUTING_TIMEOUT_MS  = 360000;
const int EXEC_MODIFYING_TIMEOUT_MS  = 30000;
const int INTEGRITY_CHECK_INTERVAL_MS= 60000;

const int MAX_MODIFIES_PER_BATCH_PER_MIN  = 360;
const int MAX_GLOBAL_CONCURRENT_MODIFIES  = 6;
const int MAX_CLOSES_PER_BATCH_PER_MIN    = 60;
const int MAX_GLOBAL_CONCURRENT_CLOSES    = 4;

// v4.07: M-AUDIT-001 fix
const int MAX_MODIFY_QUEUE_SIZE = 250;

// v4.08: Async burst dispatcher constants
// BURST_ORDER_LIMIT: max orders per burst window. Reduce to 25-30 if Exness returns 10027.
// BURST_COOLDOWN_MS: gap between bursts. 5 bursts x 200ms = ~1s for 200 orders.
// POST_ATTACH_DELAY_MS: timer-gated wait replacing Sleep(250) in PostFillAttachSLTP.
const int BURST_ORDER_LIMIT    = 40;
const int BURST_COOLDOWN_MS    = 200;
const int POST_ATTACH_DELAY_MS = 500;

//+------------------------------------------------------------------+
//| PANEL CONSTANTS                                                  |
//+------------------------------------------------------------------+
const string P              = "MOE_";
const string SL_PLACEHOLDER = "50";
const string TP_PLACEHOLDER = "100";
const int    PR             = 36;
const int    PT             = 45;
const int    PW             = 400;
const int    PH             = 774;
const int    PH_MIN         = 70;

const int    BUY_X          = 16;
const int    BUY_Y          = 336;
const int    BUY_W          = 176;
const int    BUY_H          = 44;
const int    SELL_X         = 208;
const int    SELL_Y         = 336;
const int    SELL_W         = 176;
const int    SELL_H         = 44;

const int    TRAIL_X        = 16;
const int    TRAIL_Y        = 402;
const int    TRAIL_W        = 368;
const int    TRAIL_H        = 32;

// Core colors
const color  CLR_BG          = C'14,16,22';
const color  CLR_SURFACE     = C'22,25,34';
const color  CLR_BORDER      = C'60,66,86';
const color  CLR_DIVIDER     = C'48,52,70';
const color  CLR_TEXT        = C'230,232,238';
const color  CLR_MUTED       = C'145,150,170';
const color  CLR_SUBTLE      = C'95,100,120';
const color  CLR_EDIT_BG     = C'28,32,44';
const color  CLR_EDIT_BD     = C'72,78,100';
const color  CLR_BUY         = C'0,150,95';
const color  CLR_BUY_HOVER   = C'0,190,125';
const color  CLR_SELL        = C'180,60,60';
const color  CLR_SELL_HOVER  = C'220,85,85';
const color  CLR_CLOSE       = C'135,35,35';
const color  CLR_BE          = C'25,95,150';
const color  CLR_PART        = C'115,85,10';
const color  CLR_PLACEHOLDER = C'110,115,130';

const color  CLR_TRAIL_ON        = C'0,150,95';
const color  CLR_TRAIL_ON_HOVER  = C'0,190,125';
const color  CLR_TRAIL_OFF       = C'45,55,45';
const color  CLR_TRAIL_OFF_HOVER = C'62,78,62';
const color  CLR_TRAIL_DIS       = C'38,38,42';

const color  CLR_STATUS_OK      = C'0,200,110';
const color  CLR_STATUS_WARN    = C'255,200,0';
const color  CLR_STATUS_ERROR   = C'220,80,80';
const color  CLR_STATUS_EXEC    = C'40,140,255';
const color  CLR_STATUS_RECOVER = C'255,140,0';
const color  CLR_STATUS_SYNC    = C'180,80,255';

// Panel position state
int  g_PanelRight     = PR;
int  g_PanelTop       = PT;
bool g_PanelMinimized = false;
bool g_PanelDragging  = false;
int  g_DragOffsetX    = 0;
int  g_DragOffsetY    = 0;

int PX(int lx) { return g_PanelRight + PW - lx; }
int PY(int ly) { return g_PanelTop + ly; }

//+------------------------------------------------------------------+
//| FORWARD DECLARATIONS                                             |
//+------------------------------------------------------------------+
void RefreshTrailingButton();
void RefreshBatchPanel();
void SetStatus(string msg, color clr);
string GetBreakevenSkipReason(long magic);
double BatchTotalLots(long magic, string symbol);
bool IsBatchBreakevenActive(long magic, string symbol);
void ToggleTrailing();
void ProcessExecutionBurst();
bool DispatchOrderNow(int direction, double lot, string comment, uint &rc);

//+------------------------------------------------------------------+
//| OnInit                                                           |
//+------------------------------------------------------------------+
int OnInit()
  {
   g_IsDeinitializing = false;
   LoadPanelLayout();

   g_TrailingEnabled = (TrailingStop > 0.0);

   ENUM_ACCOUNT_MARGIN_MODE marginMode =
      (ENUM_ACCOUNT_MARGIN_MODE)AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   Print("Margin mode = ", marginMode);

   if(marginMode == ACCOUNT_MARGIN_MODE_RETAIL_NETTING ||
      marginMode == ACCOUNT_MARGIN_MODE_EXCHANGE)
     {
      Alert("ApexExecution: Netting account detected. Requires Hedging.");
      return INIT_FAILED;
     }

   g_PanelNumTrades = NumTrades;
   g_PanelLotSize   = LotSize;
   g_PanelSL        = StopLoss;
   g_PanelTP        = TakeProfit;

   ENUM_ORDER_TYPE_FILLING fill = DetectFillingMode();
   trade.SetDeviationInPoints(Slippage);
   trade.SetTypeFilling(fill);
   trade.SetAsyncMode(false);   // v4.08: default sync; toggled async only during burst
   ChartSetInteger(0, CHART_EVENT_MOUSE_MOVE, true);

   RebuildBatchRegistryFromPositions();
   CleanupOrphanBatches();
   LogEvent("RECOVERY", "Recovered "+IntegerToString(ArraySize(g_Batches))+
            " active batch(es) from live positions");

   CreatePanel();

   bool acctAllowed = (bool)AccountInfoInteger(ACCOUNT_TRADE_ALLOWED);
   bool termAllowed = (bool)TerminalInfoInteger(TERMINAL_TRADE_ALLOWED);
   long tradeMode   = SymbolInfoInteger(Symbol(), SYMBOL_TRADE_MODE);
   if(!acctAllowed || !termAllowed || tradeMode == SYMBOL_TRADE_MODE_DISABLED)
     {
      SetStatus("Trading disabled. EA stopped.", CLR_STATUS_ERROR);
      Alert("ApexExecution: Trading disabled. EA stopped.");
      return INIT_FAILED;
     }

   if(!EventSetMillisecondTimer(100))
      LogEvent("INIT_WARN", "Timer setup failed. Error="+IntegerToString(GetLastError()));

   Print("ApexExecution v4.08: Ready on ", Symbol());
   return INIT_SUCCEEDED;
  }

//+------------------------------------------------------------------+
//| OnDeinit                                                         |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   g_IsDeinitializing = true;
   g_AbortFlag        = true;

   // v4.08: abort any in-progress async burst
   if(g_PendingExecActive || g_PendingExecPostAttach)
     {
      g_PendingExecActive     = false;
      g_PendingExecPostAttach = false;
      trade.SetAsyncMode(false);
      LogEvent("DEINIT", "Aborted active burst dispatcher on deinit");
     }

   SavePanelLayout();
   ChartSetInteger(0, CHART_EVENT_MOUSE_MOVE, false);
   EventKillTimer();
   DeletePanel();
   LogEvent("DEINIT", "Removed. Reason="+IntegerToString(reason));
  }

//+------------------------------------------------------------------+
//| OnTimer — 100ms heartbeat                                        |
//+------------------------------------------------------------------+
void OnTimer()
  {
   if(g_IsDeinitializing) return;

   uint nowMs = GetTickCount();
   if(g_LastTimerMs != 0 && nowMs - g_LastTimerMs < 50)
     {
      LogEvent("TIMER_SKIP", "overlap");
      g_TimerSkipCount++;
      return;
     }
   g_LastTimerMs = nowMs;

   bool connected = (bool)TerminalInfoInteger(TERMINAL_CONNECTED);

   //--- Connection state change
   if(!connected && g_WasConnected)
     {
      g_WasConnected = false;
      SetStatus("DISCONNECTED — positions unmonitored", CLR_STATUS_ERROR);
      LogEvent("RECONNECT", "Connection lost");
     }
   else if(connected && !g_WasConnected)
     {
      g_WasConnected = true;
      SetStatus("Reconnected — re-syncing...", CLR_STATUS_RECOVER);

      // H-003 fix: flush stale pre-disconnect price-level queue entries
      ArrayResize(g_ModifyQueue, 0);
      g_ModifyTotalEnqueued = 0;
      LogEvent("RECONNECT", "Flushed modify queue on reconnect");

      g_PendingReconnectRebuild = true;
      g_PendingReconnectResync  = true;
      g_ReconnectDetectedMs     = nowMs;
      g_RebuildAttempts         = 0;
      g_ResyncAttempts          = 0;
      g_RebuildNextAllowedMs    = nowMs;
      g_ResyncNextAllowedMs     = nowMs;
      LogEvent("RECONNECT", "Connection restored; resync queued");
     }

   if(connected && (g_PendingReconnectRebuild || g_PendingReconnectResync))
     {
      bool stable = (g_ReconnectDetectedMs == 0 ||
                     (nowMs - g_ReconnectDetectedMs) >= (uint)ReconnectStableMs);
      if(!stable)
        {
         LogEvent("RECONNECT", "Waiting for stable connection");
        }
      else
        {
         if(g_PendingReconnectRebuild && nowMs >= g_RebuildNextAllowedMs)
           {
            LogEvent("RECONNECT", "Processing queued registry rebuild");
            if(TimeCurrent() - g_LastRegistryRebuild >= RebuildMinIntervalSec)
              {
               if(AcquireExecState(EXEC_REBUILDING))
                 {
                  RebuildBatchRegistryFromPositions();
                  CleanupOrphanBatches();
                  ReleaseExecState(EXEC_REBUILDING);
                  g_PendingReconnectRebuild = false;
                  g_RebuildAttempts = 0;
                 }
               else
                 {
                  g_RebuildAttempts++;
                  g_RebuildSkipCount++;
                  if(g_RebuildAttempts >= RebuildMaxAttempts)
                    {
                     g_RebuildNextAllowedMs = nowMs +
                        (uint)(RebuildBackoffMs * RebuildMaxAttempts);
                     LogEvent("RECONNECT", "Rebuild deferred (max attempts)");
                    }
                  else
                    {
                     g_RebuildNextAllowedMs = nowMs +
                        (uint)(RebuildBackoffMs * MathMax(1, g_RebuildAttempts));
                     LogEvent("RECONNECT", "Skipped rebuild due to exec lock");
                    }
                 }
              }
            else
              {
               g_PendingReconnectRebuild = false;
               LogEvent("RECONNECT", "Skipped rebuild (recent snapshot)");
              }
           }

         if(g_PendingReconnectResync && !g_PendingReconnectRebuild &&
            nowMs >= g_ResyncNextAllowedMs)
           {
            LogEvent("RECONNECT", "Processing queued resync");
            if(AcquireExecState(EXEC_SYNCING))
              {
               ResyncAllBatches();
               ReleaseExecState(EXEC_SYNCING);
               g_PendingReconnectResync = false;
               g_ResyncAttempts = 0;
              }
            else
              {
               g_ResyncAttempts++;
               g_SyncSkipCount++;
               if(g_ResyncAttempts >= RebuildMaxAttempts)
                 {
                  g_ResyncNextAllowedMs = nowMs +
                     (uint)(RebuildBackoffMs * RebuildMaxAttempts);
                  LogEvent("RECONNECT", "Resync deferred (max attempts)");
                 }
               else
                 {
                  g_ResyncNextAllowedMs = nowMs +
                     (uint)(RebuildBackoffMs * MathMax(1, g_ResyncAttempts));
                  LogEvent("RECONNECT", "Skipped resync due to exec lock");
                 }
              }
           }
        }
     }

   //--- v4.08: Async burst execution engine
   if(g_PendingExecActive || g_PendingExecPostAttach)
      ProcessExecutionBurst();

   //--- Drop closed batches; purge zombie retry/lock entries
   if(connected)
     {
      if(AcquireExecState(EXEC_RECOVERING))
        {
         CleanupOrphanBatches();
         CleanupPosStates();
         CleanupRetryStates();
         CleanupModifyLocks();
         IntegrityCheck();
         ReleaseExecState(EXEC_RECOVERING);
        }
     }

   //--- Panel refresh
   RefreshPanel();

   //--- Backup SL/TP sync check (safety net for missed events)
   bool syncDidWork = false;
   if(ArraySize(g_Batches) > 0 && connected && !g_IsExecuting)
     {
      if(AcquireExecState(EXEC_SYNCING))
        {
         syncDidWork = BackupSyncCheck();
         ReleaseExecState(EXEC_SYNCING);
        }
      else
        {
         LogEvent("SYNC_SKIP", "skip BackupSyncCheck due to exec lock");
         g_SyncSkipCount++;
        }
     }

   //--- Trailing stop
   if(TrailingStop > 0.0 && g_TrailingEnabled && ArraySize(g_Batches) > 0 &&
      connected && !syncDidWork && !g_IsExecuting)
     {
      if(AcquireExecState(EXEC_TRAILING))
        {
         ProcessTrailing();
         ReleaseExecState(EXEC_TRAILING);
        }
      else
        {
         LogEvent("TRAIL_SKIP", "skip trailing due to exec lock");
         g_TrailSkipCount++;
        }
     }

   //--- Process queued modify requests
   if(connected && !g_IsExecuting)
     {
      if(AcquireExecState(EXEC_MODIFYING))
        {
         ProcessModifyQueue();
         ReleaseExecState(EXEC_MODIFYING);
        }
      else
        {
         LogEvent("MOD_Q_SKIP", "skip modify queue due to exec lock");
         g_ModQSkipCount++;
        }
     }

   DiagnosticsTick();
  }

//+------------------------------------------------------------------+
//| OnTradeTransaction — detect manual SL/TP changes                 |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest     &request,
                        const MqlTradeResult      &result)
  {
   if(g_IsExecuting || ArraySize(g_Batches) == 0) return;
   if(trans.type != TRADE_TRANSACTION_POSITION) return;

   ulong ticket = trans.position;
   if(ticket == 0) return;
   if(!PositionSelectByTicket(ticket)) return;

   long magic = (long)PositionGetInteger(POSITION_MAGIC);
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0) return;

   if(PositionGetString(POSITION_SYMBOL) != g_Batches[batchIndex].symbol) return;
   if(g_Batches[batchIndex].syncing) return;

   double pos_sl    = PositionGetDouble(POSITION_SL);
   double pos_tp    = PositionGetDouble(POSITION_TP);
   double stored_sl = g_Batches[batchIndex].sl;
   double stored_tp = g_Batches[batchIndex].tp;
   int direction    = g_Batches[batchIndex].direction;
   double drift     = SyncDriftThreshold(g_Batches[batchIndex].symbol);

   bool slImproved = IsBetterSL(direction, stored_sl, pos_sl, true);
   bool tpImproved = IsDifferentTP(stored_tp, pos_tp, drift);
   if(slImproved || tpImproved)
     {
      LogEvent("SYNC_EVENT", "Manual SL/TP change detected batch="+
               IntegerToString(magic));
      UpsertPosState(ticket, magic, g_Batches[batchIndex].symbol, direction,
                     pos_sl, pos_tp);
      double targetSL = slImproved ? pos_sl : stored_sl;
      double targetTP = tpImproved ? pos_tp : stored_tp;
      SyncAllPositions(magic, targetSL, targetTP);
     }
  }

//+------------------------------------------------------------------+
//| OnChartEvent — button clicks and hotkeys                         |
//+------------------------------------------------------------------+
void OnChartEvent(const int    id,
                  const long  &lparam,
                  const double &dparam,
                  const string &sparam)
  {
   if(id == CHARTEVENT_OBJECT_CLICK)
     {
      if(sparam == P+"E_SL")
        {
         PrepareOptionalEditForInput(P+"E_SL", SL_PLACEHOLDER);
         return;
        }
      if(sparam == P+"E_TP")
        {
         PrepareOptionalEditForInput(P+"E_TP", TP_PLACEHOLDER);
         return;
        }
      ResetButton(sparam);
      if(sparam == P+"MIN")
        {
         TogglePanelMinimized();
         return;
        }
      if(g_IsExecuting && sparam != P+"BUY" && sparam != P+"SELL")
        {
         SetStatus("Trade operation in progress.", CLR_STATUS_WARN);
         return;
        }
      if(sparam == P+"BUY")   ExecuteOrders(0);
      if(sparam == P+"SELL")  ExecuteOrders(1);
      if(sparam == P+"TRAIL") ToggleTrailing();
      if(sparam == P+"BPREV") SelectPreviousBatch();
      if(sparam == P+"BNEXT") SelectNextBatch();
      if(sparam == P+"CLOSE") CloseSelectedBatch();
      if(sparam == P+"BE")    BreakevenSelectedBatch();
      if(sparam == P+"PART")  PartialCloseSelectedBatch();
     }

   if(id == CHARTEVENT_MOUSE_MOVE)
      HandlePanelMouseMove((int)lparam, (int)dparam, sparam);

   if(id == CHARTEVENT_KEYDOWN && HotkeysEnabled && !g_IsExecuting)
     {
      if(lparam == 112) ExecuteOrders(0);          // F1 BUY
      if(lparam == 113) ExecuteOrders(1);          // F2 SELL
      if(lparam == 114) CloseSelectedBatch();      // F3 CLOSE
      if(lparam == 115) BreakevenSelectedBatch();  // F4 BREAKEVEN
      if(lparam == 116) ToggleTrailing();          // F5 TRAILING
      if(lparam == 27)
        {
         if(!g_IsExecuting)
            SetStatus("ESC pressed. No operation to abort.", CLR_MUTED);
         else
            SetStatus("ESC: Cannot abort during execution.", CLR_STATUS_WARN);
        }
     }

   if(id == CHARTEVENT_OBJECT_ENDEDIT)
     {
      string txt = ObjectGetString(0, sparam, OBJPROP_TEXT);
      if(sparam == P+"E_NUM")
        {
         g_PanelNumTrades = (int)MathMax(1, MathMin(200, StringToInteger(txt)));
         ObjectSetString(0, P+"E_NUM", OBJPROP_TEXT,
                         IntegerToString(g_PanelNumTrades));
        }
      if(sparam == P+"E_LOT")
        {
         g_PanelLotSize = MathMax(
            SymbolInfoDouble(Symbol(), SYMBOL_VOLUME_MIN),
            MathMin(200.0, StringToDouble(txt)));
         g_PanelLotSize = NormalizeLot(g_PanelLotSize);
         ObjectSetString(0, P+"E_LOT", OBJPROP_TEXT,
                         DoubleToString(g_PanelLotSize, 2));
        }
      if(sparam == P+"E_SL")
        {
         string slText = TrimText(txt);
         if(StringLen(slText) == 0)
            g_PanelSL = 0.0;
         else
            g_PanelSL = MathMax(0, StringToDouble(slText));
         ApplyOptionalEditValue(P+"E_SL", g_PanelSL, SL_PLACEHOLDER);
        }
      if(sparam == P+"E_TP")
        {
         string tpText = TrimText(txt);
         if(StringLen(tpText) == 0)
            g_PanelTP = 0.0;
         else
            g_PanelTP = MathMax(0, StringToDouble(tpText));
         ApplyOptionalEditValue(P+"E_TP", g_PanelTP, TP_PLACEHOLDER);
        }
     }
  }

//+------------------------------------------------------------------+
//| ExecuteOrders — v4.08: pure initializer, dispatches via OnTimer  |
//+------------------------------------------------------------------+
void ExecuteOrders(int direction)
  {
   if(g_IsExecuting)
     {
      LogEvent("EXEC_SKIP", "Ignored duplicate execution request");
      return;
     }
   if(g_IsDeinitializing) return;
   if(!AcquireExecState(EXEC_EXECUTING))
     {
      SetStatus("Execution busy. Try again.", CLR_STATUS_WARN);
      LogEvent("EXEC_SKIP", "Exec state busy");
      return;
     }
   if(!TerminalInfoInteger(TERMINAL_CONNECTED))
     {
      SetStatus("Disconnected. Execution blocked.", CLR_STATUS_ERROR);
      LogEvent("EXEC_FAIL", "Blocked execution while terminal disconnected");
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(!AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ||
      !TerminalInfoInteger(TERMINAL_TRADE_ALLOWED))
     {
      SetStatus("Trading disabled. Execution blocked.", CLR_STATUS_ERROR);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   long tradeMode = SymbolInfoInteger(Symbol(), SYMBOL_TRADE_MODE);
   if(tradeMode == SYMBOL_TRADE_MODE_DISABLED)
     {
      SetStatus("Symbol trading disabled. Execution blocked.", CLR_STATUS_ERROR);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode == SYMBOL_TRADE_MODE_LONGONLY && direction != 0)
     {
      SetStatus("Symbol LONGONLY. Sell blocked.", CLR_STATUS_ERROR);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode == SYMBOL_TRADE_MODE_SHORTONLY && direction == 0)
     {
      SetStatus("Symbol SHORTONLY. Buy blocked.", CLR_STATUS_ERROR);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode != SYMBOL_TRADE_MODE_FULL &&
      tradeMode != SYMBOL_TRADE_MODE_LONGONLY &&
      tradeMode != SYMBOL_TRADE_MODE_SHORTONLY)
     {
      SetStatus("Symbol trade mode not FULL. Execution blocked.", CLR_STATUS_ERROR);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }

   g_IsExecuting = true;
   // v4.08: g_AbortFlag reset clears stale state from prior operations.
   // ESC abort is dead code under MQL5 single-thread model (C-AUDIT-001).
   // g_AbortFlag guards DispatchOrderNow for deinit/disconnect conditions.
   g_AbortFlag = false;
   string dirStr = (direction == 0) ? "BUY" : "SELL";

   LockButtons();
   SetStatus("Checking pre-conditions...", CLR_STATUS_EXEC);

   //--- 1. SPREAD FILTER
   if(MaxSpread > 0.0)
     {
      double spd = (double)SymbolInfoInteger(Symbol(), SYMBOL_SPREAD)
                   * _Point / GetPipSize();
      if(spd > MaxSpread)
        {
         SetStatus("Spread "+DoubleToString(spd, 1)+" > "+
                   DoubleToString(MaxSpread, 1)+" pips. Aborted.", CLR_STATUS_WARN);
         UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
         return;
        }
     }

   //--- 2. POSITION CEILING
   int openCount = PositionsTotal();
   int slots     = 200 - openCount;
   if(slots <= 0)
     {
      SetStatus("At 200-position limit. Aborted.", CLR_STATUS_WARN);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   int numToOpen = g_PanelNumTrades;
   if(numToOpen > slots)
     {
      numToOpen = slots;
      // v4.08: Sleep(600) removed — async dispatch; status message is sufficient
      SetStatus("Reduced to "+IntegerToString(slots)+" trades (ceiling)", CLR_STATUS_WARN);
     }

   //--- 3. LOT SIZE
   double lot = g_PanelLotSize;
   if(UseRiskPercent && g_PanelSL > 0.0)
     {
      double balance  = AccountInfoDouble(ACCOUNT_BALANCE);
      double riskAmt  = balance * RiskPercent / 100.0;
      double tickVal  = SymbolInfoDouble(Symbol(), SYMBOL_TRADE_TICK_VALUE);
      double tickSize = SymbolInfoDouble(Symbol(), SYMBOL_TRADE_TICK_SIZE);
      double pipVal   = (tickSize > 0.0) ? tickVal * GetPipSize() / tickSize : 0.0;
      if(pipVal > 0.0)
         lot = riskAmt / (g_PanelSL * pipVal);
      lot = NormalizeLot(lot);
      Print("ApexExecution: Risk% lot = ", lot,
            " (", RiskPercent, "% of $", balance, ")");
     }
   else
      lot = NormalizeLot(lot);

   if(lot <= 0.0)
     {
      SetStatus("Invalid lot (symbol info unavailable).", CLR_STATUS_ERROR);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
      return;
     }

   //--- 4. MARGIN CHECK (bulk estimate for entire batch)
   double marginPer = 0.0;
   ENUM_ORDER_TYPE oType = (direction == 0) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   double priceForCheck  = (direction == 0)
      ? SymbolInfoDouble(Symbol(), SYMBOL_ASK)
      : SymbolInfoDouble(Symbol(), SYMBOL_BID);
   if(!OrderCalcMargin(oType, Symbol(), lot, priceForCheck, marginPer) ||
      marginPer <= 0.0)
     {
      SetStatus("Margin check failed. Aborted.", CLR_STATUS_ERROR);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   double required = marginPer * numToOpen * 1.2;
   if(AccountInfoDouble(ACCOUNT_MARGIN_FREE) < required)
     {
      SetStatus("Insufficient margin. Need $"+DoubleToString(required, 2), CLR_STATUS_ERROR);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
      return;
     }

   //--- 5. GENERATE BATCH ID & REGISTER
   long batchID = GenerateBatchMagic();

   BatchInfo batch;
   batch.magic     = batchID;
   batch.symbol    = Symbol();
   batch.direction = direction;
   batch.sl        = g_PanelSL;
   batch.tp        = g_PanelTP;
   batch.syncing   = false;
   batch.trailing  = (TrailingStop > 0.0) && g_TrailingEnabled;
   batch.active    = true;
   batch.created   = TimeCurrent();
   batch.requested = numToOpen;
   batch.filled    = 0;
   batch.errors    = 0;
   batch.partial   = false;
   batch.modifyCount          = 0;
   batch.modifyWindowStartMs  = 0;
   batch.closeCount           = 0;
   batch.closeWindowStartMs   = 0;

   if(!AddBatch(batch))
     {
      SetStatus("Batch registry failed. Aborted.", CLR_STATUS_ERROR);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   g_SelectedBatchIndex = FindBatchIndex(batchID);
   LogEvent("BATCH_CREATE", "magic="+IntegerToString(batchID)+
            " symbol="+Symbol()+" direction="+dirStr+
            " requested="+IntegerToString(numToOpen)+
            " lot="+DoubleToString(lot, 2));

   GlobalVariableSet("MOE_KNOWN_"+IntegerToString(batchID), 1.0);
   GlobalVariableSet("MOE_REQ_"+IntegerToString(batchID),   (double)numToOpen);
   GlobalVariableSet("MOE_BATCH_ID",   (double)batchID);
   GlobalVariableSet("MOE_BATCH_SL",   g_PanelSL);
   GlobalVariableSet("MOE_BATCH_TP",   g_PanelTP);
   GlobalVariableSet("MOE_BATCH_DIR",  (double)direction);
   GlobalVariableSet("MOE_BATCH_LOTS", lot);
   // C-002 fix: persist pip-based SL/TP for post-restart repair
   GlobalVariableSet("MOE_PIP_SL_"+IntegerToString(batchID), g_PanelSL);
   GlobalVariableSet("MOE_PIP_TP_"+IntegerToString(batchID), g_PanelTP);

   //--- 6. REFRESH FILLING MODE (L-AUDIT-003 fix)
   ENUM_ORDER_TYPE_FILLING fill = DetectFillingMode();
   trade.SetTypeFilling(fill);

   //--- 7. INITIALIZE ASYNC BURST DISPATCHER
   // g_IsExecuting remains true and g_ExecState remains EXEC_EXECUTING.
   // ProcessExecutionBurst() called on every OnTimer() tick drives all phases.
   g_PendingExecActive       = true;
   g_PendingExecTotal        = numToOpen;
   g_PendingExecDispatched   = 0;
   g_PendingExecErrors       = 0;
   g_PendingExecBatchID      = batchID;
   g_PendingExecLot          = lot;
   g_PendingExecDir          = direction;
   g_PendingExecSL           = g_PanelSL;
   g_PendingExecTP           = g_PanelTP;
   g_PendingExecLastBurstMs  = 0;    // zero = fire first burst on very next timer tick
   g_PendingExecPostAttach   = false;
   g_PendingExecPostAttachMs = 0;
   g_PendingExecBatchSLPrice = 0.0;
   g_PendingExecBatchTPPrice = 0.0;

   SetStatus("Dispatching "+dirStr+": 0/"+IntegerToString(numToOpen)+"...",
             CLR_STATUS_EXEC);
   LogEvent("BURST_INIT", "direction="+dirStr+
            " total="+IntegerToString(numToOpen)+
            " lot="+DoubleToString(lot, 2)+
            " batchID="+IntegerToString(batchID)+
            " burstSize="+IntegerToString(BURST_ORDER_LIMIT)+
            " cooldownMs="+IntegerToString(BURST_COOLDOWN_MS));

   // RETURN — execution continues via ProcessExecutionBurst() in OnTimer()
  }

//+------------------------------------------------------------------+
//| DispatchOrderNow — v4.08                                         |
//| Non-blocking single order dispatch for use in burst loop only.   |
//| No Sleep(). No retry. Reads SL/TP pips from g_PendingExec state. |
//+------------------------------------------------------------------+
bool DispatchOrderNow(int direction, double lot, string comment, uint &rc)
  {
   rc = 0;
   if(g_AbortFlag || g_IsDeinitializing ||
      !TerminalInfoInteger(TERMINAL_CONNECTED))
     {
      rc = TRADE_RETCODE_CONNECTION;
      return false;
     }

   double pip = GetPipSize();
   double price, sl_price, tp_price;

   if(direction == 0)
     {
      price    = SymbolInfoDouble(Symbol(), SYMBOL_ASK);
      sl_price = (g_PendingExecSL > 0)
                 ? NormalizeDouble(price - g_PendingExecSL * pip, _Digits) : 0;
      tp_price = (g_PendingExecTP > 0)
                 ? NormalizeDouble(price + g_PendingExecTP * pip, _Digits) : 0;
     }
   else
     {
      price    = SymbolInfoDouble(Symbol(), SYMBOL_BID);
      sl_price = (g_PendingExecSL > 0)
                 ? NormalizeDouble(price + g_PendingExecSL * pip, _Digits) : 0;
      tp_price = (g_PendingExecTP > 0)
                 ? NormalizeDouble(price - g_PendingExecTP * pip, _Digits) : 0;
     }

   sl_price = EnforceStopsLevel(Symbol(), price, sl_price, direction, false);
   tp_price = EnforceStopsLevel(Symbol(), price, tp_price, direction, true);

   // Capture price-based SL/TP from the very first dispatch for batch stops
   if(g_PendingExecDispatched == 0)
     {
      g_PendingExecBatchSLPrice = sl_price;
      g_PendingExecBatchTPPrice = tp_price;
     }

   ResetLastError();
   g_LastTradeActionMs = GetTickCount();   // record timestamp — no Sleep()

   bool ok = (direction == 0)
      ? trade.Buy(lot, Symbol(), price, sl_price, tp_price, comment)
      : trade.Sell(lot, Symbol(), price, sl_price, tp_price, comment);

   rc = trade.ResultRetcode();
   bool success = (ok && IsSuccessRetcode(rc));
   UpdateTradePacing(rc, success);
   return success;
  }

//+------------------------------------------------------------------+
//| ProcessExecutionBurst — v4.08                                    |
//| Called on every OnTimer() tick while g_IsExecuting is true.      |
//|                                                                  |
//| Phase 1 (g_PendingExecActive):                                   |
//|   Fires up to BURST_ORDER_LIMIT async orders per                 |
//|   BURST_COOLDOWN_MS window. Transitions to Phase 2 when done.    |
//|                                                                  |
//| Phase 2 (g_PendingExecPostAttach):                               |
//|   Waits POST_ATTACH_DELAY_MS for broker fills to settle.         |
//|   Runs PostFillAttachSLTP, finalizes batch, releases state.      |
//|                                                                  |
//| SetAsyncMode(true) is active ONLY inside the burst firing loop.  |
//| It is explicitly restored to false in ALL exit paths.            |
//+------------------------------------------------------------------+
void ProcessExecutionBurst()
  {
   if(g_IsDeinitializing) return;

   uint now = GetTickCount();

   //=================================================================
   //  PHASE 2 — post-attach delay completed
   //=================================================================
   if(g_PendingExecPostAttach)
     {
      if(now - g_PendingExecPostAttachMs < (uint)POST_ATTACH_DELAY_MS) return;

      // Ensure sync mode for all modifications from this point
      trade.SetAsyncMode(false);

      // Deferred SL/TP attachment — replaces the old Sleep(250)+PostFillAttachSLTP
      if(g_PendingExecSL > 0.0 || g_PendingExecTP > 0.0)
         PostFillAttachSLTP(g_PendingExecBatchID, g_PendingExecDir);

      // Finalize batch record
      int batchIndex = FindBatchIndex(g_PendingExecBatchID);
      if(batchIndex >= 0)
        {
         g_Batches[batchIndex].filled  = g_PendingExecDispatched;
         g_Batches[batchIndex].errors  = g_PendingExecErrors;
         g_Batches[batchIndex].partial = (g_PendingExecDispatched > 0 &&
                                          g_PendingExecDispatched <
                                          g_Batches[batchIndex].requested);
         if(g_PendingExecDispatched <= 0)
            RemoveBatch(g_PendingExecBatchID);
        }

      string dirStr  = (g_PendingExecDir == 0) ? "BUY" : "SELL";
      string summary = dirStr+": "+IntegerToString(g_PendingExecDispatched)+
                       "/"+IntegerToString(g_PendingExecTotal)+" dispatched";
      if(g_PendingExecErrors > 0)
         summary += " ("+IntegerToString(g_PendingExecErrors)+" errors)";

      SetStatus(summary, (g_PendingExecErrors == 0) ? CLR_STATUS_OK : CLR_STATUS_WARN);
      LogEvent("EXEC_SUMMARY", summary);

      UnlockButtons();
      g_IsExecuting           = false;
      g_PendingExecPostAttach = false;
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }

   //=================================================================
   //  PHASE 1 — burst dispatch
   //=================================================================
   if(!g_PendingExecActive) return;

   //--- Disconnect guard
   if(!TerminalInfoInteger(TERMINAL_CONNECTED))
     {
      trade.SetAsyncMode(false);
      if(g_PendingExecDispatched <= 0)
        {
         // Nothing sent — fully abort and clean up
         int batchIndex = FindBatchIndex(g_PendingExecBatchID);
         if(batchIndex >= 0) RemoveBatch(g_PendingExecBatchID);
         SetStatus("Disconnected during execution. Aborted.", CLR_STATUS_ERROR);
         LogEvent("BURST_ABORT", "Disconnect pre-dispatch batchID="+
                  IntegerToString(g_PendingExecBatchID));
         UnlockButtons();
         g_IsExecuting       = false;
         g_PendingExecActive = false;
         ReleaseExecState(EXEC_EXECUTING);
        }
      else
        {
         // Some orders sent — transition to post-attach; reconnect resync handles rest
         LogEvent("BURST_ABORT", "Disconnect mid-burst. Transitioning to post-attach. dispatched="+
                  IntegerToString(g_PendingExecDispatched));
         g_PendingExecActive      = false;
         g_PendingExecPostAttach  = true;
         g_PendingExecPostAttachMs= now;
        }
      return;
     }

   //--- Burst time gate — enforces BURST_COOLDOWN_MS between windows
   if(g_PendingExecLastBurstMs != 0 &&
      now - g_PendingExecLastBurstMs < (uint)BURST_COOLDOWN_MS)
      return;

   g_PendingExecLastBurstMs = now;

   //--- Per-burst margin check
   ENUM_ORDER_TYPE oType = (g_PendingExecDir == 0) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   double checkPrice = (g_PendingExecDir == 0)
      ? SymbolInfoDouble(Symbol(), SYMBOL_ASK)
      : SymbolInfoDouble(Symbol(), SYMBOL_BID);
   double marginPer  = 0.0;
   int    remaining  = g_PendingExecTotal - g_PendingExecDispatched;
   int    burstCount = MathMin(BURST_ORDER_LIMIT, remaining);

   bool marginOk = OrderCalcMargin(oType, Symbol(), g_PendingExecLot,
                                   checkPrice, marginPer);
   if(!marginOk || marginPer <= 0.0 ||
      AccountInfoDouble(ACCOUNT_MARGIN_FREE) < marginPer * burstCount * 1.1)
     {
      trade.SetAsyncMode(false);
      LogEvent("BURST_ABORT", "Insufficient margin before burst dispatched="+
               IntegerToString(g_PendingExecDispatched)+
               " batchID="+IntegerToString(g_PendingExecBatchID));
      g_PendingExecActive      = false;
      g_PendingExecPostAttach  = true;
      g_PendingExecPostAttachMs= now;
      return;
     }

   //--- Enable async mode for burst dispatch window ONLY
   trade.SetAsyncMode(true);
   trade.SetExpertMagicNumber(g_PendingExecBatchID);

   string dirStr    = (g_PendingExecDir == 0) ? "BUY" : "SELL";
   bool   fatalError= false;

   //--- Fire burst
   for(int i = 0; i < burstCount; i++)
     {
      if(g_AbortFlag || g_IsDeinitializing ||
         !TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         LogEvent("BURST_INTERRUPT", "Abort/disconnect in burst loop order="+
                  IntegerToString(g_PendingExecDispatched + 1));
         fatalError = true;
         break;
        }

      int    orderNum = g_PendingExecDispatched + 1;
      string cmt      = "MOE_"+IntegerToString(g_PendingExecBatchID)+
                        "_"+IntegerToString(orderNum);
      uint   rc       = 0;
      bool   sent     = DispatchOrderNow(g_PendingExecDir, g_PendingExecLot, cmt, rc);

      if(sent)
        {
         g_PendingExecDispatched++;
         if(g_PendingExecDispatched == 1)
            UpdateBatchStops(g_PendingExecBatchID,
                             g_PendingExecBatchSLPrice,
                             g_PendingExecBatchTPPrice);
        }
      else if(rc == TRADE_RETCODE_NO_MONEY       ||
              rc == TRADE_RETCODE_LIMIT_ORDERS    ||
              rc == TRADE_RETCODE_TRADE_DISABLED  ||
              rc == TRADE_RETCODE_MARKET_CLOSED   ||
              rc == TRADE_RETCODE_CONNECTION)
        {
         g_PendingExecErrors++;
         LogEvent("BURST_FATAL", "rc="+IntegerToString((int)rc)+
                  " order="+IntegerToString(orderNum)+
                  " batch="+IntegerToString(g_PendingExecBatchID));
         fatalError = true;
         break;
        }
      else
        {
         // Non-fatal (e.g. 10027 TOO_MANY_REQUESTS, requote): log and continue.
         // If 10027 is frequent, reduce BURST_ORDER_LIMIT or increase BURST_COOLDOWN_MS.
         LogTradeFailure("BURST_ORDER_"+IntegerToString(orderNum),
                         rc, 0, trade.ResultComment());
         g_PendingExecErrors++;
        }
     }

   //--- Restore sync mode immediately — never leave async mode on after burst
   trade.SetAsyncMode(false);

   // Update UI
   SetStatus("Executing "+dirStr+": "+IntegerToString(g_PendingExecDispatched)+
             "/"+IntegerToString(g_PendingExecTotal), CLR_STATUS_EXEC);

   LogEvent("BURST_TICK", "dispatched="+IntegerToString(g_PendingExecDispatched)+
            " total="+IntegerToString(g_PendingExecTotal)+
            " errors="+IntegerToString(g_PendingExecErrors)+
            " fatal="+IntegerToString(fatalError ? 1 : 0));

   // Transition to Phase 2 if all dispatched or fatally aborted
   if(fatalError || g_PendingExecDispatched >= g_PendingExecTotal)
     {
      g_PendingExecActive      = false;
      g_PendingExecPostAttach  = true;
      g_PendingExecPostAttachMs= GetTickCount();
      LogEvent("BURST_COMPLETE", "dispatched="+IntegerToString(g_PendingExecDispatched)+
               " moving to post-attach phase");
     }
  }

//+------------------------------------------------------------------+
//| PostFillAttachSLTP — safety net for market execution mode        |
//+------------------------------------------------------------------+
void PostFillAttachSLTP(long batchID, int direction)
  {
   // v4.08: Sleep(250) removed — ProcessExecutionBurst uses POST_ATTACH_DELAY_MS
   // timer-gated wait to allow async fills to settle before this runs.
   double pip = GetPipSize();
   int batchIndex = FindBatchIndex(batchID);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = true;

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != batchID) continue;
      if(posInfo.Symbol() != Symbol()) continue;

      double posSL = posInfo.StopLoss();
      double posTP = posInfo.TakeProfit();

      bool needsMod = (posSL == 0.0 && g_PanelSL > 0.0) ||
                      (posTP == 0.0 && g_PanelTP > 0.0);
      if(!needsMod) continue;

      double openPx = posInfo.PriceOpen();
      double newSL, newTP;

      if(direction == 0)
        {
         newSL = (g_PanelSL > 0)
                 ? NormalizeDouble(openPx - g_PanelSL * pip, _Digits) : 0;
         newTP = (g_PanelTP > 0)
                 ? NormalizeDouble(openPx + g_PanelTP * pip, _Digits) : 0;
        }
      else
        {
         newSL = (g_PanelSL > 0)
                 ? NormalizeDouble(openPx + g_PanelSL * pip, _Digits) : 0;
         newTP = (g_PanelTP > 0)
                 ? NormalizeDouble(openPx - g_PanelTP * pip, _Digits) : 0;
        }

      string posSymbol = posInfo.Symbol();
      newSL = EnforceStopsLevel(posSymbol, openPx, newSL, direction, false);
      newTP = EnforceStopsLevel(posSymbol, openPx, newTP, direction, true);

      uint rc = 0;
      ModifyPositionQueued(posInfo.Ticket(), newSL, newTP, rc, "POST_ATTACH", false);
     }

   batchIndex = FindBatchIndex(batchID);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = false;
  }

//+------------------------------------------------------------------+
//| RepairMissingSLTP — C-002 fix                                    |
//+------------------------------------------------------------------+
void RepairMissingSLTP()
  {
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;

      long magic = posInfo.Magic();
      if(magic <= 0) continue;

      int batchIndex = FindBatchIndex(magic);
      if(batchIndex < 0 || !g_Batches[batchIndex].active) continue;

      double posSL = posInfo.StopLoss();
      double posTP = posInfo.TakeProfit();

      string slKey = "MOE_PIP_SL_"+IntegerToString(magic);
      string tpKey = "MOE_PIP_TP_"+IntegerToString(magic);

      bool hasPipSL = GlobalVariableCheck(slKey);
      bool hasPipTP = GlobalVariableCheck(tpKey);
      if(!hasPipSL && !hasPipTP) continue;

      double pipSL = hasPipSL ? GlobalVariableGet(slKey) : 0.0;
      double pipTP = hasPipTP ? GlobalVariableGet(tpKey) : 0.0;

      bool needsSL = (posSL == 0.0 && pipSL > 0.0);
      bool needsTP = (posTP == 0.0 && pipTP > 0.0);
      if(!needsSL && !needsTP) continue;

      double openPx    = posInfo.PriceOpen();
      int    direction = g_Batches[batchIndex].direction;
      string sym       = posInfo.Symbol();
      double pip       = GetPipSize(sym);
      double newSL     = posSL;
      double newTP     = posTP;

      if(needsSL)
        {
         newSL = (direction == 0)
                 ? NormalizeDouble(openPx - pipSL * pip, _Digits)
                 : NormalizeDouble(openPx + pipSL * pip, _Digits);
         newSL = EnforceStopsLevel(sym, openPx, newSL, direction, false);
        }
      if(needsTP)
        {
         newTP = (direction == 0)
                 ? NormalizeDouble(openPx + pipTP * pip, _Digits)
                 : NormalizeDouble(openPx - pipTP * pip, _Digits);
         newTP = EnforceStopsLevel(sym, openPx, newTP, direction, true);
        }

      uint rc = 0;
      ModifyPositionQueued(posInfo.Ticket(), newSL, newTP, rc, "REPAIR_SLTP", false);
      LogEvent("REPAIR_SLTP", "ticket="+IntegerToString((long)posInfo.Ticket())+
               " batch="+IntegerToString(magic)+
               " newSL="+DoubleToString(newSL, _Digits)+
               " newTP="+DoubleToString(newTP, _Digits));
     }
  }

//+------------------------------------------------------------------+
//| SyncAllPositions — broadcast SL/TP inside one basket             |
//+------------------------------------------------------------------+
void SyncAllPositions(long magic, double newSL, double newTP)
  {
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || g_Batches[batchIndex].syncing) return;
   if(!AcquireExecState(EXEC_SYNCING))
     {
      LogEvent("SYNC_SKIP", "Exec state busy for batch="+IntegerToString(magic));
      return;
     }
   g_Batches[batchIndex].syncing = true;
   string batchSymbol = g_Batches[batchIndex].symbol;
   int direction      = g_Batches[batchIndex].direction;
   double drift       = SyncDriftThreshold(batchSymbol);

   int synced = 0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != magic) continue;
      if(posInfo.Symbol() != batchSymbol) continue;

      double curSL = posInfo.StopLoss();
      double curTP = posInfo.TakeProfit();
      bool slImprove = IsBetterSL(direction, curSL, newSL, true);
      bool tpImprove = IsDifferentTP(curTP, newTP, drift);

      if(slImprove || tpImprove)
        {
         uint rc = 0;
         double applySL = slImprove ? newSL : curSL;
         double applyTP = tpImprove ? newTP : curTP;
         if(ModifyPositionQueued(posInfo.Ticket(), applySL, applyTP,
                                 rc, "SYNC", false))
            synced++;
        }
     }

   if(synced > 0)
      UpdateBatchStops(magic, newSL, newTP);

   LogEvent("SYNC", "batch="+IntegerToString(magic)+
            " positions="+IntegerToString(synced)+
            " SL="+DoubleToString(newSL, _Digits)+
            " TP="+DoubleToString(newTP, _Digits));
   SetStatus("Synced SL/TP to "+IntegerToString(synced)+" positions", CLR_STATUS_OK);
   batchIndex = FindBatchIndex(magic);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = false;
   ReleaseExecState(EXEC_SYNCING);
  }

//+------------------------------------------------------------------+
//| BackupSyncCheck — runs on timer as safety net                    |
//+------------------------------------------------------------------+
bool BackupSyncCheck()
  {
   bool modified = false;

   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;

      long magic = posInfo.Magic();
      if(magic <= 0) continue;

      int batchIndex = FindBatchIndex(magic);
      if(batchIndex < 0 || !g_Batches[batchIndex].active ||
         g_Batches[batchIndex].syncing)
         continue;

      string batchSymbol = g_Batches[batchIndex].symbol;
      if(posInfo.Symbol() != batchSymbol) continue;

      int direction  = g_Batches[batchIndex].direction;
      double drift   = SyncDriftThreshold(batchSymbol);
      ulong ticket   = posInfo.Ticket();
      double curSL   = posInfo.StopLoss();
      double curTP   = posInfo.TakeProfit();

      int stateIdx = FindPosStateIndex(ticket);
      if(stateIdx < 0)
        {
         UpsertPosState(ticket, magic, batchSymbol, direction, curSL, curTP);
         continue;
        }

      double storedSL = g_PosStates[stateIdx].sl;
      double storedTP = g_PosStates[stateIdx].tp;
      bool storedChanged = false;

      if(IsBetterSL(direction, storedSL, curSL, true))
        {
         storedSL = curSL;
         storedChanged = true;
        }
      if(IsDifferentTP(storedTP, curTP, drift))
        {
         storedTP = curTP;
         storedChanged = true;
        }

      if(storedChanged)
         UpsertPosState(ticket, magic, batchSymbol, direction, storedSL, storedTP);

      bool slImprove = IsBetterSL(direction, curSL, storedSL, true);
      bool tpImprove = IsDifferentTP(curTP, storedTP, drift);

      if(slImprove || tpImprove)
        {
         uint rc = 0;
         double applySL = slImprove ? storedSL : curSL;
         double applyTP = tpImprove ? storedTP : curTP;
         if(ModifyPositionQueued(ticket, applySL, applyTP, rc, "BACKUP_SYNC", false))
            modified = true;
        }
     }

   return modified;
  }

//+------------------------------------------------------------------+
//| Batch navigation                                                 |
//+------------------------------------------------------------------+
void SelectPreviousBatch()
  {
   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size == 0)
     { SetStatus("No active batches.", CLR_STATUS_WARN); RefreshPanel(); return; }

   g_SelectedBatchIndex--;
   if(g_SelectedBatchIndex < 0)
      g_SelectedBatchIndex = size - 1;

   SetStatus("Selected batch "+IntegerToString(g_Batches[g_SelectedBatchIndex].magic),
             CLR_STATUS_OK);
   RefreshPanel();
  }

void SelectNextBatch()
  {
   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size == 0)
     { SetStatus("No active batches.", CLR_STATUS_WARN); RefreshPanel(); return; }

   g_SelectedBatchIndex++;
   if(g_SelectedBatchIndex >= size)
      g_SelectedBatchIndex = 0;

   SetStatus("Selected batch "+IntegerToString(g_Batches[g_SelectedBatchIndex].magic),
             CLR_STATUS_OK);
   RefreshPanel();
  }

long GetSelectedBatchMagic()
  {
   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(g_SelectedBatchIndex < 0 || g_SelectedBatchIndex >= size)
      return 0;
   return g_Batches[g_SelectedBatchIndex].magic;
  }

void CloseSelectedBatch()
  {
   g_AbortFlag = false;
   long magic = GetSelectedBatchMagic();
   if(magic == 0)
     { SetStatus("No active batch selected.", CLR_STATUS_WARN); RefreshPanel(); return; }
   CloseAllBatch(magic);
   EnsureSelectedBatch();
   RefreshPanel();
  }

void BreakevenSelectedBatch()
  {
   g_AbortFlag = false;
   long magic = GetSelectedBatchMagic();
   if(magic == 0)
     { SetStatus("No active batch selected.", CLR_STATUS_WARN); RefreshPanel(); return; }
   int done = BreakevenBatch(magic);
   if(done > 0)
      SetStatus("Breakeven applied to "+IntegerToString(done)+" positions", CLR_STATUS_OK);
   else
      SetStatus(GetBreakevenSkipReason(magic), CLR_STATUS_WARN);
   RefreshPanel();
  }

void PartialCloseSelectedBatch()
  {
   g_AbortFlag = false;
   long magic = GetSelectedBatchMagic();
   if(magic == 0)
     { SetStatus("No active batch selected.", CLR_STATUS_WARN); RefreshPanel(); return; }
   int done = PartialCloseBatch(magic);
   SetStatus("Partial close on "+IntegerToString(done)+" positions", CLR_STATUS_OK);
   RefreshPanel();
  }

//+------------------------------------------------------------------+
//| ToggleTrailing — v4.07                                           |
//+------------------------------------------------------------------+
void ToggleTrailing()
  {
   if(TrailingStop <= 0.0)
     {
      SetStatus("Trailing not configured. Set TrailingStop > 0 in inputs.", CLR_STATUS_WARN);
      return;
     }
   g_TrailingEnabled = !g_TrailingEnabled;
   for(int i = 0; i < ArraySize(g_Batches); i++)
      g_Batches[i].trailing = g_TrailingEnabled;
   SavePanelLayout();
   string msg = g_TrailingEnabled
                ? "Trailing SL: ON ("+DoubleToString(TrailingStop, 0)+" pips)"
                : "Trailing SL: OFF";
   color c = g_TrailingEnabled ? CLR_STATUS_OK : CLR_MUTED;
   SetStatus(msg, c);
   RefreshTrailingButton();
   ChartRedraw();
   LogEvent("TRAIL_TOGGLE", g_TrailingEnabled ? "ENABLED" : "DISABLED");
  }

//+------------------------------------------------------------------+
//| CloseAllBatch                                                    |
//+------------------------------------------------------------------+
bool CloseAllBatch(long magic)
  {
   bool held = false;
   if(g_ExecState != EXEC_CLOSING)
     {
      if(!AcquireExecState(EXEC_CLOSING))
        {
         SetStatus("Close already in progress.", CLR_STATUS_WARN);
         LogEvent("CLOSE_SKIP", "cannot acquire closing lock for batch="+
                  IntegerToString(magic));
         g_CloseSkipCount++;
         return false;
        }
      held = true;
     }

   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || !g_Batches[batchIndex].active)
     {
      SetStatus("No active batch to close.", CLR_STATUS_WARN);
      if(held) ReleaseExecState(EXEC_CLOSING);
      return false;
     }
   string batchSymbol = g_Batches[batchIndex].symbol;

   int closed = 0, failed = 0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != magic) continue;
      if(posInfo.Symbol() != batchSymbol) continue;

      uint rc = 0;
      bool ok = ClosePositionSafe(posInfo.Ticket(), rc, "CLOSE_BATCH");
      if(ok)  closed++;
      if(!ok) failed++;
     }

   if(failed == 0)
     {
      RemoveBatch(magic);
      SaveLatestBatchGlobals();
     }

   string msg = "Closed "+IntegerToString(closed)+" from batch "+
                IntegerToString(magic);
   if(failed > 0) msg += " | Failed: "+IntegerToString(failed);
   SetStatus(msg, (failed == 0) ? CLR_STATUS_OK : CLR_STATUS_WARN);
   LogEvent("CLOSE", msg);
   if(held) ReleaseExecState(EXEC_CLOSING);
   return (failed == 0);
  }

//+------------------------------------------------------------------+
//| BreakevenBatch                                                   |
//+------------------------------------------------------------------+
int BreakevenBatch(long magic)
  {
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || !g_Batches[batchIndex].active) return 0;

   if(!AcquireExecState(EXEC_SYNCING))
     {
      LogEvent("BREAKEVEN_SKIP", "Exec state busy for batch="+IntegerToString(magic));
      g_SyncSkipCount++;
      return 0;
     }

   string batchSymbol = g_Batches[batchIndex].symbol;
   g_Batches[batchIndex].syncing = true;
   int done = 0;
   double pip = GetPipSize(batchSymbol);

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != magic) continue;
      if(posInfo.Symbol() != batchSymbol) continue;

      double openPx = posInfo.PriceOpen();
      double curSL  = posInfo.StopLoss();
      double curTP  = posInfo.TakeProfit();

      if(posInfo.PositionType() == POSITION_TYPE_BUY)
        {
         double triggerPx = openPx + BreakevenTrigger * pip;
         double beSL      = NormalizeDouble(openPx + BreakevenBuffer * pip, _Digits);
         beSL = EnforceStopsLevel(batchSymbol,
                                  SymbolInfoDouble(batchSymbol, SYMBOL_BID),
                                  beSL, 0, false);
         if(SymbolInfoDouble(batchSymbol, SYMBOL_BID) >= triggerPx && curSL < beSL)
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), beSL, curTP,
                                    rc, "BREAKEVEN", false))
               done++;
           }
        }
      else
        {
         double triggerPx = openPx - BreakevenTrigger * pip;
         double beSL      = NormalizeDouble(openPx - BreakevenBuffer * pip, _Digits);
         beSL = EnforceStopsLevel(batchSymbol,
                                  SymbolInfoDouble(batchSymbol, SYMBOL_ASK),
                                  beSL, 1, false);
         if(SymbolInfoDouble(batchSymbol, SYMBOL_ASK) <= triggerPx
            && (curSL > beSL || curSL == 0.0))
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), beSL, curTP,
                                    rc, "BREAKEVEN", false))
               done++;
           }
        }
     }

   if(done > 0) SaveLatestBatchGlobals();
   batchIndex = FindBatchIndex(magic);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = false;
   ReleaseExecState(EXEC_SYNCING);

   LogEvent("BREAKEVEN", "Applied to "+IntegerToString(done)+
            " positions in batch "+IntegerToString(magic));
   return done;
  }

//+------------------------------------------------------------------+
//| GetBreakevenSkipReason — v4.07                                   |
//+------------------------------------------------------------------+
string GetBreakevenSkipReason(long magic)
  {
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0) return "No active batch.";
   string sym = g_Batches[batchIndex].symbol;
   double pip  = GetPipSize(sym);
   double drift= SyncDriftThreshold(sym);
   int total = 0, notTriggered = 0, alreadyBE = 0;

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() != magic || posInfo.Symbol() != sym) continue;
      total++;
      double openPx = posInfo.PriceOpen();
      double curSL  = posInfo.StopLoss();

      if(posInfo.PositionType() == POSITION_TYPE_BUY)
        {
         double trigPx = openPx + BreakevenTrigger * pip;
         double beSL   = NormalizeDouble(openPx + BreakevenBuffer * pip, _Digits);
         double bid    = SymbolInfoDouble(sym, SYMBOL_BID);
         if(bid < trigPx)               notTriggered++;
         else if(curSL >= beSL - drift) alreadyBE++;
        }
      else
        {
         double trigPx = openPx - BreakevenTrigger * pip;
         double beSL   = NormalizeDouble(openPx - BreakevenBuffer * pip, _Digits);
         double ask    = SymbolInfoDouble(sym, SYMBOL_ASK);
         if(ask > trigPx)                           notTriggered++;
         else if(curSL > 0.0 && curSL <= beSL + drift) alreadyBE++;
        }
     }

   if(total == 0)          return "No positions in batch.";
   if(alreadyBE >= total)  return "BE already active on all "+IntegerToString(total)+" positions.";
   if(notTriggered > 0)    return "Trigger not reached: "+IntegerToString(notTriggered)+
                                  "/"+IntegerToString(total)+" positions.";
   return "No eligible positions for breakeven.";
  }

//+------------------------------------------------------------------+
//| BatchTotalLots — v4.07                                           |
//+------------------------------------------------------------------+
double BatchTotalLots(long magic, string symbol)
  {
   double lots = 0.0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() != magic || posInfo.Symbol() != symbol) continue;
      lots += posInfo.Volume();
     }
   return lots;
  }

//+------------------------------------------------------------------+
//| IsBatchBreakevenActive — v4.07                                   |
//+------------------------------------------------------------------+
bool IsBatchBreakevenActive(long magic, string symbol)
  {
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() != magic || posInfo.Symbol() != symbol) continue;
      double openPx = posInfo.PriceOpen();
      double curSL  = posInfo.StopLoss();
      if(posInfo.PositionType() == POSITION_TYPE_BUY)
        { if(curSL > 0.0 && curSL >= openPx) return true; }
      else
        { if(curSL > 0.0 && curSL <= openPx) return true; }
     }
   return false;
  }

//+------------------------------------------------------------------+
//| PartialCloseBatch                                                |
//+------------------------------------------------------------------+
int PartialCloseBatch(long magic)
  {
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || !g_Batches[batchIndex].active) return 0;

   if(!AcquireExecState(EXEC_CLOSING))
     {
      LogEvent("PARTIAL_SKIP", "Exec state busy for batch="+IntegerToString(magic));
      g_CloseSkipCount++;
      return 0;
     }

   string batchSymbol = g_Batches[batchIndex].symbol;
   int done = 0;
   double vStep = SymbolInfoDouble(batchSymbol, SYMBOL_VOLUME_STEP);
   double vMin  = SymbolInfoDouble(batchSymbol, SYMBOL_VOLUME_MIN);

   if(vStep <= 0.0 || vMin <= 0.0)
     {
      LogEvent("EXEC_FAIL", "Invalid volume settings for "+batchSymbol+" in partial close");
      ReleaseExecState(EXEC_CLOSING);
      return 0;
     }

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != magic) continue;
      if(posInfo.Symbol() != batchSymbol) continue;

      double vol    = posInfo.Volume();
      double closeV = MathFloor((vol * PartialClosePct / 100.0) / vStep) * vStep;
      closeV = MathMax(vMin, NormalizeDouble(closeV, 2));
      if(closeV >= vol)
         closeV = NormalizeDouble(vol - vMin, 2);
      if(closeV < vMin || vol - closeV < vMin)
         continue;

      uint rc = 0;
      if(ClosePositionPartialSafe(posInfo.Ticket(), closeV, rc, "PARTIAL_CLOSE"))
         done++;
     }

   LogEvent("PARTIAL_CLOSE", "batch="+IntegerToString(magic)+
            " pct="+DoubleToString(PartialClosePct, 0)+
            " positions="+IntegerToString(done));
   ReleaseExecState(EXEC_CLOSING);
   return done;
  }

//+------------------------------------------------------------------+
//| ProcessTrailing                                                  |
//+------------------------------------------------------------------+
void ProcessTrailing()
  {
   int size = ArraySize(g_Batches);
   long magics[];
   ArrayResize(magics, size);
   for(int b = 0; b < size; b++)
      magics[b] = g_Batches[b].magic;

   for(int b = 0; b < size; b++)
     {
      int batchIndex = FindBatchIndex(magics[b]);
      if(batchIndex < 0 || !g_Batches[batchIndex].active ||
         !g_Batches[batchIndex].trailing)
         continue;
      ProcessTrailingBatch(magics[b]);
     }
  }

//+------------------------------------------------------------------+
//| ProcessTrailingBatch — C-AUDIT-002 fix applied                   |
//+------------------------------------------------------------------+
void ProcessTrailingBatch(long magic)
  {
   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || !g_Batches[batchIndex].active) return;

   string batchSymbol = g_Batches[batchIndex].symbol;
   g_Batches[batchIndex].syncing = true;
   double pip       = GetPipSize(batchSymbol);
   double trailDist = TrailingStop * pip;
   double trailStep = TrailingStep * pip;
   bool modified    = false;

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic()  != magic) continue;
      if(posInfo.Symbol() != batchSymbol) continue;

      double curSL = posInfo.StopLoss();
      double curTP = posInfo.TakeProfit();

      if(posInfo.PositionType() == POSITION_TYPE_BUY)
        {
         double bid      = SymbolInfoDouble(batchSymbol, SYMBOL_BID);
         double newTrail = NormalizeDouble(bid - trailDist, _Digits);
         newTrail = EnforceStopsLevel(batchSymbol, bid, newTrail, 0, false);

         // C-AUDIT-002 fix: skip if pending queue has a better SL
         int pendingIdx = FindQueuedModifyIndex(posInfo.Ticket());
         if(pendingIdx >= 0 &&
            g_ModifyQueue[pendingIdx].sl > 0.0 &&
            g_ModifyQueue[pendingIdx].sl > newTrail)
            continue;

         if(curSL == 0.0 || newTrail > curSL + trailStep)
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), newTrail, curTP,
                                    rc, "TRAILING", false))
               modified = true;
           }
        }
      else
        {
         double ask      = SymbolInfoDouble(batchSymbol, SYMBOL_ASK);
         double newTrail = NormalizeDouble(ask + trailDist, _Digits);
         newTrail = EnforceStopsLevel(batchSymbol, ask, newTrail, 1, false);

         // C-AUDIT-002 fix: skip if pending queue has a better SL
         int pendingIdx = FindQueuedModifyIndex(posInfo.Ticket());
         if(pendingIdx >= 0 &&
            g_ModifyQueue[pendingIdx].sl > 0.0 &&
            g_ModifyQueue[pendingIdx].sl < newTrail)
            continue;

         if(curSL == 0.0 || newTrail < curSL - trailStep)
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), newTrail, curTP,
                                    rc, "TRAILING", false))
               modified = true;
           }
        }
     }

   if(modified)
     {
      SaveLatestBatchGlobals();
      LogEvent("TRAILING", "batch="+IntegerToString(magic)+" updated");
     }
   batchIndex = FindBatchIndex(magic);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = false;
  }

//+------------------------------------------------------------------+
//| HELPER FUNCTIONS                                                 |
//+------------------------------------------------------------------+

void LogEvent(string tag, string message)
  {
   Print("MOE|", tag, "|", TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS),
         "|", message);
  }

bool IsSuccessRetcode(uint rc)
  {
   return (rc == TRADE_RETCODE_DONE ||
           rc == TRADE_RETCODE_PLACED ||
           rc == TRADE_RETCODE_DONE_PARTIAL);
  }

bool IsBusyRetcode(uint rc)
  {
   return (rc == TRADE_RETCODE_TOO_MANY_REQUESTS ||
           rc == TRADE_RETCODE_LOCKED);
  }

bool IsPriceRetcode(uint rc)
  {
   return (rc == TRADE_RETCODE_REQUOTE ||
           rc == TRADE_RETCODE_PRICE_CHANGED ||
           rc == TRADE_RETCODE_PRICE_OFF);
  }

int FindRetryIndex(ulong ticket)
  {
   for(int i = 0; i < ArraySize(g_RetryStates); i++)
      if(g_RetryStates[i].ticket == ticket) return i;
   return -1;
  }

void ClearRetryState(ulong ticket)
  {
   int idx = FindRetryIndex(ticket);
   if(idx < 0) return;
   int size = ArraySize(g_RetryStates);
   for(int i = idx; i < size - 1; i++)
      g_RetryStates[i] = g_RetryStates[i+1];
   ArrayResize(g_RetryStates, size - 1);
  }

void UpsertRetryStateOnFailure(ulong ticket, int attempt, uint baseCooldown)
  {
   int idx = FindRetryIndex(ticket);
   uint now = GetTickCount();
   if(idx < 0)
     {
      RetryState rs;
      rs.ticket        = ticket;
      rs.attempts      = attempt;
      rs.lastAttemptMs = now;
      rs.cooldownMs    = baseCooldown;
      int s = ArraySize(g_RetryStates);
      if(ArrayResize(g_RetryStates, s+1) == s+1)
         g_RetryStates[s] = rs;
     }
   else
     {
      g_RetryStates[idx].attempts      = attempt;
      g_RetryStates[idx].lastAttemptMs = now;
      if(attempt >= TRADE_RETRY_MAX)
         g_RetryStates[idx].cooldownMs =
            (uint)(g_RetryStates[idx].cooldownMs * RETRY_ESCALATION_FACTOR);
      else
         g_RetryStates[idx].cooldownMs = baseCooldown;
     }
  }

bool IsUnderRetryCooldown(ulong ticket)
  {
   int idx = FindRetryIndex(ticket);
   if(idx < 0) return false;
   uint now = GetTickCount();
   return (now - g_RetryStates[idx].lastAttemptMs) < g_RetryStates[idx].cooldownMs;
  }

int AdaptiveRetryDelayMs(int attempt, uint rc)
  {
   int delay = 35 + attempt * 45;
   if(IsBusyRetcode(rc))  delay += 80 + attempt * 70;
   if(IsPriceRetcode(rc)) delay += 20 + attempt * 25;
   if(!TerminalInfoInteger(TERMINAL_CONNECTED)) delay += 250;
   return delay;
  }

void UpdateTradePacing(uint rc, bool success)
  {
   if(success)
      g_TradeDelayMs = (int)MathMax(TRADE_DELAY_MIN_MS, g_TradeDelayMs - 2);
   else if(IsBusyRetcode(rc) || IsPriceRetcode(rc))
      g_TradeDelayMs = (int)MathMin(TRADE_DELAY_MAX_MS, g_TradeDelayMs + 5);
  }

//--- Modify queue helpers -------------------------------------------
int FindQueuedModifyIndex(ulong ticket)
  {
   for(int i = 0; i < ArraySize(g_ModifyQueue); i++)
      if(g_ModifyQueue[i].ticket == ticket) return i;
   return -1;
  }

bool EnqueueModifyRequest(ulong ticket, double sl, double tp, string context)
  {
   int idx = FindQueuedModifyIndex(ticket);
   uint now = GetTickCount();
   if(idx >= 0)
     {
      g_ModifyQueue[idx].sl            = sl;
      g_ModifyQueue[idx].tp            = tp;
      g_ModifyQueue[idx].context       = context;
      g_ModifyQueue[idx].requestedAtMs = now;
      return true;
     }

   int s = ArraySize(g_ModifyQueue);
   if(s >= MAX_MODIFY_QUEUE_SIZE)
     {
      LogEvent("QUEUE_FULL", "modify queue at cap="+IntegerToString(s)+
               " ticket="+IntegerToString((long)ticket)+" dropped ctx="+context);
      return false;
     }

   ModifyRequest req;
   req.ticket        = ticket;
   req.sl            = sl;
   req.tp            = tp;
   req.context       = context;
   req.rc            = 0;
   req.status        = 0;
   req.requestedAtMs = now;
   if(ArrayResize(g_ModifyQueue, s+1) != s+1) return false;
   g_ModifyQueue[s] = req;
   g_ModifyTotalEnqueued++;
   return true;
  }

void RemoveModifyQueueItem(int index, bool &skipped[])
  {
   int qsize = ArraySize(g_ModifyQueue);
   if(index < 0 || index >= qsize) return;

   for(int k = index; k < qsize - 1; k++)
      g_ModifyQueue[k] = g_ModifyQueue[k+1];
   ArrayResize(g_ModifyQueue, qsize - 1);

   int ssize = ArraySize(skipped);
   if(index < ssize)
     {
      for(int k = index; k < ssize - 1; k++)
         skipped[k] = skipped[k+1];
      ArrayResize(skipped, ssize - 1);
     }
  }

void ProcessModifyQueue()
  {
   const int capacity = 6;
   int processed = 0;
   if(ArraySize(g_ModifyQueue) == 0) return;

   bool skipped[];
   ArrayResize(skipped, ArraySize(g_ModifyQueue));
   ArrayInitialize(skipped, false);

   while(processed < capacity)
     {
      int qsize = ArraySize(g_ModifyQueue);
      if(qsize == 0) break;

      int oldest = -1;
      for(int j = 0; j < qsize; j++)
        {
         if(j < ArraySize(skipped) && skipped[j]) continue;
         if(oldest < 0 ||
            g_ModifyQueue[j].requestedAtMs < g_ModifyQueue[oldest].requestedAtMs)
            oldest = j;
        }
      if(oldest < 0) break;

      ModifyRequest req = g_ModifyQueue[oldest];

      if(g_InFlightModifies >= MAX_GLOBAL_CONCURRENT_MODIFIES)
        {
         LogEvent("FLOOD_GLOBAL", "in-flight modify cap reached");
         break;
        }

      if(!PositionSelectByTicket(req.ticket))
        {
         RemoveModifyQueueItem(oldest, skipped);
         continue;
        }

      long mg   = (long)PositionGetInteger(POSITION_MAGIC);
      int  bidx = FindBatchIndex(mg);

      if(bidx >= 0 && g_Batches[bidx].syncing)
        {
         if(oldest < ArraySize(skipped)) skipped[oldest] = true;
         continue;
        }

      if(IsModifyLocked(req.ticket))
        {
         if(oldest < ArraySize(skipped)) skipped[oldest] = true;
         continue;
        }

      if(bidx >= 0)
        {
         uint now = GetTickCount();
         if(g_Batches[bidx].modifyWindowStartMs == 0 ||
            now - g_Batches[bidx].modifyWindowStartMs > 60000)
           {
            g_Batches[bidx].modifyWindowStartMs = now;
            g_Batches[bidx].modifyCount = 0;
           }
         if(g_Batches[bidx].modifyCount >= MAX_MODIFIES_PER_BATCH_PER_MIN)
           {
            LogEvent("FLOOD_BATCH", "batch="+IntegerToString(g_Batches[bidx].magic)+
                     " rate limit reached");
            if(oldest < ArraySize(skipped)) skipped[oldest] = true;
            continue;
           }
        }

      uint rc = 0;
      g_InFlightModifies++;
      bool ok = ModifyPositionSafe(req.ticket, req.sl, req.tp, rc,
                                   req.context+"_QUEUED");
      g_InFlightModifies--;

      if(ok)
        {
         g_ModifySuccessCount++;
         if(bidx >= 0) g_Batches[bidx].modifyCount++;
        }
      else
         g_ModifyFailCount++;

      RemoveModifyQueueItem(oldest, skipped);
      processed++;
     }
  }

bool ModifyPositionQueued(ulong ticket, double sl, double tp, uint &rc,
                          string context, bool blocking)
  {
   if(blocking)
      return ModifyPositionSafe(ticket, sl, tp, rc, context);

   bool en = EnqueueModifyRequest(ticket, sl, tp, context);
   if(!en)
     {
      rc = TRADE_RETCODE_REJECT;
      return false;
     }
   rc = 0;
   return true;
  }

//--- Execution-state helpers ----------------------------------------
bool AcquireExecState(int desired)
  {
   if(g_ExecState != EXEC_IDLE) return false;
   g_ExecState        = desired;
   g_ExecStateStartMs = GetTickCount();
   return true;
  }

void ReleaseExecState(int expected)
  {
   uint now = GetTickCount();
   uint timeout;
   if(g_ExecState == EXEC_EXECUTING)
      timeout = (uint)EXEC_EXECUTING_TIMEOUT_MS;
   else if(g_ExecState == EXEC_MODIFYING)
      timeout = (uint)EXEC_MODIFYING_TIMEOUT_MS;
   else
      timeout = (uint)EXEC_STATE_TIMEOUT_MS;

   if(g_ExecState == expected)
     {
      g_ExecState        = EXEC_IDLE;
      g_ExecStateStartMs = 0;
     }
   else if(g_ExecStateStartMs > 0 && now - g_ExecStateStartMs > timeout)
     {
      // C-001 fix: force-release clears g_IsExecuting and UI locks when stuck.
      // v4.08: also resets burst dispatcher state and restores sync mode.
      if(g_ExecState == EXEC_EXECUTING)
        {
         g_IsExecuting           = false;
         g_PendingExecActive     = false;
         g_PendingExecPostAttach = false;
         trade.SetAsyncMode(false);
         UnlockButtons();
        }
      LogEvent("EXEC_FORCE_RELEASE",
               "state="+ExecStateText(g_ExecState)+
               " expected="+ExecStateText(expected)+
               " held="+IntegerToString((int)(now - g_ExecStateStartMs))+"ms");
      g_ExecState        = EXEC_IDLE;
      g_ExecStateStartMs = 0;
     }
  }

string ExecStateText(int state)
  {
   if(state == EXEC_EXECUTING)  return "EXECUTING";
   if(state == EXEC_SYNCING)    return "SYNCING";
   if(state == EXEC_TRAILING)   return "TRAILING";
   if(state == EXEC_CLOSING)    return "CLOSING";
   if(state == EXEC_RECOVERING) return "RECOVERING";
   if(state == EXEC_REBUILDING) return "REBUILDING";
   if(state == EXEC_MODIFYING)  return "MODIFYING";
   return "IDLE";
  }

//+------------------------------------------------------------------+
//| DiagnosticsTick                                                  |
//+------------------------------------------------------------------+
void DiagnosticsTick()
  {
   if(!EnableDiagnostics) return;
   uint now = GetTickCount();
   if(g_LastDiagMs > 0 && now - g_LastDiagMs < (uint)DiagnosticsIntervalMs)
      return;

   g_LastDiagMs = now;
   int qsize    = ArraySize(g_ModifyQueue);
   int batchCnt = ArraySize(g_Batches);
   int retryCnt = ArraySize(g_RetryStates);
   int lockCnt  = ArraySize(g_ModifyLocks);
   int posCnt   = PositionsTotal();
   string state = ExecStateText(g_ExecState);

   LogEvent("DIAG", "state="+state+
            " batches="+IntegerToString(batchCnt)+
            " pos="+IntegerToString(posCnt)+
            " q="+IntegerToString(qsize)+
            " totalEnq="+IntegerToString(g_ModifyTotalEnqueued)+
            " inflightM="+IntegerToString(g_InFlightModifies)+
            " inflightC="+IntegerToString(g_InFlightCloses)+
            " retryStates="+IntegerToString(retryCnt)+
            " locks="+IntegerToString(lockCnt)+
            " modOk="+IntegerToString(g_ModifySuccessCount)+
            " modFail="+IntegerToString(g_ModifyFailCount)+
            " trailing="+IntegerToString(g_TrailingEnabled ? 1 : 0)+
            " burstDispatched="+IntegerToString(g_PendingExecDispatched)+
            " burstTotal="+IntegerToString(g_PendingExecTotal)+
            " tSkip="+IntegerToString(g_TimerSkipCount)+
            " sSkip="+IntegerToString(g_SyncSkipCount)+
            " trSkip="+IntegerToString(g_TrailSkipCount)+
            " qSkip="+IntegerToString(g_ModQSkipCount)+
            " rSkip="+IntegerToString(g_RebuildSkipCount)+
            " cSkip="+IntegerToString(g_CloseSkipCount));

   if(qsize > DiagnosticsQueueWarn)
      LogEvent("DIAG_WARN", "modify queue backlog="+IntegerToString(qsize));

   if(g_ExecState != EXEC_IDLE &&
      (now - g_ExecStateStartMs) > (uint)DiagnosticsStateWarnMs)
      LogEvent("DIAG_WARN", "state held "+state+" for "+
               IntegerToString((int)(now - g_ExecStateStartMs))+"ms");

   g_TimerSkipCount   = 0;
   g_SyncSkipCount    = 0;
   g_TrailSkipCount   = 0;
   g_ModQSkipCount    = 0;
   g_RebuildSkipCount = 0;
   g_CloseSkipCount   = 0;
  }

//--- Per-ticket modify lock helpers ---------------------------------
int FindModifyLockIndex(ulong ticket)
  {
   for(int i = 0; i < ArraySize(g_ModifyLocks); i++)
      if(g_ModifyLocks[i].ticket == ticket) return i;
   return -1;
  }

bool IsModifyLocked(ulong ticket)
  {
   uint now = GetTickCount();
   for(int i = ArraySize(g_ModifyLocks) - 1; i >= 0; i--)
     {
      if(g_ModifyLocks[i].unlockAtMs <= now)
        {
         int sz = ArraySize(g_ModifyLocks);
         for(int j = i; j < sz - 1; j++)
            g_ModifyLocks[j] = g_ModifyLocks[j+1];
         ArrayResize(g_ModifyLocks, sz - 1);
        }
     }
   return (FindModifyLockIndex(ticket) >= 0);
  }

void LockModify(ulong ticket)
  {
   uint now = GetTickCount();
   int idx  = FindModifyLockIndex(ticket);
   ModifyLock ml;
   ml.ticket     = ticket;
   ml.unlockAtMs = now + MODIFY_COOLDOWN_MS;
   if(idx >= 0)
      g_ModifyLocks[idx] = ml;
   else
     {
      int s = ArraySize(g_ModifyLocks);
      if(ArrayResize(g_ModifyLocks, s+1) == s+1)
         g_ModifyLocks[s] = ml;
     }
  }

void ThrottleTradeRequest()
  {
   uint now = GetTickCount();
   if(g_LastTradeActionMs > 0)
     {
      uint elapsed = now - g_LastTradeActionMs;
      if(elapsed < (uint)g_TradeDelayMs)
         Sleep(g_TradeDelayMs - (int)elapsed);
     }
   g_LastTradeActionMs = GetTickCount();
  }

void LogTradeFailure(string action, uint rc, int err, string comment)
  {
   LogEvent("EXEC_FAIL", action+
            " retcode="+IntegerToString((int)rc)+
            " err="+IntegerToString(err)+
            " delay="+IntegerToString(g_TradeDelayMs)+
            " comment="+comment);
  }

bool ShouldRetryTrade(uint rc)
  {
   return (IsBusyRetcode(rc) || IsPriceRetcode(rc));
  }

bool SendMarketOrderSafe(int direction, double lot, double price,
                         double sl, double tp, string comment, uint &rc)
  {
   rc = 0;
   for(int attempt = 0; attempt < TRADE_RETRY_MAX; attempt++)
     {
      if(g_AbortFlag || g_IsDeinitializing ||
         !TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         rc = TRADE_RETCODE_CONNECTION;
         return false;
        }

      ResetLastError();
      ThrottleTradeRequest();
      double livePrice = (direction == 0)
         ? SymbolInfoDouble(Symbol(), SYMBOL_ASK)
         : SymbolInfoDouble(Symbol(), SYMBOL_BID);
      if(attempt == 0 && price > 0.0)
         livePrice = price;
      double liveSL = EnforceStopsLevel(Symbol(), livePrice, sl, direction, false);
      double liveTP = EnforceStopsLevel(Symbol(), livePrice, tp, direction, true);
      bool ok = (direction == 0)
         ? trade.Buy(lot, Symbol(), livePrice, liveSL, liveTP, comment)
         : trade.Sell(lot, Symbol(), livePrice, liveSL, liveTP, comment);
      rc = trade.ResultRetcode();
      bool success = (ok && IsSuccessRetcode(rc));
      UpdateTradePacing(rc, success);
      if(success) return true;

      LogTradeFailure("ORDER_ATTEMPT_"+IntegerToString(attempt+1),
                      rc, 0, trade.ResultComment());
      if(!ShouldRetryTrade(rc)) return false;

      Sleep(AdaptiveRetryDelayMs(attempt, rc));
     }
   return false;
  }

bool ModifyPositionSafe(ulong ticket, double sl, double tp, uint &rc,
                        string context)
  {
   rc = 0;
   if(IsFrozen(ticket, sl, tp))
     {
      rc = TRADE_RETCODE_REJECT;
      LogEvent("FROZEN", context+" ticket="+IntegerToString((long)ticket));
      return false;
     }

   if(!PositionSelectByTicket(ticket))
     {
      LogEvent("MOD_SKIP", context+" ticket="+IntegerToString((long)ticket)+" not-found");
      return false;
     }

   int psIdx = FindPosStateIndex(ticket);
   if(psIdx >= 0)
     {
      string sym   = PositionGetString(POSITION_SYMBOL);
      double drift = SyncDriftThreshold(sym);
      if(g_PosStates[psIdx].sl == sl &&
         !IsDifferentTP(g_PosStates[psIdx].tp, tp, drift))
        {
         LogEvent("MOD_NOP", context+" ticket="+IntegerToString((long)ticket)+" no-change");
         return true;
        }
     }

   if(IsModifyLocked(ticket))
     {
      rc = TRADE_RETCODE_LOCKED;
      LogEvent("MOD_LOCK", context+" ticket="+IntegerToString((long)ticket)+" locked");
      return false;
     }

   if(IsUnderRetryCooldown(ticket))
     {
      rc = TRADE_RETCODE_LOCKED;
      LogEvent("MOD_COOLDOWN", context+" ticket="+IntegerToString((long)ticket)+
               " under retry cooldown");
      return false;
     }

   for(int attempt = 0; attempt < TRADE_RETRY_MAX; attempt++)
     {
      if(g_IsDeinitializing || !TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         rc = TRADE_RETCODE_CONNECTION;
         return false;
        }

      ResetLastError();
      ThrottleTradeRequest();
      bool ok = trade.PositionModify(ticket, sl, tp);
      rc = trade.ResultRetcode();
      bool success = (ok && IsSuccessRetcode(rc));
      UpdateTradePacing(rc, success);
      if(success)
        {
         ClearRetryState(ticket);
         LockModify(ticket);
         if(PositionSelectByTicket(ticket))
           {
            long magic = (long)PositionGetInteger(POSITION_MAGIC);
            string sym = PositionGetString(POSITION_SYMBOL);
            int dir    = (PositionGetInteger(POSITION_TYPE) ==
                          POSITION_TYPE_BUY) ? 0 : 1;
            UpsertPosState(ticket, magic, sym, dir, sl, tp);
            int bidx = FindBatchIndex(magic);
            if(bidx >= 0)
              {
               double drift = SyncDriftThreshold(sym);
               double curSL = g_Batches[bidx].sl;
               double curTP = g_Batches[bidx].tp;
               bool slImprove = IsBetterSL(dir, curSL, sl, false);
               bool tpImprove = IsDifferentTP(curTP, tp, drift);
               if(slImprove || tpImprove)
                 {
                  double newSL = slImprove ? sl : curSL;
                  double newTP = tpImprove ? tp : curTP;
                  UpdateBatchStops(magic, newSL, newTP);
                 }
              }
           }
         return true;
        }

      LogTradeFailure(context+"_MODIFY_ATTEMPT_"+IntegerToString(attempt+1),
                      rc, 0, trade.ResultComment());
      uint baseCd = (uint)AdaptiveRetryDelayMs(attempt, rc);
      UpsertRetryStateOnFailure(ticket, attempt+1, baseCd);
      if(!ShouldRetryTrade(rc))
        {
         LockModify(ticket);
         return false;
        }

      if(attempt + 1 >= TRADE_RETRY_MAX)
        {
         uint longCd = MODIFY_COOLDOWN_MS * RETRY_ESCALATION_FACTOR;
         int idx = FindModifyLockIndex(ticket);
         ModifyLock ml;
         ml.ticket     = ticket;
         ml.unlockAtMs = GetTickCount() + longCd;
         if(idx >= 0)
            g_ModifyLocks[idx] = ml;
         else
           {
            int s = ArraySize(g_ModifyLocks);
            if(ArrayResize(g_ModifyLocks, s+1) == s+1)
               g_ModifyLocks[s] = ml;
           }
        }

      Sleep(AdaptiveRetryDelayMs(attempt, rc));
     }
   return false;
  }

// H-002 fix: retry cooldown check before close-count accounting
bool ClosePositionSafe(ulong ticket, uint &rc, string context)
  {
   rc = 0;

   if(g_InFlightCloses >= MAX_GLOBAL_CONCURRENT_CLOSES)
     {
      rc = TRADE_RETCODE_TOO_MANY_REQUESTS;
      LogEvent("FLOOD_CLOSE_GLOBAL", context+" ticket="+IntegerToString((long)ticket));
      return false;
     }

   if(IsUnderRetryCooldown(ticket))
     {
      rc = TRADE_RETCODE_LOCKED;
      LogEvent("CLOSE_COOLDOWN", context+" ticket="+IntegerToString((long)ticket));
      return false;
     }

   int bidx = -1;
   if(PositionSelectByTicket(ticket))
     {
      long mg = (long)PositionGetInteger(POSITION_MAGIC);
      bidx    = FindBatchIndex(mg);
     }
   if(bidx >= 0)
     {
      uint now = GetTickCount();
      if(g_Batches[bidx].closeWindowStartMs == 0 ||
         now - g_Batches[bidx].closeWindowStartMs > 60000)
        {
         g_Batches[bidx].closeWindowStartMs = now;
         g_Batches[bidx].closeCount = 0;
        }
      if(g_Batches[bidx].closeCount >= MAX_CLOSES_PER_BATCH_PER_MIN)
        {
         rc = TRADE_RETCODE_TOO_MANY_REQUESTS;
         LogEvent("FLOOD_CLOSE_BATCH", "batch="+IntegerToString(g_Batches[bidx].magic));
         return false;
        }
      g_Batches[bidx].closeCount++;
     }

   for(int attempt = 0; attempt < TRADE_RETRY_MAX; attempt++)
     {
      if(g_IsDeinitializing || !TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         rc = TRADE_RETCODE_CONNECTION;
         return false;
        }

      ResetLastError();
      ThrottleTradeRequest();
      g_InFlightCloses++;
      bool ok = trade.PositionClose(ticket);
      g_InFlightCloses--;
      rc = trade.ResultRetcode();
      bool success = (ok && IsSuccessRetcode(rc));
      UpdateTradePacing(rc, success);
      if(success)
        {
         ClearRetryState(ticket);
         return true;
        }

      LogTradeFailure(context+"_CLOSE_ATTEMPT_"+IntegerToString(attempt+1),
                      rc, 0, trade.ResultComment());
      uint baseCd = (uint)AdaptiveRetryDelayMs(attempt, rc);
      UpsertRetryStateOnFailure(ticket, attempt+1, baseCd);
      if(!ShouldRetryTrade(rc)) return false;

      Sleep(AdaptiveRetryDelayMs(attempt, rc));
     }
   return false;
  }

bool ClosePositionPartialSafe(ulong ticket, double volume, uint &rc,
                              string context)
  {
   rc = 0;
   if(volume <= 0.0) return false;

   if(g_InFlightCloses >= MAX_GLOBAL_CONCURRENT_CLOSES)
     {
      rc = TRADE_RETCODE_TOO_MANY_REQUESTS;
      LogEvent("FLOOD_CLOSE_GLOBAL", context+" ticket="+IntegerToString((long)ticket));
      return false;
     }

   if(IsUnderRetryCooldown(ticket))
     {
      rc = TRADE_RETCODE_LOCKED;
      LogEvent("CLOSE_COOLDOWN", context+" ticket="+IntegerToString((long)ticket));
      return false;
     }

   int bidx = -1;
   if(PositionSelectByTicket(ticket))
     {
      long mg = (long)PositionGetInteger(POSITION_MAGIC);
      bidx    = FindBatchIndex(mg);
     }
   if(bidx >= 0)
     {
      uint now = GetTickCount();
      if(g_Batches[bidx].closeWindowStartMs == 0 ||
         now - g_Batches[bidx].closeWindowStartMs > 60000)
        {
         g_Batches[bidx].closeWindowStartMs = now;
         g_Batches[bidx].closeCount = 0;
        }
      if(g_Batches[bidx].closeCount >= MAX_CLOSES_PER_BATCH_PER_MIN)
        {
         rc = TRADE_RETCODE_TOO_MANY_REQUESTS;
         LogEvent("FLOOD_CLOSE_BATCH", "batch="+IntegerToString(g_Batches[bidx].magic));
         return false;
        }
      g_Batches[bidx].closeCount++;
     }

   for(int attempt = 0; attempt < TRADE_RETRY_MAX; attempt++)
     {
      if(g_IsDeinitializing || !TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         rc = TRADE_RETCODE_CONNECTION;
         return false;
        }

      ResetLastError();
      ThrottleTradeRequest();
      g_InFlightCloses++;
      bool ok = trade.PositionClosePartial(ticket, volume);
      g_InFlightCloses--;
      rc = trade.ResultRetcode();
      bool success = (ok && IsSuccessRetcode(rc));
      UpdateTradePacing(rc, success);
      if(success)
        {
         ClearRetryState(ticket);
         return true;
        }

      LogTradeFailure(context+"_PARTIAL_ATTEMPT_"+IntegerToString(attempt+1),
                      rc, 0, trade.ResultComment());
      uint baseCd = (uint)AdaptiveRetryDelayMs(attempt, rc);
      UpsertRetryStateOnFailure(ticket, attempt+1, baseCd);
      if(!ShouldRetryTrade(rc)) return false;

      Sleep(AdaptiveRetryDelayMs(attempt, rc));
     }
   return false;
  }

//--- Registry helpers -----------------------------------------------
void EnsureSelectedBatch()
  {
   int size = ArraySize(g_Batches);
   if(size <= 0)
     { g_SelectedBatchIndex = -1; return; }
   if(g_SelectedBatchIndex < 0)
      g_SelectedBatchIndex = 0;
   if(g_SelectedBatchIndex >= size)
      g_SelectedBatchIndex = size - 1;
  }

string BatchDirectionText(int direction)
  {
   return (direction == 0) ? "BUY" : "SELL";
  }

int CountBatchPositions(long magic, string symbol)
  {
   int n = 0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() == magic && posInfo.Symbol() == symbol) n++;
     }
   return n;
  }

double BatchFloatingPnl(long magic, string symbol)
  {
   double pnl = 0.0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() != magic || posInfo.Symbol() != symbol) continue;
      pnl += PositionGetDouble(POSITION_PROFIT);
     }
   return pnl;
  }

int FindBatchIndex(long magic)
  {
   for(int i = 0; i < ArraySize(g_Batches); i++)
      if(g_Batches[i].magic == magic) return i;
   return -1;
  }

bool BatchExists(long magic)
  {
   return (FindBatchIndex(magic) >= 0);
  }

int FindPosStateIndex(ulong ticket)
  {
   for(int i = 0; i < ArraySize(g_PosStates); i++)
      if(g_PosStates[i].ticket == ticket) return i;
   return -1;
  }

void RemovePosStateAt(int index)
  {
   int size = ArraySize(g_PosStates);
   if(index < 0 || index >= size) return;
   for(int i = index; i < size - 1; i++)
      g_PosStates[i] = g_PosStates[i+1];
   ArrayResize(g_PosStates, size - 1);
  }

void UpsertPosState(ulong ticket, long magic, string symbol, int direction,
                    double sl, double tp)
  {
   int idx = FindPosStateIndex(ticket);
   PositionState ps;
   ps.ticket     = ticket;
   ps.magic      = magic;
   ps.symbol     = symbol;
   ps.direction  = direction;
   ps.sl         = sl;
   ps.tp         = tp;
   ps.hasSL      = (sl > 0.0);
   ps.hasTP      = (tp > 0.0);
   ps.lastUpdate = TimeCurrent();

   if(idx < 0)
     {
      int size = ArraySize(g_PosStates);
      if(ArrayResize(g_PosStates, size+1) != size+1) return;
      g_PosStates[size] = ps;
     }
   else
      g_PosStates[idx] = ps;
  }

void RemovePosStatesByMagic(long magic)
  {
   for(int i = ArraySize(g_PosStates)-1; i >= 0; i--)
      if(g_PosStates[i].magic == magic) RemovePosStateAt(i);
  }

void CleanupPosStates()
  {
   for(int i = ArraySize(g_PosStates)-1; i >= 0; i--)
      if(!PositionSelectByTicket(g_PosStates[i].ticket))
         RemovePosStateAt(i);
  }

void CleanupRetryStates()
  {
   for(int i = ArraySize(g_RetryStates)-1; i >= 0; i--)
      if(!PositionSelectByTicket(g_RetryStates[i].ticket))
        {
         int sz = ArraySize(g_RetryStates);
         for(int j = i; j < sz-1; j++)
            g_RetryStates[j] = g_RetryStates[j+1];
         ArrayResize(g_RetryStates, sz-1);
        }
  }

void CleanupModifyLocks()
  {
   for(int i = ArraySize(g_ModifyLocks)-1; i >= 0; i--)
      if(!PositionSelectByTicket(g_ModifyLocks[i].ticket))
        {
         int sz = ArraySize(g_ModifyLocks);
         for(int j = i; j < sz-1; j++)
            g_ModifyLocks[j] = g_ModifyLocks[j+1];
         ArrayResize(g_ModifyLocks, sz-1);
        }
  }

void IntegrityCheck()
  {
   uint now = GetTickCount();
   if(g_LastIntegrityCheckMs > 0 &&
      now - g_LastIntegrityCheckMs < (uint)INTEGRITY_CHECK_INTERVAL_MS)
      return;
   g_LastIntegrityCheckMs = now;

   for(int b = 0; b < ArraySize(g_Batches); b++)
     {
      int cnt = CountBatchPositions(g_Batches[b].magic, g_Batches[b].symbol);
      if(cnt == 0)
         LogEvent("INTEGRITY_WARN", "orphan batch="+
                  IntegerToString(g_Batches[b].magic)+
                  " recorded_fill="+IntegerToString(g_Batches[b].filled)+
                  " live=0");
      else if(cnt != g_Batches[b].filled)
         LogEvent("INTEGRITY_INFO", "batch="+
                  IntegerToString(g_Batches[b].magic)+
                  " recorded_fill="+IntegerToString(g_Batches[b].filled)+
                  " live="+IntegerToString(cnt));
     }

   int managedPosCount = 0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(FindBatchIndex((long)posInfo.Magic()) >= 0)
         managedPosCount++;
     }
   int stateCount = ArraySize(g_PosStates);
   if(managedPosCount != stateCount)
      LogEvent("INTEGRITY_INFO", "managed_pos="+IntegerToString(managedPosCount)+
               " pos_states="+IntegerToString(stateCount)+
               " delta="+IntegerToString(managedPosCount - stateCount));
  }

bool IsOurBatchMagic(long magic, string comment)
  {
   string key = "MOE_KNOWN_"+IntegerToString(magic);
   if(GlobalVariableCheck(key)) return true;
   string prefix = "MOE_"+IntegerToString(magic)+"_";
   return (StringFind(comment, prefix) == 0);
  }

void RebuildBatchRegistryFromPositions()
  {
   ArrayResize(g_Batches, 0);
   ArrayResize(g_PosStates, 0);

   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;

      long magic = posInfo.Magic();
      if(magic <= 0) continue;

      string comment = PositionGetString(POSITION_COMMENT);
      if(!IsOurBatchMagic(magic, comment)) continue;

      int posDir      = (posInfo.PositionType() == POSITION_TYPE_BUY) ? 0 : 1;
      int batchIndex  = FindBatchIndex(magic);
      datetime opened = (datetime)PositionGetInteger(POSITION_TIME);

      UpsertPosState(posInfo.Ticket(), magic, posInfo.Symbol(), posDir,
                     posInfo.StopLoss(), posInfo.TakeProfit());

      if(batchIndex < 0)
        {
         BatchInfo batch;
         batch.magic     = magic;
         batch.symbol    = posInfo.Symbol();
         batch.direction = posDir;
         batch.sl        = posInfo.StopLoss();
         batch.tp        = posInfo.TakeProfit();
         batch.syncing   = false;
         batch.trailing  = (TrailingStop > 0.0) && g_TrailingEnabled;
         batch.active    = true;
         batch.created   = opened;
         string reqKey   = "MOE_REQ_"+IntegerToString(magic);
         int req = GlobalVariableCheck(reqKey)
                   ? (int)GlobalVariableGet(reqKey)
                   : 1;
         batch.requested          = (req > 0) ? req : 1;
         batch.filled             = 1;
         batch.errors             = 0;
         batch.partial            = (batch.requested > batch.filled);
         batch.modifyCount        = 0;
         batch.modifyWindowStartMs= 0;
         batch.closeCount         = 0;
         batch.closeWindowStartMs = 0;
         AddBatch(batch);
        }
      else
        {
         g_Batches[batchIndex].filled++;
         int direction = g_Batches[batchIndex].direction;
         double posSL  = posInfo.StopLoss();
         double posTP  = posInfo.TakeProfit();
         if(IsBetterSL(direction, g_Batches[batchIndex].sl, posSL, false))
            g_Batches[batchIndex].sl = posSL;
         if(posTP > 0.0)
            g_Batches[batchIndex].tp = posTP;
         if(g_Batches[batchIndex].requested > 0)
            g_Batches[batchIndex].partial =
               (g_Batches[batchIndex].filled < g_Batches[batchIndex].requested);
         if(opened > 0 && opened < g_Batches[batchIndex].created)
            g_Batches[batchIndex].created = opened;
        }
     }

   SaveLatestBatchGlobals();
   g_LastRegistryRebuild = TimeCurrent();

   for(int b = 0; b < ArraySize(g_Batches); b++)
      LogEvent("RECOVERY", "batch="+IntegerToString(g_Batches[b].magic)+
               " symbol="+g_Batches[b].symbol+
               " positions="+IntegerToString(g_Batches[b].filled)+
               " SL="+DoubleToString(g_Batches[b].sl, _Digits)+
               " TP="+DoubleToString(g_Batches[b].tp, _Digits));

   // C-002 fix: repair SL=0/TP=0 positions after VPS restart
   RepairMissingSLTP();
  }

void CleanupOrphanBatches()
  {
   for(int b = ArraySize(g_Batches)-1; b >= 0; b--)
     {
      if(CountBatchPositions(g_Batches[b].magic, g_Batches[b].symbol) > 0)
         continue;
      long magic = g_Batches[b].magic;
      RemoveBatch(magic);
      LogEvent("CLEANUP", "Removed orphan batch="+IntegerToString(magic));
     }
   SaveLatestBatchGlobals();
  }

void ResyncAllBatches()
  {
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;

      long magic = posInfo.Magic();
      if(magic <= 0) continue;

      int batchIndex = FindBatchIndex(magic);
      if(batchIndex < 0 || !g_Batches[batchIndex].active ||
         g_Batches[batchIndex].syncing)
         continue;

      string batchSymbol = g_Batches[batchIndex].symbol;
      if(posInfo.Symbol() != batchSymbol) continue;

      ulong ticket  = posInfo.Ticket();
      int stateIdx  = FindPosStateIndex(ticket);
      if(stateIdx < 0)
        {
         UpsertPosState(ticket, magic, batchSymbol,
                        g_Batches[batchIndex].direction,
                        posInfo.StopLoss(), posInfo.TakeProfit());
         continue;
        }

      int direction  = g_PosStates[stateIdx].direction;
      double drift   = SyncDriftThreshold(batchSymbol);
      double curSL   = posInfo.StopLoss();
      double curTP   = posInfo.TakeProfit();
      double storedSL= g_PosStates[stateIdx].sl;
      double storedTP= g_PosStates[stateIdx].tp;

      bool slImprove = IsBetterSL(direction, curSL, storedSL, true);
      bool tpImprove = IsDifferentTP(curTP, storedTP, drift);

      if(slImprove || tpImprove)
        {
         uint rc = 0;
         double applySL = slImprove ? storedSL : curSL;
         double applyTP = tpImprove ? storedTP : curTP;
         ModifyPositionQueued(ticket, applySL, applyTP, rc, "RESYNC", false);
        }
     }
  }

bool MagicInOpenPositions(long magic)
  {
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() == magic) return true;
     }
   return false;
  }

bool AddBatch(BatchInfo &batch)
  {
   if(batch.magic <= 0 || BatchExists(batch.magic)) return false;
   int size = ArraySize(g_Batches);
   if(ArrayResize(g_Batches, size+1) != size+1) return false;
   g_Batches[size] = batch;
   EnsureSelectedBatch();
   LogEvent("BATCH_ADD", "magic="+IntegerToString(batch.magic)+
            " symbol="+batch.symbol+
            " direction="+BatchDirectionText(batch.direction));
   return true;
  }

void UpdateBatchStops(long magic, double sl, double tp)
  {
   int index = FindBatchIndex(magic);
   if(index < 0) return;
   g_Batches[index].sl = sl;
   g_Batches[index].tp = tp;
   SaveLatestBatchGlobals();
  }

void SaveLatestBatchGlobals()
  {
   if(ArraySize(g_Batches) == 0)
     {
      GlobalVariableDel("MOE_BATCH_ID");
      GlobalVariableDel("MOE_BATCH_SL");
      GlobalVariableDel("MOE_BATCH_TP");
      GlobalVariableDel("MOE_BATCH_DIR");
      GlobalVariableDel("MOE_BATCH_LOTS");
      return;
     }
   int index = ArraySize(g_Batches) - 1;
   GlobalVariableSet("MOE_BATCH_ID",  (double)g_Batches[index].magic);
   GlobalVariableSet("MOE_BATCH_SL",  g_Batches[index].sl);
   GlobalVariableSet("MOE_BATCH_TP",  g_Batches[index].tp);
   GlobalVariableSet("MOE_BATCH_DIR", (double)g_Batches[index].direction);
  }

bool RemoveBatch(long magic)
  {
   int index = FindBatchIndex(magic);
   if(index < 0) return false;
   GlobalVariableDel("MOE_KNOWN_"+IntegerToString(magic));
   GlobalVariableDel("MOE_REQ_"+IntegerToString(magic));
   // C-002 fix: clean up persisted pip SL/TP globals
   GlobalVariableDel("MOE_PIP_SL_"+IntegerToString(magic));
   GlobalVariableDel("MOE_PIP_TP_"+IntegerToString(magic));
   RemovePosStatesByMagic(magic);
   int size = ArraySize(g_Batches);
   for(int i = index; i < size - 1; i++)
      g_Batches[i] = g_Batches[i+1];
   ArrayResize(g_Batches, size - 1);
   EnsureSelectedBatch();
   LogEvent("BATCH_REMOVE", "magic="+IntegerToString(magic));
   return true;
  }

// C-003 fix: ChartID() salt prevents cross-instance magic collision
long GenerateBatchMagic()
  {
   static long counter = 0;
   long magic = 0;
   long salt = (long)(AccountInfoInteger(ACCOUNT_LOGIN) % 1000) * 10000
             + (long)(ChartID() % 10000);
   do
     {
      counter++;
      magic = MagicBase + (salt * 1000000000) +
              ((long)TimeCurrent() * 1000) + counter;
     }
   while(BatchExists(magic) || MagicInOpenPositions(magic));
   return magic;
  }

double GetPipSize(string symbol)
  {
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return (digits == 3 || digits == 5)
          ? SymbolInfoDouble(symbol, SYMBOL_POINT) * 10.0
          : SymbolInfoDouble(symbol, SYMBOL_POINT);
  }

double GetPipSize()
  {
   return GetPipSize(Symbol());
  }

double SyncDriftThreshold(string symbol)
  {
   return GetPipSize(symbol) * 0.5;
  }

bool IsBetterSL(int direction, double current, double candidate, bool allowRemoval)
  {
   if(candidate <= 0.0 && current <= 0.0) return false;
   if(candidate <= 0.0) return allowRemoval;
   if(current   <= 0.0) return true;
   return (direction == 0) ? (candidate > current) : (candidate < current);
  }

bool IsDifferentTP(double current, double candidate, double drift)
  {
   if(candidate <= 0.0 && current <= 0.0) return false;
   if(candidate <= 0.0 && current  > 0.0) return true;
   if(current   <= 0.0 && candidate > 0.0) return true;
   return (MathAbs(candidate - current) > drift);
  }

double NormalizeLot(double lot)
  {
   double vMin  = SymbolInfoDouble(Symbol(), SYMBOL_VOLUME_MIN);
   double vMax  = MathMin(SymbolInfoDouble(Symbol(), SYMBOL_VOLUME_MAX), 200.0);
   double vStep = SymbolInfoDouble(Symbol(), SYMBOL_VOLUME_STEP);
   if(vStep <= 0.0 || vMin <= 0.0 || vMax <= 0.0) return 0.0;
   lot = MathFloor(lot / vStep) * vStep;
   return NormalizeDouble(MathMax(vMin, MathMin(vMax, lot)), 2);
  }

bool IsFrozen(ulong ticket, double newSL, double newTP)
  {
   if(!PositionSelectByTicket(ticket)) return false;
   string symbol   = PositionGetString(POSITION_SYMBOL);
   long freezeLvl  = SymbolInfoInteger(symbol, SYMBOL_TRADE_FREEZE_LEVEL);
   if(freezeLvl <= 0) return false;
   double point    = SymbolInfoDouble(symbol, SYMBOL_POINT);
   double minDist  = freezeLvl * point;
   int posType     = (int)PositionGetInteger(POSITION_TYPE);
   double price    = (posType == POSITION_TYPE_BUY)
                     ? SymbolInfoDouble(symbol, SYMBOL_BID)
                     : SymbolInfoDouble(symbol, SYMBOL_ASK);
   double posSL    = PositionGetDouble(POSITION_SL);
   double posTP    = PositionGetDouble(POSITION_TP);
   bool slFrozen   = ((posSL > 0.0 && MathAbs(price - posSL) < minDist) ||
                      (newSL > 0.0 && MathAbs(price - newSL) < minDist));
   bool tpFrozen   = ((posTP > 0.0 && MathAbs(price - posTP) < minDist) ||
                      (newTP > 0.0 && MathAbs(price - newTP) < minDist));
   return (slFrozen || tpFrozen);
  }

double EnforceStopsLevel(string symbol, double price, double level,
                         int dir, bool isTP)
  {
   if(level == 0.0) return 0.0;
   int stopsLvl = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   if(stopsLvl <= 0) return level;
   double point   = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int digits     = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double minDist = (stopsLvl + 2) * point;
   double dist    = MathAbs(price - level);
   if(dist >= minDist) return level;
   if(dir == 0)
      return isTP ? NormalizeDouble(price + minDist, digits)
                  : NormalizeDouble(price - minDist, digits);
   else
      return isTP ? NormalizeDouble(price - minDist, digits)
                  : NormalizeDouble(price + minDist, digits);
  }

ENUM_ORDER_TYPE_FILLING DetectFillingMode()
  {
   int mode = (int)SymbolInfoInteger(Symbol(), SYMBOL_FILLING_MODE);
   if((mode & SYMBOL_FILLING_IOC) != 0) return ORDER_FILLING_IOC;
   if((mode & SYMBOL_FILLING_FOK) != 0) return ORDER_FILLING_FOK;
   return ORDER_FILLING_RETURN;
  }

//+------------------------------------------------------------------+
//| PANEL FUNCTIONS                                                   |
//+------------------------------------------------------------------+

void LockButtons()
  {
   ObjectSetInteger(0, P+"BUY",  OBJPROP_BGCOLOR, C'55,55,55');
   ObjectSetInteger(0, P+"SELL", OBJPROP_BGCOLOR, C'55,55,55');
   ChartRedraw();
  }

void UnlockButtons()
  {
   ObjectSetInteger(0, P+"BUY",  OBJPROP_BGCOLOR, CLR_BUY);
   ObjectSetInteger(0, P+"SELL", OBJPROP_BGCOLOR, CLR_SELL);
   ChartRedraw();
  }

void ResetButton(string name)
  {
   ObjectSetInteger(0, name, OBJPROP_STATE, false);
  }

string TrimText(string value)
  {
   StringTrimLeft(value);
   StringTrimRight(value);
   return value;
  }

void ApplyOptionalEditValue(string name, double value, string placeholder)
  {
   string txt   = (value <= 0.0) ? placeholder : DoubleToString(value, 0);
   color txtClr = (value <= 0.0) ? CLR_PLACEHOLDER : CLR_TEXT;
   ObjectSetString(0,  name, OBJPROP_TEXT,  txt);
   ObjectSetInteger(0, name, OBJPROP_COLOR, txtClr);
  }

void PrepareOptionalEditForInput(string name, string placeholder)
  {
   string txt = TrimText(ObjectGetString(0, name, OBJPROP_TEXT));
   if(txt == placeholder)
     {
      ObjectSetString(0,  name, OBJPROP_TEXT,  "");
      ObjectSetInteger(0, name, OBJPROP_COLOR, CLR_TEXT);
     }
  }

bool IsMouseOverPanelRect(int x, int y, int lx, int ly, int w, int h)
  {
   int left = PanelLeftPx() + lx;
   int top  = g_PanelTop + ly;
   return (x >= left && x <= left + w && y >= top && y <= top + h);
  }

void UpdateHoverState(int x, int y)
  {
   if(g_IsExecuting || g_PanelMinimized) return;
   if(ObjectFind(0, P+"BUY") < 0 || ObjectFind(0, P+"SELL") < 0) return;

   bool overBuy   = IsMouseOverPanelRect(x, y, BUY_X,   BUY_Y,   BUY_W,   BUY_H);
   bool overSell  = IsMouseOverPanelRect(x, y, SELL_X,  SELL_Y,  SELL_W,  SELL_H);
   bool overTrail = (TrailingStop > 0.0) &&
                    IsMouseOverPanelRect(x, y, TRAIL_X, TRAIL_Y, TRAIL_W, TRAIL_H);
   bool changed   = false;

   if(overBuy != g_HoverBuy)
     {
      g_HoverBuy = overBuy;
      ObjectSetInteger(0, P+"BUY", OBJPROP_BGCOLOR,
                       overBuy ? CLR_BUY_HOVER : CLR_BUY);
      changed = true;
     }
   if(overSell != g_HoverSell)
     {
      g_HoverSell = overSell;
      ObjectSetInteger(0, P+"SELL", OBJPROP_BGCOLOR,
                       overSell ? CLR_SELL_HOVER : CLR_SELL);
      changed = true;
     }
   if(overTrail != g_HoverTrail)
     {
      g_HoverTrail = overTrail;
      RefreshTrailingButton();
      changed = true;
     }
   if(changed) ChartRedraw();
  }

string FitPanelText(string text, int maxLen)
  {
   if(maxLen <= 3 || StringLen(text) <= maxLen) return text;
   return StringSubstr(text, 0, maxLen-3)+"...";
  }

string ShortMagic(long magic)
  {
   string value = IntegerToString(magic);
   int len = StringLen(value);
   if(len <= 10) return value;
   return "..."+StringSubstr(value, len-8, 8);
  }

void LoadPanelLayout()
  {
   if(GlobalVariableCheck("MOE_PANEL_RIGHT"))
      g_PanelRight = (int)MathMax(0.0, GlobalVariableGet("MOE_PANEL_RIGHT"));
   if(GlobalVariableCheck("MOE_PANEL_TOP"))
      g_PanelTop   = (int)MathMax(0.0, GlobalVariableGet("MOE_PANEL_TOP"));
   if(GlobalVariableCheck("MOE_PANEL_MIN"))
      g_PanelMinimized = (GlobalVariableGet("MOE_PANEL_MIN") > 0.5);
   if(GlobalVariableCheck("MOE_PANEL_TRAIL") && TrailingStop > 0.0)
      g_TrailingEnabled = (GlobalVariableGet("MOE_PANEL_TRAIL") > 0.5);
  }

void SavePanelLayout()
  {
   GlobalVariableSet("MOE_PANEL_RIGHT", (double)g_PanelRight);
   GlobalVariableSet("MOE_PANEL_TOP",   (double)g_PanelTop);
   GlobalVariableSet("MOE_PANEL_MIN",   g_PanelMinimized ? 1.0 : 0.0);
   GlobalVariableSet("MOE_PANEL_TRAIL", g_TrailingEnabled ? 1.0 : 0.0);
  }

void TogglePanelMinimized()
  {
   g_PanelMinimized = !g_PanelMinimized;
   SavePanelLayout();
   DeletePanel();
   CreatePanel();
  }

bool IsMouseLeftDown(string state)
  {
   int mask = (int)StringToInteger(state);
   return ((mask & 1) == 1);
  }

int ChartWidthPixels()
  {
   return (int)ChartGetInteger(0, CHART_WIDTH_IN_PIXELS, 0);
  }

int ChartHeightPixels()
  {
   return (int)ChartGetInteger(0, CHART_HEIGHT_IN_PIXELS, 0);
  }

int PanelLeftPx()
  {
   return ChartWidthPixels() - g_PanelRight - PW;
  }

bool IsPointOnPanelTitle(int x, int y)
  {
   int left   = PanelLeftPx();
   int right  = left + PW;
   int bottom = g_PanelTop + PH_MIN;
   if(x < left || x > right || y < g_PanelTop || y > bottom) return false;
   int minLeft = right - 46;
   if(x >= minLeft && y <= g_PanelTop + 44) return false;
   return true;
  }

void MovePanelTo(int newRight, int newTop)
  {
   int chartW = ChartWidthPixels();
   int chartH = ChartHeightPixels();
   int panelH = g_PanelMinimized ? PH_MIN : PH;

   if(chartW > PW)
      newRight = (int)MathMax(0.0, MathMin((double)(chartW-PW), (double)newRight));
   else
      newRight = 0;

   if(chartH > panelH)
      newTop = (int)MathMax(0.0, MathMin((double)(chartH-panelH), (double)newTop));
   else
      newTop = 0;

   int dx = newRight - g_PanelRight;
   int dy = newTop   - g_PanelTop;
   if(dx == 0 && dy == 0) return;

   int total = ObjectsTotal(0, 0, -1);
   for(int i = total-1; i >= 0; i--)
     {
      string obj = ObjectName(0, i, 0, -1);
      if(StringFind(obj, P) != 0) continue;
      long xdist = ObjectGetInteger(0, obj, OBJPROP_XDISTANCE);
      long ydist = ObjectGetInteger(0, obj, OBJPROP_YDISTANCE);
      ObjectSetInteger(0, obj, OBJPROP_XDISTANCE, xdist + dx);
      ObjectSetInteger(0, obj, OBJPROP_YDISTANCE, ydist + dy);
     }
   g_PanelRight = newRight;
   g_PanelTop   = newTop;
   ChartRedraw();
  }

void HandlePanelMouseMove(int x, int y, string state)
  {
   UpdateHoverState(x, y);
   bool leftDown = IsMouseLeftDown(state);

   if(!leftDown)
     {
      if(g_PanelDragging)
        {
         g_PanelDragging = false;
         SavePanelLayout();
        }
      return;
     }

   if(!g_PanelDragging)
     {
      if(!IsPointOnPanelTitle(x, y)) return;
      g_PanelDragging = true;
      g_DragOffsetX   = x - PanelLeftPx();
      g_DragOffsetY   = y - g_PanelTop;
     }

   int chartW   = ChartWidthPixels();
   int newLeft  = x - g_DragOffsetX;
   int newTop   = y - g_DragOffsetY;
   int newRight = chartW - newLeft - PW;
   MovePanelTo(newRight, newTop);
  }

void MakePanelDragHandle(string name)
  {
   if(ObjectFind(0, name) < 0) return;
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_SELECTED,   false);
   ObjectSetString(0,  name, OBJPROP_TOOLTIP, "Hold and drag to move panel");
  }

void SetStatus(string msg, color clr)
  {
   if(ObjectFind(0, P+"STATUS") >= 0)
     {
      ObjectSetString(0,  P+"STATUS", OBJPROP_TEXT,  FitPanelText(msg, 46));
      ObjectSetInteger(0, P+"STATUS", OBJPROP_COLOR, clr);
      ChartRedraw();
     }
   Print("MOE_STATUS: ", msg);
  }

//+------------------------------------------------------------------+
//| RefreshTrailingButton                                             |
//+------------------------------------------------------------------+
void RefreshTrailingButton()
  {
   if(ObjectFind(0, P+"TRAIL") < 0) return;

   if(TrailingStop <= 0.0)
     {
      ObjectSetString(0,  P+"TRAIL", OBJPROP_TEXT,   "TRAILING SL  ·  NOT CONFIGURED  F5");
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_BGCOLOR, CLR_TRAIL_DIS);
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_COLOR,   C'90,90,90');
      if(ObjectFind(0, P+"TINFO") >= 0)
        {
         ObjectSetString(0,  P+"TINFO", OBJPROP_TEXT,  "Set TrailingStop > 0 in EA inputs to enable");
         ObjectSetInteger(0, P+"TINFO", OBJPROP_COLOR, CLR_SUBTLE);
        }
     }
   else if(g_TrailingEnabled)
     {
      ObjectSetString(0,  P+"TRAIL", OBJPROP_TEXT,
                      "TRAILING SL  ·  ACTIVE  F5");
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_BGCOLOR,
                       g_HoverTrail ? CLR_TRAIL_ON_HOVER : CLR_TRAIL_ON);
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_COLOR, clrWhite);
      if(ObjectFind(0, P+"TINFO") >= 0)
        {
         string info = "Trail: "+DoubleToString(TrailingStop,0)+" pips   "+
                       "Step: "+DoubleToString(TrailingStep,0)+" pips";
         ObjectSetString(0,  P+"TINFO", OBJPROP_TEXT,  info);
         ObjectSetInteger(0, P+"TINFO", OBJPROP_COLOR, CLR_STATUS_OK);
        }
     }
   else
     {
      ObjectSetString(0,  P+"TRAIL", OBJPROP_TEXT, "TRAILING SL  ·  OFF  F5");
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_BGCOLOR,
                       g_HoverTrail ? CLR_TRAIL_OFF_HOVER : CLR_TRAIL_OFF);
      ObjectSetInteger(0, P+"TRAIL", OBJPROP_COLOR, CLR_MUTED);
      if(ObjectFind(0, P+"TINFO") >= 0)
        {
         string info = "Trail: "+DoubleToString(TrailingStop,0)+" pips   "+
                       "Step: "+DoubleToString(TrailingStep,0)+" pips";
         ObjectSetString(0,  P+"TINFO", OBJPROP_TEXT,  info);
         ObjectSetInteger(0, P+"TINFO", OBJPROP_COLOR, CLR_SUBTLE);
        }
     }
  }

void RefreshPanel()
  {
   if(ObjectFind(0, P+"SPREAD") < 0) return;

   double spdPips = (double)SymbolInfoInteger(Symbol(), SYMBOL_SPREAD)
                    * _Point / GetPipSize();
   color spdClr   = (MaxSpread > 0 && spdPips > MaxSpread)
                    ? CLR_STATUS_WARN : CLR_STATUS_OK;
   ObjectSetString(0,  P+"SPREAD", OBJPROP_TEXT,
                   "Spread  "+DoubleToString(spdPips, 1)+" pips");
   ObjectSetInteger(0, P+"SPREAD", OBJPROP_COLOR, spdClr);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   double eq  = AccountInfoDouble(ACCOUNT_EQUITY);
   ObjectSetString(0, P+"BAL", OBJPROP_TEXT,
                   "Bal $"+DoubleToString(bal,2)+"   Eq $"+DoubleToString(eq,2));

   int   pos    = PositionsTotal();
   color posClr = (pos >= 190) ? CLR_STATUS_WARN : CLR_MUTED;
   ObjectSetString(0,  P+"POS",  OBJPROP_TEXT,
                   "Positions  "+IntegerToString(pos)+" / 200");
   ObjectSetInteger(0, P+"POS",  OBJPROP_COLOR, posClr);

   RefreshTrailingButton();
   RefreshBatchPanel();
   ChartRedraw();
  }

void SetButtonVisual(string name, color bg, color fg)
  {
   if(ObjectFind(0, name) < 0) return;
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR, bg);
   ObjectSetInteger(0, name, OBJPROP_COLOR,   fg);
   ObjectSetInteger(0, name, OBJPROP_STATE,   false);
  }

void SetManagementButtonsEnabled(bool enabled)
  {
   color disabledBg = C'45,45,45';
   color disabledFg = C'150,150,150';
   SetButtonVisual(P+"CLOSE", enabled ? CLR_CLOSE   : disabledBg,
                              enabled ? clrWhite    : disabledFg);
   SetButtonVisual(P+"BE",    enabled ? CLR_BE      : disabledBg,
                              enabled ? clrWhite    : disabledFg);
   SetButtonVisual(P+"PART",  enabled ? CLR_PART    : disabledBg,
                              enabled ? clrWhite    : disabledFg);
   SetButtonVisual(P+"BPREV", enabled ? CLR_SURFACE : disabledBg,
                              enabled ? CLR_TEXT    : disabledFg);
   SetButtonVisual(P+"BNEXT", enabled ? CLR_SURFACE : disabledBg,
                              enabled ? CLR_TEXT    : disabledFg);
  }

//+------------------------------------------------------------------+
//| RefreshBatchPanel                                                |
//+------------------------------------------------------------------+
void RefreshBatchPanel()
  {
   if(ObjectFind(0, P+"BSEL") < 0) return;

   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size <= 0 || g_SelectedBatchIndex < 0 || g_SelectedBatchIndex >= size)
     {
      ObjectSetString(0,  P+"BSEL",   OBJPROP_TEXT,  "No Active Batches");
      ObjectSetInteger(0, P+"BSEL",   OBJPROP_COLOR, clrOrange);
      ObjectSetString(0,  P+"BMETA",  OBJPROP_TEXT,  "Symbol --   Pos 0");
      ObjectSetInteger(0, P+"BMETA",  OBJPROP_COLOR, CLR_MUTED);
      ObjectSetString(0,  P+"BLOTS",  OBJPROP_TEXT,  "Lots: --");
      ObjectSetInteger(0, P+"BLOTS",  OBJPROP_COLOR, CLR_MUTED);
      ObjectSetString(0,  P+"BPNL",   OBJPROP_TEXT,  "P/L: --");
      ObjectSetInteger(0, P+"BPNL",   OBJPROP_COLOR, CLR_MUTED);
      ObjectSetString(0,  P+"BTRAIL", OBJPROP_TEXT,  "Trail: --");
      ObjectSetInteger(0, P+"BTRAIL", OBJPROP_COLOR, CLR_MUTED);
      ObjectSetString(0,  P+"BBE",    OBJPROP_TEXT,  "BE: --");
      ObjectSetInteger(0, P+"BBE",    OBJPROP_COLOR, CLR_MUTED);
      SetManagementButtonsEnabled(false);
      return;
     }

   BatchInfo batch    = g_Batches[g_SelectedBatchIndex];
   int positions      = CountBatchPositions(batch.magic, batch.symbol);
   double lots        = BatchTotalLots(batch.magic, batch.symbol);
   double pnl         = BatchFloatingPnl(batch.magic, batch.symbol);
   bool beActive      = IsBatchBreakevenActive(batch.magic, batch.symbol);
   bool trailActive   = batch.trailing && g_TrailingEnabled && (TrailingStop > 0.0);

   color pnlClr = (pnl > 0.0)  ? CLR_STATUS_OK
                : (pnl < 0.0)  ? CLR_STATUS_ERROR
                :                 CLR_MUTED;

   ObjectSetString(0,  P+"BSEL", OBJPROP_TEXT,
                   "Batch "+IntegerToString(g_SelectedBatchIndex+1)+
                   "/"+IntegerToString(size)+"   #"+ShortMagic(batch.magic));
   ObjectSetInteger(0, P+"BSEL", OBJPROP_COLOR, CLR_TEXT);

   ObjectSetString(0,  P+"BMETA", OBJPROP_TEXT,
                   batch.symbol+"   "+BatchDirectionText(batch.direction)+
                   "   Pos: "+IntegerToString(positions));
   ObjectSetInteger(0, P+"BMETA", OBJPROP_COLOR, CLR_MUTED);

   ObjectSetString(0,  P+"BLOTS", OBJPROP_TEXT,
                   "Lots: "+DoubleToString(lots, 2));
   ObjectSetInteger(0, P+"BLOTS", OBJPROP_COLOR, CLR_MUTED);

   string pnlSign = (pnl >= 0.0) ? "+" : "";
   ObjectSetString(0,  P+"BPNL", OBJPROP_TEXT,
                   "P/L: "+pnlSign+"$"+DoubleToString(pnl, 2));
   ObjectSetInteger(0, P+"BPNL", OBJPROP_COLOR, pnlClr);

   ObjectSetString(0,  P+"BTRAIL", OBJPROP_TEXT,
                   trailActive ? "Trail: ON" : "Trail: OFF");
   ObjectSetInteger(0, P+"BTRAIL", OBJPROP_COLOR,
                    trailActive ? CLR_STATUS_OK : CLR_SUBTLE);

   ObjectSetString(0,  P+"BBE", OBJPROP_TEXT,
                   beActive ? "BE: YES" : "BE: NO");
   ObjectSetInteger(0, P+"BBE", OBJPROP_COLOR,
                    beActive ? CLR_STATUS_OK : CLR_SUBTLE);

   SetManagementButtonsEnabled(true);
  }

void MakeLabel(string name, int lx, int ly, string txt,
               color clr, int sz, bool bold)
  {
   if(ObjectFind(0, name) >= 0) ObjectDelete(0, name);
   ObjectCreate(0, name, OBJ_LABEL, 0, 0, 0);
   ObjectSetInteger(0, name, OBJPROP_CORNER,     CORNER_RIGHT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE,  PX(lx));
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE,  PY(ly));
   ObjectSetString(0,  name, OBJPROP_TEXT,       txt);
   ObjectSetInteger(0, name, OBJPROP_COLOR,      clr);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE,   sz);
   ObjectSetString(0,  name, OBJPROP_FONT,       bold ? "Tahoma Bold" : "Tahoma");
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_BACK,       false);
  }

void MakeText(string name, int lx, int ly, string txt,
              color clr, int sz, bool bold)
  {
   MakeLabel(name, lx, ly, txt, clr, sz, bold);
   ObjectSetInteger(0, name, OBJPROP_ANCHOR, ANCHOR_LEFT_UPPER);
  }

void MakeEdit(string name, int lx, int ly, int w, string txt)
  {
   if(ObjectFind(0, name) >= 0) ObjectDelete(0, name);
   ObjectCreate(0, name, OBJ_EDIT, 0, 0, 0);
   ObjectSetInteger(0, name, OBJPROP_CORNER,       CORNER_RIGHT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE,    PX(lx));
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE,    PY(ly));
   ObjectSetInteger(0, name, OBJPROP_XSIZE,        w + 10);
   ObjectSetInteger(0, name, OBJPROP_YSIZE,        28);
   ObjectSetString(0,  name, OBJPROP_TEXT,         txt);
   ObjectSetInteger(0, name, OBJPROP_COLOR,        CLR_TEXT);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR,      CLR_EDIT_BG);
   ObjectSetInteger(0, name, OBJPROP_BORDER_COLOR, CLR_EDIT_BD);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE,     10);
   ObjectSetString(0,  name, OBJPROP_FONT,         "Tahoma");
   ObjectSetInteger(0, name, OBJPROP_ALIGN,        ALIGN_CENTER);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE,   false);
   ObjectSetInteger(0, name, OBJPROP_BACK,         false);
  }

void MakeButton(string name, int lx, int ly, int w, int h,
                string txt, color bg, color fg)
  {
   if(ObjectFind(0, name) >= 0) ObjectDelete(0, name);
   ObjectCreate(0, name, OBJ_BUTTON, 0, 0, 0);
   ObjectSetInteger(0, name, OBJPROP_CORNER,       CORNER_RIGHT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE,    PX(lx));
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE,    PY(ly));
   ObjectSetInteger(0, name, OBJPROP_XSIZE,        w);
   ObjectSetInteger(0, name, OBJPROP_YSIZE,        h);
   ObjectSetString(0,  name, OBJPROP_TEXT,         txt);
   ObjectSetInteger(0, name, OBJPROP_COLOR,        fg);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR,      bg);
   ObjectSetInteger(0, name, OBJPROP_BORDER_COLOR, CLR_BORDER);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE,     10);
   ObjectSetString(0,  name, OBJPROP_FONT,         "Tahoma Bold");
   ObjectSetInteger(0, name, OBJPROP_STATE,        false);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE,   false);
   ObjectSetInteger(0, name, OBJPROP_BACK,         false);
  }

void MakeRect(string name, int lx, int ly, int w, int h,
              color bg, color border)
  {
   if(ObjectFind(0, name) >= 0) ObjectDelete(0, name);
   ObjectCreate(0, name, OBJ_RECTANGLE_LABEL, 0, 0, 0);
   ObjectSetInteger(0, name, OBJPROP_CORNER,       CORNER_RIGHT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE,    PX(lx));
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE,    PY(ly));
   ObjectSetInteger(0, name, OBJPROP_XSIZE,        w);
   ObjectSetInteger(0, name, OBJPROP_YSIZE,        h);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR,      bg);
   ObjectSetInteger(0, name, OBJPROP_BORDER_COLOR, border);
   ObjectSetInteger(0, name, OBJPROP_BORDER_TYPE,  BORDER_FLAT);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE,   false);
   ObjectSetInteger(0, name, OBJPROP_BACK,         false);
  }

//+------------------------------------------------------------------+
//| CreatePanel — v4.07 layout, v4.08 version label                  |
//+------------------------------------------------------------------+
void CreatePanel()
  {
   MakeRect(P+"BG", 0, 0, PW, g_PanelMinimized ? PH_MIN : PH, CLR_BG, CLR_BORDER);
   MakePanelDragHandle(P+"BG");

   MakeRect(P+"TBAR", 0, 0, PW, PH_MIN, CLR_SURFACE, CLR_BORDER);
   MakePanelDragHandle(P+"TBAR");
   MakeText(P+"TITLE", 16, 14, "APEX EXECUTION", CLR_TEXT, 12, true);
   MakePanelDragHandle(P+"TITLE");
   MakeText(P+"TSYM",  16, 43,
            Symbol()+"  "+EnumToString((ENUM_TIMEFRAMES)Period()),
            CLR_MUTED, 8, true);
   MakePanelDragHandle(P+"TSYM");
   MakeButton(P+"MIN", 354, 18, 30, 28,
              g_PanelMinimized ? "+" : "-", CLR_SURFACE, CLR_TEXT);

   if(g_PanelMinimized)
     {
      ChartRedraw();
      return;
     }

   MakeRect(P+"LIVE", 12, 84, PW-24, 80, C'18,21,30', CLR_DIVIDER);
   MakeText(P+"SPREAD", 22,  98, "Spread  --",        CLR_STATUS_OK, 9, true);
   MakeText(P+"BAL",    22, 122, "Bal --",             CLR_MUTED,     8, false);
   MakeText(P+"POS",    22, 146, "Positions  0 / 200", CLR_MUTED,     8, false);

   MakeRect(P+"DIV1", 12, 172, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);
   MakeText(P+"SEC1", 16, 184, "ORDER SETUP", CLR_SUBTLE, 8, true);

   MakeText(P+"L_NUM", 16,  204, "TRADES",           CLR_MUTED, 7, true);
   MakeEdit(P+"E_NUM", 16,  220, 170, IntegerToString(g_PanelNumTrades));
   MakeText(P+"L_LOT", 214, 204, "LOTS",             CLR_MUTED, 7, true);
   MakeEdit(P+"E_LOT", 214, 220, 170, DoubleToString(g_PanelLotSize, 2));

   MakeText(P+"L_SL",  16,  258, "STOP LOSS PIPS",   CLR_MUTED, 7, true);
   MakeEdit(P+"E_SL",  16,  274, 170, "");
   ApplyOptionalEditValue(P+"E_SL", g_PanelSL, SL_PLACEHOLDER);
   MakeText(P+"L_TP",  214, 258, "TAKE PROFIT PIPS", CLR_MUTED, 7, true);
   MakeEdit(P+"E_TP",  214, 274, 170, "");
   ApplyOptionalEditValue(P+"E_TP", g_PanelTP, TP_PLACEHOLDER);
   MakeText(P+"H_SLTP", 16, 308, "Blank = No SL / TP", CLR_SUBTLE, 7, false);

   MakeRect(P+"DIV2", 12, 322, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeButton(P+"BUY",  BUY_X,  BUY_Y,  BUY_W,  BUY_H,
              "BUY  F1",  CLR_BUY,  clrWhite);
   MakeButton(P+"SELL", SELL_X, SELL_Y, SELL_W, SELL_H,
              "SELL  F2", CLR_SELL, clrWhite);

   MakeRect(P+"DIV3", 12, 390, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   {
      color trailBg  = CLR_TRAIL_DIS;
      color trailFg  = C'90,90,90';
      string trailTxt= "TRAILING SL  ·  NOT CONFIGURED  F5";
      if(TrailingStop > 0.0)
        {
         trailTxt = g_TrailingEnabled ? "TRAILING SL  ·  ACTIVE  F5"
                                      : "TRAILING SL  ·  OFF  F5";
         trailBg  = g_TrailingEnabled ? CLR_TRAIL_ON  : CLR_TRAIL_OFF;
         trailFg  = g_TrailingEnabled ? clrWhite      : CLR_MUTED;
        }
      MakeButton(P+"TRAIL", TRAIL_X, TRAIL_Y, TRAIL_W, TRAIL_H,
                 trailTxt, trailBg, trailFg);
   }

   {
      string tinfo = (TrailingStop > 0.0)
                     ? "Trail: "+DoubleToString(TrailingStop,0)+" pips   "+
                       "Step: "+DoubleToString(TrailingStep,0)+" pips"
                     : "Set TrailingStop > 0 in EA inputs to enable";
      color tinfoClr = (TrailingStop > 0.0 && g_TrailingEnabled)
                       ? CLR_STATUS_OK : CLR_SUBTLE;
      MakeText(P+"TINFO", 22, 444, tinfo, tinfoClr, 8, false);
   }

   MakeRect(P+"DIV4", 12, 462, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeButton(P+"CLOSE", 16, 474, 368, 32,
              "CLOSE SELECTED  F3",    CLR_CLOSE, clrWhite);
   MakeButton(P+"BE",    16, 514, 368, 32,
              "MOVE TO BREAKEVEN  F4", CLR_BE,    clrWhite);
   MakeButton(P+"PART",  16, 554, 368, 32,
              "PARTIAL CLOSE  "+DoubleToString(PartialClosePct, 0)+"%",
              CLR_PART, clrWhite);

   MakeRect(P+"DIV5", 12, 596, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeText(P+"SEC2",  16, 608, "ACTIVE BATCH", CLR_SUBTLE, 8, true);

   MakeButton(P+"BPREV", 16,  626, 42, 28, "<", CLR_SURFACE, CLR_TEXT);
   MakeButton(P+"BNEXT", 342, 626, 42, 28, ">", CLR_SURFACE, CLR_TEXT);
   MakeText(P+"BSEL",  70, 631, "No Active Batches", clrOrange, 8, true);

   MakeText(P+"BMETA",  16,  664, "Symbol --   Pos 0",  CLR_MUTED, 8, false);
   MakeText(P+"BLOTS",  16,  680, "Lots: --",            CLR_MUTED, 8, false);
   MakeText(P+"BPNL",   200, 680, "P/L: --",             CLR_MUTED, 8, true);
   MakeText(P+"BTRAIL", 16,  696, "Trail: --",           CLR_MUTED, 8, false);
   MakeText(P+"BBE",    190, 696, "BE: --",              CLR_MUTED, 8, false);

   MakeRect(P+"DIV6", 12, 714, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeRect(P+"SBAR",   0,   718, PW, 56,  CLR_SURFACE, CLR_BORDER);
   MakeText(P+"SLAB",  16,  726, "STATUS",  CLR_SUBTLE,    7, true);
   MakeText(P+"STATUS",16,  742, "Ready",   CLR_STATUS_OK, 9, true);

   RefreshBatchPanel();
   ChartRedraw();
  }

//+------------------------------------------------------------------+
//| DeletePanel                                                      |
//+------------------------------------------------------------------+
void DeletePanel()
  {
   ObjectsDeleteAll(0, P, 0, -1);
   ChartRedraw();
  }
//+------------------------------------------------------------------+