//+------------------------------------------------------------------+
//|                                             ApexExecution.mq5   |
//|                                               Arunaditya Lal     |
//|                                        https://www.mql5.com      |
//+------------------------------------------------------------------+
#property copyright "Arunaditya Lal"
#property link      "https://www.mql5.com"
#property version   "4.02"
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
//| INPUT PARAMETERS                                                  |
//+------------------------------------------------------------------+
input group "=== EXECUTION ==="
input int      NumTrades           = 1;      // Number of Trades (1-200)
input double   LotSize             = 0.01;   // Lot Size per Trade
input double   StopLoss            = 0;      // Stop Loss (pips, 0=none)
input double   TakeProfit          = 0;      // Take Profit (pips, 0=none)
input int      Slippage            = 10;     // Max Slippage (points)

input group "=== FILTERS ==="
input double   MaxSpread           = 3.0;    // Max Spread Filter (pips, 0=off)

input group "=== TRAILING STOP ==="
input double   TrailingStop        = 0;      // Trailing Stop (pips, 0=disabled)
input double   TrailingStep        = 5;      // Trailing Step (pips, min move)

input group "=== BREAKEVEN ==="
input double   BreakevenTrigger    = 20;     // Breakeven Trigger (pips in profit)
input double   BreakevenBuffer     = 2;      // Breakeven Buffer (pips above entry)

input group "=== RISK MANAGEMENT ==="
input bool     UseRiskPercent      = false;  // Use Risk % instead of fixed lot
input double   RiskPercent         = 1.0;    // Risk Per Trade (% of balance)
input double   PartialClosePct     = 50.0;   // Partial Close Percentage (%)

input group "=== EA SETTINGS ==="
input long     MagicBase           = 123456; // Magic Number Base
input bool     HotkeysEnabled      = true;   // Enable Hotkeys

input group "=== RECOVERY ==="
input int      ReconnectStableMs     = 500;  // Wait after reconnect before rebuild/resync
input int      RebuildMinIntervalSec = 2;    // Minimum seconds between rebuilds
input int      RebuildBackoffMs      = 500;  // Backoff base for rebuild/resync retries
input int      RebuildMaxAttempts    = 5;    // Max consecutive rebuild/resync attempts

input group "=== DIAGNOSTICS ==="
input bool     EnableDiagnostics   = true;   // Enable diagnostics logging
input int      DiagnosticsIntervalMs = 5000; // Diagnostics interval (ms)
input int      DiagnosticsQueueWarn = 50;    // Queue backlog warn threshold
input int      DiagnosticsStateWarnMs = 3000;// Exec-state held warn (ms)

//+------------------------------------------------------------------+
//| OBJECTS                                                           |
//+------------------------------------------------------------------+
CTrade         trade;
CPositionInfo  posInfo;

//+------------------------------------------------------------------+
//| GLOBAL STATE                                                      |
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

BatchInfo g_Batches[];
PositionState g_PosStates[];
bool    g_IsExecuting      = false;
bool    g_WasConnected     = true;
bool    g_AbortFlag        = false;
bool    g_IsDeinitializing = false;
bool    g_PendingReconnectResync  = false;
bool    g_PendingReconnectRebuild = false;
datetime g_LastRegistryRebuild  = 0;
uint    g_ReconnectDetectedMs   = 0;
int     g_RebuildAttempts       = 0;
uint    g_RebuildNextAllowedMs  = 0;
int     g_ResyncAttempts        = 0;
uint    g_ResyncNextAllowedMs   = 0;
bool    g_HoverBuy      = false;
bool    g_HoverSell     = false;
int     g_SelectedBatchIndex = -1;
int     g_PanelNumTrades = 0;
double  g_PanelLotSize   = 0;
double  g_PanelSL        = 0;
double  g_PanelTP        = 0;
uint    g_LastTradeActionMs = 0;
int     g_TradeDelayMs   = 10;

const int TRADE_DELAY_MIN_MS = 10;
const int TRADE_DELAY_MAX_MS = 25;
const int TRADE_RETRY_MAX    = 4;
const int MODIFY_COOLDOWN_MS = 200;

// FIX 4: State-dependent exec-state timeouts
const int EXEC_STATE_TIMEOUT_MS     = 10000;  // 10 s  — all states except executing
const int EXEC_EXECUTING_TIMEOUT_MS = 120000; // 2 min — execution loop (200 trades max)

struct ModifyLock
  {
   ulong ticket;
   uint  unlockAtMs;
  };

ModifyLock g_ModifyLocks[];

enum EXEC_STATE { EXEC_IDLE=0, EXEC_EXECUTING=1, EXEC_SYNCING=2, EXEC_TRAILING=3,
                  EXEC_CLOSING=4, EXEC_RECOVERING=5, EXEC_REBUILDING=6 };
int    g_ExecState        = EXEC_IDLE;
uint   g_ExecStateStartMs = 0;
uint   g_LastTimerMs      = 0;

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

ModifyRequest g_ModifyQueue[];

int g_ModifyQueuedCount  = 0;
int g_ModifySuccessCount = 0;
int g_ModifyFailCount    = 0;

uint g_LastDiagMs    = 0;
int  g_TimerSkipCount   = 0;
int  g_SyncSkipCount    = 0;
int  g_TrailSkipCount   = 0;
int  g_ModQSkipCount    = 0;
int  g_RebuildSkipCount = 0;
int  g_CloseSkipCount   = 0;

struct RetryState
  {
   ulong ticket;
   int   attempts;
   uint  lastAttemptMs;
   uint  cooldownMs;
  };

RetryState g_RetryStates[];
const int RETRY_ESCALATION_FACTOR = 4;

// FIX: Raised from 120 to 360 to match capacity=6 throughput.
// At capacity=6 and 100ms timer, max throughput = 6 * 600 = 3600/min.
// 120 was calibrated for the old capacity=2 (120/min exact match).
// With capacity=6, 120 was saturated in ~2 seconds of sustained trailing,
// blocking ~80 of 200 trailing positions for the remaining 58 seconds.
// 360 = 6 per tick * 600 ticks/min — matches actual processing capacity.
const int MAX_MODIFIES_PER_BATCH_PER_MIN  = 360;
const int MAX_GLOBAL_CONCURRENT_MODIFIES  = 6;
int g_InFlightModifies = 0;
const int MAX_CLOSES_PER_BATCH_PER_MIN    = 60;
const int MAX_GLOBAL_CONCURRENT_CLOSES    = 4;
int g_InFlightCloses   = 0;

//+------------------------------------------------------------------+
//| PANEL CONSTANTS                                                   |
//+------------------------------------------------------------------+
const string P            = "MOE_";
const string SL_PLACEHOLDER = "50";
const string TP_PLACEHOLDER = "100";
const int    PR           = 36;
const int    PT           = 45;
const int    PW           = 400;
const int    PH           = 700;
const int    PH_MIN       = 70;
const int    BUY_X        = 16;
const int    BUY_Y        = 366;
const int    BUY_W        = 176;
const int    BUY_H        = 42;
const int    SELL_X       = 208;
const int    SELL_Y       = 366;
const int    SELL_W       = 176;
const int    SELL_H       = 42;

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

int  g_PanelRight     = PR;
int  g_PanelTop       = PT;
bool g_PanelMinimized = false;
bool g_PanelDragging  = false;
int  g_DragOffsetX    = 0;
int  g_DragOffsetY    = 0;

int PX(int lx) { return g_PanelRight + PW - lx; }
int PY(int ly) { return g_PanelTop + ly; }

//+------------------------------------------------------------------+
//| OnInit                                                            |
//+------------------------------------------------------------------+
int OnInit()
  {
   g_IsDeinitializing = false;
   LoadPanelLayout();

   if((ENUM_ACCOUNT_MARGIN_MODE)AccountInfoInteger(ACCOUNT_MARGIN_MODE)
      != ACCOUNT_MARGIN_MODE_RETAIL_HEDGING)
     {
      Alert("ApexExecution: Requires a HEDGING account. EA stopped.");
      return INIT_FAILED;
     }

   g_PanelNumTrades = NumTrades;
   g_PanelLotSize   = LotSize;
   g_PanelSL        = StopLoss;
   g_PanelTP        = TakeProfit;

   ENUM_ORDER_TYPE_FILLING fill = DetectFillingMode();
   trade.SetDeviationInPoints(Slippage);
   trade.SetTypeFilling(fill);
   trade.SetAsyncMode(false);
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
      SetStatus("Trading disabled. EA stopped.", clrOrange);
      Alert("ApexExecution: Trading disabled. EA stopped.");
      return INIT_FAILED;
     }

   if(!EventSetMillisecondTimer(100))
      LogEvent("INIT_WARN", "Timer setup failed. Error="+IntegerToString(GetLastError()));

   Print("ApexExecution: Ready on ", Symbol());
   return INIT_SUCCEEDED;
  }

//+------------------------------------------------------------------+
//| OnDeinit                                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   g_IsDeinitializing = true;
   g_AbortFlag = true;
   SavePanelLayout();
   ChartSetInteger(0, CHART_EVENT_MOUSE_MOVE, false);
   EventKillTimer();
   DeletePanel();
   LogEvent("DEINIT", "Removed. Reason="+IntegerToString(reason));
  }

//+------------------------------------------------------------------+
//| OnTimer — 100ms heartbeat                                         |
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
      SetStatus("DISCONNECTED — positions unmonitored", clrRed);
      LogEvent("RECONNECT", "Connection lost");
     }
   else if(connected && !g_WasConnected)
     {
      g_WasConnected = true;
      SetStatus("Reconnected — re-syncing...", clrYellow);
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
                     g_RebuildNextAllowedMs = nowMs + (uint)(RebuildBackoffMs * RebuildMaxAttempts);
                     LogEvent("RECONNECT", "Rebuild deferred (max attempts)");
                    }
                  else
                    {
                     g_RebuildNextAllowedMs = nowMs + (uint)(RebuildBackoffMs * MathMax(1, g_RebuildAttempts));
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

         if(g_PendingReconnectResync && !g_PendingReconnectRebuild && nowMs >= g_ResyncNextAllowedMs)
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
                  g_ResyncNextAllowedMs = nowMs + (uint)(RebuildBackoffMs * RebuildMaxAttempts);
                  LogEvent("RECONNECT", "Resync deferred (max attempts)");
                 }
               else
                 {
                  g_ResyncNextAllowedMs = nowMs + (uint)(RebuildBackoffMs * MathMax(1, g_ResyncAttempts));
                  LogEvent("RECONNECT", "Skipped resync due to exec lock");
                 }
              }
           }
        }
     }

   //--- Drop closed batches before any management pass
   if(connected)
     {
      if(AcquireExecState(EXEC_RECOVERING))
        {
         CleanupOrphanBatches();
         CleanupPosStates();
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
   if(TrailingStop > 0.0 && ArraySize(g_Batches) > 0 && connected && !syncDidWork && !g_IsExecuting)
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

   //--- Process queued modify requests (throttled)
   if(connected && !g_IsExecuting)
     {
      if(AcquireExecState(EXEC_EXECUTING))
        {
         ProcessModifyQueue();
         ReleaseExecState(EXEC_EXECUTING);
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
         SetStatus("Trade operation in progress.", clrYellow);
         return;
        }
      if(sparam == P+"BUY")   ExecuteOrders(0);
      if(sparam == P+"SELL")  ExecuteOrders(1);
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
      if(lparam == 112) ExecuteOrders(0);
      if(lparam == 113) ExecuteOrders(1);
      if(lparam == 114) CloseSelectedBatch();
      if(lparam == 115) BreakevenSelectedBatch();
      if(lparam == 27)  g_AbortFlag = true;
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
//| ExecuteOrders — main BUY/SELL engine                             |
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
      SetStatus("Execution busy. Try again.", clrOrange);
      LogEvent("EXEC_SKIP", "Exec state busy");
      return;
     }
   if(!TerminalInfoInteger(TERMINAL_CONNECTED))
     {
      SetStatus("Disconnected. Execution blocked.", clrOrange);
      LogEvent("EXEC_FAIL", "Blocked execution while terminal disconnected");
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(!AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ||
      !TerminalInfoInteger(TERMINAL_TRADE_ALLOWED))
     {
      SetStatus("Trading disabled. Execution blocked.", clrOrange);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   long tradeMode = SymbolInfoInteger(Symbol(), SYMBOL_TRADE_MODE);
   if(tradeMode == SYMBOL_TRADE_MODE_DISABLED)
     {
      SetStatus("Symbol trading disabled. Execution blocked.", clrOrange);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode == SYMBOL_TRADE_MODE_LONGONLY && direction != 0)
     {
      SetStatus("Symbol LONGONLY. Sell blocked.", clrOrange);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode == SYMBOL_TRADE_MODE_SHORTONLY && direction == 0)
     {
      SetStatus("Symbol SHORTONLY. Buy blocked.", clrOrange);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   if(tradeMode != SYMBOL_TRADE_MODE_FULL &&
      tradeMode != SYMBOL_TRADE_MODE_LONGONLY &&
      tradeMode != SYMBOL_TRADE_MODE_SHORTONLY)
     {
      SetStatus("Symbol trade mode not FULL. Execution blocked.", clrOrange);
      ReleaseExecState(EXEC_EXECUTING);
      return;
     }
   g_IsExecuting = true;
   g_AbortFlag   = false;
   string dirStr = (direction == 0) ? "BUY" : "SELL";

   LockButtons();
   SetStatus("Checking pre-conditions...", clrYellow);

   //--- 1. SPREAD FILTER
   if(MaxSpread > 0.0)
     {
      double spd = (double)SymbolInfoInteger(Symbol(), SYMBOL_SPREAD)
                   * _Point / GetPipSize();
      if(spd > MaxSpread)
        {
         SetStatus("Spread "+DoubleToString(spd, 1)+" > "+
                   DoubleToString(MaxSpread, 1)+" pips. Aborted.", clrOrange);
         UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
        }
     }

   //--- 2. POSITION CEILING
   int openCount = PositionsTotal();
   int slots     = 200 - openCount;
   if(slots <= 0)
     {
      SetStatus("At 200-position limit. Aborted.", clrOrange);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
     }
   int numToOpen = g_PanelNumTrades;
   if(numToOpen > slots)
     {
      numToOpen = slots;
      SetStatus("Reduced to "+IntegerToString(slots)+" trades (ceiling)", clrYellow);
      Sleep(600);
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
      SetStatus("Invalid lot (symbol info unavailable).", clrOrange);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
     }

   //--- 4. MARGIN CHECK
   double marginPer = 0.0;
   ENUM_ORDER_TYPE oType = (direction == 0) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   double priceForCheck  = (direction == 0)
      ? SymbolInfoDouble(Symbol(), SYMBOL_ASK)
      : SymbolInfoDouble(Symbol(), SYMBOL_BID);
   if(!OrderCalcMargin(oType, Symbol(), lot, priceForCheck, marginPer) || marginPer <= 0.0)
     {
      SetStatus("Margin check failed. Aborted.", clrOrange);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
     }
   double required = marginPer * numToOpen * 1.2;
   if(AccountInfoDouble(ACCOUNT_MARGIN_FREE) < required)
     {
      SetStatus("Insufficient margin. Need $"+DoubleToString(required, 2), clrOrange);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
     }

   //--- 5. GENERATE BATCH ID
   long batchID = GenerateBatchMagic();

   BatchInfo batch;
   batch.magic     = batchID;
   batch.symbol    = Symbol();
   batch.direction = direction;
   batch.sl        = g_PanelSL;
   batch.tp        = g_PanelTP;
   batch.syncing   = false;
   batch.trailing  = (TrailingStop > 0.0);
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
      SetStatus("Batch registry failed. Aborted.", clrOrange);
      UnlockButtons(); g_IsExecuting = false; ReleaseExecState(EXEC_EXECUTING); return;
     }
   g_SelectedBatchIndex = FindBatchIndex(batchID);
   LogEvent("BATCH_CREATE", "magic="+IntegerToString(batchID)+
            " symbol="+Symbol()+" direction="+dirStr+
            " requested="+IntegerToString(numToOpen)+
            " lot="+DoubleToString(lot, 2));

   GlobalVariableSet("MOE_KNOWN_"+IntegerToString(batchID), 1.0);
   GlobalVariableSet("MOE_REQ_"+IntegerToString(batchID), (double)numToOpen);
   GlobalVariableSet("MOE_BATCH_ID",   (double)batchID);
   GlobalVariableSet("MOE_BATCH_SL",   g_PanelSL);
   GlobalVariableSet("MOE_BATCH_TP",   g_PanelTP);
   GlobalVariableSet("MOE_BATCH_DIR",  (double)direction);
   GlobalVariableSet("MOE_BATCH_LOTS", lot);

   //--- 6. EXECUTION LOOP
   int filled = 0;
   int errors = 0;
   double pip = GetPipSize();

   for(int i = 1; i <= numToOpen; i++)
     {
      if(g_AbortFlag)
        {
         Print("ApexExecution: Aborted by ESC at order ", i);
         break;
        }

      double price, sl_price, tp_price;
      if(direction == 0)
        {
         price    = SymbolInfoDouble(Symbol(), SYMBOL_ASK);
         sl_price = (g_PanelSL > 0)
                    ? NormalizeDouble(price - g_PanelSL * pip, _Digits) : 0;
         tp_price = (g_PanelTP > 0)
                    ? NormalizeDouble(price + g_PanelTP * pip, _Digits) : 0;
        }
      else
        {
         price    = SymbolInfoDouble(Symbol(), SYMBOL_BID);
         sl_price = (g_PanelSL > 0)
                    ? NormalizeDouble(price + g_PanelSL * pip, _Digits) : 0;
         tp_price = (g_PanelTP > 0)
                    ? NormalizeDouble(price - g_PanelTP * pip, _Digits) : 0;
        }

      sl_price = EnforceStopsLevel(Symbol(), price, sl_price, direction, false);
      tp_price = EnforceStopsLevel(Symbol(), price, tp_price, direction, true);

      string cmt = "MOE_"+IntegerToString(batchID)+"_"+IntegerToString(i);
      trade.SetExpertMagicNumber(batchID);

      double marginPer2 = 0.0;
      if(!OrderCalcMargin(oType, Symbol(), lot, price, marginPer2) ||
         AccountInfoDouble(ACCOUNT_MARGIN_FREE) < marginPer2 * 1.1)
        {
         errors++;
         LogEvent("EXEC_FAIL", "Insufficient margin mid-loop at order "+
                  IntegerToString(i)+" batch="+IntegerToString(batchID));
         break;
        }

      uint rc = 0;
      // NOTE: SL/TP are sent at order time; PostFillAttachSLTP is a safety net.
      bool sent = SendMarketOrderSafe(direction, lot, price, sl_price, tp_price, cmt, rc);

      if(sent)
        {
         filled++;
         if(filled == 1)
            UpdateBatchStops(batchID, sl_price, tp_price);
        }
      else if(rc == TRADE_RETCODE_NO_MONEY       ||
              rc == TRADE_RETCODE_LIMIT_ORDERS    ||
              rc == TRADE_RETCODE_TRADE_DISABLED  ||
              rc == TRADE_RETCODE_MARKET_CLOSED   ||
              rc == TRADE_RETCODE_CONNECTION)
        {
         errors++;
         LogEvent("EXEC_FAIL", "Fatal retcode="+IntegerToString((int)rc)+
                  " at order "+IntegerToString(i)+
                  " batch="+IntegerToString(batchID));
         break;
        }
      else
        {
         LogTradeFailure("ORDER", rc, 0, trade.ResultComment());
         errors++;
        }

      if(i % 5 == 0)
         SetStatus("Executing "+dirStr+": "+IntegerToString(i)+
                   "/"+IntegerToString(numToOpen), clrYellow);
     }

   //--- 7. POST-FILL SL/TP ATTACH
   if(g_PanelSL > 0.0 || g_PanelTP > 0.0)
      PostFillAttachSLTP(batchID, direction);

   int batchIndex = FindBatchIndex(batchID);
   if(batchIndex >= 0)
     {
      g_Batches[batchIndex].filled  = filled;
      g_Batches[batchIndex].errors  = errors;
      g_Batches[batchIndex].partial = (filled > 0 &&
                                       filled < g_Batches[batchIndex].requested);
      if(filled <= 0)
         RemoveBatch(batchID);
     }

   string summary = dirStr+": "+IntegerToString(filled)+
                    "/"+IntegerToString(numToOpen)+" filled";
   if(errors > 0)
      summary += " ("+IntegerToString(errors)+" errors)";

   SetStatus(summary, (errors == 0) ? clrLime : clrYellow);
   LogEvent("EXEC_SUMMARY", summary);

   UnlockButtons();
   g_IsExecuting = false;
   ReleaseExecState(EXEC_EXECUTING);
  }

//+------------------------------------------------------------------+
//| PostFillAttachSLTP — safety net for market execution mode        |
//|                                                                   |
//| FIX: Removed premature UpsertPosState and UpdateBatchStops calls |
//| after EnqueueModifyRequest (blocking=false). Those calls updated  |
//| PosState and batch.sl/tp before broker confirmation, which caused |
//| BackupSyncCheck to skip repair for positions that hadn't yet had  |
//| their SL/TP confirmed. ModifyPositionSafe's success path already  |
//| handles UpsertPosState and UpdateBatchStops on confirmed modify.  |
//+------------------------------------------------------------------+
void PostFillAttachSLTP(long batchID, int direction)
  {
   Sleep(250);
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
      // Enqueue only — do NOT call UpsertPosState or UpdateBatchStops here.
      // ModifyPositionSafe's confirmed success path handles both updates.
      // Premature state updates before broker confirmation caused
      // BackupSyncCheck to suppress repair for unconfirmed positions.
      ModifyPositionQueued(posInfo.Ticket(), newSL, newTP, rc, "POST_ATTACH", false);
     }

   batchIndex = FindBatchIndex(batchID);
   if(batchIndex >= 0)
      g_Batches[batchIndex].syncing = false;
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
         if(ModifyPositionQueued(posInfo.Ticket(), applySL, applyTP, rc, "SYNC", false))
            synced++;
        }
     }

   if(synced > 0)
      UpdateBatchStops(magic, newSL, newTP);

   LogEvent("SYNC", "batch="+IntegerToString(magic)+
            " positions="+IntegerToString(synced)+
            " SL="+DoubleToString(newSL, _Digits)+
            " TP="+DoubleToString(newTP, _Digits));
   SetStatus("Synced SL/TP to "+IntegerToString(synced)+" positions", clrLime);
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
//| Batch navigation                                                  |
//+------------------------------------------------------------------+
void SelectPreviousBatch()
  {
   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size == 0)
     { SetStatus("No active batches.", clrOrange); RefreshPanel(); return; }

   g_SelectedBatchIndex--;
   if(g_SelectedBatchIndex < 0)
      g_SelectedBatchIndex = size - 1;

   SetStatus("Selected batch "+IntegerToString(g_Batches[g_SelectedBatchIndex].magic),
             clrLime);
   RefreshPanel();
  }

void SelectNextBatch()
  {
   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size == 0)
     { SetStatus("No active batches.", clrOrange); RefreshPanel(); return; }

   g_SelectedBatchIndex++;
   if(g_SelectedBatchIndex >= size)
      g_SelectedBatchIndex = 0;

   SetStatus("Selected batch "+IntegerToString(g_Batches[g_SelectedBatchIndex].magic),
             clrLime);
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
     { SetStatus("No active batch selected.", clrOrange); RefreshPanel(); return; }
   CloseAllBatch(magic);
   EnsureSelectedBatch();
   RefreshPanel();
  }

void BreakevenSelectedBatch()
  {
   g_AbortFlag = false;
   long magic = GetSelectedBatchMagic();
   if(magic == 0)
     { SetStatus("No active batch selected.", clrOrange); RefreshPanel(); return; }
   int done = BreakevenBatch(magic);
   SetStatus("Breakeven set on "+IntegerToString(done)+" positions", clrLime);
   RefreshPanel();
  }

void PartialCloseSelectedBatch()
  {
   g_AbortFlag = false;
   long magic = GetSelectedBatchMagic();
   if(magic == 0)
     { SetStatus("No active batch selected.", clrOrange); RefreshPanel(); return; }
   int done = PartialCloseBatch(magic);
   SetStatus("Partial close on "+IntegerToString(done)+" positions", clrLime);
   RefreshPanel();
  }

//+------------------------------------------------------------------+
//| CloseAllBatch — close only one target batch                      |
//+------------------------------------------------------------------+
bool CloseAllBatch(long magic)
  {
   bool held = false;
   if(g_ExecState != EXEC_CLOSING)
     {
      if(!AcquireExecState(EXEC_CLOSING))
        {
         SetStatus("Close already in progress.", clrOrange);
         LogEvent("CLOSE_SKIP", "cannot acquire closing lock for batch="+IntegerToString(magic));
         g_CloseSkipCount++;
         return false;
        }
      held = true;
     }

   int batchIndex = FindBatchIndex(magic);
   if(batchIndex < 0 || !g_Batches[batchIndex].active)
     {
      SetStatus("No active batch to close.", clrOrange);
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

   string msg = "Closed "+IntegerToString(closed)+" from batch "+IntegerToString(magic);
   if(failed > 0) msg += " | Failed: "+IntegerToString(failed);
   SetStatus(msg, (failed == 0) ? clrLime : clrOrange);
   LogEvent("CLOSE", msg);
   if(held) ReleaseExecState(EXEC_CLOSING);
   return (failed == 0);
  }

//+------------------------------------------------------------------+
//| BreakevenBatch — move SL to breakeven for one target batch       |
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
            if(ModifyPositionQueued(posInfo.Ticket(), beSL, curTP, rc, "BREAKEVEN", false))
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
            if(ModifyPositionQueued(posInfo.Ticket(), beSL, curTP, rc, "BREAKEVEN", false))
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
//| PartialCloseBatch — partially close one target batch             |
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

   // FIX 1: Release exec-state before this early return.
   if(vStep <= 0.0 || vMin <= 0.0)
     {
      LogEvent("EXEC_FAIL", "Invalid volume settings for "+batchSymbol+
               " in partial close");
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
//| ProcessTrailing — trail SL for every registered batch            |
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
//| ProcessTrailingBatch — trail SL for one target batch             |
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
         if(curSL == 0.0 || newTrail > curSL + trailStep)
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), newTrail, curTP, rc, "TRAILING", false))
               modified = true;
           }
        }
      else
        {
         double ask      = SymbolInfoDouble(batchSymbol, SYMBOL_ASK);
         double newTrail = NormalizeDouble(ask + trailDist, _Digits);
         newTrail = EnforceStopsLevel(batchSymbol, ask, newTrail, 1, false);
         if(curSL == 0.0 || newTrail < curSL - trailStep)
           {
            uint rc = 0;
            if(ModifyPositionQueued(posInfo.Ticket(), newTrail, curTP, rc, "TRAILING", false))
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
//| HELPER FUNCTIONS                                                  |
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
         g_RetryStates[idx].cooldownMs = (uint)(g_RetryStates[idx].cooldownMs * RETRY_ESCALATION_FACTOR);
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
   if(IsBusyRetcode(rc))
      delay += 80 + attempt * 70;
   if(IsPriceRetcode(rc))
      delay += 20 + attempt * 25;
   if(!TerminalInfoInteger(TERMINAL_CONNECTED))
      delay += 250;
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
      g_ModifyQueue[idx].sl             = sl;
      g_ModifyQueue[idx].tp             = tp;
      g_ModifyQueue[idx].context        = context;
      g_ModifyQueue[idx].requestedAtMs  = now;
      return true;
     }

   ModifyRequest req;
   req.ticket         = ticket;
   req.sl             = sl;
   req.tp             = tp;
   req.context        = context;
   req.rc             = 0;
   req.status         = 0;
   req.requestedAtMs  = now;
   int s = ArraySize(g_ModifyQueue);
   if(ArrayResize(g_ModifyQueue, s+1) != s+1)
      return false;
   g_ModifyQueue[s] = req;
   g_ModifyQueuedCount++;
   return true;
  }

//+------------------------------------------------------------------+
//| RemoveModifyQueueItem — remove item at index from queue,         |
//| keeping the skipped[] sentinel array synchronized.               |
//+------------------------------------------------------------------+
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

//+------------------------------------------------------------------+
//| ProcessModifyQueue — throttled modify worker                     |
//|                                                                   |
//| FIX 2: Rewritten as while loop with skipped[] sentinel.          |
//| FIX 3: modifyCount incremented ONLY on successful modify.        |
//+------------------------------------------------------------------+
void ProcessModifyQueue()
  {
   const int capacity = 6; // max successful modifies dispatched per timer tick
   int processed = 0;
   if(ArraySize(g_ModifyQueue) == 0) return;

   bool skipped[];
   ArrayResize(skipped, ArraySize(g_ModifyQueue));
   ArrayInitialize(skipped, false);

   while(processed < capacity)
     {
      int qsize = ArraySize(g_ModifyQueue);
      if(qsize == 0) break;

      // Find the oldest non-skipped request
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
         // FIX 3: Only count against rate limit on success.
         if(bidx >= 0) g_Batches[bidx].modifyCount++;
        }
      else
        {
         g_ModifyFailCount++;
        }

      RemoveModifyQueueItem(oldest, skipped);
      processed++;
     }
  }

//--- Public API: queued modify with optional blocking ---------------
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
   uint timeout = (g_ExecState == EXEC_EXECUTING)
                  ? (uint)EXEC_EXECUTING_TIMEOUT_MS
                  : (uint)EXEC_STATE_TIMEOUT_MS;
   if(g_ExecState == expected ||
      (g_ExecStateStartMs > 0 && now - g_ExecStateStartMs > timeout))
     {
      g_ExecState        = EXEC_IDLE;
      g_ExecStateStartMs = 0;
     }
  }

string ExecStateText(int state)
  {
   if(state == EXEC_EXECUTING) return "EXECUTING";
   if(state == EXEC_SYNCING)   return "SYNCING";
   if(state == EXEC_TRAILING)  return "TRAILING";
   if(state == EXEC_CLOSING)   return "CLOSING";
   if(state == EXEC_RECOVERING)return "RECOVERING";
   if(state == EXEC_REBUILDING)return "REBUILDING";
   return "IDLE";
  }

void DiagnosticsTick()
  {
   if(!EnableDiagnostics) return;
   uint now = GetTickCount();
   if(g_LastDiagMs > 0 && now - g_LastDiagMs < (uint)DiagnosticsIntervalMs)
      return;

   g_LastDiagMs = now;
   int qsize = ArraySize(g_ModifyQueue);
   string state = ExecStateText(g_ExecState);
   LogEvent("DIAG", "state="+state+
            " q="+IntegerToString(qsize)+
            " inflightM="+IntegerToString(g_InFlightModifies)+
            " inflightC="+IntegerToString(g_InFlightCloses)+
            " modOk="+IntegerToString(g_ModifySuccessCount)+
            " modFail="+IntegerToString(g_ModifyFailCount)+
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
   ml.ticket      = ticket;
   ml.unlockAtMs  = now + MODIFY_COOLDOWN_MS;
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
      LogEvent("MOD_SKIP", context+" ticket="+IntegerToString((long)ticket)+
               " not-found");
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
         LogEvent("MOD_NOP", context+" ticket="+IntegerToString((long)ticket)+
                  " no-change");
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
            int dir    = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? 0 : 1;
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

bool ClosePositionSafe(ulong ticket, uint &rc, string context)
  {
   rc = 0;
   if(g_InFlightCloses >= MAX_GLOBAL_CONCURRENT_CLOSES)
     {
      rc = TRADE_RETCODE_TOO_MANY_REQUESTS;
      LogEvent("FLOOD_CLOSE_GLOBAL", context+" ticket="+IntegerToString((long)ticket));
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

   if(IsUnderRetryCooldown(ticket))
     {
      rc = TRADE_RETCODE_LOCKED;
      LogEvent("CLOSE_COOLDOWN", context+" ticket="+IntegerToString((long)ticket));
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

//+------------------------------------------------------------------+
//| ClosePositionPartialSafe                                          |
//|                                                                   |
//| FIX: Added IsUnderRetryCooldown check to match ClosePositionSafe.|
//| Previously missing, allowing cooling-down tickets to be hammered  |
//| by partial close retries without per-ticket cooldown enforcement. |
//+------------------------------------------------------------------+
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

   // FIX Priority 4: Added retry cooldown check — consistent with ClosePositionSafe.
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

//--- Keep selected batch index valid after registry changes ---------
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

      int posDir     = (posInfo.PositionType() == POSITION_TYPE_BUY) ? 0 : 1;
      int batchIndex = FindBatchIndex(magic);
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
         batch.trailing  = (TrailingStop > 0.0);
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

      ulong ticket   = posInfo.Ticket();
      int stateIdx   = FindPosStateIndex(ticket);
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
   RemovePosStatesByMagic(magic);
   int size = ArraySize(g_Batches);
   for(int i = index; i < size - 1; i++)
      g_Batches[i] = g_Batches[i+1];
   ArrayResize(g_Batches, size - 1);
   EnsureSelectedBatch();
   LogEvent("BATCH_REMOVE", "magic="+IntegerToString(magic));
   return true;
  }

long GenerateBatchMagic()
  {
   static long counter = 0;
   long magic = 0;
   long salt = (long)(AccountInfoInteger(ACCOUNT_LOGIN) % 1000);
   do
     {
      counter++;
      magic = MagicBase + (salt * 1000000000) + ((long)TimeCurrent() * 1000) + counter;
     }
   while(BatchExists(magic) || MagicInOpenPositions(magic));
   return magic;
  }

int CountBatchPositions(long magic)
  {
   int n = 0;
   for(int i = PositionsTotal()-1; i >= 0; i--)
     {
      if(!posInfo.SelectByIndex(i)) continue;
      if(posInfo.Magic() == magic) n++;
     }
   return n;
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
   string symbol    = PositionGetString(POSITION_SYMBOL);
   long freezeLvl   = SymbolInfoInteger(symbol, SYMBOL_TRADE_FREEZE_LEVEL);
   if(freezeLvl <= 0) return false;
   double point     = SymbolInfoDouble(symbol, SYMBOL_POINT);
   double minDist   = freezeLvl * point;
   int posType      = (int)PositionGetInteger(POSITION_TYPE);
   double price     = (posType == POSITION_TYPE_BUY)
                      ? SymbolInfoDouble(symbol, SYMBOL_BID)
                      : SymbolInfoDouble(symbol, SYMBOL_ASK);
   double posSL     = PositionGetDouble(POSITION_SL);
   double posTP     = PositionGetDouble(POSITION_TP);
   bool slFrozen    = ((posSL > 0.0 && MathAbs(price - posSL) < minDist) ||
                       (newSL > 0.0 && MathAbs(price - newSL) < minDist));
   bool tpFrozen    = ((posTP > 0.0 && MathAbs(price - posTP) < minDist) ||
                       (newTP > 0.0 && MathAbs(price - newTP) < minDist));
   return (slFrozen || tpFrozen);
  }

double EnforceStopsLevel(string symbol, double price, double level, int dir,
                         bool isTP)
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

//+------------------------------------------------------------------+
//| DetectFillingMode                                                 |
//|                                                                   |
//| FIX C-8: Removed orphaned code block that was accidentally       |
//| copy-pasted here during Phase 6-C implementation. The block      |
//| referenced undeclared identifiers magic/sym/sl/tp/dir, causing   |
//| 8 compilation errors. The correct implementation of that logic   |
//| already exists in ModifyPositionSafe's success path.             |
//+------------------------------------------------------------------+
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
   string txt = (value <= 0.0) ? placeholder : DoubleToString(value, 0);
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

bool IsMouseOverObject(string name, int x, int y)
  {
   if(ObjectFind(0, name) < 0) return false;
   long corner = ObjectGetInteger(0, name, OBJPROP_CORNER);
   long xdist  = ObjectGetInteger(0, name, OBJPROP_XDISTANCE);
   long ydist  = ObjectGetInteger(0, name, OBJPROP_YDISTANCE);
   long w      = ObjectGetInteger(0, name, OBJPROP_XSIZE);
   long h      = ObjectGetInteger(0, name, OBJPROP_YSIZE);
   int left = (int)xdist;
   int top  = (int)ydist;
   if(corner == CORNER_RIGHT_UPPER)
      left = ChartWidthPixels() - (int)xdist - (int)w;
   return (x >= left && x <= left + (int)w && y >= top && y <= top + (int)h);
  }

void UpdateHoverState(int x, int y)
  {
   if(g_IsExecuting || g_PanelMinimized) return;
   if(ObjectFind(0, P+"BUY") < 0 || ObjectFind(0, P+"SELL") < 0) return;

   bool overBuy  = IsMouseOverPanelRect(x, y, BUY_X,  BUY_Y,  BUY_W,  BUY_H);
   bool overSell = IsMouseOverPanelRect(x, y, SELL_X, SELL_Y, SELL_W, SELL_H);
   bool changed  = false;

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
  }

void SavePanelLayout()
  {
   GlobalVariableSet("MOE_PANEL_RIGHT", (double)g_PanelRight);
   GlobalVariableSet("MOE_PANEL_TOP",   (double)g_PanelTop);
   GlobalVariableSet("MOE_PANEL_MIN",   g_PanelMinimized ? 1.0 : 0.0);
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
      ObjectSetString(0,  P+"STATUS", OBJPROP_TEXT,  FitPanelText(msg, 38));
      ObjectSetInteger(0, P+"STATUS", OBJPROP_COLOR, clr);
      ChartRedraw();
     }
   Print("MOE_STATUS: ", msg);
  }

void RefreshPanel()
  {
   if(ObjectFind(0, P+"SPREAD") < 0) return;

   double spdPips = (double)SymbolInfoInteger(Symbol(), SYMBOL_SPREAD)
                    * _Point / GetPipSize();
   color spdClr   = (MaxSpread > 0 && spdPips > MaxSpread) ? clrOrange : clrLime;
   ObjectSetString(0,  P+"SPREAD", OBJPROP_TEXT,
                   "Spread  "+DoubleToString(spdPips, 1)+" pips");
   ObjectSetInteger(0, P+"SPREAD", OBJPROP_COLOR, spdClr);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   double eq  = AccountInfoDouble(ACCOUNT_EQUITY);
   ObjectSetString(0, P+"BAL", OBJPROP_TEXT,
                   "Bal $"+DoubleToString(bal, 2)+"   Eq $"+DoubleToString(eq, 2));

   int   pos    = PositionsTotal();
   color posClr = (pos >= 190) ? clrOrange : CLR_MUTED;
   ObjectSetString(0,  P+"POS",  OBJPROP_TEXT,
                   "Positions  "+IntegerToString(pos)+" / 200");
   ObjectSetInteger(0, P+"POS",  OBJPROP_COLOR, posClr);

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

void RefreshBatchPanel()
  {
   if(ObjectFind(0, P+"BSEL") < 0) return;

   EnsureSelectedBatch();
   int size = ArraySize(g_Batches);
   if(size <= 0 || g_SelectedBatchIndex < 0 || g_SelectedBatchIndex >= size)
     {
      ObjectSetString(0,  P+"BSEL",  OBJPROP_TEXT,  "No Active Batches");
      ObjectSetInteger(0, P+"BSEL",  OBJPROP_COLOR, clrOrange);
      ObjectSetString(0,  P+"BMETA", OBJPROP_TEXT,  "Symbol --   Pos 0");
      ObjectSetString(0,  P+"BPNL",  OBJPROP_TEXT,  "PnL --");
      ObjectSetInteger(0, P+"BMETA", OBJPROP_COLOR, CLR_MUTED);
      ObjectSetInteger(0, P+"BPNL",  OBJPROP_COLOR, CLR_MUTED);
      SetManagementButtonsEnabled(false);
      return;
     }

   BatchInfo batch    = g_Batches[g_SelectedBatchIndex];
   int positions      = CountBatchPositions(batch.magic, batch.symbol);
   double pnl         = BatchFloatingPnl(batch.magic, batch.symbol);
   color pnlClr       = (pnl >= 0.0) ? clrLime : clrOrange;

   ObjectSetString(0,  P+"BSEL", OBJPROP_TEXT,
                   "Batch "+IntegerToString(g_SelectedBatchIndex+1)+
                   "/"+IntegerToString(size)+"   #"+ShortMagic(batch.magic));
   ObjectSetInteger(0, P+"BSEL", OBJPROP_COLOR, CLR_TEXT);
   ObjectSetString(0,  P+"BMETA", OBJPROP_TEXT,
                   batch.symbol+"   "+BatchDirectionText(batch.direction)+
                   "   Pos "+IntegerToString(positions));
   ObjectSetInteger(0, P+"BMETA", OBJPROP_COLOR, CLR_MUTED);
   ObjectSetString(0,  P+"BPNL", OBJPROP_TEXT,
                   "PnL $"+DoubleToString(pnl, 2));
   ObjectSetInteger(0, P+"BPNL", OBJPROP_COLOR, pnlClr);
   SetManagementButtonsEnabled(true);
  }

void MakeLabel(string name, int lx, int ly, string txt, color clr, int sz, bool bold)
  {
   if(ObjectFind(0, name) >= 0) ObjectDelete(0, name);
   ObjectCreate(0, name, OBJ_LABEL, 0, 0, 0);
   ObjectSetInteger(0, name, OBJPROP_CORNER,    CORNER_RIGHT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE, PX(lx));
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE, PY(ly));
   ObjectSetString(0,  name, OBJPROP_TEXT,      txt);
   ObjectSetInteger(0, name, OBJPROP_COLOR,     clr);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE,  sz);
   ObjectSetString(0,  name, OBJPROP_FONT,      bold ? "Tahoma Bold" : "Tahoma");
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE,false);
   ObjectSetInteger(0, name, OBJPROP_BACK,      false);
  }

void MakeText(string name, int lx, int ly, string txt, color clr, int sz, bool bold)
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

void MakeRect(string name, int lx, int ly, int w, int h, color bg, color border)
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
//| CreatePanel — build the full UI                                   |
//+------------------------------------------------------------------+
void CreatePanel()
  {
   MakeRect(P+"BG", 0, 0, PW, g_PanelMinimized ? PH_MIN : PH, CLR_BG, CLR_BORDER);
   MakePanelDragHandle(P+"BG");

   MakeRect(P+"TBAR", 0, 0, PW, PH_MIN, CLR_SURFACE, CLR_BORDER);
   MakePanelDragHandle(P+"TBAR");
   MakeText(P+"TITLE", 16, 12, "APEX EXECUTION", CLR_TEXT, 12, true);
   MakePanelDragHandle(P+"TITLE");
   MakeText(P+"TSYM",  16, 43, Symbol()+"  "+EnumToString((ENUM_TIMEFRAMES)Period()),
            CLR_MUTED, 8, true);
   MakePanelDragHandle(P+"TSYM");
   MakeButton(P+"MIN", 354, 18, 30, 28, g_PanelMinimized ? "+" : "-", CLR_SURFACE, CLR_TEXT);

   if(g_PanelMinimized)
     {
      ChartRedraw();
      return;
     }

   MakeRect(P+"LIVE", 12, 84, PW-24, 76, C'18,21,30', CLR_DIVIDER);
   MakeText(P+"SPREAD", 22,  98, "Spread  --",     clrLime,  9, true);
   MakeText(P+"BAL",    22, 126, "Bal --",          CLR_MUTED, 8, false);
   MakeText(P+"POS",    22, 146, "Positions  0 / 200", CLR_MUTED, 8, false);

   MakeRect(P+"DIV1", 12, 174, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeText(P+"SEC1", 16, 188, "ORDER SETUP", CLR_SUBTLE, 8, true);

   MakeText(P+"L_NUM", 16,  214, "TRADES",           CLR_MUTED, 7, true);
   MakeEdit(P+"E_NUM", 16,  240, 170, IntegerToString(g_PanelNumTrades));
   MakeText(P+"L_LOT", 214, 214, "LOTS",             CLR_MUTED, 7, true);
   MakeEdit(P+"E_LOT", 214, 240, 170, DoubleToString(g_PanelLotSize, 2));
   MakeText(P+"L_SL",  16,  278, "STOP LOSS PIPS",   CLR_MUTED, 7, true);
   MakeEdit(P+"E_SL",  16,  304, 170, "");
   ApplyOptionalEditValue(P+"E_SL", g_PanelSL, SL_PLACEHOLDER);
   MakeText(P+"L_TP",  214, 278, "TAKE PROFIT PIPS", CLR_MUTED, 7, true);
   MakeEdit(P+"E_TP",  214, 304, 170, "");
   ApplyOptionalEditValue(P+"E_TP", g_PanelTP, TP_PLACEHOLDER);
   MakeText(P+"H_SLTP", 16, 336, "Blank = No SL/TP", CLR_MUTED, 7, false);

   MakeRect(P+"DIV2", 12, 350, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeButton(P+"BUY",  BUY_X,  BUY_Y,  BUY_W,  BUY_H,  "BUY  F1",  CLR_BUY,  clrWhite);
   MakeButton(P+"SELL", SELL_X, SELL_Y, SELL_W, SELL_H, "SELL  F2", CLR_SELL, clrWhite);

   MakeButton(P+"CLOSE", 16, 422, 368, 34, "CLOSE SELECTED  F3",       CLR_CLOSE, clrWhite);
   MakeButton(P+"BE",    16, 464, 368, 34, "MOVE TO BREAKEVEN  F4",    CLR_BE,    clrWhite);
   MakeButton(P+"PART",  16, 506, 368, 34,
              "PARTIAL CLOSE  "+DoubleToString(PartialClosePct, 0)+"%",
              CLR_PART, clrWhite);

   MakeRect(P+"DIV3", 12, 556, PW-24, 1, CLR_DIVIDER, CLR_DIVIDER);

   MakeText(P+"SEC2",  16, 572, "ACTIVE BATCH",         CLR_SUBTLE, 8, true);
   MakeButton(P+"BPREV", 16,  602, 42, 28, "<", CLR_SURFACE, CLR_TEXT);
   MakeButton(P+"BNEXT", 342, 602, 42, 28, ">", CLR_SURFACE, CLR_TEXT);
   MakeText(P+"BSEL",  70, 607, "No Active Batches",    clrOrange,  8, true);
   MakeText(P+"BMETA", 16, 640, "Symbol --   Pos 0",    CLR_MUTED,  8, false);
   MakeText(P+"BPNL",  240, 640, "PnL --",              CLR_MUTED,  8, true);

   MakeRect(P+"SBAR",   0, 664, PW,  44, CLR_SURFACE, CLR_BORDER);
   MakeText(P+"SLAB",  16, 672, "STATUS", CLR_SUBTLE, 7, true);
   MakeText(P+"STATUS",16, 690, "Ready",  clrLime,    9, true);

   RefreshBatchPanel();
   ChartRedraw();
  }

//+------------------------------------------------------------------+
//| DeletePanel — remove all panel objects from chart                |
//+------------------------------------------------------------------+
void DeletePanel()
  {
   ObjectsDeleteAll(0, P, 0, -1);
   ChartRedraw();
  }

//+------------------------------------------------------------------+