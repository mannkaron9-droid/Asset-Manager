import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Activity, Play, CheckCircle2, Clock } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useTriggerBot } from "@/hooks/use-betting";
import { useToast } from "@/hooks/use-toast";
import type { BotStatus, BotRunResult, Bet } from "@workspace/api-client-react";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const TYPE_STYLES: Record<string, string> = {
  MONEYLINE: "bg-blue-500/10 text-blue-400 border-blue-500/30",
  OVER:      "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  UNDER:     "bg-orange-500/10 text-orange-400 border-orange-500/30",
  SPREAD:    "bg-purple-500/10 text-purple-400 border-purple-500/30",
  POINTS:    "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  points:    "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  REBOUNDS:  "bg-sky-500/10 text-sky-400 border-sky-500/30",
  rebounds:  "bg-sky-500/10 text-sky-400 border-sky-500/30",
  ASSISTS:   "bg-rose-500/10 text-rose-400 border-rose-500/30",
  assists:   "bg-rose-500/10 text-rose-400 border-rose-500/30",
  THREES:    "bg-orange-500/10 text-orange-400 border-orange-500/30",
  threes:    "bg-orange-500/10 text-orange-400 border-orange-500/30",
};

const STAT_LABELS: Record<string, string> = {
  points: "PTS", rebounds: "REB", assists: "AST", threes: "3PM",
  player_points: "PTS", player_rebounds: "REB", player_assists: "AST", player_threes: "3PM",
};

function parsePick(pick: string, line: number | null | undefined): string {
  const parts = pick.split("|");
  const dir  = parts[0]?.trim() ?? pick;
  const stat = parts[1] ? (STAT_LABELS[parts[1].trim()] ?? parts[1].toUpperCase()) : "";
  const lineStr = line != null ? ` ${line}` : "";
  return stat ? `${dir}${lineStr} ${stat}` : `${dir}${lineStr}`;
}

function formatEdge(val: number) {
  const pct = val <= 1 ? val * 100 : val;
  return `${pct.toFixed(1)}%`;
}

function PickCard({ pick, idx }: { pick: Bet; idx: number }) {
  const type = pick.betType ?? "TOTAL";
  const typeStyle = TYPE_STYLES[type] ?? TYPE_STYLES["OVER"];

  const title  = (pick as any).player ?? pick.game;
  const sub    = (pick as any).player ? pick.game : null;
  const pickLine = parsePick(pick.pick, pick.line);

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: idx * 0.07 }}
      className="rounded-xl border border-white/8 bg-background/40 p-4 space-y-2"
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex-1 min-w-0">
          <p className="text-xs font-semibold text-foreground truncate">{title}</p>
          {sub && <p className="text-[10px] text-muted-foreground truncate">{sub}</p>}
        </div>
        <span className={cn("shrink-0 px-2 py-0.5 rounded text-xs font-bold border uppercase tracking-wider", typeStyle)}>
          {type}
        </span>
      </div>

      <div className="flex items-center gap-2 flex-wrap">
        <span className="font-bold text-foreground text-sm">{pickLine}</span>
      </div>

      <div className="space-y-0.5 text-xs text-muted-foreground">
        {pick.bet_size != null && (
          <p>💰 Bet: <span className="text-foreground font-semibold">${pick.bet_size}</span></p>
        )}
        {pick.sharp != null && (
          <p>{pick.sharp}</p>
        )}
        {pick.movement != null && (
          <p>Line Move: <span className="text-foreground font-semibold">{pick.movement}</span></p>
        )}
        {pick.confidence != null && (
          <p>Confidence: <span className={cn(
            "font-bold",
            pick.confidence >= 70 ? "text-emerald-400" :
            pick.confidence >= 40 ? "text-yellow-400" : "text-foreground"
          )}>{pick.confidence}%</span></p>
        )}
      </div>
    </motion.div>
  );
}

interface BotControlPanelProps {
  status?: BotStatus;
}

export function BotControlPanel({ status }: BotControlPanelProps) {
  const { mutate: runBot, isPending } = useTriggerBot();
  const { toast } = useToast();
  const [lastResult, setLastResult] = useState<BotRunResult | null>(null);

  const handleRun = () => {
    setLastResult(null);
    runBot(undefined, {
      onSuccess: (res) => {
        setLastResult(res);
        toast({
          title: res.success ? "Scan Complete" : "Bot Run Failed",
          description: res.message,
          variant: res.success ? "default" : "destructive",
        });
      },
      onError: (err: any) => {
        toast({
          title: "Error Triggering Bot",
          description: err.message || "An unexpected error occurred",
          variant: "destructive",
        });
      },
    });
  };

  const isRunning = status?.running || isPending;
  const lastRunText = status?.lastRun
    ? formatDistanceToNow(new Date(status.lastRun), { addSuffix: true })
    : "Never";

  return (
    <motion.div
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.6, delay: 0.4 }}
      className="glass-panel p-6 rounded-2xl flex flex-col gap-6"
    >
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h3 className="text-xl font-display font-bold uppercase tracking-wider text-foreground">
            System Engine
          </h3>
          <p className="text-sm text-muted-foreground mt-1">Status &amp; Controls</p>
        </div>
        <div
          className={cn(
            "p-2 rounded-full border",
            isRunning
              ? "bg-primary/20 border-primary text-primary animate-pulse"
              : "bg-muted border-white/10 text-muted-foreground"
          )}
        >
          <Activity className="w-5 h-5" />
        </div>
      </div>

      {/* Status rows */}
      <div className="space-y-3">
        <div className="flex items-center justify-between p-3 rounded-xl bg-background/50 border border-white/5">
          <div className="flex items-center space-x-3">
            <div className={cn("w-2 h-2 rounded-full", isRunning ? "bg-primary animate-ping" : "bg-green-500")} />
            <span className="text-sm font-medium">Engine Status</span>
          </div>
          <span className="text-sm text-muted-foreground font-display tracking-wide uppercase">
            {isRunning ? "Analyzing..." : "Standby"}
          </span>
        </div>

        <div className="flex items-center justify-between p-3 rounded-xl bg-background/50 border border-white/5">
          <div className="flex items-center space-x-3">
            <Clock className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm font-medium">Last Scan</span>
          </div>
          <span className="text-sm text-muted-foreground font-display">{lastRunText}</span>
        </div>

        <div className="flex items-center justify-between p-3 rounded-xl bg-background/50 border border-white/5">
          <div className="flex items-center space-x-3">
            <CheckCircle2 className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm font-medium">Picks Found Today</span>
          </div>
          <span className="text-lg font-bold font-display text-foreground">
            {status?.picksToday ?? 0}
          </span>
        </div>
      </div>

      {/* Run button */}
      <button
        onClick={handleRun}
        disabled={isRunning}
        className="w-full relative group overflow-hidden rounded-xl font-display font-bold tracking-wider uppercase disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-300"
      >
        <div className="absolute inset-0 bg-gradient-to-r from-primary to-blue-600 opacity-80 group-hover:opacity-100 transition-opacity" />
        <div className="relative px-6 py-4 flex items-center justify-center space-x-2 text-primary-foreground">
          {isRunning ? (
            <>
              <Activity className="w-5 h-5 animate-spin" />
              <span>Scanning Markets...</span>
            </>
          ) : (
            <>
              <Play className="w-5 h-5 fill-current" />
              <span>Run Bot Now</span>
            </>
          )}
        </div>
      </button>

      {/* Picks from last run */}
      <AnimatePresence>
        {lastResult && (
          <motion.div
            key="picks-result"
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="overflow-hidden"
          >
            <div className="pt-2 border-t border-white/8 space-y-3">
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-display font-bold uppercase tracking-wider text-muted-foreground">
                    This Run
                  </p>
                  <span className="text-xs text-muted-foreground">{lastResult.message}</span>
                </div>

                {/* Run stats: count / win_rate / roi */}
                <div className="grid grid-cols-3 gap-2">
                  <div className="flex flex-col items-center py-2 px-3 rounded-lg bg-background/50 border border-white/5">
                    <span className="text-lg font-bold font-display text-foreground">{lastResult.count}</span>
                    <span className="text-[10px] text-muted-foreground uppercase tracking-widest font-display">Picks</span>
                  </div>
                  <div className="flex flex-col items-center py-2 px-3 rounded-lg bg-background/50 border border-white/5">
                    <span className="text-lg font-bold font-display text-emerald-400">{lastResult.win_rate}%</span>
                    <span className="text-[10px] text-muted-foreground uppercase tracking-widest font-display">Win Rate</span>
                  </div>
                  <div className="flex flex-col items-center py-2 px-3 rounded-lg bg-background/50 border border-white/5">
                    <span className="text-lg font-bold font-display text-primary">+{lastResult.roi}u</span>
                    <span className="text-[10px] text-muted-foreground uppercase tracking-widest font-display">ROI</span>
                  </div>
                </div>
              </div>

              {lastResult.picks.length === 0 ? (
                <p className="text-sm text-muted-foreground text-center py-3">
                  No new edges found this scan.
                </p>
              ) : (
                <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
                  {lastResult.picks.map((pick, i) => (
                    <PickCard key={`${pick.game}-${pick.betType}-${i}`} pick={pick} idx={i} />
                  ))}
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
