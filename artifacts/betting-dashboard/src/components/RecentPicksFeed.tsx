import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Trophy, XCircle, Clock, Flame, TrendingUp } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs: ClassValue[]) { return twMerge(clsx(inputs)); }

const BASE_URL = import.meta.env.BASE_URL?.replace(/\/$/, "") ?? "";

type StreakData = {
  last10: any[];
  wins: number;
  losses: number;
  streak: number;
  streakType: "W" | "L" | null;
};

const TYPE_STYLES: Record<string, string> = {
  MONEYLINE:  "bg-blue-500/10 text-blue-400 border-blue-500/20",
  OVER:       "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
  UNDER:      "bg-orange-500/10 text-orange-400 border-orange-500/20",
  SPREAD:     "bg-purple-500/10 text-purple-400 border-purple-500/20",
  SGP:        "bg-cyan-500/10 text-cyan-400 border-cyan-500/20",
  CROSS_SGP:  "bg-violet-500/10 text-violet-400 border-violet-500/20",
  PARLAY:     "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
};

const STAT_LABELS: Record<string, string> = {
  points:          "PTS",
  rebounds:        "REB",
  assists:         "AST",
  threes:          "3PM",
  player_points:   "PTS",
  player_rebounds: "REB",
  player_assists:  "AST",
  player_threes:   "3PM",
};

function parsePick(pick: string, line: number | null): string {
  const parts = pick.split("|");
  const dir   = parts[0]?.trim() ?? pick;
  const stat  = parts[1] ? (STAT_LABELS[parts[1].trim()] ?? parts[1].toUpperCase()) : "";
  const lineStr = line != null ? ` ${line}` : "";
  return stat ? `${dir}${lineStr} ${stat}` : `${dir}${lineStr}`;
}

function ResultIcon({ result }: { result?: string | null }) {
  if (result === "win") return <Trophy className="w-4 h-4 text-emerald-400" />;
  if (result === "loss") return <XCircle className="w-4 h-4 text-red-400" />;
  return <Clock className="w-4 h-4 text-muted-foreground" />;
}

export function RecentPicksFeed() {
  const [data, setData] = useState<StreakData | null>(null);

  useEffect(() => {
    const load = () =>
      fetch(`${BASE_URL}/api/bets/streak`)
        .then((r) => r.json())
        .then(setData)
        .catch(() => {});
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, []);

  const last10 = data?.last10 ?? [];
  const l10Label = data ? `${data.wins}W-${data.losses}L` : "—";
  const isHot = (data?.streakType === "W" && (data?.streak ?? 0) >= 3);
  const isCold = (data?.streakType === "L" && (data?.streak ?? 0) >= 3);

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.4 }}
      className="glass-panel rounded-2xl p-6 flex flex-col gap-4"
    >
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-display font-bold uppercase tracking-wider text-foreground">
            Recent Picks
          </h3>
          <p className="text-xs text-muted-foreground mt-0.5">Last 10 settled picks</p>
        </div>
        <div className="flex items-center gap-3">
          {(isHot || isCold) && (
            <div className={cn(
              "flex items-center gap-1.5 px-3 py-1.5 rounded-full border text-xs font-bold font-display",
              isHot ? "bg-orange-500/10 border-orange-500/20 text-orange-400" : "bg-blue-500/10 border-blue-500/20 text-blue-400"
            )}>
              {isHot ? <Flame className="w-3.5 h-3.5" /> : <TrendingUp className="w-3.5 h-3.5 rotate-180" />}
              {data?.streak}{data?.streakType} streak
            </div>
          )}
          <div className="px-3 py-1.5 rounded-full bg-primary/10 border border-primary/20">
            <span className="text-xs font-bold font-display text-primary">L10: {l10Label}</span>
          </div>
        </div>
      </div>

      <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
        <AnimatePresence>
          {last10.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-10">
              No settled picks yet — results appear here after games finish
            </p>
          ) : (
            last10.map((bet, i) => {
              const type = bet.betType ?? "MONEYLINE";
              const typeStyle = TYPE_STYLES[type] ?? TYPE_STYLES["MONEYLINE"];
              const timeAgo = bet.time ? formatDistanceToNow(new Date(bet.time), { addSuffix: true }) : "";
              return (
                <motion.div
                  key={`${bet.game}-${bet.time}-${i}`}
                  initial={{ opacity: 0, x: 10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.04 }}
                  className={cn(
                    "flex items-center gap-3 rounded-xl px-4 py-3 border",
                    bet.result === "win" ? "bg-emerald-500/5 border-emerald-500/15" :
                    bet.result === "loss" ? "bg-red-500/5 border-red-500/15" :
                    "bg-background/40 border-white/8"
                  )}
                >
                  <ResultIcon result={bet.result} />
                  <div className="flex-1 min-w-0">
                    <p className="text-xs text-muted-foreground truncate">
                      {bet.player ? bet.player : bet.game}
                    </p>
                    <p className="text-xs text-muted-foreground/60 truncate">
                      {bet.player ? bet.game : ""}
                    </p>
                    <p className="text-sm font-semibold text-foreground truncate">
                      {parsePick(bet.pick, bet.line)}
                    </p>
                  </div>
                  <div className="flex flex-col items-end gap-1 shrink-0">
                    <span className={cn("px-2 py-0.5 rounded text-xs font-bold border uppercase tracking-wide", typeStyle)}>
                      {type}
                    </span>
                    <span className="text-xs text-muted-foreground font-display">{timeAgo}</span>
                  </div>
                </motion.div>
              );
            })
          )}
        </AnimatePresence>
      </div>
    </motion.div>
  );
}
