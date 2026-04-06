"""
Adaptive Threshold System
=========================
Reads the last N settled bets from the DB, computes the bot's win rate,
and adjusts every tunable decision-engine threshold accordingly.  The
updated values are written into decision_engine module globals so every
subsequent pick that session uses the correct filter.

Win-rate tiers
--------------
  > 70%  HOT   — bot is dialled-in, slight relaxation across all params
  60-70% GOOD  — solid, default settings
  50-60% AVG   — average, mild tightening
  < 50%  COLD  — struggling, significant tightening everywhere

Permanent safety rails (never touched)
---------------------------------------
  JUICE_HARD_BLOCK  (-600)  absolute wall — nothing ever passes at worse
  MAX_LEGS          (7)     structural cap on slip size
  MIN_LEGS          (3)     structural floor on slip size
  JUICE_GREEN       (-120)  sweet-spot benchmark
  MIN_MINUTES       (20)    player qualification — minutes on court

Everything else is adaptive.

Adjustable parameters
---------------------
  JUICE_RED       — juice level that triggers RED scrutiny
  JUICE_YELLOW    — juice level that triggers YELLOW caution flag
  PUBLIC_FADE_PCT — minimum public-bet % needed to trigger a star fade
  LINE_TRAP_DIFF  — units above avg before a line is blocked as BAD
  LINE_ELITE_DIFF — units below avg required to call a line ELITE
  MAX_FADES       — max fade legs allowed per slip
  MAX_JUICE_LEGS  — max highly-juiced legs allowed per slip
"""

import json
import bot.decision_engine as engine

LOOKBACK_BETS = 30   # settled bets used to compute win rate

# ──────────────────────────────────────────────────────────────────────────────
# Each preset is a full snapshot of every tunable threshold.
# When win rate moves into a new tier, ALL of these are written to the engine.
# ──────────────────────────────────────────────────────────────────────────────
_PRESETS = {
    #          LINE gate              Juice gates        Fade config      Slip config
    #          TRAP   ELITE     RED     YELLOW  FADE%   MAX_F  MAX_JL
    "HOT": {
        "LINE_TRAP_DIFF":   1.5,
        "LINE_ELITE_DIFF": -0.8,   # easier to earn ELITE badge → more confidence boosts
        "JUICE_RED":       -320,   # allow slightly more juice before RED flag
        "JUICE_YELLOW":    -210,   # caution flag raised too
        "PUBLIC_FADE_PCT":   68,   # HOT: slight relaxation on public fade threshold
        "MAX_FADES":          2,   # up to 2 fades per slip
        "MAX_JUICE_LEGS":     3,   # up to 3 juiced legs allowed
        "min_confidence":   62.0,  # L9 floor: bot is hot, slightly relaxed
    },
    "GOOD": {
        "LINE_TRAP_DIFF":   1.0,
        "LINE_ELITE_DIFF": -1.0,
        "JUICE_RED":       -300,
        "JUICE_YELLOW":    -200,
        "PUBLIC_FADE_PCT":   72,   # GOOD: clean rule — public ≥72% + RLM required
        "MAX_FADES":          2,
        "MAX_JUICE_LEGS":     3,
        "min_confidence":   65.0,  # L9 floor: default operating confidence
    },
    "AVG": {
        "LINE_TRAP_DIFF":   0.75,
        "LINE_ELITE_DIFF": -1.2,   # line must be further below avg to earn ELITE
        "JUICE_RED":       -280,
        "JUICE_YELLOW":    -190,
        "PUBLIC_FADE_PCT":   74,   # AVG: tighter fade filter
        "MAX_FADES":          1,   # limit to 1 fade — be more selective
        "MAX_JUICE_LEGS":     2,   # max 2 juiced legs — cleaner slip composition
        "min_confidence":   68.0,  # L9 floor: tighter when results are average
    },
    "COLD": {
        "LINE_TRAP_DIFF":   0.5,
        "LINE_ELITE_DIFF": -1.5,   # only the most obvious gift lines qualify as ELITE
        "JUICE_RED":       -260,
        "JUICE_YELLOW":    -180,
        "PUBLIC_FADE_PCT":   77,   # COLD: only the most extreme public spots qualify
        "MAX_FADES":          1,
        "MAX_JUICE_LEGS":     2,
        "min_confidence":   72.0,  # L9 floor: cold streak — only cleanest edges pass
    },
}

_TIER_LABELS = {
    "HOT":  "Bot is HOT (>70% hit rate) — all thresholds relaxed",
    "GOOD": "Solid run (60-70%) — default settings",
    "AVG":  "Average run (50-60%) — filters tightened across the board",
    "COLD": "Cold run (<50%) — maximum filter tightening, cleanest edges only",
}

# Human-readable description of what each parameter controls
_PARAM_DESCRIPTIONS = {
    "LINE_TRAP_DIFF":   "max units above avg before a line is blocked",
    "LINE_ELITE_DIFF":  "units below avg required to earn ELITE badge",
    "JUICE_RED":        "juice level that triggers RED scrutiny",
    "JUICE_YELLOW":     "juice level that triggers YELLOW caution",
    "PUBLIC_FADE_PCT":  "min public-bet % to trigger a star fade",
    "MAX_FADES":        "max fade legs per slip",
    "MAX_JUICE_LEGS":   "max high-juice legs per slip",
}


def _win_rate_to_tier(win_rate: float) -> str:
    if win_rate >= 0.70:
        return "HOT"
    if win_rate >= 0.60:
        return "GOOD"
    if win_rate >= 0.50:
        return "AVG"
    return "COLD"


def compute_win_rate_from_db(conn, lookback: int = LOOKBACK_BETS) -> tuple:
    """
    Returns (win_rate, wins, losses) from the last `lookback` settled bets.
    Returns (0.60, 0, 0) — the neutral default — when no data is available.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT result FROM bets
               WHERE result IN ('win', 'loss')
               ORDER BY created_at DESC
               LIMIT %s""",
            (lookback,)
        )
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return 0.60, 0, 0
        wins   = sum(1 for r in rows if r[0] == "win")
        losses = len(rows) - wins
        rate   = wins / len(rows)
        return round(rate, 4), wins, losses
    except Exception as e:
        print(f"[Adaptive] compute_win_rate error: {e}")
        return 0.60, 0, 0


def apply_thresholds_to_engine(tier: str) -> dict:
    """
    Overwrite ALL tunable decision_engine module globals with the tier preset.
    Every pick built after this call uses the updated thresholds.
    """
    preset = _PRESETS.get(tier, _PRESETS["GOOD"])

    engine.LINE_TRAP_DIFF   = preset["LINE_TRAP_DIFF"]
    engine.LINE_ELITE_DIFF  = preset["LINE_ELITE_DIFF"]
    engine.JUICE_RED        = preset["JUICE_RED"]
    engine.JUICE_YELLOW     = preset["JUICE_YELLOW"]
    engine.PUBLIC_FADE_PCT  = preset["PUBLIC_FADE_PCT"]
    engine.MAX_FADES        = preset["MAX_FADES"]
    engine.MAX_JUICE_LEGS   = preset["MAX_JUICE_LEGS"]
    engine.min_confidence   = preset["min_confidence"]

    print(
        f"[Adaptive] tier={tier} applied → "
        f"LINE_TRAP={engine.LINE_TRAP_DIFF}  "
        f"LINE_ELITE={engine.LINE_ELITE_DIFF}  "
        f"JUICE_RED={engine.JUICE_RED}  "
        f"JUICE_YELLOW={engine.JUICE_YELLOW}  "
        f"FADE_PCT={engine.PUBLIC_FADE_PCT}  "
        f"MAX_FADES={engine.MAX_FADES}  "
        f"MAX_JUICE_LEGS={engine.MAX_JUICE_LEGS}  "
        f"min_confidence={engine.min_confidence}"
    )
    return preset


def save_thresholds_to_db(conn, tier: str, win_rate: float, wins: int, losses: int):
    """Persist the full threshold state to the learning_data table."""
    payload = {
        "tier":       tier,
        "win_rate":   win_rate,
        "wins":       wins,
        "losses":     losses,
        "label":      _TIER_LABELS.get(tier, ""),
        "thresholds": _PRESETS.get(tier, _PRESETS["GOOD"]),
    }
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO learning_data (key, value)
               VALUES ('adaptive_thresholds', %s::jsonb)
               ON CONFLICT (key) DO UPDATE
               SET value = EXCLUDED.value, updated_at = NOW()""",
            (json.dumps(payload),)
        )
        conn.commit()
        cur.close()
        print(
            f"[Adaptive] Saved: tier={tier}  "
            f"win_rate={win_rate:.1%}  W{wins}-L{losses}"
        )
    except Exception as e:
        print(f"[Adaptive] save_thresholds error: {e}")


def load_thresholds_from_db(conn) -> dict:
    """Load the last saved threshold state from learning_data."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM learning_data WHERE key='adaptive_thresholds'"
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
    except Exception as e:
        print(f"[Adaptive] load_thresholds error: {e}")
    return {}


def classify_tier(conn) -> str:
    """
    Return the current performance tier string based on recent DB win rate.
    Used by L9 of run_full_pipeline to select the correct threshold preset.
    """
    win_rate, _, _ = compute_win_rate_from_db(conn)
    return _win_rate_to_tier(win_rate)


def run_adaptive_update(conn) -> dict:
    """
    Main entry point. Call this:
      1. At the start of every EdgeFade7 run
      2. After any bet result is settled

    Computes win rate → determines tier → applies ALL thresholds to engine
    → saves state to DB.  Returns a summary dict.
    """
    win_rate, wins, losses = compute_win_rate_from_db(conn)
    tier   = _win_rate_to_tier(win_rate)
    preset = apply_thresholds_to_engine(tier)
    save_thresholds_to_db(conn, tier, win_rate, wins, losses)
    return {
        "tier":       tier,
        "win_rate":   win_rate,
        "wins":       wins,
        "losses":     losses,
        "thresholds": preset,
        "label":      _TIER_LABELS.get(tier, ""),
    }


def get_threshold_status(conn) -> str:
    """Human-readable status string for admin display (/thresholds command)."""
    data = load_thresholds_from_db(conn)
    if not data:
        return (
            "📊 *Adaptive Engine*\n"
            "No history yet — running default thresholds\n"
            f"(needs {LOOKBACK_BETS} settled bets to activate)"
        )

    t   = data.get("thresholds", {})
    wr  = data.get("win_rate", 0)
    w   = data.get("wins", 0)
    l   = data.get("losses", 0)
    tier = data.get("tier", "GOOD")

    tier_emoji = {"HOT": "🔥", "GOOD": "✅", "AVG": "⚠️", "COLD": "🧊"}.get(tier, "📊")

    lines = [
        f"📊 *Adaptive Engine — {tier_emoji} {tier}*",
        f"_{data.get('label', '')}_",
        f"Last {LOOKBACK_BETS} bets: W{w}-L{l} ({wr:.1%})",
        "",
        "*Active thresholds:*",
        f"  Line gate (BAD):   line > avg + {t.get('LINE_TRAP_DIFF')}",
        f"  Line gate (ELITE): line < avg - {abs(t.get('LINE_ELITE_DIFF', 1.0))}",
        f"  Juice RED:         {t.get('JUICE_RED')} or worse",
        f"  Juice YELLOW:      {t.get('JUICE_YELLOW')} or worse",
        f"  Fade trigger:      {t.get('PUBLIC_FADE_PCT')}% public money",
        f"  Max fades/slip:    {t.get('MAX_FADES')}",
        f"  Max juiced legs:   {t.get('MAX_JUICE_LEGS')}",
        "",
        "*Permanent safety rails (never change):*",
        f"  Hard block: {getattr(engine, 'JUICE_HARD_BLOCK', -600)}  |  Min legs: {getattr(engine, 'MIN_LEGS', 3)}  |  Max legs: {getattr(engine, 'MAX_LEGS', 7)}  |  Min mins: {getattr(engine, 'MIN_MINUTES', 20)}",
    ]
    return "\n".join(lines)
