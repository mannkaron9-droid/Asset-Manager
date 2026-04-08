"""
7-STEP DECISION ENGINE
======================
Every pick must pass all 7 steps before being included in a slip.

Step 1: Juice Test
  - ≤ -180         → RED FLAG (needs strong EV to justify)
  - -150 to -179   → YELLOW (check EV carefully)
  - -110 to +150   → GREEN (sweet spot)

Step 2: Public Pressure Check
  - High public % + juiced line + star player → FADE candidate

Step 3: Game Script Fit
  - Does this pick make sense given pace, flow, offense style, defense?

Step 4: Role Assignment
  - Is this player in the right role for their stat line?

Step 5: EV Check
  - True win probability > book implied probability → positive EV → include

Step 6: Slip Validation (6 checks)
  1. Fade integrity      - fades are truly public-heavy + juiced
  2. Benefactor connection - benefactors are on same team / same game
  3. Role distribution   - pts + reb + ast covered across legs
  4. Juice check         - no more than 2 legs at -150 or worse
  5. Script alignment    - every pick fits game script
  6. Hidden trap check   - no "safe trash" (-300 locks that kill value)

Step 7: Parlay Structure
  - 2 fades max + 5 benefactors
  - 1 stat per player
  - Stat diversity enforced (pts + reb + ast)
  - Target payout: +250 to +400
"""

import math
import random
from dataclasses import dataclass, field
from typing import Optional
from bot.game_script import GameScript, PlayerRole, get_script_summary, TEAM_STYLES, _DEFAULT_STYLE


# ── Juice thresholds ──────────────────────────────────────────────────────────
JUICE_HARD_BLOCK = -600   # absolute wall — no pick ever goes out at this juice or worse
JUICE_RED        = -300   # heavy juice — scrutinize, need strong edge to justify
JUICE_YELLOW     = -200   # caution — check EV carefully
JUICE_GREEN      = -120   # sweet spot (-120 to -200)

# ── Public pressure threshold ──────────────────────────────────────────────────
PUBLIC_FADE_PCT = 72      # % public bets + RLM required → star is "inflated"

# ── Slip payout targets ────────────────────────────────────────────────────────
PAYOUT_MIN = 200          # +200 minimum
PAYOUT_MAX = 700          # +700 maximum

# ── Slip validation constants ─────────────────────────────────────────────────
MAX_FADES_PER_TEAM = 2   # max fades from any single team
MAX_FADES          = 3   # total fades in slip (3 fades + 3 benes + 1 value = 7)
MAX_LEGS           = 7
MIN_LEGS     = 3
MAX_JUICE_LEGS = 3        # max legs allowed at -170 or worse

# ── Line value thresholds (line vs player average) ────────────────────────────
LINE_ELITE_DIFF = -1.0   # line 1.0+ BELOW avg → gift line (ELITE)
LINE_TRAP_DIFF  =  1.0   # line 1.0+ ABOVE avg → trap line (BAD)

# ── Monte Carlo slip optimizer ────────────────────────────────────────────────
MONTE_CARLO_ATTEMPTS = 50          # shuffle attempts per target size
SLIP_TARGET_SIZES    = [3, 4, 5, 6, 7]   # build at each size, keep highest-EV winner

# ── Minutes threshold ─────────────────────────────────────────────────────────
MIN_MINUTES = 20          # players averaging < 20 min/game are not valid legs


@dataclass
class JuiceResult:
    odds: float
    flag: str            # GREEN | YELLOW | RED
    implied_prob: float
    reason: str


@dataclass
class EVResult:
    true_prob: float
    implied_prob: float
    ev: float            # positive = value, negative = no value
    is_positive: bool
    recommendation: str


@dataclass
class SlipLeg:
    player: str
    team: str
    game: str
    stat: str            # points | rebounds | assists | threes
    direction: str       # OVER | UNDER
    line: float
    odds: float
    prediction: float
    true_prob: float
    implied_prob: float
    ev: float
    role: str            # go_to_scorer | floor_general | glass_cleaner | rim_anchor | spot_up_shooter | combo_creator | sixth_man | utility_player
    is_fade: bool = False
    is_benefactor: bool = False
    fade_target: str = ""    # name of the player this benefactor shadows
    juice_flag: str = "GREEN"
    game_script_label: str = ""
    confidence: float = 0.0
    line_rating: str = "GOOD"     # ELITE | GOOD | MID | BAD
    line_decision: str = "SAFE"   # SAFE (line ≤ avg) | RISK (line > avg)
    edge: float = 0.0             # true_prob − implied_prob (raw probability edge)
    game_pace: str = "AVERAGE_PACE"   # HALFCOURT | SLOW_PACED | AVERAGE_PACE | UPTEMPO | TRANSITION_HEAVY
    game_phase: str = "pregame"   # pregame | live | late


@dataclass
class ValidationResult:
    passed: bool
    checks: dict         # check_name → (passed, reason)
    failures: list       # list of failed check names


@dataclass
class Slip:
    legs: list           # list of SlipLeg
    grade: str           # A | B | C | D
    grade_reason: str
    estimated_payout: float
    fades: list          # SlipLeg list
    benefactors: list    # SlipLeg list
    stat_diversity: dict # {points: n, rebounds: n, assists: n}
    validation: ValidationResult
    game_script: Optional[GameScript] = None
    send_to_vip: bool = False
    send_to_free: bool = False
    parlay_hit_prob: float = 0.0   # probability all legs hit simultaneously
    parlay_ev: float = 0.0         # expected value on $100 stake across the full slip


# ── Line value evaluator (line vs player average) ────────────────────────────

def evaluate_line_value(line: float, avg: float, odds: float) -> str:
    """
    Rate the value of a prop line against the player's season average.

    ELITE  → line 1.0+ below avg — book set it too low (gift)
    GOOD   → line at or up to 0.5 above avg — fair/value zone
    MID    → line 0.5–1.0 above avg — borderline
    BAD    → line 1.0+ above avg OR odds -200 or worse — trap
    """
    if odds <= -200:
        return "BAD"
    diff = line - avg   # positive = line harder to hit, negative = easier
    if diff < LINE_ELITE_DIFF:   # line well below avg → ELITE
        return "ELITE"
    if diff <= 0.5:              # line at or slightly above avg → GOOD
        return "GOOD"
    if diff <= LINE_TRAP_DIFF:   # line 0.5–1.0 above avg → MID
        return "MID"
    return "BAD"                 # line 1.0+ above avg → TRAP


# ── Parlay hit probability & Kelly stake helpers ──────────────────────────────

def compute_parlay_hit_prob(legs: list) -> float:
    """Probability all legs hit simultaneously (product of true probs)."""
    if not legs:
        return 0.0
    prob = 1.0
    for leg in legs:
        prob *= leg.true_prob
    return round(prob, 4)


def kelly_stake_1k(true_prob: float, odds: float, bankroll: float = 1000) -> float:
    """
    Kelly criterion stake for a given bankroll.
    Returns the suggested dollar amount to bet.
    """
    if odds > 0:
        b = odds / 100
    else:
        b = 100 / abs(odds) if odds != 0 else 0.909
    q = 1 - true_prob
    f = max((b * true_prob - q) / b, 0)
    return round(f * bankroll, 2)


# ── Step 1: Juice Test ────────────────────────────────────────────────────────

def juice_test(odds: float) -> JuiceResult:
    ip = implied_probability(odds)

    if odds <= JUICE_HARD_BLOCK:
        flag = "BLOCKED"
        reason = f"Hard block ({odds}) — beyond -400 wall. Zero value, never send."
    elif odds <= JUICE_RED:
        flag = "RED"
        reason = f"Heavy juice ({odds}) — line inflated by public money. Need 67%+ true probability."
    elif odds <= JUICE_YELLOW:
        flag = "YELLOW"
        reason = f"Moderate juice ({odds}) — check EV before including."
    elif odds >= 200:
        flag = "YELLOW"
        reason = f"Long shot ({odds}) — high variance, ensure game script supports it."
    else:
        flag = "GREEN"
        reason = f"Sweet spot odds ({odds}) — good value range."

    return JuiceResult(odds=odds, flag=flag, implied_prob=ip, reason=reason)


# ── Step 2: Public Pressure Check ─────────────────────────────────────────────

def public_pressure_check(
    player: str,
    prop_type: str,
    odds: float,
    public_pct: float = 0.0,
    is_star: bool = False,
    line_movement: float = 0.0,
    bet_type: str = "",
) -> dict:
    """
    Returns dict with fade_candidate bool, rlm_confirmed bool, and reason string.

    Fade fires ONLY when all three conditions hold (confluence rule):
      1. public_pct >= PUBLIC_FADE_PCT (≥72%) — heavy public side
      2. line_movement <= -0.5 — line moved DOWN despite public backing OVER
         (books absorbing sharp money on UNDER = reverse line movement)
      3. is_star — public darlings are the fade targets, not role players
      4. Line is juiced — book has inflated the price (extra signal)

    Moneylines are flagged as a caution: public wins ML more often, fade is
    weaker than on spreads/props.
    """
    juice = juice_test(odds)
    is_public_heavy = public_pct >= PUBLIC_FADE_PCT
    is_juiced       = juice.flag in ("YELLOW", "RED")

    # Reverse line movement: line dropped while public is loading the OVER
    # → sharp money confirmed on the UNDER → strongest fade signal
    rlm_confirmed = line_movement <= -0.5

    # Moneylines are harder to fade blindly — flag but don't block
    is_moneyline = bet_type.lower() in ("ml", "moneyline", "h2h")

    # Fade requires: high public % AND RLM AND star AND juiced
    fade_candidate = is_public_heavy and rlm_confirmed and is_star and is_juiced

    reasons = []
    if is_public_heavy:
        reasons.append(f"{public_pct:.0f}% public backing OVER")
    if rlm_confirmed:
        reasons.append(f"RLM {line_movement:+.1f} (sharp money on UNDER)")
    elif is_public_heavy:
        reasons.append(f"⚠️ no RLM yet ({line_movement:+.1f}) — fade not confirmed")
    if is_juiced:
        reasons.append(f"line juiced to {odds}")
    if is_star:
        reasons.append("star player (public darling)")
    if is_moneyline:
        reasons.append("⚠️ ML bet — public wins ML more often, fade weaker than spread")

    return {
        "fade_candidate":  fade_candidate,
        "rlm_confirmed":   rlm_confirmed,
        "is_public_heavy": is_public_heavy,
        "is_juiced":       is_juiced,
        "is_moneyline":    is_moneyline,
        "reason": " · ".join(reasons) if reasons else "No public pressure detected",
    }


# ── Step 3: Game Script Fit ────────────────────────────────────────────────────

def game_script_fit(leg: SlipLeg, gs: GameScript) -> dict:
    """
    Check whether this pick makes sense given the game script.
    Returns {fits: bool, reason: str}
    """
    fits = True
    reasons = []

    # HALFCOURT / SLOW_PACED game — low possessions
    if gs.pace in ("HALFCOURT", "SLOW_PACED"):
        if leg.stat == "points" and leg.direction == "OVER" and leg.line >= 25:
            fits = False
            reasons.append(f"{gs.pace} — high points OVER is risky in slow game")
        if leg.stat == "assists" and leg.direction == "OVER":
            fits = False
            reasons.append(f"{gs.pace} — fewer possessions limit assist opportunities")

    # UPTEMPO / TRANSITION_HEAVY game — lots of possessions
    if gs.pace in ("UPTEMPO", "TRANSITION_HEAVY"):
        if leg.stat == "rebounds" and leg.direction == "OVER" and leg.line >= 12:
            fits = False
            reasons.append(f"{gs.pace} — fast breaks reduce defensive rebounds")

    # BLOWOUT / DOUBLE_DIGIT_LEAD game
    if gs.flow in ("BLOWOUT", "DOUBLE_DIGIT_LEAD"):
        if leg.is_fade and leg.role in ("go_to_scorer", "spot_up_shooter"):
            fits = True   # fading a star in blowout is smart — they rest
            reasons.append(f"{gs.flow} boosts fade (star may rest early)")
        if not leg.is_fade and leg.role in ("glass_cleaner", "rim_anchor") and gs.pace in ("UPTEMPO", "TRANSITION_HEAVY"):
            reasons.append(f"{gs.flow} + {gs.pace} = bench bigs get minutes")

    # TIGHT_GAME — stars play heavy minutes
    if gs.flow == "TIGHT_GAME":
        if leg.is_fade and leg.stat == "points" and leg.line >= 30:
            fits = False
            reasons.append("TIGHT_GAME — stars play heavy minutes, harder to fade")

    # FACILITATOR offense
    if (gs.offense_home if leg.team == gs.home_team else gs.offense_away) == "FACILITATOR":
        if leg.stat == "assists" and leg.direction == "OVER":
            reasons.append("FACILITATOR offense boosts assists OVER")

    # PRESSURE defense (opponent)
    opp_def = gs.defense_away if leg.team == gs.home_team else gs.defense_home
    if opp_def == "PRESSURE":
        if leg.stat == "assists" and leg.direction == "OVER":
            reasons.append("PRESSURE defense creates fast-break assists")
        if leg.stat == "points" and leg.role in ("go_to_scorer", "sixth_man") and leg.direction == "OVER" and leg.line >= 28:
            fits = False
            reasons.append("PRESSURE defense contests star scoring")

    # ── Pace-aware edge bonuses ──────────────────────────────────────────────
    edge_bonus = 0.0

    if gs.pace in ("UPTEMPO", "TRANSITION_HEAVY"):
        if leg.stat == "rebounds" and leg.direction == "OVER":
            edge_bonus += 0.06   # more possessions = more board opportunities
            reasons.append(f"{gs.pace} boosts rebounds edge")
        if (not leg.is_fade and leg.role in ("go_to_scorer", "spot_up_shooter", "combo_creator")
                and leg.stat == "points" and leg.direction == "OVER"):
            edge_bonus += 0.04   # more possessions = more secondary scoring
            reasons.append(f"{gs.pace} boosts secondary scorer edge")

    if gs.pace in ("HALFCOURT", "SLOW_PACED"):
        if leg.role == "floor_general" and leg.stat == "assists" and leg.direction == "OVER":
            edge_bonus += 0.05   # slow game → primary PG distributes more
            reasons.append(f"{gs.pace} boosts primary PG assists edge")

    if gs.flow in ("BLOWOUT", "DOUBLE_DIGIT_LEAD") and leg.is_fade:
        edge_bonus += 0.05       # faded star more likely to sit/rest early
        reasons.append(f"{gs.flow} increases fade confidence")

    # ── Matchup bias: TIGHT_GAME boosts assists (team reliant on passing) ────
    if gs.flow == "TIGHT_GAME" and leg.stat == "assists" and leg.direction == "OVER":
        edge_bonus += 0.04
        reasons.append("TIGHT_GAME boosts assists edge (team passing more)")

    # ── Matchup bias: high game total boosts points (more scoring opps) ─────
    if gs.total > 225 and leg.stat == "points" and leg.direction == "OVER":
        edge_bonus += 0.03
        reasons.append(f"High game total ({gs.total}) boosts points OVER edge")

    # ── Head-to-head matchup engine ───────────────────────────────────────────
    # Pull both teams' full style profiles and compare head-to-head.
    # This is the core of "the matchup determines the game script."

    team_style = TEAM_STYLES.get(leg.team, _DEFAULT_STYLE)
    opp_team   = gs.away_team if leg.team == gs.home_team else gs.home_team
    opp_style  = TEAM_STYLES.get(opp_team, _DEFAULT_STYLE)

    ts_pace        = team_style.get("pace",         100)
    ts_assist_bias = team_style.get("assist_bias",  0.53)
    ts_reb_bias    = team_style.get("reb_bias",     0.50)
    ts_off         = team_style.get("off_strength", 72)
    ts_def         = team_style.get("def_strength", 68)
    ts_3pt         = team_style.get("three_pt_rate",65)
    ts_strength    = team_style.get("strength",     65)

    opp_def        = opp_style.get("def_strength",  68)
    opp_off        = opp_style.get("off_strength",  72)
    opp_reb        = opp_style.get("reb_bias",      0.50)
    opp_strength   = opp_style.get("strength",      65)

    # ── 1. Scoring matchup: own offense vs opponent defense ───────────────────
    scoring_edge = ts_off - opp_def   # positive = favorable; negative = tough
    if leg.stat == "points" and not leg.is_fade:
        if scoring_edge >= 15:
            edge_bonus += 0.04
            reasons.append(
                f"Scoring matchup FAVOURABLE ({leg.team} off={ts_off} vs {opp_team} def={opp_def})"
            )
        elif scoring_edge >= 5:
            edge_bonus += 0.02
            reasons.append(
                f"Scoring matchup slight edge ({leg.team} off={ts_off} vs {opp_team} def={opp_def})"
            )
        elif scoring_edge <= -15:
            edge_bonus -= 0.05
            reasons.append(
                f"Scoring matchup TOUGH ({leg.team} off={ts_off} vs {opp_team} def={opp_def}) — points OVER penalised"
            )
        elif scoring_edge <= -5:
            edge_bonus -= 0.02
            reasons.append(
                f"Scoring matchup below average ({leg.team} off={ts_off} vs {opp_team} def={opp_def})"
            )

    # ── 2. Assist bonus: every team passes — scale by their assist_bias ───────
    if leg.stat == "assists" and leg.direction == "OVER":
        ast_bonus = round(0.02 + (ts_assist_bias - 0.53) * 0.40, 3)   # 0.02–0.07
        ast_bonus = max(0.02, min(0.07, ast_bonus))
        edge_bonus += ast_bonus
        reasons.append(
            f"{leg.team} assist_bias={ts_assist_bias:.2f} — passing system boosts assists edge (+{ast_bonus:.2f})"
        )

    # ── 3. Rebounding matchup: own reb_bias vs opponent's reb_bias ───────────
    reb_edge = ts_reb_bias - opp_reb
    if leg.stat == "rebounds" and leg.direction == "OVER":
        if reb_edge >= 0.04:
            edge_bonus += 0.05
            reasons.append(
                f"Rebounding matchup dominant ({leg.team} reb={ts_reb_bias:.2f} vs {opp_team} reb={opp_reb:.2f})"
            )
        elif reb_edge >= 0.01:
            edge_bonus += 0.03
            reasons.append(
                f"Rebounding matchup favourable ({leg.team} reb={ts_reb_bias:.2f} vs {opp_team} reb={opp_reb:.2f})"
            )
        elif reb_edge <= -0.04:
            edge_bonus -= 0.03
            reasons.append(
                f"Rebounding matchup unfavourable — {opp_team} crashes boards harder"
            )

    # ── 4. Pace bonus for points OVER ────────────────────────────────────────
    if leg.stat == "points" and leg.direction == "OVER" and ts_pace > 100 and not leg.is_fade:
        edge_bonus += 0.03
        reasons.append(f"{leg.team} pace={ts_pace} — up-tempo pace boosts points OVER")

    # ── 5. 3-point shooting: high three_pt_rate in a high-total game ─────────
    if (leg.stat == "points" and leg.direction == "OVER"
            and ts_3pt >= 75 and gs.total > 220 and not leg.is_fade):
        edge_bonus += 0.03
        reasons.append(
            f"{leg.team} 3pt_rate={ts_3pt} in high-total ({gs.total}) game — shooter bonus"
        )

    # ── 6. Favorite / underdog factor ────────────────────────────────────────
    strength_diff = ts_strength - opp_strength   # positive = favorite
    if strength_diff >= 15:
        # Team is a clear favorite — star usage may drop late; fade more confident
        if leg.is_fade:
            edge_bonus += 0.03
            reasons.append(
                f"{leg.team} heavy favourite (str={ts_strength} vs {opp_strength}) — fade confirmed, star rests late"
            )
        elif leg.stat == "points" and leg.direction == "OVER" and leg.line >= 28:
            edge_bonus -= 0.02
            reasons.append(
                f"{leg.team} heavy favourite — star may sit in 4th, high line risky"
            )
    elif strength_diff <= -15:
        # Team is a clear underdog — star carries a heavier load
        if not leg.is_fade and leg.role in ("go_to_scorer", "floor_general") and leg.stat == "points":
            edge_bonus += 0.03
            reasons.append(
                f"{leg.team} underdog (str={ts_strength} vs {opp_strength}) — star usage spikes, points OVER boost"
            )
        if leg.is_fade:
            edge_bonus -= 0.03
            reasons.append(
                f"{leg.team} underdog — star player will be leaned on, fade less safe"
            )

    return {
        "fits": fits,
        "reason": " · ".join(reasons) if reasons else "Script fit confirmed",
        "edge_bonus": edge_bonus,
    }


# ── Step 5: EV Check ──────────────────────────────────────────────────────────

def implied_probability(odds: float) -> float:
    if odds == 0:
        return 0.5
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def estimate_hit_prob(avg: float, line: float) -> float:
    """
    Linear probability model from the analyze_parlay pipeline.
    Base = 0.5; each unit of (avg - line) adds/subtracts 8%.
    Clamped to [0.10, 0.90].
    Faster to compute than the statistical model and well-calibrated for
    lines that are close to the player's average.
    """
    diff = avg - line
    prob = 0.5 + (diff * 0.08)
    return max(0.1, min(0.9, prob))


def compute_stat_std(game_log: list, fallback: float = 4.0) -> float:
    """
    Compute a player's real game-to-game standard deviation from their
    actual game log (list of per-game stat values from BDL).

    Requires at least 5 games — otherwise returns the league-wide fallback.
    This replaces fixed STAT_STD_MAP constants so each player's probability
    is calibrated to their own consistency, not a league average.

    Examples from real data:
      Jokic pts std ≈ 4.1  (very consistent)
      Trae Young ast std ≈ 2.6
      A volatile scorer std ≈ 7-8
    """
    values = [float(v) for v in game_log if v is not None]
    if len(values) < 5:
        return fallback
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = variance ** 0.5
    # Clamp to a reasonable NBA range: never below 1.0 or above 12.0
    return round(max(1.0, min(12.0, std)), 2)


def calculate_true_probability(
    prediction: float,
    line: float,
    stat_std: float = 4.0,
) -> float:
    """
    Blended true probability: 50% statistical (normal CDF sigmoid) + 50% linear.
    The statistical model is better for large gaps; the linear model is better
    near the average. Blending gives more robust calibration across all cases.

    stat_std should be the player's real computed std dev from compute_stat_std().
    Falls back to 4.0 only if no game log is available.
    """
    if stat_std <= 0:
        stat_std = 4.0

    # Statistical model: sigmoid approximation of P(X > line)
    z = (line - prediction) / stat_std
    prob_statistical = 1 / (1 + math.exp(1.7 * z))

    # Linear model: simple avg-vs-line adjustment
    prob_linear = estimate_hit_prob(prediction, line)

    # Blend 50/50
    blended = (prob_statistical + prob_linear) / 2
    return round(blended, 4)


def ev_check(
    prediction: float,
    line: float,
    odds: float,
    direction: str = "OVER",
    stat_std: float = 4.0,
) -> EVResult:
    """
    Calculate Expected Value for a prop bet.
    EV = (true_prob × win_payout) - (1 - true_prob)
    """
    true_prob = calculate_true_probability(prediction, line, stat_std)
    if direction == "UNDER":
        true_prob = 1 - true_prob

    ip = implied_probability(odds)

    # Win payout per $1 wagered
    if odds > 0:
        win_payout = odds / 100
    elif odds < 0:
        win_payout = 100 / abs(odds)
    else:
        win_payout = 0.909   # -110 default

    ev = (true_prob * win_payout) - (1 - true_prob)

    is_positive = true_prob > ip   # true prob beats book implied prob

    if ev > 0.15:
        rec = "STRONG VALUE — include"
    elif ev > 0.05:
        rec = "POSITIVE EV — include"
    elif ev > -0.05:
        rec = "BREAKEVEN — include if script fits perfectly"
    else:
        rec = "NEGATIVE EV — skip"

    return EVResult(
        true_prob     = round(true_prob, 4),
        implied_prob  = round(ip, 4),
        ev            = round(ev, 4),
        is_positive   = is_positive,
        recommendation = rec,
    )


# ── Step 6: Slip Validation (6 checks) ────────────────────────────────────────

def validate_slip(legs: list, game_script: Optional[GameScript] = None) -> ValidationResult:
    checks = {}

    fades        = [l for l in legs if l.is_fade]
    benefactors  = [l for l in legs if l.is_benefactor]
    stats        = [l.stat for l in legs]
    juice_heavy  = [l for l in legs if l.juice_flag in ("YELLOW", "RED")]
    juice_blocked = [l for l in legs if l.juice_flag == "BLOCKED"]
    games        = set(l.game for l in legs)

    # ── Hard block: any BLOCKED leg instantly fails the entire slip ────────────
    if juice_blocked:
        blocked_names = ", ".join(f"{l.player} ({l.odds})" for l in juice_blocked)
        checks["juice_check"] = (False, f"BLOCKED leg(s) beyond -400 wall: {blocked_names}")
        checks["fade_integrity"]       = (True, "N/A")
        checks["benefactor_connection"] = (True, "N/A")
        checks["role_distribution"]    = (True, "N/A")
        checks["script_alignment"]     = (True, "N/A")
        checks["hidden_trap"]          = (True, "N/A")
        return ValidationResult(passed=False, checks=checks,
                                failures=["juice_check"])

    # ── Check 1: Fade integrity ────────────────────────────────────────────────
    fade_ok = len(fades) <= MAX_FADES
    fade_reason = (
        f"{len(fades)} fade(s) — OK (max {MAX_FADES})" if fade_ok
        else f"Too many fades: {len(fades)} (max {MAX_FADES})"
    )
    checks["fade_integrity"] = (fade_ok, fade_reason)

    # ── Check 2: Benefactor connection (same game as their fade) ───────────────
    bene_ok = True
    bene_reason = "All benefactors connected to fades in same game"
    for b in benefactors:
        if b.fade_target:
            fade_game = next((f.game for f in fades if f.player == b.fade_target), None)
            if fade_game and fade_game != b.game:
                bene_ok = False
                bene_reason = f"{b.player} is benefactor of {b.fade_target} but in different game"
                break
    checks["benefactor_connection"] = (bene_ok, bene_reason)

    # ── Check 3: Role (stat) diversity ────────────────────────────────────────
    stat_counts = {s: stats.count(s) for s in set(stats)}
    has_pts = stat_counts.get("points", 0) > 0
    has_reb = stat_counts.get("rebounds", 0) > 0
    has_ast = stat_counts.get("assists", 0) > 0
    diversity_ok = has_pts and (has_reb or has_ast)
    diversity_reason = (
        f"Good diversity: {stat_counts}" if diversity_ok
        else f"Poor diversity — missing key stats: {stat_counts}"
    )
    checks["role_distribution"] = (diversity_ok, diversity_reason)

    # ── Check 4: Juice check (max 2 heavy juice legs) ─────────────────────────
    juice_ok = len(juice_heavy) <= MAX_JUICE_LEGS
    juice_reason = (
        f"{len(juice_heavy)} juice-heavy legs (max {MAX_JUICE_LEGS})" if juice_ok
        else f"Too many juiced legs: {len(juice_heavy)} — kills parlay value"
    )
    checks["juice_check"] = (juice_ok, juice_reason)

    # ── Check 5: Script alignment ──────────────────────────────────────────────
    # Script misalignment is now handled upstream via a -10% confidence penalty
    # before Monte Carlo runs. Off-script legs lose to aligned legs naturally
    # across shuffles. Hard-failing here stacked with other failures and caused
    # too many D grades — confidence-based filtering is more accurate since the
    # live monitor validates outcomes and the engine learns over time.
    checks["script_alignment"] = (True, "Handled via confidence penalty pre-Monte Carlo")

    # ── Check 6: Hidden trap check ─────────────────────────────────────────────
    # Only flag as trap if BOTH odds are heavy AND line_rating is poor.
    # A -250 leg where the player consistently clears the line (ELITE/GOOD rating)
    # is not a trap — FanDuel priced it heavy because it's likely to hit.
    trap_legs = [l for l in legs if l.odds <= -250 and l.line_rating in ("MID", "BAD")]
    trap_ok = len(trap_legs) == 0
    trap_reason = (
        "No hidden traps" if trap_ok
        else f"TRAP DETECTED: {[l.player for l in trap_legs]} — heavy odds + poor line value"
    )
    checks["hidden_trap"] = (trap_ok, trap_reason)

    failures = [k for k, (passed, _) in checks.items() if not passed]
    overall_passed = len(failures) == 0

    return ValidationResult(
        passed   = overall_passed,
        checks   = checks,
        failures = failures,
    )


# ── Step 7: Grade the slip ────────────────────────────────────────────────────

def grade_slip(legs: list, validation: ValidationResult) -> tuple:
    """
    Returns (grade: str, reason: str)

    A — Elite: correlated legs, correct role diversity, low juice, all 6 checks pass
    B — Strong: minor juice issue or 1 validation warning, still positive EV
    C — Weak: multiple checks fail, fades are soft or uncorrelated
    D — Trap: major failures, don't send
    """
    failures = validation.failures
    n_fail = len(failures)

    fades        = [l for l in legs if l.is_fade]
    benefactors  = [l for l in legs if l.is_benefactor]
    avg_ev       = sum(l.ev for l in legs) / len(legs) if legs else 0
    juice_heavy  = [l for l in legs if l.juice_flag in ("YELLOW", "RED")]
    juice_blocked = [l for l in legs if l.juice_flag == "BLOCKED"]
    stat_set     = {l.stat for l in legs}
    has_diversity = len(stat_set) >= 2

    # D: instant fail — any leg beyond the -400 wall
    if juice_blocked:
        return "D", f"BLOCKED leg(s) beyond -400 wall — slip rejected"

    # D: trap detected or 4+ failures
    if "hidden_trap" in failures or n_fail >= 4:
        return "D", "Major failures or trap detected — DO NOT SEND"

    # D: all negative EV
    if avg_ev < -0.10:
        return "D", f"Negative EV across slip (avg {avg_ev:.3f}) — skip"

    # A: all checks pass + good EV + diversity
    if n_fail == 0 and avg_ev > 0.05 and has_diversity and len(juice_heavy) == 0:
        return "A", "Elite slip — all 6 checks pass, positive EV, clean juice, stat diversity"

    # A: all checks pass even with slight juice
    if n_fail == 0 and avg_ev > 0.02 and has_diversity:
        return "A", "Strong slip — all checks pass, positive EV, good diversity"

    # B: 1 failure (not trap), positive EV
    if n_fail <= 1 and avg_ev > 0 and has_diversity:
        failed_name = failures[0] if failures else "none"
        return "B", f"Good slip — minor issue ({failed_name}), still positive EV"

    # B: 2 failures but EV is positive and no trap
    if n_fail <= 2 and avg_ev > 0:
        return "B", f"Acceptable slip — {n_fail} minor issues but positive EV"

    # C: multiple failures, borderline EV
    if n_fail <= 3 and avg_ev >= -0.05:
        return "C", f"{n_fail} checks failed, weak fades or poor diversity — risky"

    # D: everything else
    return "D", f"Too many failures ({n_fail}) — not worth sending"


# ── Estimate parlay payout ─────────────────────────────────────────────────────

def estimate_payout(legs: list) -> float:
    """
    Estimate parlay payout by multiplying decimal odds across all legs.
    Returns American-style payout (positive number representing profit on $100).
    """
    if not legs:
        return 0.0

    decimal_product = 1.0
    for leg in legs:
        odds = leg.odds if leg.odds != 0 else -110
        if odds > 0:
            decimal = 1 + (odds / 100)
        else:
            decimal = 1 + (100 / abs(odds))
        decimal_product *= decimal

    # Convert back to American
    profit_per_100 = (decimal_product - 1) * 100
    return round(profit_per_100, 0)


def calculate_parlay_ev(hit_prob: float, legs: list, stake: float = 100.0) -> float:
    """
    Parlay-level Expected Value on a given stake.

    EV = P(all hit) × (decimal_odds × stake) − P(any miss) × stake

    Uses the same decimal-odds product as estimate_payout() so the two
    are always consistent.
    """
    if not legs or hit_prob <= 0:
        return 0.0

    decimal_product = 1.0
    for leg in legs:
        odds = leg.odds if leg.odds != 0 else -110
        if odds > 0:
            decimal_product *= 1 + (odds / 100)
        else:
            decimal_product *= 1 + (100 / abs(odds))

    win  = hit_prob * (decimal_product * stake)
    lose = (1 - hit_prob) * stake
    return round(win - lose, 2)


# ── Full 7-step pipeline for a single candidate pick ──────────────────────────

def run_pick_through_engine(
    player: str,
    team: str,
    game: str,
    stat: str,
    direction: str,
    line: float,
    odds: float,
    prediction: float,
    stat_std: float,
    role: PlayerRole,
    game_script: GameScript,
    is_fade: bool = False,
    is_benefactor: bool = False,
    fade_target: str = "",
    public_pct: float = 0.0,
    line_decision: str = "RISK",
    shadow_hit_rates: dict = None,  # {"{player}:{stat}": {"rate": 0.xx, "total": n}}
    win_rate_context: dict = None,  # all stored win-rate learning: by_type, by_script, fade_roles
) -> Optional[SlipLeg]:
    """
    Run a candidate pick through steps 1-5.
    Returns a SlipLeg if it passes, None if it should be rejected.
    """

    # Step 1: Juice Test
    j = juice_test(odds)

    # Step 2: Public Pressure
    pp = public_pressure_check(player, stat, odds, public_pct, role.is_star)

    # Step 3: Game Script Fit (pre-check — full validation in step 6)
    is_home = team == game_script.home_team
    script_result = game_script_fit(
        SlipLeg(
            player=player, team=team, game=game,
            stat=stat, direction=direction, line=line, odds=odds,
            prediction=prediction, true_prob=0, implied_prob=0, ev=0,
            role=role.role, is_fade=is_fade, is_benefactor=is_benefactor,
            fade_target=fade_target, juice_flag=j.flag,
            game_script_label=game_script.label,
        ),
        game_script,
    )

    # Step 3b: Line value check (line vs player average)
    # For fades (UNDER), a "BAD" over line = inflated line = GOOD fade target — allow it
    line_rating = evaluate_line_value(line, prediction, odds)
    if line_rating == "BAD" and not is_fade:
        print(f"  [Engine] REJECT {player} {stat}: BAD line "
              f"(line {line} vs avg {prediction:.1f}, odds {odds})")
        return None

    # Step 4: Role alignment — soft edge weight, not a hard block.
    # Any player can score/rebound/assist regardless of role.
    # Role only nudges the edge bonus: +2% on primary stat, -2% on secondary.
    # Real data (EV, game logs, line value) is always the primary filter.
    role_primary_map = {
        ROLE_PRIMARY_SCORER:   ["points", "pra"],
        ROLE_PLAYMAKER_HUB:    ["assists", "pra"],
        ROLE_SECONDARY_SCORER: ["points", "pra"],
        ROLE_SPACER_SHOOTER:   ["3pm", "threes", "points"],
        ROLE_REBOUND_ANCHOR:   ["rebounds", "blocks"],
        ROLE_CONNECTOR:        ["pra"],
    }
    primary_stats = role_primary_map.get(role.role, ["points", "rebounds", "assists"])
    role_fits = True   # never a hard block — role is informational only

    # Step 5: EV Check
    ev = ev_check(prediction, line, odds, direction, stat_std)

    # Pull pace-aware edge bonus from game script result
    edge_bonus = script_result.get("edge_bonus", 0.0)

    # ── Shadow learning bonus ─────────────────────────────────────────────────
    # If the bot has graded shadow picks for this player+stat, use the hit rate
    # to directly boost or penalize confidence — this is the feedback loop.
    if shadow_hit_rates:
        sh_key  = f"{player.lower()}:{stat.lower()}"
        sh_data = shadow_hit_rates.get(sh_key)
        if sh_data and sh_data.get("total", 0) >= 10:
            sh_rate = sh_data["rate"]
            # Use actual historical hit rate as the primary true_prob.
            # This replaces the sigmoid model when we have enough real samples.
            ip_real = implied_probability(odds)
            if odds > 0:
                wp = odds / 100
            elif odds < 0:
                wp = 100 / abs(odds)
            else:
                wp = 0.909
            sh_ev_val = (sh_rate * wp) - (1 - sh_rate)
            ev = EVResult(
                true_prob      = round(sh_rate, 4),
                implied_prob   = round(ip_real, 4),
                ev             = round(sh_ev_val, 4),
                is_positive    = sh_rate > ip_real,
                recommendation = ev.recommendation,
            )
            print(f"  [HitRate] {player} {stat}: historical {sh_rate:.0%} (n={sh_data['total']}) used as true_prob | ev={sh_ev_val:.3f}")
        elif sh_data and sh_data.get("total", 0) >= 5:
            sh_rate = sh_data["rate"]
            # Fewer samples — use as edge_bonus only (not full override)
            if sh_rate >= 0.65:
                edge_bonus += 0.05
                print(f"  [Shadow] {player} {stat}: hit {sh_rate:.0%} → +0.05 edge")
            elif sh_rate >= 0.55:
                edge_bonus += 0.02
            elif sh_rate <= 0.35:
                edge_bonus -= 0.06
                print(f"  [Shadow] {player} {stat}: hit {sh_rate:.0%} → -0.06 edge")
            elif sh_rate <= 0.45:
                edge_bonus -= 0.03

    # ── Historical win-rate context (all stored learning data) ───────────────
    # Every pick uses what the bot has actually learned from settled results:
    #   • by_type  — which prop types (points/rebounds/assists) have been hitting
    #   • by_script — which game scripts actually predicted correct outcomes
    #   • fade_roles — fade vs benefactor vs neutral win rates
    if win_rate_context:
        wr_bonus = 0.0

        # 1. Prop-type win rate — e.g. if rebounds props are only hitting 42%
        #    the bot learned that stat is harder to predict → penalize it
        type_data = win_rate_context.get("by_type", {}).get(stat)
        if type_data and type_data.get("count", 0) >= 10:
            rate = type_data["win_rate"] / 100
            if rate >= 0.62:
                wr_bonus += 0.04
            elif rate >= 0.56:
                wr_bonus += 0.02
            elif rate <= 0.40:
                wr_bonus -= 0.05
            elif rate <= 0.46:
                wr_bonus -= 0.02

        # 2. Game-script win rate — if PACE_ADVANTAGE has been hitting 67%,
        #    picks in that script get a boost; if a script keeps losing, penalize
        script_data = win_rate_context.get("by_script", {}).get(game_script.label)
        if script_data and script_data.get("count", 0) >= 8:
            rate = script_data["win_rate"] / 100
            if rate >= 0.62:
                wr_bonus += 0.04
            elif rate >= 0.56:
                wr_bonus += 0.02
            elif rate <= 0.40:
                wr_bonus -= 0.05
            elif rate <= 0.46:
                wr_bonus -= 0.02

        # 3. Fade / benefactor role win rate — if fades are hitting 63%,
        #    boost fade picks; if benefactors keep missing, penalize them
        if is_fade:
            role_data = win_rate_context.get("fade_roles", {}).get("fade")
        elif is_benefactor:
            role_data = win_rate_context.get("fade_roles", {}).get("beneficiary")
        else:
            role_data = None
        if role_data and role_data.get("count", 0) >= 5:
            rate = role_data["win_rate"] / 100
            if rate >= 0.60:
                wr_bonus += 0.03
            elif rate <= 0.42:
                wr_bonus -= 0.04

        if wr_bonus != 0.0:
            print(
                f"  [WinRate] {player} {stat} | type={type_data['win_rate'] if type_data else 'n/a'}% "
                f"script={script_data['win_rate'] if script_data else 'n/a'}% "
                f"→ wr_bonus={wr_bonus:+.3f}"
            )
        edge_bonus += wr_bonus

    # Step 4b: Role alignment edge nudge (soft — never a hard block).
    # +2% when betting the player's primary stat (validates the role signal).
    # -2% when betting outside it (any prop is still allowed — just slightly
    #  less confident since it's an off-role performance).
    if stat in primary_stats:
        edge_bonus += 0.02
    else:
        edge_bonus -= 0.02

    # ELITE line gets an additional confidence boost
    if line_rating == "ELITE":
        edge_bonus += 0.08

    # Rejection rules
    if j.flag == "RED" and not ev.is_positive:
        print(f"  [Engine] REJECT {player} {stat}: RED juice + negative EV")
        return None

    if ev.ev < -0.05 and not is_fade:
        # Reject only strongly negative EV. Breakeven and mildly negative legs
        # still compete — the slip-level grading filters quality from there.
        # Fades always pass because the UNDER is the pick.
        print(f"  [Engine] REJECT {player} {stat}: EV too negative ({ev.ev:.3f} < -0.05)")
        return None

    if not script_result["fits"] and not is_fade:
        print(f"  [Engine] REJECT {player} {stat}: script mismatch — {script_result['reason']}")
        return None

    boosted_confidence = min(round((ev.true_prob + edge_bonus) * 100, 1), 99.0)
    print(f"  [Engine] line_rating={line_rating} edge_bonus={edge_bonus:+.2f} "
          f"confidence={boosted_confidence}%")

    raw_edge = round(ev.true_prob - ev.implied_prob, 4)

    return SlipLeg(
        player            = player,
        team              = team,
        game              = game,
        stat              = stat,
        direction         = direction,
        line              = line,
        odds              = odds,
        prediction        = prediction,
        true_prob         = ev.true_prob,
        implied_prob      = ev.implied_prob,
        ev                = ev.ev,
        role              = role.role,
        is_fade           = is_fade,
        is_benefactor     = is_benefactor,
        fade_target       = fade_target,
        juice_flag        = j.flag,
        game_script_label = game_script.label,
        confidence        = boosted_confidence,
        line_rating       = line_rating,
        line_decision     = line_decision,
        edge              = raw_edge,
        game_pace         = getattr(game_script, "pace", "AVERAGE_PACE"),
        game_phase        = "pregame",
    )


# ══════════════════════════════════════════════════════════════════════════════
# MASTER 11-LAYER PIPELINE
# Every pick — real or shadow — must pass all 11 layers before sending.
# Hard block at any layer = pick is dead, never sent.
# ══════════════════════════════════════════════════════════════════════════════

# Self-learning channel confidence floors (VIP/Free)
_channel_floors: dict = {
    "VIP":  {"floor": 72.0, "hits": 0, "total": 0},
    "FREE": {"floor": 65.0, "hits": 0, "total": 0},
}

# Self-learning Kelly fraction
_kelly_tracker: dict = {"fraction": 0.5, "roi_sum": 0.0, "bets": 0}

# Layer 9 adaptive-tier cache — refreshed at most once per 10 minutes
# so we never open a new DB connection for every pick in a run
_l9_tier_cache: dict = {"tier": "AVG", "thresh": {}, "ts": 0.0}


def _get_channel_floor(channel: str) -> float:
    """Return current self-calibrated confidence floor for VIP or FREE."""
    d = _channel_floors.get(channel.upper(), _channel_floors["VIP"])
    if d["total"] < 20:
        return d["floor"]
    hit_rate = d["hits"] / d["total"]
    if hit_rate >= 0.65:
        d["floor"] = max(d["floor"] - 0.5, 60.0)  # performing well → ease floor
    elif hit_rate < 0.55:
        d["floor"] = min(d["floor"] + 0.5, 82.0)  # underperforming → raise bar
    return d["floor"]


def record_channel_outcome(channel: str, hit: bool):
    """Update VIP/FREE floor self-learning after a pick is graded."""
    d = _channel_floors.get(channel.upper(), _channel_floors["VIP"])
    d["total"] += 1
    if hit:
        d["hits"] += 1


def _get_kelly_fraction() -> float:
    """Return current self-calibrated Kelly fraction (0.25–0.75)."""
    kt = _kelly_tracker
    if kt["bets"] < 15:
        return kt["fraction"]
    avg_roi = kt["roi_sum"] / kt["bets"]
    if avg_roi > 0.10:
        kt["fraction"] = min(kt["fraction"] + 0.02, 0.75)
    elif avg_roi < -0.05:
        kt["fraction"] = max(kt["fraction"] - 0.02, 0.25)
    return kt["fraction"]


def record_kelly_outcome(roi: float):
    """Update Kelly fraction self-learning after a pick is graded."""
    _kelly_tracker["roi_sum"] += roi
    _kelly_tracker["bets"]    += 1


def run_full_pipeline(
    player:          str,
    team:            str,
    game:            str,
    stat:            str,
    direction:       str,
    line:            float,
    odds:            float,
    prediction:      float,
    stat_std:        float,
    player_stats:    dict,
    game_script:     "GameScript",
    is_fade:         bool  = False,
    is_benefactor:   bool  = False,
    fade_target:     str   = "",
    public_pct:      float = 0.0,
    line_movement:   float = 0.0,
    line_decision:   str   = "RISK",
    shadow_hit_rates: dict = None,
    win_rate_context: dict = None,
    ml_prediction:   float = None,
    elo_edge:        float = None,
    h2h_edge:        float = None,
    context_tracker: "ContextTracker" = None,
    injury_report:   dict  = None,
    back_to_back:    bool  = False,
    target_channel:  str   = "VIP",
    is_shadow:       bool  = False,
    shot_status:     str   = "NEUTRAL",
    shot_detail:     str   = "",
) -> Optional[SlipLeg]:
    """
    Run a candidate pick through all 11 layers. Hard block at any failure.
    Returns a SlipLeg if golden, None if rejected at any layer.

    Layer 1  — Game Context Detection
    Layer 2  — Line & Odds Evaluation
    Layer 3  — Model & Prediction (ML weight)
    Layer 4  — Script Fit & Fade Signal
    Layer 5  — Player & Role Analysis
    Layer 6  — Historical Learning
    Layer 7  — EV & Validation Gate
    Layer 8  — Live Context (ContextTracker)
    Layer 9  — Adaptive Thresholds
    Layer 10 — Unit Sizing (Kelly)
    Layer 11 — Send Decision (channel floor)
    """
    tag = f"[{'SHADOW' if is_shadow else 'PICK'}][{player}|{stat}]"

    # ── Layer 1: Game Context Detection ───────────────────────────────────────
    # Block: back-to-back fatigue on a road team + no value line
    if back_to_back and line_decision == "RISK":
        print(f"  {tag} L1 BLOCK: B2B road + RISK line")
        return None
    # Block: no valid game script
    if not game_script or not getattr(game_script, "label", ""):
        print(f"  {tag} L1 BLOCK: missing game script")
        return None

    # ── Layer 2: Line & Odds Evaluation ───────────────────────────────────────
    j = juice_test(odds)
    if j.flag == "BLOCKED":
        print(f"  {tag} L2 BLOCK: juice wall ({odds})")
        return None

    line_rating = evaluate_line_value(line, prediction, odds)
    if line_rating == "BAD" and not is_fade:
        print(f"  {tag} L2 BLOCK: BAD line (line={line} avg={prediction:.1f})")
        return None

    # ── Layer 3: Model & Prediction ───────────────────────────────────────────
    # ML weight is self-adjusting; below 0.87 the model is cold → stricter gate
    ml_w    = _get_ml_weight()
    ml_conf = 0.0
    if ml_prediction is not None:
        ml_conf = float(ml_prediction) * ml_w
        # Hard block: ML says <40% AND no ELO/H2H support
        if ml_conf < 0.40 and (elo_edge is None or elo_edge <= 0) \
                            and (h2h_edge is None or h2h_edge <= 0):
            print(f"  {tag} L3 BLOCK: cold ML ({ml_conf:.0%}) + no ELO/H2H support")
            return None

    # ── Layer 4: Script Fit & Fade Signal ─────────────────────────────────────
    is_home = team == game_script.home_team
    _tmp_leg = SlipLeg(
        player=player, team=team, game=game,
        stat=stat, direction=direction, line=line, odds=odds,
        prediction=prediction, true_prob=0, implied_prob=0, ev=0,
        role="", is_fade=is_fade, is_benefactor=is_benefactor,
        fade_target=fade_target, juice_flag=j.flag,
        game_script_label=game_script.label,
    )
    script_result = game_script_fit(_tmp_leg, game_script)

    pp = public_pressure_check(
        player, stat, odds, public_pct,
        is_star=(player_stats.get("usage_rate", 0) >= 28),
        line_movement=line_movement,
        bet_type=stat,
    )
    if pp["rlm_confirmed"] and pp["is_public_heavy"]:
        print(f"  {tag} L4 RLM+PUBLIC confluence confirmed → fade signal valid")
    elif pp["is_public_heavy"] and not pp["rlm_confirmed"]:
        print(f"  {tag} L4 public heavy ({public_pct:.0f}%) but no RLM (move={line_movement:+.1f}) → fade blocked")

    # Block: script clearly doesn't fit AND it's not a fade AND not a benefactor
    if not script_result["fits"] and not is_fade and not is_benefactor:
        print(f"  {tag} L4 BLOCK: script mismatch — {script_result['reason']}")
        return None

    # ── Layer 5: Player & Role Analysis ───────────────────────────────────────
    role_str  = assign_role_v2(player_stats)
    role_obj  = PlayerRole(player=player, team=team, role=role_str, is_star=(role_str == ROLE_PRIMARY_SCORER))

    # Role vs prop mismatch hard-blocks
    _role_mismatches = {
        ROLE_PRIMARY_SCORER:   ["blocks"],
        ROLE_PLAYMAKER_HUB:    ["rebounds", "blocks"],
        ROLE_SECONDARY_SCORER: ["rebounds", "blocks"],
        ROLE_SPACER_SHOOTER:   ["rebounds", "blocks", "assists"],
        ROLE_REBOUND_ANCHOR:   ["points", "assists", "threes", "3pm"],
        ROLE_CONNECTOR:        [],
    }
    if stat in _role_mismatches.get(role_str, []):
        print(f"  {tag} L5 BLOCK: role mismatch ({role_str} vs {stat})")
        return None

    # Player context: script fit mult + injury mult
    ctx = build_player_context(player, role_str, stat, game_script.label, injury_report)
    ctx_mult = ctx.get("final_edge_mult", 1.0)

    # Block: injury context tanks the edge below 80% of base
    if ctx_mult < 0.80 and not is_fade:
        print(f"  {tag} L5 BLOCK: context mult too low ({ctx_mult:.2f})")
        return None

    # ── Layer 6: Historical Learning ──────────────────────────────────────────
    _pe_auto_load()
    bet_ctx = {
        "betType":     stat,
        "game_pace":   getattr(game_script, "pace", "AVERAGE_PACE"),
        "script":      game_script.label,
        "game_phase":  "PREGAME",
        "role":        role_str,
        "tier":        "STRONG BET" if line_rating in ("ELITE", "GOOD") else "LEAN",
    }
    ctx_key     = build_context(bet_ctx)
    pat_score   = evaluate_pattern(ctx_key) + evaluate_meta(ctx_key)
    exp_pen     = exposure_penalty(bet_ctx)
    conf_pen    = conflict_penalty(bet_ctx)
    l6_adj      = pat_score - exp_pen - conf_pen

    # Block: pattern engine strongly against this pick (≥30 samples, very negative)
    if l6_adj <= -0.25:
        print(f"  {tag} L6 BLOCK: pattern score {l6_adj:.3f} — historical evidence against")
        return None

    # Shadow hit rate learning — edge bonus for 5-9 samples.
    # Players with 10+ samples get a full EV override at L7 instead;
    # applying sh_bonus here too would double-count the signal.
    sh_bonus = 0.0
    if shadow_hit_rates:
        sh_key  = f"{player.lower()}:{stat.lower()}"
        sh_data = shadow_hit_rates.get(sh_key)
        sh_total = sh_data.get("total", 0) if sh_data else 0
        if sh_data and 5 <= sh_total < 10:
            sh_rate = sh_data["rate"]
            if sh_rate >= 0.65:   sh_bonus =  0.05
            elif sh_rate >= 0.55: sh_bonus =  0.02
            elif sh_rate <= 0.35: sh_bonus = -0.06
            elif sh_rate <= 0.45: sh_bonus = -0.03

    # Win-rate context bonus
    wr_bonus = 0.0
    if win_rate_context:
        type_d   = win_rate_context.get("by_type",   {}).get(stat)
        script_d = win_rate_context.get("by_script", {}).get(game_script.label)
        if type_d and type_d.get("count", 0) >= 10:
            r = type_d["win_rate"] / 100
            wr_bonus += 0.04 if r >= 0.62 else (0.02 if r >= 0.56 else (-0.05 if r <= 0.40 else (-0.02 if r <= 0.46 else 0)))
        if script_d and script_d.get("count", 0) >= 8:
            r = script_d["win_rate"] / 100
            wr_bonus += 0.04 if r >= 0.62 else (0.02 if r >= 0.56 else (-0.05 if r <= 0.40 else (-0.02 if r <= 0.46 else 0)))

    # Causality penalty — check live causality events against historical hit rates
    _live_causes = []
    if context_tracker is not None:
        try:
            _live_causes = [
                c for entry in context_tracker.get_causality_log()
                for c in entry.get("causes", [])
            ]
        except Exception:
            pass
    causal_mult = get_causality_penalty(stat, role_str, _live_causes)
    if causal_mult == 0.0:
        print(f"  {tag} L6 BLOCK: causality hard-block ({len(_live_causes)} events, "
              f"stat={stat} role={role_str})")
        return None

    # ── Layer 7: EV & Validation Gate ─────────────────────────────────────────
    ev = ev_check(prediction, line, odds, direction, stat_std)

    # Override true_prob with real historical hit rate when enough data exists.
    # This is more accurate than the sigmoid model because it reflects actual
    # player-vs-line outcomes rather than an estimated distribution.
    if shadow_hit_rates:
        sh_key_l7  = f"{player.lower()}:{stat.lower()}"
        sh_data_l7 = shadow_hit_rates.get(sh_key_l7)
        if sh_data_l7 and sh_data_l7.get("total", 0) >= 10:
            sh_rate_l7 = sh_data_l7["rate"]
            ip_l7 = implied_probability(odds)
            wp_l7 = (odds / 100) if odds > 0 else (100 / abs(odds)) if odds < 0 else 0.909
            sh_ev_l7 = (sh_rate_l7 * wp_l7) - (1 - sh_rate_l7)
            ev = EVResult(
                true_prob      = round(sh_rate_l7, 4),
                implied_prob   = round(ip_l7, 4),
                ev             = round(sh_ev_l7, 4),
                is_positive    = sh_rate_l7 > ip_l7,
                recommendation = ev.recommendation,
            )
            print(f"  [HitRate-L7] {player} {stat}: historical {sh_rate_l7:.0%} (n={sh_data_l7['total']}) used as true_prob | ev={sh_ev_l7:.3f}")

    if j.flag == "RED" and not ev.is_positive:
        print(f"  {tag} L7 BLOCK: RED juice + negative EV")
        return None
    if ev.ev < -0.05 and not is_fade:
        # Reject only strongly negative EV — breakeven legs compete at slip level.
        print(f"  {tag} L7 BLOCK: EV too negative ({ev.ev:.3f} < -0.05)")
        return None

    # ── Layer 8: Live Context (ContextTracker) ─────────────────────────────────
    l8_flag = ""
    if context_tracker is not None:
        live_script = context_tracker.current_script()
        live_flow   = context_tracker.current_flow()
        # Block: game has blown out AND we're betting a star's points OVER
        if live_flow == "BLOWOUT" and stat in ("points", "pra") \
                and direction == "OVER" and not is_fade:
            print(f"  {tag} L8 BLOCK: live BLOWOUT — star minutes at risk")
            return None
        # Flag: script has shifted from prediction
        if live_script and live_script != game_script.label:
            l8_flag = f"⚠️ Script shifted {game_script.label} → {live_script}"
            context_tracker.flag_pick(f"{player}|{stat}", l8_flag)

    # ── Layer 9: Adaptive Thresholds ──────────────────────────────────────────
    # Cache the tier for 10 minutes — avoids opening a new DB connection per pick
    import time as _time9
    _now9 = _time9.time()
    if _now9 - _l9_tier_cache["ts"] > 600:
        try:
            from bot.adaptive_thresholds import apply_thresholds_to_engine, classify_tier
            import psycopg2 as _pg9, os as _os9
            _conn9 = _pg9.connect(_os9.environ.get("DATABASE_URL", ""))
            _tier9_fresh = classify_tier(_conn9)
            _conn9.close()
            _thresh9_fresh = apply_thresholds_to_engine(_tier9_fresh)
            _l9_tier_cache["tier"]   = _tier9_fresh
            _l9_tier_cache["thresh"] = _thresh9_fresh
            _l9_tier_cache["ts"]     = _now9
        except Exception:
            _l9_tier_cache["tier"]   = "AVG"
            _l9_tier_cache["thresh"] = {}
            _l9_tier_cache["ts"]     = _now9
    _tier9    = _l9_tier_cache["tier"]
    _min_conf = _l9_tier_cache["thresh"].get("min_confidence", 0)

    # ── Layer 10: Unit Sizing ──────────────────────────────────────────────────
    kelly_frac = _get_kelly_fraction()
    kelly_prob  = ev.true_prob
    kelly_dec   = abs(odds) / 100 + 1 if odds > 0 else 100 / abs(odds) + 1
    kelly_raw   = (kelly_prob * kelly_dec - 1) / (kelly_dec - 1) if kelly_dec > 1 else 0
    kelly_bet   = kelly_raw * kelly_frac
    if kelly_raw < -0.05:
        # Only hard-block when Kelly strongly says no edge — matches EV gate.
        # Breakeven legs (kelly_raw ≈ 0) get 1-unit sizing and compete at slip level.
        print(f"  {tag} L10 BLOCK: Kelly strongly negative ({kelly_raw:.3f})")
        return None
    kelly_bet   = max(kelly_bet, 0)
    kelly_units = 3 if kelly_bet >= 0.08 else (2 if kelly_bet >= 0.04 else 1)

    # ── Build final confidence ─────────────────────────────────────────────────
    edge_bonus  = script_result.get("edge_bonus", 0.0)
    edge_bonus += sh_bonus + wr_bonus + l6_adj * 0.05
    edge_bonus += 0.08 if line_rating == "ELITE" else 0
    edge_bonus += 0.02 if stat in {
        ROLE_PRIMARY_SCORER:   ["points", "pra"],
        ROLE_PLAYMAKER_HUB:    ["assists", "pra"],
        ROLE_SECONDARY_SCORER: ["points", "pra"],
        ROLE_SPACER_SHOOTER:   ["3pm", "threes", "points"],
        ROLE_REBOUND_ANCHOR:   ["rebounds", "blocks"],
        ROLE_CONNECTOR:        ["pra"],
    }.get(role_str, []) else -0.02
    edge_bonus += (ctx_mult - 1.0) * 0.5   # role × script × injury multiplier

    if j.flag == "RED":       edge_bonus -= 0.04
    elif j.flag == "GREEN":   edge_bonus += 0.02

    if shot_status == "HOT":
        edge_bonus += 0.04
        print(f"  {tag} L8 SHOT: HOT streak → +0.04 edge ({shot_detail})")
    elif shot_status == "COLD":
        edge_bonus -= 0.04
        print(f"  {tag} L8 SHOT: COLD streak → -0.04 edge ({shot_detail})")

    raw_conf   = (ev.true_prob + edge_bonus) * causal_mult
    final_conf = min(round(raw_conf * 100, 1), 99.0)

    # Adaptive threshold minimum
    if final_conf < _min_conf:
        print(f"  {tag} L9 BLOCK: conf {final_conf}% < adaptive floor {_min_conf}% ({_tier9})")
        return None

    # ── Layer 11: Send Decision ────────────────────────────────────────────────
    channel_floor = _get_channel_floor(target_channel)
    if final_conf < channel_floor:
        print(f"  {tag} L11 BLOCK: conf {final_conf}% < {target_channel} floor {channel_floor}%")
        return None

    raw_edge = round(ev.true_prob - ev.implied_prob, 4)
    print(f"  {tag} ✅ GOLDEN — conf={final_conf}% | role={role_str} | units={kelly_units}u"
          f" | L6={l6_adj:+.3f} | ml_w={ml_w:.2f}" + (f" | FLAG:{l8_flag}" if l8_flag else ""))

    return SlipLeg(
        player            = player,
        team              = team,
        game              = game,
        stat              = stat,
        direction         = direction,
        line              = line,
        odds              = odds,
        prediction        = prediction,
        true_prob         = ev.true_prob,
        implied_prob      = ev.implied_prob,
        ev                = ev.ev,
        role              = role_str,
        is_fade           = is_fade,
        is_benefactor     = is_benefactor,
        fade_target       = fade_target,
        juice_flag        = j.flag,
        game_script_label = game_script.label,
        confidence        = final_conf,
        line_rating       = line_rating,
        line_decision     = line_decision,
        edge              = raw_edge,
        game_pace         = getattr(game_script, "pace", "AVERAGE_PACE"),
        game_phase        = "pregame",
    )


# ── Build and validate the final slip ─────────────────────────────────────────

def _swap_risk_legs(legs: list, all_candidates: list) -> list:
    """
    For each RISK leg (line set above the player's average), attempt to find a
    SAFE alternative from the same game and same stat that is not already in the
    slip.  Mirrors swap_over_juiced() from the analytics layer.

    Pinned fades/benefactors are never swapped — only fill legs are eligible.
    """
    used_players = {l.player for l in legs}
    result = []
    for leg in legs:
        # Only swap non-pinned RISK fill legs
        if leg.line_decision == "RISK" and not leg.is_fade and not leg.is_benefactor:
            alt = next(
                (
                    c for c in all_candidates
                    if c.game == leg.game
                    and c.stat == leg.stat
                    and c.player != leg.player
                    and c.player not in used_players
                    and c.line_decision == "SAFE"
                ),
                None,
            )
            if alt:
                used_players.discard(leg.player)
                used_players.add(alt.player)
                result.append(alt)
                continue
        result.append(leg)
    return result


def _assemble_slip_attempt(
    fades: list,
    filtered_bene: list,
    fill_pool: list,
    target_legs: int,
    game_script: Optional[GameScript],
    all_candidates: list,
) -> Optional[Slip]:
    """
    Try to assemble a single slip of `target_legs` from pinned fades/benefactors
    plus a (pre-shuffled) fill pool.  Returns None if the slip grades D.
    Internal helper for build_and_grade_slip().
    """
    pinned = fades + filtered_bene
    slots_left = target_legs - len(pinned)

    if slots_left < 0:
        # More pinned legs than the target — just use the pinned legs capped at target
        legs = pinned[:target_legs]
    else:
        legs = pinned + fill_pool[:slots_left]

    if len(legs) < MIN_LEGS:
        return None

    # Swap RISK fill legs for SAFE alternatives where possible
    legs = _swap_risk_legs(legs, all_candidates)

    stat_diversity: dict = {}
    for l in legs:
        stat_diversity[l.stat] = stat_diversity.get(l.stat, 0) + 1

    validation = validate_slip(legs, game_script)
    grade, grade_reason = grade_slip(legs, validation)

    if grade == "D":
        return None

    hit_prob = compute_parlay_hit_prob(legs)
    slip_ev  = calculate_parlay_ev(hit_prob, legs)
    payout   = estimate_payout(legs)
    send_vip  = grade in ("A", "B", "C")   # C = best available tonight
    send_free = grade == "A"

    return Slip(
        legs             = legs,
        grade            = grade,
        grade_reason     = grade_reason,
        estimated_payout = payout,
        fades            = [l for l in legs if l.is_fade],
        benefactors      = [l for l in legs if l.is_benefactor],
        stat_diversity   = stat_diversity,
        validation       = validation,
        game_script      = game_script,
        send_to_vip      = send_vip,
        send_to_free     = send_free,
        parlay_hit_prob  = hit_prob,
        parlay_ev        = slip_ev,
    )


def build_and_grade_slip(
    candidates: list,      # list of SlipLeg objects
    game_script: Optional[GameScript] = None,
) -> Optional[Slip]:
    """
    Monte Carlo slip optimizer — runs MONTE_CARLO_ATTEMPTS shuffles for each
    target size in SLIP_TARGET_SIZES [3, 5, 7], then returns the single
    highest-EV non-D slip found across all combinations.

    Fades and benefactors are pinned at the front of every attempt (they
    always pass the 7-step engine).  The fill pool is shuffled each attempt
    so different combinations of supporting legs are evaluated, exposing the
    highest-value arrangement the candidate pool can produce.
    """
    if not candidates:
        return None

    # ── Pin fades — up to MAX_FADES_PER_TEAM per team, both teams represented ──
    all_fades = sorted(
        [l for l in candidates if l.is_fade],
        key=lambda x: x.ev, reverse=True
    )
    # Group by team, cap per team
    _fades_by_team: dict = {}
    for f in all_fades:
        t = f.team or "unknown"
        if len(_fades_by_team.get(t, [])) < MAX_FADES_PER_TEAM:
            _fades_by_team.setdefault(t, []).append(f)

    # Interleave teams so both are represented — alternating strongest from each
    _teams_ordered = sorted(_fades_by_team.keys(),
                            key=lambda t: _fades_by_team[t][0].ev, reverse=True)
    fades: list = []
    _iters = {t: iter(_fades_by_team[t]) for t in _teams_ordered}
    while len(fades) < MAX_FADES:
        added = False
        for t in _teams_ordered:
            if len(fades) >= MAX_FADES:
                break
            nxt = next(_iters[t], None)
            if nxt:
                fades.append(nxt)
                added = True
        if not added:
            break

    # ── Deduplicate benefactors (1 per player) ────────────────────────────────
    seen_players: set = {l.player for l in fades}
    filtered_bene = []
    for b in [l for l in candidates if l.is_benefactor]:
        if b.player not in seen_players:
            seen_players.add(b.player)
            filtered_bene.append(b)

    # ── Build fill pool (everything not pinned) ───────────────────────────────
    _fill_all = [
        l for l in candidates
        if not l.is_fade and not l.is_benefactor
        and l.player not in seen_players
    ]
    # Max 1 leg per player in fill pool — keep highest-confidence leg only.
    # Prevents the same player stacking multiple props in a single slip
    # (e.g. Vassell points + rebounds + assists = 3 correlated legs that all
    # win/lose together, massively amplifying variance).
    _fill_dedup: dict = {}
    for _fl in sorted(_fill_all, key=lambda l: l.confidence, reverse=True):
        if _fl.player not in _fill_dedup:
            _fill_dedup[_fl.player] = _fl
    fill_pool_base = list(_fill_dedup.values())

    # ── Script confidence penalty — off-script legs stay in pool but take
    #    a -10% confidence hit so they lose to script-aligned legs naturally
    #    across Monte Carlo shuffles. The live monitor validates/learns from
    #    outcomes so the engine self-calibrates over time. ─────────────────
    if game_script:
        for leg in fill_pool_base:
            fit = game_script_fit(leg, game_script)
            if not fit["fits"]:
                leg.confidence = max(0.0, leg.confidence - 10.0)
                print(f"  [ScriptPenalty] {leg.player} -{10}% conf → {leg.confidence:.1f}% ({fit['reason']})")

    best_slip: Optional[Slip] = None
    best_ev = float("-inf")
    total_attempts = 0

    for target in SLIP_TARGET_SIZES:
        capped_target = min(target, MAX_LEGS)
        for _ in range(MONTE_CARLO_ATTEMPTS):
            total_attempts += 1
            fill = fill_pool_base[:]
            random.shuffle(fill)
            slip = _assemble_slip_attempt(
                fades, filtered_bene, fill, capped_target, game_script, candidates
            )
            if slip is not None and slip.parlay_ev > best_ev:
                best_ev   = slip.parlay_ev
                best_slip = slip

    if best_slip is None:
        print(f"  [Slip] Monte Carlo ({total_attempts} attempts) — no valid slip found")
    else:
        print(
            f"  [Slip] Monte Carlo best: {len(best_slip.legs)}-leg "
            f"| Grade {best_slip.grade} | EV={best_slip.parlay_ev:.2f} "
            f"| Hit%={best_slip.parlay_hit_prob:.1%} "
            f"({total_attempts} attempts evaluated)"
        )

    return best_slip


# =============================================================================
# CONTEXT-AWARE PATTERN LEARNING ENGINE
# =============================================================================
# All improvements requested by user — added on top of existing engine.
# Nothing above this line is changed.
# =============================================================================

import json as _jpe
import math as _mpe

# ── In-memory caches (persisted to / loaded from learning_data table) ─────────
_pattern_db:          dict = {}   # str(context_key) → {sharp, lucky, overconf, fade, total}
_meta_db:             dict = {}   # str(key[:3])      → {score, count}
_exposure_tracker:    dict = {}   # str(context_key)  → int
_conflict_db:         dict = {}   # str(context_key)  → {sharp, overconf, total}
_pattern_adjustments: dict = {}   # str(context_key)  → float

# ── Causality hit-rate map (cause_type|stat|role → {wins, losses, total}) ──────
# Persisted to DB; survives restart and errors
_causality_hit_rates: dict = {}   # e.g. "STAR_QUIET|points|PRIMARY_SCORER" → {wins,losses,total}

# ── Causality decay buckets — how aggressively to decay each situation ─────────
_CAUSALITY_DECAY = {
    "clean_win":   0.992,   # no events, pick won cleanly → slow decay, reinforce
    "causal_win":  0.965,   # events fired but pick still won → faster decay (lucky)
    "causal_loss": 0.975,   # events fired, pick lost → targeted decay (context broke it)
    "blind_loss":  0.930,   # no events, pick lost → aggressive decay (model was wrong)
}

_pe_config = {
    "decay_rate":    0.98,
    "learning_rate": 0.05,
    "min_samples":   0,
}

# ── Auto-load flag — prevents losing patterns on restart / error ───────────────
_pe_loaded: bool = False


def _pe_auto_load():
    """
    Called lazily the first time gate_pick() or run_learning_cycle() runs.
    Opens its own DB connection so nothing is lost across restarts/crashes.
    Fail-open: if DB is unreachable, patterns stay empty and gate stays off.
    """
    global _pe_loaded
    if _pe_loaded:
        return
    _pe_loaded = True   # set now to prevent re-entry on error
    try:
        import os as _os
        import psycopg2 as _pg2
        _db_url = _os.environ.get("DATABASE_URL", "")
        if not _db_url:
            print("[PatternEngine] no DATABASE_URL — skipping auto-load")
            return
        _conn = _pg2.connect(_db_url)
        _pe_load(_conn)
        _conn.close()
        print(f"[PatternEngine] auto-loaded on startup/restart: {len(_pattern_db)} patterns")
    except Exception as _ale:
        print(f"[PatternEngine] auto-load error (fail-open): {_ale}")


# ─── 1. Decision grading ──────────────────────────────────────────────────────

def grade_decision(bet: dict) -> str:
    """Grade a settled bet: sharp / lucky / overconfident / correct_fade."""
    try:
        result     = bet.get("result", "")
        prediction = float(bet.get("prediction") or 0.0)
        line       = float(bet.get("line") or 0.0)
        raw_conf   = float(bet.get("confidence") or 50)   # 'or 50' handles None safely
        conf       = (raw_conf / 100.0) if raw_conf > 1.0 else raw_conf
        edge       = (prediction - line) if line else (conf - 0.5)

        if result == "win"  and edge > 0:  return "sharp"
        if result == "win"  and edge <= 0: return "lucky"
        if result == "loss" and edge > 0:  return "overconfident"
        if result == "loss" and edge <= 0: return "correct_fade"
        return "neutral"
    except Exception:
        return "neutral"   # any bad data → treat as neutral, never crash the cycle


# ─── 2. Context key ───────────────────────────────────────────────────────────

# ── Allowed values for each fingerprint field ─────────────────────────────────
_ALLOWED_PACE  = {"HALFCOURT", "SLOW_PACED", "AVERAGE_PACE", "UPTEMPO", "TRANSITION_HEAVY"}
_ALLOWED_SCRIPT = {
    "BLOWOUT_DEFENSIVE_BATTLE", "BLOWOUT_NORMAL_SCORING", "BLOWOUT_HIGH_SCORING", "BLOWOUT_SHOOTOUT",
    "DOUBLE_DIGIT_LEAD_DEFENSIVE_BATTLE", "DOUBLE_DIGIT_LEAD_NORMAL_SCORING",
    "DOUBLE_DIGIT_LEAD_HIGH_SCORING", "DOUBLE_DIGIT_LEAD_SHOOTOUT",
    "COMFORTABLE_LEAD_DEFENSIVE_BATTLE", "COMFORTABLE_LEAD_NORMAL_SCORING",
    "COMFORTABLE_LEAD_HIGH_SCORING", "COMFORTABLE_LEAD_SHOOTOUT",
    "COMPETITIVE_DEFENSIVE_BATTLE", "COMPETITIVE_NORMAL_SCORING",
    "COMPETITIVE_HIGH_SCORING", "COMPETITIVE_SHOOTOUT",
    "TIGHT_GAME_DEFENSIVE_BATTLE", "TIGHT_GAME_NORMAL_SCORING",
    "TIGHT_GAME_HIGH_SCORING", "TIGHT_GAME_SHOOTOUT",
}
_ALLOWED_PHASE = {"PREGAME", "LIVE_EARLY", "LIVE_MID", "LIVE_LATE", "HALFTIME"}
_ALLOWED_ROLE  = {
    "PRIMARY_SCORER", "PLAYMAKER_HUB", "SECONDARY_SCORER",
    "SPACER_SHOOTER", "REBOUND_ANCHOR", "CONNECTOR",
    "go_to_scorer", "floor_general", "glass_cleaner", "rim_anchor",
    "spot_up_shooter", "combo_creator", "sixth_man", "utility_player",
    "TEAM", "UNKNOWN",
}
_ALLOWED_TIER  = {"STRONG BET", "LEAN", "PASS"}

# ── Migration map: old labels → new labels ────────────────────────────────────
_PACE_MIGRATION = {
    "GRIND": "HALFCOURT",
    "MID":   "AVERAGE_PACE",
    "HIGH":  "UPTEMPO",
}
_SCRIPT_MIGRATION = {
    "GRIND_BLOWOUT":   "BLOWOUT_DEFENSIVE_BATTLE",
    "GRIND_CLOSE":     "TIGHT_GAME_DEFENSIVE_BATTLE",
    "GRIND_MODERATE":  "COMPETITIVE_DEFENSIVE_BATTLE",
    "MID_BLOWOUT":     "BLOWOUT_NORMAL_SCORING",
    "MID_CLOSE":       "TIGHT_GAME_NORMAL_SCORING",
    "MID_MODERATE":    "COMPETITIVE_NORMAL_SCORING",
    "HIGH_BLOWOUT":    "BLOWOUT_HIGH_SCORING",
    "HIGH_CLOSE":      "TIGHT_GAME_HIGH_SCORING",
    "HIGH_MODERATE":   "COMPETITIVE_HIGH_SCORING",
    "NORMAL":          "COMPETITIVE_NORMAL_SCORING",  # bare default from old code
}


def _normalize_pace(raw: str) -> str:
    v = str(raw or "").upper().strip()
    return _PACE_MIGRATION.get(v, v) if v in _PACE_MIGRATION else (v if v in _ALLOWED_PACE else "AVERAGE_PACE")


def _normalize_script(raw: str) -> str:
    v = str(raw or "").upper().strip()
    return _SCRIPT_MIGRATION.get(v, v) if v in _SCRIPT_MIGRATION else (v if v in _ALLOWED_SCRIPT else "COMPETITIVE_NORMAL_SCORING")


def _normalize_phase(raw: str) -> str:
    v = str(raw or "").upper().strip()
    return v if v in _ALLOWED_PHASE else "PREGAME"


def _normalize_role(raw: str) -> str:
    v = str(raw or "").upper().strip()
    return v if v in _ALLOWED_ROLE else "UNKNOWN"


def _normalize_tier(raw: str) -> str:
    v = str(raw or "").strip()
    return v if v in _ALLOWED_TIER else "LEAN"


def build_context(bet: dict) -> tuple:
    """6-field normalized fingerprint: (bet_type, pace, script, phase, role, tier).
    All fields are canonicalized + old labels migrated to new terms automatically.
    """
    raw_type   = bet.get("betType") or bet.get("bet_type") or "UNKNOWN"
    raw_pace   = bet.get("game_pace") or "AVERAGE_PACE"
    raw_script = bet.get("script") or bet.get("game_script") or "COMPETITIVE_NORMAL_SCORING"
    raw_phase  = bet.get("game_phase") or "PREGAME"
    raw_role   = bet.get("role") or "UNKNOWN"
    raw_tier   = bet.get("tier") or "LEAN"

    return (
        raw_type.upper() if raw_type != "UNKNOWN" else "UNKNOWN",
        _normalize_pace(raw_pace),
        _normalize_script(raw_script),
        _normalize_phase(raw_phase),
        _normalize_role(raw_role),
        _normalize_tier(raw_tier),
    )


def _pk(key: tuple) -> str:
    return str(key)


# ─── 3. Pattern storage & scoring ────────────────────────────────────────────

def store_pattern(key: tuple, grade: str):
    k = _pk(key)
    if k not in _pattern_db:
        _pattern_db[k] = {"sharp": 0, "lucky": 0, "overconf": 0, "fade": 0, "total": 0}
    d = _pattern_db[k]
    if grade == "sharp":            d["sharp"]   += 1
    elif grade == "lucky":          d["lucky"]   += 1
    elif grade == "overconfident":  d["overconf"] += 1
    elif grade == "correct_fade":   d["fade"]    += 1
    d["total"] += 1


def evaluate_pattern(key: tuple) -> float:
    """Score a pattern. Applies sample weight gate + Laplace smoothing."""
    k    = _pk(key)
    data = _pattern_db.get(k)
    if not data or data["total"] == 0:
        return 0.0
    total = data["total"]
    # Sample weight gate
    if total < 30:   weight = 0.0
    elif total < 75: weight = 0.5
    else:            weight = 1.0
    raw = (data["sharp"] - data["overconf"]) / total
    return raw * weight


# ─── 4. Meta pattern ─────────────────────────────────────────────────────────

def update_meta(key: tuple, grade: str):
    mk = _pk(key[:3])
    if mk not in _meta_db:
        _meta_db[mk] = {"score": 0, "count": 0}
    if grade == "sharp":           _meta_db[mk]["score"] += 1
    elif grade == "overconfident": _meta_db[mk]["score"] -= 1
    _meta_db[mk]["count"] += 1


def evaluate_meta(key: tuple) -> float:
    mk   = _pk(key[:3])
    data = _meta_db.get(mk)
    if not data or data["count"] == 0:
        return 0.0
    return data["score"] / data["count"]


# ─── 5. Exposure ─────────────────────────────────────────────────────────────

def track_exposure(key: tuple):
    k = _pk(key)
    _exposure_tracker[k] = _exposure_tracker.get(k, 0) + 1


def exposure_penalty(key: tuple) -> float:
    k     = _pk(key)
    exp   = _exposure_tracker.get(k, 0)
    total = sum(_exposure_tracker.values()) + 1
    # Cap at -0.15 — prevents accumulated exposure from dragging high-confidence
    # picks below the LEAN gate and silently blocking them from being saved.
    return max(-0.15, -(exp / total))


# ─── 6. Conflict detection ────────────────────────────────────────────────────

def track_conflict(key: tuple, grade: str):
    k = _pk(key)
    if k not in _conflict_db:
        _conflict_db[k] = {"sharp": 0, "overconf": 0, "total": 0}
    if grade == "sharp":           _conflict_db[k]["sharp"]   += 1
    elif grade == "overconfident": _conflict_db[k]["overconf"] += 1
    _conflict_db[k]["total"] += 1


def conflict_penalty(key: tuple) -> float:
    k    = _pk(key)
    data = _conflict_db.get(k)
    if not data or data["total"] == 0:
        return 0.0
    s, o = data["sharp"], data["overconf"]
    if s > 0 and o > 0:
        return -(min(s, o) / data["total"])
    return 0.0


# ─── 7. Combined adjustment ───────────────────────────────────────────────────

def get_adjustment(bet: dict) -> float:
    """Sum of all pattern signals for a given bet context."""
    key = build_context(bet)
    return (
        evaluate_pattern(key)
        + evaluate_meta(key)
        + exposure_penalty(key)
        + _pattern_adjustments.get(_pk(key), 0.0)
        + conflict_penalty(key)
    )


# ─── 8. Process one settled bet ──────────────────────────────────────────────

def process_bet(bet: dict):
    """Called after settlement — grades + stores pattern for this bet."""
    grade = grade_decision(bet)
    key   = build_context(bet)
    store_pattern(key, grade)
    update_meta(key, grade)
    track_exposure(key)
    track_conflict(key, grade)


# ─── 9. Pattern analysis ─────────────────────────────────────────────────────

def analyze_patterns() -> list:
    insights = []
    totals   = [d["total"] for d in _pattern_db.values() if d["total"] > 0]
    avg_samp = sum(totals) / len(totals) if totals else 1
    _pe_config["min_samples"] = min(30, max(3, int(avg_samp * 0.5)))
    for k, data in _pattern_db.items():
        total = data["total"]
        if total < _pe_config["min_samples"]:
            continue
        strength = (data["sharp"] - data["overconf"]) / total
        insights.append((k, strength, total))
    return insights


def update_pattern_adjustments():
    insights = analyze_patterns()
    if not insights:
        return
    avg_str = sum(s for _, s, _ in insights) / len(insights)
    for k, strength, _ in insights:
        diff = strength - avg_str
        _pattern_adjustments[k] = (
            _pattern_adjustments.get(k, 0.0) + diff * _pe_config["learning_rate"]
        )
    for k in list(_pattern_adjustments.keys()):
        _pattern_adjustments[k] *= 0.95


def merge_similar_patterns(key_str: str):
    try:
        # Match on first 3 tuple fields (bet_type, pace, script)
        prefix = key_str[:key_str.find(",", key_str.find(",") + 1)]
        similar = [k for k in list(_pattern_db.keys()) if k.startswith(prefix) and k != key_str]
        for sk in similar:
            if _pattern_db[sk]["total"] < _pe_config["min_samples"]:
                continue
            for stat in ["sharp", "lucky", "overconf", "fade", "total"]:
                _pattern_db[key_str][stat] = (
                    _pattern_db[key_str].get(stat, 0) + _pattern_db[sk].get(stat, 0)
                )
            del _pattern_db[sk]
    except Exception:
        pass


# ─── 10. Decay ───────────────────────────────────────────────────────────────

def record_causality_outcome(
    result: str,
    stat: str,
    role_str: str,
    causality_events: list,
):
    """
    Feed a graded pick outcome back into the causality hit-rate map.
    Called after every settled pick (props, parlays, totals, SGPs, all types).

    - result:           "win" or "loss"
    - stat:             prop type e.g. "points", "rebounds"
    - role_str:         canonical role e.g. "PRIMARY_SCORER"
    - causality_events: list of cause strings from ContextTracker.get_causality_log()
                        e.g. ["STAR_QUIET — LeBron scoreless in Q3"]
    """
    global _causality_hit_rates
    hit = (result == "win")

    if not causality_events:
        # No cause detected — still record as "NO_CAUSE" bucket
        _record_cause_bucket("NO_CAUSE", stat, role_str, hit)
        return

    for event_str in causality_events:
        # Extract the cause type (first word before " — ")
        cause_type = str(event_str).split("—")[0].strip().split()[0].strip()
        _record_cause_bucket(cause_type, stat, role_str, hit)


def _record_cause_bucket(cause_type: str, stat: str, role_str: str, hit: bool):
    """Update the in-memory causality hit-rate for one cause+stat+role combo."""
    k = f"{cause_type}|{stat}|{role_str}"
    if k not in _causality_hit_rates:
        _causality_hit_rates[k] = {"wins": 0, "losses": 0, "total": 0}
    _causality_hit_rates[k]["wins"]   += (1 if hit else 0)
    _causality_hit_rates[k]["losses"] += (0 if hit else 1)
    _causality_hit_rates[k]["total"]  += 1


def get_causality_penalty(
    stat: str,
    role_str: str,
    live_causality_events: list,
    min_samples: int = 8,
) -> float:
    """
    Returns a confidence multiplier (0.70–1.05) for Layer 6 based on whether
    the live causality events firing right now historically hurt this stat+role combo.

    - Values below 1.0 → pick weakened
    - Values above 1.0 → pick reinforced (e.g. PACE_SURGE + REBOUND_ANCHOR)
    - Hard block signal returned as 0.0 if hit rate < 30% with ≥15 samples
    """
    if not live_causality_events:
        return 1.0

    total_weight = 0.0
    total_count  = 0

    for event_str in live_causality_events:
        cause_type = str(event_str).split("—")[0].strip().split()[0].strip()
        k = f"{cause_type}|{stat}|{role_str}"
        data = _causality_hit_rates.get(k)
        if not data or data["total"] < min_samples:
            continue
        hit_rate = data["wins"] / data["total"]
        n = data["total"]
        total_count += 1

        # Hard block threshold: <30% hit rate with ≥15 confirmed samples
        if hit_rate < 0.30 and n >= 15:
            print(f"  [CausalityL6] HARD BLOCK: {k} hit_rate={hit_rate:.0%} (n={n})")
            return 0.0

        # Confidence shift: 50% hit rate = neutral, above = boost, below = penalty
        shift = (hit_rate - 0.50) * 0.30   # max ±0.15
        total_weight += shift

    if total_count == 0:
        return 1.0

    avg_shift = total_weight / total_count
    return max(0.70, min(1.05, 1.0 + avg_shift))


def decay_patterns():
    """
    Context-weighted exponential decay — run nightly.

    Decay rate depends on what happened:
      clean_win   → very slow decay (0.992) — reinforce strong patterns
      causal_win  → faster decay (0.965) — lucky, don't over-rely
      causal_loss → moderate decay (0.975) — context broke it, not the model
      blind_loss  → aggressive decay (0.930) — model was simply wrong
    """
    # Default fallback rate
    rate = _pe_config.get("decay_rate", 0.98)
    for k in _pattern_db:
        d = _pattern_db[k]
        total = d.get("total", 0)
        if total <= 0:
            for stat in ["sharp", "lucky", "overconf", "fade"]:
                d[stat] *= rate
            d["total"] *= rate
            continue

        wins   = d.get("sharp", 0) + d.get("lucky", 0)
        losses = d.get("overconf", 0) + d.get("fade", 0)

        # Determine decay bucket from pattern composition
        win_rate = wins / max(total, 1)
        # Check if this key has known causality context
        # (we use the pattern key string to detect causal vs blind)
        has_causal = any(
            cause in k for cause in
            ("STAR_QUIET", "STAR_EXPLOSION", "INJURY_OUT",
             "PACE_SURGE", "GAME_TIGHTENED", "BLOWOUT")
        )

        if win_rate >= 0.60 and not has_causal:
            bucket_rate = _CAUSALITY_DECAY["clean_win"]
        elif win_rate >= 0.55 and has_causal:
            bucket_rate = _CAUSALITY_DECAY["causal_win"]
        elif win_rate < 0.50 and has_causal:
            bucket_rate = _CAUSALITY_DECAY["causal_loss"]
        else:
            bucket_rate = _CAUSALITY_DECAY["blind_loss"]

        for stat in ["sharp", "lucky", "overconf", "fade"]:
            d[stat] *= bucket_rate
        d["total"] *= bucket_rate

    # Meta DB always decays at standard rate
    for k in _meta_db:
        _meta_db[k]["score"] *= rate
        _meta_db[k]["count"] *= rate

    # Causality hit rates decay slowly — preserve long-run signal
    _causal_decay = 0.995
    for k in _causality_hit_rates:
        _causality_hit_rates[k]["wins"]   *= _causal_decay
        _causality_hit_rates[k]["losses"] *= _causal_decay
        _causality_hit_rates[k]["total"]  *= _causal_decay


# ─── 11. Auto tuning ─────────────────────────────────────────────────────────

def auto_adjust_system():
    totals = [d["total"] for d in _pattern_db.values() if d["total"] > 0]
    if not totals:
        return
    avg = sum(totals) / len(totals)
    _pe_config["learning_rate"] = min(0.1, max(0.01, 1 / (avg + 1)))
    _pe_config["decay_rate"]    = 0.99 if avg > 20 else 0.95


# ─── 12. DB persistence ──────────────────────────────────────────────────────

_PE_STORE = {
    "pattern_engine:pattern_db":           "_pattern_db",
    "pattern_engine:meta_db":              "_meta_db",
    "pattern_engine:exposure_tracker":     "_exposure_tracker",
    "pattern_engine:conflict_db":          "_conflict_db",
    "pattern_engine:pattern_adjustments":  "_pattern_adjustments",
    "pipeline:channel_floors":             "_channel_floors",
    "pipeline:kelly_tracker":              "_kelly_tracker",
    "pipeline:ml_weight_tracker":          "_ml_weight_tracker",
    "pipeline:role_threshold_adjustments": "_role_threshold_adjustments",
    "pipeline:causality_hit_rates":        "_causality_hit_rates",
}


def _pe_save(conn):
    """Persist all pattern data + pipeline self-learning state to learning_data table."""
    try:
        mapping = {
            "pattern_engine:pattern_db":          _pattern_db,
            "pattern_engine:meta_db":             _meta_db,
            "pattern_engine:exposure_tracker":    _exposure_tracker,
            "pattern_engine:conflict_db":         _conflict_db,
            "pattern_engine:pattern_adjustments": _pattern_adjustments,
            "pipeline:channel_floors":             _channel_floors,
            "pipeline:kelly_tracker":              _kelly_tracker,
            "pipeline:ml_weight_tracker":          _ml_weight_tracker,
            "pipeline:role_threshold_adjustments": _role_threshold_adjustments,
            "pipeline:causality_hit_rates":        _causality_hit_rates,
        }
        class _DtEnc(_jpe.JSONEncoder):
            def default(self, o):
                return o.isoformat() if hasattr(o, "isoformat") else super().default(o)

        cur = conn.cursor()
        for db_key, data in mapping.items():
            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
            """, (db_key, _jpe.dumps(data, cls=_DtEnc)))
        conn.commit()
        cur.close()
        print(f"[PatternEngine] saved {len(_pattern_db)} patterns to DB")
    except Exception as e:
        print(f"[PatternEngine] save error: {e}")


def pe_flush():
    """
    Immediately persist pattern DB + causality hit rates to DB.
    Opens its own connection — safe to call anywhere after bet settlement
    without needing to pass a connection.  This prevents losing intraday
    causality + pattern signal on a restart before the nightly cycle runs.
    """
    try:
        import os as _osf, psycopg2 as _pgf
        _url = _osf.environ.get("DATABASE_URL", "")
        if not _url:
            return
        _c = _pgf.connect(_url)
        _pe_save(_c)
        _c.close()
        print("[PatternEngine] pe_flush: pattern + causality saved to DB")
    except Exception as _fe:
        print(f"[PatternEngine] pe_flush error (non-fatal): {_fe}")


def _pe_load(conn):
    """Load all pattern data + pipeline self-learning state from learning_data table."""
    global _pattern_db, _meta_db, _exposure_tracker, _conflict_db, _pattern_adjustments
    global _channel_floors, _kelly_tracker, _ml_weight_tracker, _role_threshold_adjustments
    try:
        cur = conn.cursor()
        for db_key, target in _PE_STORE.items():
            cur.execute("SELECT value FROM learning_data WHERE key=%s", (db_key,))
            row = cur.fetchone()
            if row:
                try:
                    val = row[0] if isinstance(row[0], dict) else _jpe.loads(row[0])
                    if target == "_pattern_db":                    _pattern_db = val
                    elif target == "_meta_db":                     _meta_db = val
                    elif target == "_exposure_tracker":            _exposure_tracker = val
                    elif target == "_conflict_db":                 _conflict_db = val
                    elif target == "_pattern_adjustments":         _pattern_adjustments = val
                    elif target == "_channel_floors":              _channel_floors.update(val)
                    elif target == "_kelly_tracker":               _kelly_tracker.update(val)
                    elif target == "_ml_weight_tracker":           _ml_weight_tracker.update(val)
                    elif target == "_role_threshold_adjustments":  _role_threshold_adjustments.update(val)
                    elif target == "_causality_hit_rates":         _causality_hit_rates.update(val)
                except Exception:
                    pass
        cur.close()
        print(f"[PatternEngine] loaded {len(_pattern_db)} patterns, {len(_meta_db)} meta from DB")
    except Exception as e:
        print(f"[PatternEngine] load error: {e}")


# ─── 13. Nightly learning cycle ──────────────────────────────────────────────

def run_learning_cycle(conn=None) -> list:
    """
    Nightly: loads settled bets → grades each → updates patterns →
    merges thin keys → auto-tunes → decays → saves back to DB.
    Returns list of report lines for admin DM.
    """
    global _pe_loaded
    _pe_auto_load()   # restore from DB if restarted since last save
    report = ["🧠 *Pattern Learning Cycle*\n"]
    try:
        if conn:
            _pe_load(conn)   # always do a fresh full load at cycle start
            _pe_loaded = True

        all_bets = []
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT game, player, pick, bet_type, line, prediction,
                           confidence, result, role, tier, script,
                           game_pace, game_phase, ev
                    FROM bets
                    WHERE result IN ('win','loss')
                    ORDER BY created_at DESC LIMIT 500
                """)
                for row in cur.fetchall():
                    all_bets.append({
                        "game":       row[0],  "player":     row[1],
                        "pick":       row[2],  "betType":    row[3],
                        "line":       row[4],  "prediction": row[5],
                        "confidence": row[6],  "result":     row[7],
                        "role":       row[8],  "tier":       row[9],
                        "script":     row[10], "game_pace":  row[11],
                        "game_phase": row[12], "ev":         row[13],
                    })
                cur.close()
                report.append(f"Loaded {len(all_bets)} settled bets")
            except Exception as e:
                report.append(f"⚠️ DB read error: {e}")

        if not all_bets:
            report.append("No settled bets yet — patterns will build as results come in")
            return report

        # Grade and store all bets — per-bet guard so one bad row never kills the cycle
        counts = {"sharp": 0, "lucky": 0, "overconfident": 0, "correct_fade": 0, "neutral": 0}
        skipped = 0
        for bet in all_bets:
            try:
                grade = grade_decision(bet)
                counts[grade] = counts.get(grade, 0) + 1
                process_bet(bet)
            except Exception as _be:
                skipped += 1
                print(f"[PatternEngine] skipped bet {bet.get('pick','?')}: {_be}")
        if skipped:
            report.append(f"⚠️ {skipped} bets skipped due to missing data")

        report.append(
            f"Grades: 🎯 {counts['sharp']} sharp | "
            f"🍀 {counts['lucky']} lucky | "
            f"😬 {counts['overconfident']} overconf | "
            f"✅ {counts['correct_fade']} correct fade"
        )

        # Merge thin patterns
        for key in list(_pattern_db.keys()):
            merge_similar_patterns(key)

        update_pattern_adjustments()
        auto_adjust_system()

        # Winrate safety brake — last 50 settled bets
        recent = [b for b in all_bets[:50] if b.get("result") in ("win", "loss")]
        if len(recent) >= 20:
            wins = sum(1 for b in recent if b["result"] == "win")
            rate = wins / len(recent)
            if rate < 0.48:
                for k in list(_pattern_adjustments.keys()):
                    _pattern_adjustments[k] *= 0.85
                report.append(
                    f"⚠️ Cold streak brake: last {len(recent)} bets at {rate*100:.0f}% "
                    f"— all weights reduced 15%"
                )
            else:
                report.append(f"✅ Recent form: last {len(recent)} bets at {rate*100:.0f}%")

        decay_patterns()

        # Pattern report
        insights = analyze_patterns()
        strong   = [(k, s, t) for k, s, t in insights if s > 0.15]
        weak     = [(k, s, t) for k, s, t in insights if s < -0.15]

        if strong:
            report.append(f"\n*🔥 Strong Patterns ({len(strong)}):*")
            for k, s, t in sorted(strong, key=lambda x: x[1], reverse=True)[:5]:
                report.append(f"  {k}: {s*100:.0f}% sharp ({int(t)} samples)")
        if weak:
            report.append(f"\n*❄️ Weak Patterns ({len(weak)}):*")
            for k, s, t in sorted(weak, key=lambda x: x[1])[:5]:
                report.append(f"  {k}: {abs(s)*100:.0f}% overconf ({int(t)} samples)")

        report.append(
            f"\n*Config:* lr={_pe_config['learning_rate']:.3f} | "
            f"decay={_pe_config['decay_rate']:.3f} | "
            f"min_samples={_pe_config['min_samples']}"
        )
        report.append(
            f"*Patterns:* {len(_pattern_db)} context keys | {len(_meta_db)} meta keys"
        )

        if conn:
            _pe_save(conn)

    except Exception as e:
        report.append(f"❌ Learning cycle error: {e}")
        print(f"[PatternEngine] run_learning_cycle error: {e}")

    return report


# ─── 14. evaluate_pick — full pipeline ───────────────────────────────────────

def evaluate_pick(pick: dict, recent_results: list = None) -> tuple:
    """
    Full pipeline: context → pattern adjustment → regression → kill switch →
    decision tier.
    Returns (final_conf_0_to_1, decision) where decision is:
      STRONG BET | LEAN | PASS
    """
    raw_conf  = float(pick.get("confidence", 50))
    base_conf = (raw_conf / 100.0) if raw_conf > 1.0 else raw_conf

    # Pattern adjustment
    adjustment = get_adjustment(pick)
    adjusted   = base_conf + adjustment

    # Regression to mean — only applied once pattern DB has real track record (50+).
    # At 10 patterns the system is still cold; regression at that threshold dragged
    # every legitimate pick (65-80% confidence) below the LEAN cutoff, silently
    # blocking ALL picks and leaving the card empty.
    if len(_pattern_db) >= 50:
        # Gentle regression: 80% weight on model confidence, 20% toward mean.
        # Old formula (adjusted + 0.5) / 2 averaged equally with 0.5, which
        # forced a 65% pick down to 0.575 — below the LEAN gate.
        adjusted = adjusted * 0.80 + 0.5 * 0.20

    # Conflict hard suppress
    key   = build_context(pick)
    c_pen = conflict_penalty(key)
    if c_pen < -0.20:
        adjusted *= 0.85

    # Clamp
    final_conf = max(0.0, min(1.0, adjusted))

    # Kill switch — 20-bet cold streak
    if recent_results and len(recent_results) >= 20:
        winrate = sum(recent_results[-20:]) / 20
        if winrate < 0.48:
            final_conf *= 0.85

    # Decision tier — thresholds relative to base_conf so quality bar is preserved.
    # With the gentle regression above a 72% pick → 0.676, an 80% pick → 0.74.
    if final_conf >= 0.70:   decision = "STRONG BET"
    elif final_conf >= 0.55: decision = "LEAN"
    else:                    decision = "PASS"

    return final_conf, decision


# ─── 15. Gate pick — called by save_bet ──────────────────────────────────────

def gate_pick(bet: dict, recent_results: list = None) -> bool:
    """
    Returns True → send pick.  False → hold pick (save_bet returns False,
    Telegram send is skipped for all pick senders that check `if saved:`).

    Gate is inactive until ≥10 pattern keys exist so cold-start never blocks.
    Patterns are auto-loaded from DB on first call after any restart/crash.
    """
    _pe_auto_load()   # no-op after first call; restores patterns after restart
    if len(_pattern_db) < 10:
        return True   # not enough data yet — pass everything through

    _, decision = evaluate_pick(bet, recent_results)
    if decision == "PASS":
        print(
            f"[PatternEngine] HELD: {bet.get('pick','?')} "
            f"| type={bet.get('betType','?')} "
            f"| context={build_context(bet)}"
        )
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 16. PLAYER CONTEXT ENGINE
# ══════════════════════════════════════════════════════════════════════════════

# Role values (canonical)
# ── Canonical role constants ───────────────────────────────────────────────────
ROLE_PRIMARY_SCORER   = "PRIMARY_SCORER"
ROLE_PLAYMAKER_HUB    = "PLAYMAKER_HUB"
ROLE_SECONDARY_SCORER = "SECONDARY_SCORER"
ROLE_SPACER_SHOOTER   = "SPACER_SHOOTER"
ROLE_REBOUND_ANCHOR   = "REBOUND_ANCHOR"
ROLE_CONNECTOR        = "CONNECTOR"

_ALL_ROLES = (
    ROLE_PRIMARY_SCORER, ROLE_PLAYMAKER_HUB, ROLE_SECONDARY_SCORER,
    ROLE_SPACER_SHOOTER, ROLE_REBOUND_ANCHOR, ROLE_CONNECTOR,
)

# Role → game script fit matrix.
# Keys are script labels (pace or flow portion).  Values are multipliers
# applied to confidence.  > 1.0 = boost, < 1.0 = fade, 1.0 = neutral.
_ROLE_SCRIPT_FIT = {
    ROLE_PRIMARY_SCORER: {
        "TRANSITION_HEAVY":      1.12,
        "UPTEMPO":               1.08,
        "HIGH_SCORING":          1.10,
        "SHOOTOUT":              1.15,
        "HALFCOURT":             0.90,
        "SLOW_PACED":            0.88,
        "DEFENSIVE_BATTLE":      0.82,
        "BLOWOUT":               0.85,
        "DOUBLE_DIGIT_LEAD":     0.88,
    },
    ROLE_PLAYMAKER_HUB: {
        "TIGHT_GAME":            1.12,
        "COMPETITIVE":           1.08,
        "HALFCOURT":             1.07,
        "COMFORTABLE_LEAD":      0.90,
        "BLOWOUT":               0.78,
        "TRANSITION_HEAVY":      0.92,
    },
    ROLE_SECONDARY_SCORER: {
        "TRANSITION_HEAVY":      1.08,
        "UPTEMPO":               1.05,
        "HIGH_SCORING":          1.07,
        "BLOWOUT":               0.88,
        "DEFENSIVE_BATTLE":      0.85,
        "TIGHT_GAME":            0.92,
    },
    ROLE_SPACER_SHOOTER: {
        "TRANSITION_HEAVY":      1.15,
        "UPTEMPO":               1.10,
        "SHOOTOUT":              1.18,
        "HIGH_SCORING":          1.12,
        "HALFCOURT":             0.85,
        "DEFENSIVE_BATTLE":      0.78,
        "BLOWOUT":               0.82,
    },
    ROLE_REBOUND_ANCHOR: {
        "HALFCOURT":             1.15,
        "SLOW_PACED":            1.10,
        "DEFENSIVE_BATTLE":      1.18,
        "COMPETITIVE":           1.05,
        "TRANSITION_HEAVY":      0.82,
        "UPTEMPO":               0.85,
        "BLOWOUT":               0.80,
    },
    ROLE_CONNECTOR: {
        "COMPETITIVE":           1.05,
        "TIGHT_GAME":            1.03,
        "HALFCOURT":             1.02,
        "BLOWOUT":               0.85,
        "TRANSITION_HEAVY":      0.95,
    },
}

# Role self-learning: threshold nudges stored per (role, script) pair.
# Format: { "PRIMARY_SCORER|TRANSITION_HEAVY": {"delta": 0.02, "samples": 15} }
_role_threshold_adjustments: dict = {}

# ML weight self-learning: track ML prediction accuracy
_ml_weight_tracker: dict = {
    "correct": 0, "total": 0, "weight": 1.0,
    "last_reset": None,
}


def _get_ml_weight() -> float:
    """Return current ML confidence weight (0.85–1.15, min 20 samples)."""
    t = _ml_weight_tracker
    if t["total"] < 20:
        return 1.0
    accuracy = t["correct"] / t["total"]
    if accuracy >= 0.65:
        return min(t["weight"] + 0.02, 1.15)
    elif accuracy <= 0.45:
        return max(t["weight"] - 0.02, 0.85)
    return t["weight"]


def record_ml_outcome(correct: bool):
    """Called after a pick is graded to update ML weight tracker."""
    import datetime as _dt
    t = _ml_weight_tracker
    today = str(_dt.date.today())
    if t.get("last_reset") and (
        _dt.date.today() - _dt.date.fromisoformat(t["last_reset"])
    ).days >= 7 and t["total"] == 0:
        t["correct"] = 0
        t["total"]   = 0
        t["weight"]  = 1.0
    t["total"]  += 1
    if correct:
        t["correct"] += 1
    t["weight"] = _get_ml_weight()
    t["last_reset"] = today


def assign_role_v2(stats: dict) -> str:
    """
    Assign a player role from live stats using the v2 system.

    Roles: PRIMARY_SCORER, PLAYMAKER_HUB, SECONDARY_SCORER,
           SPACER_SHOOTER, REBOUND_ANCHOR, CONNECTOR

    Touches proxy  = FGA + (FTA/2) + AST + TOV  (all in BDL box score)
    Reb chances proxy = raw rebounds × 1.3
    """
    usage   = float(stats.get("usage_rate") or 0)
    pts     = float(stats.get("points")     or stats.get("pred_pts") or 0)
    ast     = float(stats.get("assists")    or stats.get("pred_ast") or 0)
    reb     = float(stats.get("rebounds")   or stats.get("pred_reb") or 0)
    fga     = float(stats.get("fga")        or 0)
    fta     = float(stats.get("fta")        or 0)
    tov     = float(stats.get("turnover")   or stats.get("tov")      or 0)
    fg3a    = float(stats.get("fg3a")       or stats.get("3pa")      or 0)

    # Real tracking data takes priority; fall back to proxies
    touches       = float(stats.get("touches")          or (fga + fta / 2 + ast + tov))
    reb_chances   = float(stats.get("rebound_chances")  or reb * 1.3)

    # Apply any learned threshold adjustments for this role
    def _nudged(base_val, role_key, field):
        adj_key = f"{role_key}|{field}"
        delta   = _role_threshold_adjustments.get(adj_key, {}).get("delta", 0.0)
        return base_val + delta

    if usage >= _nudged(28, ROLE_PRIMARY_SCORER,   "usage") and pts >= _nudged(20, ROLE_PRIMARY_SCORER,   "pts"):
        return ROLE_PRIMARY_SCORER
    if ast >= _nudged(7, ROLE_PLAYMAKER_HUB, "ast") and touches > _nudged(70, ROLE_PLAYMAKER_HUB, "touches"):
        return ROLE_PLAYMAKER_HUB
    if pts >= _nudged(14, ROLE_SECONDARY_SCORER, "pts") and usage < _nudged(26, ROLE_SECONDARY_SCORER, "usage"):
        return ROLE_SECONDARY_SCORER
    if fg3a >= _nudged(5, ROLE_SPACER_SHOOTER, "fg3a") and usage < _nudged(22, ROLE_SPACER_SHOOTER, "usage"):
        return ROLE_SPACER_SHOOTER
    if reb_chances >= _nudged(12, ROLE_REBOUND_ANCHOR, "reb_chances") or reb >= _nudged(9, ROLE_REBOUND_ANCHOR, "reb"):
        return ROLE_REBOUND_ANCHOR
    return ROLE_CONNECTOR


def update_role_threshold(role: str, field: str, hit: bool):
    """
    Self-learning: nudge role assignment thresholds based on pick outcomes.
    Called after a pick is graded. Small nudges only (max ±3.0 total).
    Requires 5+ samples before adjusting.
    """
    key  = f"{role}|{field}"
    entry = _role_threshold_adjustments.setdefault(key, {"delta": 0.0, "samples": 0, "hits": 0})
    entry["samples"] += 1
    if hit:
        entry["hits"] += 1
    if entry["samples"] < 5:
        return
    hit_rate = entry["hits"] / entry["samples"]
    if hit_rate >= 0.65:
        entry["delta"] = max(entry["delta"] - 0.1, -3.0)
    elif hit_rate <= 0.40:
        entry["delta"] = min(entry["delta"] + 0.1,  3.0)


_DEFAULT_ROLE_MULT = {r: 1.0 for r in _ALL_ROLES}


def get_injury_impact(injury_report: dict = None) -> str:
    """
    Classify the injury severity for a pick.

    Walks the 'out' list first — a star (PRIMARY_SCORER or PLAYMAKER_HUB) out is
    CRITICAL, a secondary role out is MODERATE.  Then the 'gtd' list — a star GTD
    is LOW.  Returns "NONE" when the report is empty or absent.

    Returns: "CRITICAL" | "MODERATE" | "LOW" | "NONE"
    """
    if not injury_report:
        return "NONE"

    _star_roles   = (ROLE_PRIMARY_SCORER, ROLE_PLAYMAKER_HUB)
    _role_p_roles = (ROLE_SECONDARY_SCORER, ROLE_SPACER_SHOOTER,
                     ROLE_REBOUND_ANCHOR, ROLE_CONNECTOR)

    for p in (injury_report.get("out") or []):
        role = str(p.get("role", "")).upper().strip()
        if role in _star_roles:
            return "CRITICAL"
        if role in _role_p_roles:
            return "MODERATE"

    for p in (injury_report.get("gtd") or []):
        role = str(p.get("role", "")).upper().strip()
        if role in _star_roles:
            return "LOW"

    return "NONE"


def build_player_context(player_name: str, role: str, stat_type: str,
                         game_script: str, injury_report: dict = None) -> dict:
    """
    Build a context dict for one player prop.

    Args:
        player_name:   e.g. "LeBron James"
        role:          go_to_scorer | floor_general | glass_cleaner | rim_anchor |
                       spot_up_shooter | combo_creator | sixth_man | utility_player
        stat_type:     "pts" | "reb" | "ast" | "3pm" | "pra" | "blocks"
        game_script:   label from game_script.py e.g. "COMPETITIVE_HIGH_SCORING"
        injury_report: {"out": [...], "gtd": [...]} where each entry is {"name":str,"role":str}

    Returns:
        dict with keys: player, role, stat_type, game_script,
                        script_fit_mult, injury_mult, final_edge_mult, note
    """
    role = str(role).upper().strip()
    if role not in _ALL_ROLES:
        role = ROLE_PRIMARY_SCORER

    fit_map = _ROLE_SCRIPT_FIT.get(role, {})
    parts   = game_script.split("_") if game_script else []
    mult    = 1.0
    matched = None
    for part in [game_script] + parts:
        if part in fit_map:
            mult    = fit_map[part]
            matched = part
            break

    # Injury context: usage redistribution boost
    _scoring_roles  = (ROLE_PRIMARY_SCORER, ROLE_SECONDARY_SCORER, ROLE_SPACER_SHOOTER)
    _creating_roles = (ROLE_PLAYMAKER_HUB,)
    inj_note = ""
    inj_mult = 1.0
    if injury_report:
        out_stars = [p for p in (injury_report.get("out") or [])
                     if str(p.get("role", "")).upper() in (*_scoring_roles, *_creating_roles)]
        if out_stars:
            inj_mult = 1.08
            names    = ", ".join(p.get("name", "?") for p in out_stars[:2])
            inj_note = f"Usage boost — {names} OUT"
        else:
            gtd_stars = [p for p in (injury_report.get("gtd") or [])
                         if str(p.get("role", "")).upper() in (*_scoring_roles, *_creating_roles)]
            if gtd_stars:
                inj_mult = 1.04
                names    = ", ".join(p.get("name", "?") for p in gtd_stars[:2])
                inj_note = f"Possible usage bump — {names} GTD"

    final_mult = round(mult * inj_mult, 4)
    note_parts = []
    if matched:
        note_parts.append(f"Script fit ({matched}): ×{mult}")
    if inj_note:
        note_parts.append(inj_note)

    injury_impact = get_injury_impact(injury_report)

    return {
        "player":          player_name,
        "role":            role,
        "stat_type":       stat_type,
        "game_script":     game_script,
        "script_fit_mult": round(mult, 4),
        "injury_mult":     round(inj_mult, 4),
        "final_edge_mult": final_mult,
        "injury_impact":   injury_impact,
        "note":            " · ".join(note_parts) if note_parts else "Neutral fit",
    }


def get_upset_flag(odds: float = 0, confidence: float = 0.0) -> bool:
    """
    Returns True when this looks like a strong underdog pick:
      - American odds >= +150  (underdog)
      - Model confidence >= 0.65  (model still likes it)
    Stored in player_ctx["upset_flag"] by apply_player_context.
    """
    return bool(odds >= 150 and confidence >= 0.65)


def apply_player_context(base_confidence: float, player_ctx: dict,
                         odds: float = 0) -> float:
    """
    Apply player context multiplier to base confidence, clamped 0–1.
    Also stamps player_ctx["upset_flag"] in-place so callers can read it.
    """
    adjusted = base_confidence * player_ctx.get("final_edge_mult", 1.0)
    adjusted = round(max(0.0, min(1.0, adjusted)), 4)
    player_ctx["upset_flag"] = get_upset_flag(odds, adjusted)
    return adjusted


def auto_pick_decision(base_confidence: float, player_ctx: dict,
                       line: float = None, proj: float = None,
                       odds: float = 0):
    """
    Combine player context with base model confidence to decide whether to pick.

    CRITICAL injury → immediate PASS regardless of confidence.
    MODERATE injury → confidence penalised by -0.05 before thresholding.

    Returns:
        (final_confidence: float, decision: str, note: str)
        decision = "STRONG BET" | "LEAN" | "PASS"
    """
    injury_impact = player_ctx.get("injury_impact", "NONE")
    if injury_impact == "CRITICAL":
        return 0.0, "PASS", "Key player OUT — injury block"

    conf = apply_player_context(base_confidence, player_ctx, odds=odds)
    note = player_ctx.get("note", "")

    if injury_impact == "MODERATE":
        conf  = round(max(0.0, conf - 0.05), 4)
        note += " · Injury penalty -0.05"

    # Upset signal learning nudge — only kicks in after 5+ samples (Laplace gate)
    if player_ctx.get("upset_flag"):
        sig_wr = get_signal_confidence("upset_signal")
        if sig_wr >= 0.60:
            conf  = round(min(1.0, conf + 0.03), 4)
            note += f" · Upset signal +{round(sig_wr * 100, 1)}%"
        elif sig_wr <= 0.50:
            conf  = round(max(0.0, conf - 0.03), 4)
            note += f" · Upset signal {round(sig_wr * 100, 1)}%"

    if line is not None and proj is not None and line > 0:
        edge = (proj - line) / line
        if edge > 0.05:
            conf  = min(1.0, conf * 1.05)
            note += f" · Proj edge +{round(edge*100,1)}%"
        elif edge < -0.05:
            conf  = conf * 0.95
            note += f" · Proj edge {round(edge*100,1)}%"

    if conf >= 0.70:   decision = "STRONG BET"
    elif conf >= 0.60: decision = "LEAN"
    else:              decision = "PASS"

    return round(conf, 4), decision, note


def extract_signals(pick: dict) -> list:
    """
    Build the list of active signal keys for a settled pick.

    Signal keys follow the format understood by record_signal / get_signal_confidence:
      "<betType>|<context_key>"

    Special signals (e.g. upset_signal) are appended when the relevant flag is set.
    Pass the list to record_signal(key, hit) to update the learning DB.
    """
    signals = []
    bet_type = pick.get("betType") or pick.get("bet_type", "")
    script   = pick.get("script", "")
    if bet_type and script:
        signals.append(f"{bet_type}|{script}")
    if pick.get("upset_flag"):
        signals.append("upset_signal")
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 17. SIGNAL LEARNING ENGINE  (DB-backed — no pickle)
# ══════════════════════════════════════════════════════════════════════════════

_signal_db: dict     = {}
_signal_loaded: bool = False


def _signal_load(conn=None) -> None:
    """Load signal stats from PostgreSQL learning_data table."""
    global _signal_db, _signal_loaded
    try:
        import os, json as _json, psycopg2
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return
        _conn = conn or psycopg2.connect(db_url)
        cur   = _conn.cursor()
        cur.execute("SELECT value FROM learning_data WHERE key = 'signal_db' LIMIT 1")
        row = cur.fetchone()
        if row:
            loaded    = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
            _signal_db = loaded if isinstance(loaded, dict) else {}
        if conn is None:
            _conn.close()
        _signal_loaded = True
    except Exception as e:
        print(f"[SignalEngine] load error: {e}")


def _signal_save(conn=None) -> None:
    """Persist signal stats to PostgreSQL learning_data table."""
    try:
        import os, json as _json, psycopg2
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return

        class _DtEnc(_json.JSONEncoder):
            def default(self, o):
                return o.isoformat() if hasattr(o, "isoformat") else super().default(o)

        blob  = _json.dumps(_signal_db, cls=_DtEnc)
        _conn = conn or psycopg2.connect(db_url)
        cur   = _conn.cursor()
        cur.execute("""
            INSERT INTO learning_data (key, value)
            VALUES ('signal_db', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (blob,))
        _conn.commit()
        if conn is None:
            _conn.close()
    except Exception as e:
        print(f"[SignalEngine] save error: {e}")


def record_signal(signal_key: str, hit: bool, conn=None) -> None:
    """
    Record a signal hit or miss.
    signal_key format: "<betType>|<context_key>"
      e.g. "prop_pts|COMPETITIVE_HIGH_SCORING:home:Q2:+3"
    """
    global _signal_db, _signal_loaded
    if not _signal_loaded:
        _signal_load(conn)
    entry = _signal_db.setdefault(signal_key, {"hits": 0, "misses": 0})
    if hit:
        entry["hits"]   += 1
    else:
        entry["misses"] += 1
    _signal_save(conn)


def get_signal_confidence(signal_key: str, fallback: float = 0.50) -> float:
    """
    Return historical hit-rate for a signal key (Laplace-smoothed).
    Returns fallback if fewer than 5 samples.
    """
    global _signal_loaded
    if not _signal_loaded:
        _signal_load()
    entry = _signal_db.get(signal_key, {})
    hits  = entry.get("hits",   0)
    total = hits + entry.get("misses", 0)
    if total < 5:
        return fallback
    return round((hits + 1) / (total + 2), 4)


def signal_stats_summary(top_n: int = 10) -> list:
    """Return top_n signal keys by (hit_rate DESC, volume DESC)."""
    global _signal_loaded
    if not _signal_loaded:
        _signal_load()
    rows = []
    for k, v in _signal_db.items():
        total = v.get("hits", 0) + v.get("misses", 0)
        if total < 5:
            continue
        rows.append({
            "key":      k,
            "hit_rate": round((v["hits"] + 1) / (total + 2), 4),
            "n":        total,
        })
    rows.sort(key=lambda r: (-r["hit_rate"], -r["n"]))
    return rows[:top_n]


# ══════════════════════════════════════════════════════════════════════════════
# 18. KELLY UNIT SIZING  (1u / 2u / 3u — no dollar amounts)
# ══════════════════════════════════════════════════════════════════════════════

def kelly_units(win_prob: float, odds: float) -> str:
    """
    Return unit size label ("1u", "2u", "3u") based on half-Kelly edge.
    odds = American format (e.g. -110, +150).

    Buckets (half-Kelly):
        ≤ 0.02  → 1u  (thin edge)
        ≤ 0.06  → 2u  (moderate edge)
        > 0.06  → 3u  (strong edge)
    """
    try:
        dec = (odds / 100 + 1.0) if odds >= 100 else (100 / abs(odds) + 1.0)
        b   = dec - 1.0
        if b <= 0 or win_prob <= 0:
            return "1u"
        q      = 1.0 - win_prob
        half_k = ((b * win_prob - q) / b) / 2.0
        if half_k <= 0.0:   return "1u"
        if half_k <= 0.02:  return "1u"
        if half_k <= 0.06:  return "2u"
        return "3u"
    except Exception:
        return "1u"


# ══════════════════════════════════════════════════════════════════════════════
# 19. CONTEXT TRACKER  (live BDL game state — wired into live bet checker)
# ══════════════════════════════════════════════════════════════════════════════

class ContextTracker:
    """
    Tracks live game state changes for one BDL game.

    Capabilities:
    - Quarter transitions, momentum swings, blowouts, close-game collapses
    - Full pace + game script re-evaluation every quarter (Layer 1 live feed)
    - Causality engine: detects WHY script/pace changed
      (star goes quiet, foul trouble, B2B fatigue, run size, lineup shifts)
    - Script change flagging: picks based on old script get flagged but still sent
    - Outcome logging: predicted vs actual script + cause → feeds Layer 6 learning

    Usage in bot.py live loop:
        ct = get_context_tracker(game_id, home_team, away_team)
        events = ct.update(period, time_str, home_score, away_score,
                           player_stats=..., injuries=...)
        for ev in events:
            if ev["type"] == "script_change":
                # flag pending picks, log cause for learning
    """

    _MOMENTUM_SWING = 8     # point swing per update = momentum shift
    _BLOWOUT_DIFF   = 18    # score diff ≥ this = blowout territory

    def __init__(self, game_id, home_team: str, away_team: str,
                 initial_script: str = "", initial_pace: str = "AVERAGE_PACE"):
        self.game_id        = game_id
        self.home_team      = home_team
        self.away_team      = away_team

        # Live state
        self._period        = None
        self._home          = 0
        self._away          = 0
        self._diff          = 0
        self._events        = []

        # Script / pace tracking
        self._initial_script   = initial_script
        self._current_script   = initial_script
        self._initial_pace     = initial_pace
        self._current_pace     = initial_pace
        self._script_history   = []   # [(period, script, pace, cause)]
        self._causality_log    = []   # full cause-effect records for learning
        self._flagged_picks    = []   # picks flagged due to script change

        # Per-quarter player stat snapshots for causality detection
        self._player_snapshots = {}   # {period: {player: stats_dict}}

    # ── Causality detection ────────────────────────────────────────────────────

    def _detect_causes(self, period, home_score, away_score,
                       prev_script, new_script,
                       player_stats: dict = None,
                       injuries: dict = None) -> list:
        """
        Determine WHY the script/pace changed.
        Returns a list of cause strings for the causality log.
        """
        causes = []
        diff   = abs(home_score - away_score)

        # 1. Score-based causes
        if diff >= self._BLOWOUT_DIFF:
            leader = self.home_team if home_score > away_score else self.away_team
            causes.append(f"BLOWOUT — {leader} leads by {diff} in Q{period}")
        elif diff <= 2 and self._diff > 8:
            causes.append(f"GAME_TIGHTENED — lead collapsed from {self._diff} to {diff} pts")

        # 2. Player-based causes (star going quiet or exploding)
        if player_stats and period > 1:
            prev_snap = self._player_snapshots.get(period - 1, {})
            for pname, cur in player_stats.items():
                prev = prev_snap.get(pname, {})
                cur_pts  = float(cur.get("pts", 0) or 0)
                prev_pts = float(prev.get("pts", 0) or 0)
                qtr_pts  = cur_pts - prev_pts
                if qtr_pts <= 0 and prev_pts >= 8:
                    causes.append(f"STAR_QUIET — {pname} scoreless in Q{period}")
                elif qtr_pts >= 12:
                    causes.append(f"STAR_EXPLOSION — {pname} dropped {qtr_pts:.0f} in Q{period}")

        # 3. Injury-based causes
        if injuries:
            for p in (injuries.get("out") or []):
                role = str(p.get("role", "")).upper()
                if role in (ROLE_PRIMARY_SCORER, ROLE_PLAYMAKER_HUB):
                    causes.append(f"INJURY_OUT — {p.get('name','?')} ({role}) out")
            for p in (injuries.get("gtd") or []):
                role = str(p.get("role", "")).upper()
                if role in (ROLE_PRIMARY_SCORER, ROLE_PLAYMAKER_HUB):
                    causes.append(f"INJURY_GTD — {p.get('name','?')} ({role}) questionable")

        # 4. Pace-based causes (from projected total rate)
        projected_rate = (home_score + away_score) / max(period, 1)
        if projected_rate > 58:
            causes.append(f"PACE_SURGE — projected total pace {projected_rate:.1f} pts/qtr")
        elif projected_rate < 42:
            causes.append(f"PACE_STALL — projected total pace {projected_rate:.1f} pts/qtr")

        # 5. Script change description
        if prev_script and new_script and prev_script != new_script:
            causes.append(f"SCRIPT_SHIFT — {prev_script} → {new_script}")

        return causes if causes else ["NATURAL_PROGRESSION"]

    # ── Script re-evaluation ───────────────────────────────────────────────────

    def re_evaluate_script(self, period, home_score: int, away_score: int,
                           player_stats: dict = None, injuries: dict = None) -> dict:
        """
        Re-run full pace + game script classification live.
        Called every quarter. Returns a script_change event if script shifted.
        """
        diff      = abs(home_score - away_score)
        total_pts = home_score + away_score

        # Live flow detection
        if diff >= 18:        flow = "BLOWOUT"
        elif diff >= 12:      flow = "DOUBLE_DIGIT_LEAD"
        elif diff >= 6:       flow = "COMFORTABLE_LEAD"
        elif diff >= 2:       flow = "COMPETITIVE"
        else:                 flow = "TIGHT_GAME"

        # Live pace detection from scoring rate
        projected_total = (total_pts / max(period, 1)) * 4
        if projected_total >= 232:   pace = "TRANSITION_HEAVY"
        elif projected_total >= 220: pace = "UPTEMPO"
        elif projected_total >= 208: pace = "AVERAGE_PACE"
        elif projected_total >= 196: pace = "SLOW_PACED"
        else:                        pace = "HALFCOURT"

        # Live scoring label
        if projected_total >= 230:   scoring = "SHOOTOUT"
        elif projected_total >= 218: scoring = "HIGH_SCORING"
        elif projected_total <= 200: scoring = "DEFENSIVE_BATTLE"
        else:                        scoring = "NORMAL_SCORING"

        new_script = f"{flow}_{scoring}"
        prev_script = self._current_script

        # Store player snapshot for next quarter's causality detection
        if player_stats:
            self._player_snapshots[period] = dict(player_stats)

        # Detect causes regardless of whether script changed
        causes = self._detect_causes(
            period, home_score, away_score,
            prev_script, new_script, player_stats, injuries
        )

        # Record in history
        self._script_history.append({
            "period": period, "script": new_script, "pace": pace, "causes": causes,
        })

        event = None
        if new_script != prev_script or pace != self._current_pace:
            self._current_script = new_script
            self._current_pace   = pace

            cause_str = " | ".join(causes)
            event = {
                "type":          "script_change",
                "description":   (f"Script shifted {prev_script} → {new_script} "
                                  f"(pace: {pace}) | Cause: {cause_str}"),
                "period":        period,
                "prev_script":   prev_script,
                "new_script":    new_script,
                "pace":          pace,
                "causes":        causes,
                "home_score":    home_score,
                "away_score":    away_score,
                "flag_picks":    True,   # flag pending picks — still send, but mark
            }

            # Causality log for Layer 6 learning
            self._causality_log.append({
                "period":      period,
                "from_script": prev_script,
                "to_script":   new_script,
                "pace":        pace,
                "causes":      causes,
                "home_score":  home_score,
                "away_score":  away_score,
            })

        return event

    def flag_pick(self, pick_id: str, reason: str):
        """Mark a pending pick as sent under a changed script."""
        self._flagged_picks.append({"pick_id": pick_id, "reason": reason})

    def get_causality_log(self) -> list:
        """Return all cause-effect records for post-game learning."""
        return list(self._causality_log)

    def get_flagged_picks(self) -> list:
        return list(self._flagged_picks)

    # ── Standard live update ───────────────────────────────────────────────────

    def update(self, period, time_str, home_score: int, away_score: int,
               player_stats: dict = None, injuries: dict = None):
        """
        Feed latest BDL live data.
        Returns list of events (may include script_change, quarter_change,
        momentum_swing, blowout, close_game).
        """
        try:
            hs   = int(home_score or 0)
            as_  = int(away_score or 0)
            diff = hs - as_
        except (TypeError, ValueError):
            return []

        events = []

        # Quarter change → trigger full script re-evaluation
        if self._period is not None and period != self._period:
            qtr_event = {
                "type":        "quarter_change",
                "description": f"{self.home_team} {hs} – {self.away_team} {as_} entering Q{period}",
                "period":      period,
                "home_score":  hs,
                "away_score":  as_,
                "diff":        diff,
            }
            events.append(qtr_event)

            # Re-evaluate script every quarter
            script_event = self.re_evaluate_script(period, hs, as_, player_stats, injuries)
            if script_event:
                events.append(script_event)

        elif self._period is not None:
            swing = diff - self._diff
            if abs(swing) >= self._MOMENTUM_SWING:
                leader = self.home_team if diff > 0 else self.away_team
                causes = self._detect_causes(
                    period, hs, as_, self._current_script,
                    self._current_script, player_stats, injuries
                )
                event = {
                    "type":        "momentum_swing",
                    "description": (f"{leader} goes on a {abs(swing)}-pt run — "
                                    f"now {hs}–{as_} (Q{period} {time_str})"),
                    "period":      period,
                    "home_score":  hs,
                    "away_score":  as_,
                    "diff":        diff,
                    "causes":      causes,
                }
                events.append(event)
                # Big enough swing → also re-evaluate script mid-quarter
                if abs(swing) >= 12:
                    script_event = self.re_evaluate_script(period, hs, as_, player_stats, injuries)
                    if script_event:
                        events.append(script_event)

            elif abs(diff) >= self._BLOWOUT_DIFF and abs(self._diff) < self._BLOWOUT_DIFF:
                leader = self.home_team if diff > 0 else self.away_team
                causes = self._detect_causes(
                    period, hs, as_, self._current_script,
                    self._current_script, player_stats, injuries
                )
                event = {
                    "type":        "blowout",
                    "description": (f"BLOWOUT alert — {leader} leads by {abs(diff)} "
                                    f"({hs}–{as_}, Q{period})"),
                    "period":      period,
                    "home_score":  hs,
                    "away_score":  as_,
                    "diff":        diff,
                    "causes":      causes,
                }
                events.append(event)

            elif period == 4 and abs(self._diff) > 8 and abs(diff) <= 4:
                event = {
                    "type":        "close_game",
                    "description": (f"Game tightening — {self.home_team} {hs} – "
                                    f"{self.away_team} {as_} ({time_str} Q4)"),
                    "period":      period,
                    "home_score":  hs,
                    "away_score":  as_,
                    "diff":        diff,
                    "causes":      ["GAME_TIGHTENED_Q4"],
                }
                events.append(event)

        self._period = period
        self._home   = hs
        self._away   = as_
        self._diff   = diff
        for ev in events:
            self._events.append(ev)
        return events

    def current_flow(self) -> str:
        """Return current flow label based on live score diff."""
        diff = abs(self._diff)
        if diff >= 18:  return "BLOWOUT"
        if diff >= 12:  return "DOUBLE_DIGIT_LEAD"
        if diff >= 6:   return "COMFORTABLE_LEAD"
        if diff >= 2:   return "COMPETITIVE"
        return "TIGHT_GAME"

    def current_script(self) -> str:
        return self._current_script or self._initial_script

    def current_pace(self) -> str:
        return self._current_pace or self._initial_pace

    def event_log(self) -> list:
        return list(self._events)

    def reset(self):
        self._period = None
        self._home   = 0
        self._away   = 0
        self._diff   = 0
        self._events = []
        self._script_history = []
        self._causality_log  = []
        self._flagged_picks  = []
        self._player_snapshots = {}


# Registry so bot.py can look up trackers by BDL game_id
_context_registry: dict = {}


def get_context_tracker(game_id, home_team: str = "", away_team: str = "") -> "ContextTracker":
    """Get or create a ContextTracker for a BDL game_id."""
    if game_id not in _context_registry:
        _context_registry[game_id] = ContextTracker(game_id, home_team, away_team)
    return _context_registry[game_id]


def purge_context_trackers(active_game_ids: list) -> None:
    """Remove trackers for games no longer in the live feed."""
    stale = [gid for gid in _context_registry if gid not in active_game_ids]
    for gid in stale:
        del _context_registry[gid]


# ══════════════════════════════════════════════════════════════════════════════
# SHOT EFFICIENCY SIGNAL — uses CDN play-by-play data stored in player_observations
# ══════════════════════════════════════════════════════════════════════════════

def get_shot_efficiency_signal(conn, player_name: str, stat_type: str,
                               lookback_games: int = 5) -> float:
    """
    Returns a confidence multiplier (0.85–1.15) based on recent shot efficiency
    pulled from player_observations (populated by CDN play-by-play).

    stat_type:
      "threes"   → uses fg3a/fg3m — hot from three boosts, cold penalises
      "points"   → uses all shot types combined
      "rebounds" → shot-agnostic, returns 1.0 (no signal)
      "assists"  → shot-agnostic, returns 1.0

    Returns 1.0 (neutral) if insufficient data.
    """
    if stat_type not in ("threes", "points"):
        return 1.0

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT fg3a, fg3m, rim_a, rim_m, mid_a, mid_m
            FROM player_observations
            WHERE player_name = %s
              AND (fg3a + rim_a + mid_a) > 0
            ORDER BY observed_at DESC
            LIMIT %s
        """, (player_name, lookback_games))
        rows = cur.fetchall()
        cur.close()
    except Exception:
        return 1.0

    if not rows:
        return 1.0

    if stat_type == "threes":
        total_fg3a = sum(r[0] for r in rows)
        total_fg3m = sum(r[1] for r in rows)
        if total_fg3a < 5:          # Need at least 5 attempts for signal
            return 1.0
        pct = total_fg3m / total_fg3a
        # NBA average 3PT% ≈ 36%
        if pct >= 0.44:   return 1.12    # Running very hot from three
        if pct >= 0.40:   return 1.06    # Above average
        if pct <= 0.28:   return 0.88    # Very cold from three
        if pct <= 0.32:   return 0.94    # Below average
        return 1.0                       # Around average — neutral

    if stat_type == "points":
        total_att = sum(r[0]+r[2]+r[4] for r in rows)
        total_made = sum(r[1]+r[3]+r[5] for r in rows)
        if total_att < 10:
            return 1.0
        pct = total_made / total_att
        # NBA average FG% ≈ 46%
        if pct >= 0.54:   return 1.10
        if pct >= 0.50:   return 1.05
        if pct <= 0.36:   return 0.90
        if pct <= 0.40:   return 0.95
        return 1.0

    return 1.0
