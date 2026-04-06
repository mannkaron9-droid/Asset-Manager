"""
GAME SCRIPT ANALYZER
====================
Classifies each game along 4 dimensions, assigns player roles,
and determines which players are fade candidates vs benefactors.

Dimensions:
  1. Pace                        : HALFCOURT | SLOW_PACED | AVERAGE_PACE | UPTEMPO | TRANSITION_HEAVY
  2. Game Flow / Competitiveness : BLOWOUT | DOUBLE_DIGIT_LEAD | COMFORTABLE_LEAD | COMPETITIVE | TIGHT_GAME
  3. Scoring Environment         : DEFENSIVE_BATTLE | NORMAL_SCORING | HIGH_SCORING | SHOOTOUT
  4. Team Offensive Style        : STAR_HEAVY | BALANCED | FACILITATOR
  5. Defensive Style             : TACTICAL | PRESSURE | ZONE

Roles:
  go_to_scorer   - high usage star, primary scoring option (points, pra)
  floor_general  - high assists, facilitator / pressure env (assists, pra)
  glass_cleaner  - dominant rebounder, physical/halfcourt game (rebounds)
  rim_anchor     - shot blocker / interior defender (rebounds, blocks)
  spot_up_shooter- off-ball 3PT threat, moderate usage (3pm, points)
  combo_creator  - secondary playmaker in deficit/transition scripts (pra)
  sixth_man      - bench scorer, limited minutes (points)
  utility_player - balanced stats, no dominant category (pra)
"""

from dataclasses import dataclass, field
from typing import Optional


# ── Pace thresholds (based on game total O/U) ─────────────────────────────────
PACE_TRANSITION  = 232    # ≥ this total → TRANSITION_HEAVY (fastest)
PACE_UPTEMPO     = 224    # ≥ this total → UPTEMPO
PACE_AVERAGE     = 216    # ≥ this total → AVERAGE_PACE
PACE_SLOW        = 208    # ≥ this total → SLOW_PACED
                          # < PACE_SLOW   → HALFCOURT (grind)

# ── Flow thresholds (based on point spread) ───────────────────────────────────
SPREAD_BLOWOUT         = 12.0   # spread ≥ this   → BLOWOUT
SPREAD_DOUBLE_DIGIT    = 8.0    # spread ≥ this   → DOUBLE_DIGIT_LEAD
SPREAD_COMFORTABLE     = 5.5    # spread ≥ this   → COMFORTABLE_LEAD
SPREAD_TIGHT           = 3.5    # spread ≤ this   → TIGHT_GAME
                                # between TIGHT and COMFORTABLE → COMPETITIVE

# ── Scoring environment thresholds (based on total O/U) ──────────────────────
SCORING_SHOOTOUT       = 232    # ≥ this total → SHOOTOUT
SCORING_HIGH           = 222    # ≥ this total → HIGH_SCORING
SCORING_NORMAL         = 212    # ≥ this total → NORMAL_SCORING
                                # < SCORING_NORMAL → DEFENSIVE_BATTLE

# ── Public pressure thresholds ────────────────────────────────────────────────
PUBLIC_HEAVY_PCT = 65     # % of bets on one side → "public heavy"

# ── Juice thresholds ──────────────────────────────────────────────────────────
JUICE_RED_FLAG   = -300   # ≤ this → red flag (needs justification)
JUICE_YELLOW     = -200   # between YELLOW and RED → check EV carefully
JUICE_GREEN_MAX  = 150    # ≤ this on positive side → sweet spot

# ── Per-team style profiles (2024-25 NBA) ─────────────────────────────────────
# pace        : estimated possessions per game (NBA avg ~100)
# assist_bias : fraction of FGM that are assisted (team play rate, avg ~0.52)
# reb_bias    : rebounding strength indicator (avg ~0.50; >0.52 = above-average)
#
# Thresholds (mirroring ai_adjust_matchup() from the analytics layer):
#   assist_bias > 0.52 → bonus for assists OVER
#   reb_bias    > 0.52 → bonus for rebounds OVER
#   pace        > 100  → bonus for points OVER (more possessions)
_DEFAULT_STYLE = {
    "pace": 100, "assist_bias": 0.53, "reb_bias": 0.50,
    "def_strength": 68, "off_strength": 72, "three_pt_rate": 65, "strength": 65,
}

# ── 2024-25 season baselines (auto-calibrated; update via /calibrate) ─────────
# pace        = estimated possessions/48 min (96-108 range)
# assist_bias = team ast / team fgm  (>0.52 = pass-first bonus)
# reb_bias    = team reb / league-avg 44  (>0.52 = rebounding bonus)
TEAM_STYLES: dict = {
    # Fields:
    #   pace         – possessions/48 min proxy (96-108)
    #   assist_bias  – ast/fgm; floor 0.53 (all teams pass)
    #   reb_bias     – team reb / 44 league avg; >0.52 = rebounding bonus
    #   def_strength – how well the team defends (0-100)
    #   off_strength – how well the team scores offensively (0-100)
    #   three_pt_rate– reliance on 3-point shooting (0-100)
    #   strength     – overall team power / win-rate proxy (0-100)
    #                  Used to determine favorite (high) vs underdog (low)

    # ── Atlantic ──────────────────────────────────────────────────────
    "Celtics":       {"pace": 101, "assist_bias": 0.57, "reb_bias": 0.50,
                      "def_strength": 92, "off_strength": 88, "three_pt_rate": 90, "strength": 90},
    "Nets":          {"pace": 99,  "assist_bias": 0.53, "reb_bias": 0.48,
                      "def_strength": 62, "off_strength": 66, "three_pt_rate": 60, "strength": 52},
    "Knicks":        {"pace": 98,  "assist_bias": 0.53, "reb_bias": 0.51,
                      "def_strength": 79, "off_strength": 78, "three_pt_rate": 70, "strength": 78},
    "76ers":         {"pace": 99,  "assist_bias": 0.53, "reb_bias": 0.49,
                      "def_strength": 65, "off_strength": 72, "three_pt_rate": 65, "strength": 60},
    "Raptors":       {"pace": 99,  "assist_bias": 0.54, "reb_bias": 0.49,
                      "def_strength": 66, "off_strength": 69, "three_pt_rate": 65, "strength": 60},

    # ── Central ───────────────────────────────────────────────────────
    "Bulls":         {"pace": 101, "assist_bias": 0.53, "reb_bias": 0.50,
                      "def_strength": 64, "off_strength": 71, "three_pt_rate": 65, "strength": 63},
    "Cavaliers":     {"pace": 100, "assist_bias": 0.60, "reb_bias": 0.54,
                      "def_strength": 83, "off_strength": 84, "three_pt_rate": 72, "strength": 87},
    "Pistons":       {"pace": 99,  "assist_bias": 0.53, "reb_bias": 0.50,
                      "def_strength": 60, "off_strength": 65, "three_pt_rate": 60, "strength": 50},
    "Pacers":        {"pace": 103, "assist_bias": 0.56, "reb_bias": 0.52,
                      "def_strength": 62, "off_strength": 82, "three_pt_rate": 80, "strength": 74},
    "Bucks":         {"pace": 101, "assist_bias": 0.55, "reb_bias": 0.52,
                      "def_strength": 73, "off_strength": 79, "three_pt_rate": 75, "strength": 72},

    # ── Southeast ─────────────────────────────────────────────────────
    "Hawks":         {"pace": 100, "assist_bias": 0.55, "reb_bias": 0.47,
                      "def_strength": 60, "off_strength": 76, "three_pt_rate": 72, "strength": 64},
    "Hornets":       {"pace": 102, "assist_bias": 0.55, "reb_bias": 0.46,
                      "def_strength": 58, "off_strength": 72, "three_pt_rate": 68, "strength": 56},
    "Heat":          {"pace": 97,  "assist_bias": 0.54, "reb_bias": 0.50,
                      "def_strength": 80, "off_strength": 72, "three_pt_rate": 68, "strength": 72},
    "Magic":         {"pace": 101, "assist_bias": 0.53, "reb_bias": 0.47,
                      "def_strength": 82, "off_strength": 70, "three_pt_rate": 55, "strength": 70},
    "Wizards":       {"pace": 97,  "assist_bias": 0.53, "reb_bias": 0.49,
                      "def_strength": 55, "off_strength": 63, "three_pt_rate": 58, "strength": 42},

    # ── Northwest ─────────────────────────────────────────────────────
    "Nuggets":       {"pace": 100, "assist_bias": 0.62, "reb_bias": 0.52,
                      "def_strength": 72, "off_strength": 83, "three_pt_rate": 60, "strength": 80},
    "Timberwolves":  {"pace": 100, "assist_bias": 0.53, "reb_bias": 0.54,
                      "def_strength": 87, "off_strength": 76, "three_pt_rate": 65, "strength": 82},
    "Thunder":       {"pace": 103, "assist_bias": 0.57, "reb_bias": 0.53,
                      "def_strength": 88, "off_strength": 85, "three_pt_rate": 78, "strength": 92},
    "Trail Blazers": {"pace": 99,  "assist_bias": 0.54, "reb_bias": 0.50,
                      "def_strength": 60, "off_strength": 68, "three_pt_rate": 65, "strength": 57},
    "Jazz":          {"pace": 98,  "assist_bias": 0.53, "reb_bias": 0.51,
                      "def_strength": 65, "off_strength": 68, "three_pt_rate": 65, "strength": 58},

    # ── Pacific ───────────────────────────────────────────────────────
    "Warriors":      {"pace": 101, "assist_bias": 0.66, "reb_bias": 0.47,
                      "def_strength": 71, "off_strength": 80, "three_pt_rate": 95, "strength": 76},
    "Clippers":      {"pace": 99,  "assist_bias": 0.53, "reb_bias": 0.49,
                      "def_strength": 68, "off_strength": 75, "three_pt_rate": 65, "strength": 70},
    "Lakers":        {"pace": 100, "assist_bias": 0.53, "reb_bias": 0.52,
                      "def_strength": 74, "off_strength": 78, "three_pt_rate": 60, "strength": 75},
    "Suns":          {"pace": 100, "assist_bias": 0.56, "reb_bias": 0.48,
                      "def_strength": 66, "off_strength": 78, "three_pt_rate": 75, "strength": 68},
    "Kings":         {"pace": 103, "assist_bias": 0.55, "reb_bias": 0.50,
                      "def_strength": 63, "off_strength": 80, "three_pt_rate": 72, "strength": 72},

    # ── Southwest ─────────────────────────────────────────────────────
    "Mavericks":     {"pace": 101, "assist_bias": 0.56, "reb_bias": 0.49,
                      "def_strength": 70, "off_strength": 80, "three_pt_rate": 78, "strength": 75},
    "Rockets":       {"pace": 98,  "assist_bias": 0.53, "reb_bias": 0.56,
                      "def_strength": 70, "off_strength": 73, "three_pt_rate": 70, "strength": 68},
    "Grizzlies":     {"pace": 97,  "assist_bias": 0.53, "reb_bias": 0.56,
                      "def_strength": 75, "off_strength": 72, "three_pt_rate": 55, "strength": 65},
    "Pelicans":      {"pace": 97,  "assist_bias": 0.53, "reb_bias": 0.49,
                      "def_strength": 63, "off_strength": 68, "three_pt_rate": 60, "strength": 50},
    "Spurs":         {"pace": 100, "assist_bias": 0.63, "reb_bias": 0.48,
                      "def_strength": 65, "off_strength": 70, "three_pt_rate": 60, "strength": 58},
}

# ── ESPN team IDs for live calibration ────────────────────────────────────────
_ESPN_TEAM_IDS: dict = {
    "Hawks": 1, "Celtics": 2, "Pelicans": 3, "Bulls": 4, "Cavaliers": 5,
    "Mavericks": 6, "Nuggets": 7, "Pistons": 8, "Warriors": 9, "Rockets": 10,
    "Pacers": 11, "Clippers": 12, "Lakers": 13, "Heat": 14, "Bucks": 15,
    "Timberwolves": 16, "Nets": 17, "Knicks": 18, "Magic": 19, "76ers": 20,
    "Suns": 21, "Trail Blazers": 22, "Kings": 23, "Spurs": 24, "Thunder": 25,
    "Jazz": 26, "Wizards": 27, "Raptors": 28, "Grizzlies": 29, "Hornets": 30,
}

_ESPN_STATS_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"


def _espn_team_season_stats(team_id: int) -> dict:
    """
    Fetch season-average stats for one team from ESPN.
    Returns dict of stat_name → value (floats).
    """
    import urllib.request as _ur
    import json as _json
    url = f"{_ESPN_STATS_BASE}/teams/{team_id}/statistics"
    try:
        with _ur.urlopen(url, timeout=8) as r:
            data = _json.loads(r.read())
    except Exception:
        return {}

    result = {}
    for group in data.get("results", []):
        for stat in group.get("stats", []):
            name  = stat.get("name", "")
            value = stat.get("value")
            if name and value is not None:
                try:
                    result[name] = float(value)
                except (TypeError, ValueError):
                    pass
    return result


def fetch_calibrated_team_styles() -> dict:
    """
    Pull live ESPN season-average stats for all 30 teams and compute
    calibrated pace / assist_bias / reb_bias values.

    Metrics:
      pace        — derived from avgPoints (team) as a proxy for possessions.
                    Mapped to 96-108 range: pace = round(98 + (avgPts - 113) * 0.5)
      assist_bias — avgAssists / avgFieldGoalsMade
                    (clamped 0.44–0.70)
      reb_bias    — avgRebounds / 44.0
                    (clamped 0.42–0.58)

    Falls back to the current TEAM_STYLES entry on any per-team error.
    Returns the new styles dict; does NOT mutate TEAM_STYLES (caller decides).
    """
    calibrated = {}
    for team_name, espn_id in _ESPN_TEAM_IDS.items():
        try:
            stats = _espn_team_season_stats(espn_id)
            if not stats:
                calibrated[team_name] = TEAM_STYLES.get(team_name, _DEFAULT_STYLE).copy()
                continue

            avg_pts  = stats.get("avgPoints",         113.0)
            avg_ast  = stats.get("avgAssists",         25.0)
            avg_fgm  = stats.get("avgFieldGoalsMade",  41.0)
            avg_reb  = stats.get("avgRebounds",        44.0)

            pace = round(98.0 + (avg_pts - 113.0) * 0.5)
            pace = max(95, min(108, pace))

            assist_bias = round(avg_ast / avg_fgm, 2) if avg_fgm > 0 else 0.52
            assist_bias = max(0.44, min(0.70, assist_bias))

            reb_bias = round(avg_reb / 44.0, 2)
            reb_bias = max(0.42, min(0.58, reb_bias))

            # def_strength is not available from ESPN — carry forward existing value
            existing_def = TEAM_STYLES.get(team_name, _DEFAULT_STYLE).get("def_strength", 68)
            calibrated[team_name] = {
                "pace":         pace,
                "assist_bias":  assist_bias,
                "reb_bias":     reb_bias,
                "def_strength": existing_def,
            }
        except Exception:
            calibrated[team_name] = TEAM_STYLES.get(team_name, _DEFAULT_STYLE).copy()

    return calibrated


def check_style_accuracy(conn) -> dict:
    """
    Reads graded bets from the DB and checks whether each team's style
    bonuses predicted the correct prop direction.

    Returns:
      {
        "assists":  {"total": N, "correct": N, "accuracy": 0.64, "suggestion": "keep"},
        "rebounds": {"total": N, "correct": N, "accuracy": 0.48, "suggestion": "reduce"},
        "points":   {"total": N, "correct": N, "accuracy": 0.59, "suggestion": "keep"},
        "notes":    ["Warriors assist bonus: 8/10 correct", ...]
      }
    """
    result: dict = {
        "assists":  {"total": 0, "correct": 0, "accuracy": 0.0, "suggestion": "keep"},
        "rebounds": {"total": 0, "correct": 0, "accuracy": 0.0, "suggestion": "keep"},
        "points":   {"total": 0, "correct": 0, "accuracy": 0.0, "suggestion": "keep"},
        "notes":    [],
    }
    if conn is None:
        result["notes"].append("No DB connection — accuracy check skipped.")
        return result

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT player, game, pick, bet_type, result FROM bets "
            "WHERE result IN ('WIN','LOSS') AND bet_type IS NOT NULL"
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        result["notes"].append(f"DB query error: {e}")
        return result

    if not rows:
        result["notes"].append("No graded bets in DB yet.")
        return result

    # bucket bets by stat category and check style bonus alignment
    stat_map = {
        "assists":  ["ast", "assist"],
        "rebounds": ["reb", "rebound"],
        "points":   ["pts", "point", "score"],
    }

    team_notes: dict = {}   # team_name → {stat: {correct, total}}

    for player, game, pick, bet_type, res in rows:
        bt = (bet_type or "").lower()
        pick_str = (pick or "").upper()
        won = (res == "WIN")

        # infer which team the player was on from the game string
        # game format is typically "TEAM1 vs TEAM2" or "TEAM1 @ TEAM2"
        matched_team = None
        for team_name in TEAM_STYLES:
            if team_name.lower() in (game or "").lower():
                style = TEAM_STYLES[team_name]
                matched_team = team_name
                break

        for stat_key, keywords in stat_map.items():
            if not any(k in bt for k in keywords):
                continue

            style = TEAM_STYLES.get(matched_team, _DEFAULT_STYLE) if matched_team else _DEFAULT_STYLE

            # Was a bonus applied (i.e. style triggers the bonus)?
            if stat_key == "assists":
                bonus_applied = style["assist_bias"] > 0.52
                bonus_dir = "OVER"
            elif stat_key == "rebounds":
                bonus_applied = style["reb_bias"] > 0.52
                bonus_dir = "OVER"
            else:  # points
                bonus_applied = style["pace"] > 100
                bonus_dir = "OVER"

            if not bonus_applied:
                continue   # only grade bets where we applied a bonus

            result[stat_key]["total"] += 1
            # correct = bonus predicted OVER and bet won
            correct = ("OVER" in pick_str and won) or ("UNDER" in pick_str and not won)
            if correct:
                result[stat_key]["correct"] += 1

            if matched_team:
                tk = team_notes.setdefault(matched_team, {})
                sk = tk.setdefault(stat_key, {"correct": 0, "total": 0})
                sk["total"] += 1
                if correct:
                    sk["correct"] += 1

    # compute accuracy + suggestion per stat
    for stat_key in ("assists", "rebounds", "points"):
        tot = result[stat_key]["total"]
        cor = result[stat_key]["correct"]
        if tot > 0:
            acc = cor / tot
            result[stat_key]["accuracy"] = round(acc, 3)
            if acc < 0.50:
                result[stat_key]["suggestion"] = "reduce or flip bias threshold"
            elif acc >= 0.60:
                result[stat_key]["suggestion"] = "strengthen — bonus is working"
            else:
                result[stat_key]["suggestion"] = "keep"

    # per-team notes for standouts
    for team_name, stats in team_notes.items():
        for stat_key, counts in stats.items():
            if counts["total"] >= 3:
                acc = counts["correct"] / counts["total"]
                flag = "✅" if acc >= 0.60 else ("⚠️" if acc < 0.50 else "")
                result["notes"].append(
                    f"{flag} {team_name} {stat_key}: {counts['correct']}/{counts['total']} correct ({acc:.0%})"
                )

    return result


@dataclass
class GameScript:
    game: str
    home_team: str
    away_team: str
    total: float = 0.0
    spread: float = 0.0           # absolute value of spread

    pace: str = "AVERAGE_PACE"    # HALFCOURT | SLOW_PACED | AVERAGE_PACE | UPTEMPO | TRANSITION_HEAVY
    flow: str = "COMPETITIVE"     # BLOWOUT | DOUBLE_DIGIT_LEAD | COMFORTABLE_LEAD | COMPETITIVE | TIGHT_GAME
    scoring: str = "NORMAL_SCORING"  # DEFENSIVE_BATTLE | NORMAL_SCORING | HIGH_SCORING | SHOOTOUT
    offense_home: str = "BALANCED"   # STAR_HEAVY | BALANCED | FACILITATOR
    offense_away: str = "BALANCED"
    defense_home: str = "TACTICAL"  # TACTICAL | PRESSURE | ZONE
    defense_away: str = "TACTICAL"

    label: str = "COMPETITIVE_NORMAL_SCORING"   # {flow}_{scoring} combo label


@dataclass
class PlayerRole:
    player: str
    team: str
    role: str                     # go_to_scorer | floor_general | glass_cleaner | rim_anchor | spot_up_shooter | combo_creator | sixth_man | utility_player
    is_star: bool = False
    avg_pts: float = 0.0
    avg_reb: float = 0.0
    avg_ast: float = 0.0
    avg_mins: float = 0.0
    avg_usage: float = 0.0
    fade_candidate: bool = False
    benefactor_of: Optional[str] = None   # name of the faded star this player benefits from
    role_reason: str = ""


# ── Historical team offensive style profiles ──────────────────────────────────
# Based on real team tendencies; updated manually each season
TEAM_STYLE_MAP = {
    # STAR_HEAVY teams (1-2 players carry everything)
    "lakers":          "STAR_HEAVY",
    "nuggets":         "STAR_HEAVY",
    "76ers":           "STAR_HEAVY",
    "bucks":           "STAR_HEAVY",
    "mavs":            "STAR_HEAVY",
    "mavericks":       "STAR_HEAVY",
    "thunder":         "STAR_HEAVY",
    "knicks":          "STAR_HEAVY",

    # FACILITATOR_DRIVEN teams (plays through a passer/playmaker)
    "spurs":           "FACILITATOR",
    "hawks":           "FACILITATOR",
    "wolves":          "FACILITATOR",
    "timberwolves":    "FACILITATOR",
    "magic":           "FACILITATOR",
    "heat":            "FACILITATOR",

    # BALANCED / SPREAD (multiple contributors)
    "celtics":         "BALANCED",
    "warriors":        "BALANCED",
    "suns":            "BALANCED",
    "clippers":        "BALANCED",
    "kings":           "BALANCED",
    "nets":            "BALANCED",
    "pacers":          "BALANCED",
    "cavaliers":       "BALANCED",
    "cavs":            "BALANCED",
}

# ── Defensive scheme profiles ─────────────────────────────────────────────────
TEAM_DEFENSE_MAP = {
    "heat":         "PRESSURE",   # heavy trap / press
    "celtics":      "ZONE",       # zone switching heavy
    "wolves":       "ZONE",
    "timberwolves": "ZONE",
    "pacers":       "PRESSURE",
    "warriors":     "PRESSURE",
    "bucks":        "TACTICAL",
    "nuggets":      "TACTICAL",
    "lakers":       "TACTICAL",
    "knicks":       "TACTICAL",
    "76ers":        "ZONE",
    "spurs":        "ZONE",
}


def _keyword(team_name: str) -> str:
    """Extract the last word (usually nickname) from a team's full name."""
    return team_name.strip().split()[-1].lower() if team_name else ""


def classify_pace(total: float) -> str:
    """5-bucket pace from game total O/U."""
    if total >= PACE_TRANSITION:
        return "TRANSITION_HEAVY"
    if total >= PACE_UPTEMPO:
        return "UPTEMPO"
    if total >= PACE_AVERAGE:
        return "AVERAGE_PACE"
    if total >= PACE_SLOW:
        return "SLOW_PACED"
    return "HALFCOURT"


def classify_flow(spread: float) -> str:
    """5-bucket flow from absolute spread value."""
    if spread >= SPREAD_BLOWOUT:
        return "BLOWOUT"
    if spread >= SPREAD_DOUBLE_DIGIT:
        return "DOUBLE_DIGIT_LEAD"
    if spread >= SPREAD_COMFORTABLE:
        return "COMFORTABLE_LEAD"
    if spread <= SPREAD_TIGHT:
        return "TIGHT_GAME"
    return "COMPETITIVE"


def classify_scoring(total: float) -> str:
    """4-bucket scoring environment from game total O/U."""
    if total >= SCORING_SHOOTOUT:
        return "SHOOTOUT"
    if total >= SCORING_HIGH:
        return "HIGH_SCORING"
    if total >= SCORING_NORMAL:
        return "NORMAL_SCORING"
    return "DEFENSIVE_BATTLE"


def classify_offense(team_name: str) -> str:
    key = _keyword(team_name)
    return TEAM_STYLE_MAP.get(key, "BALANCED")


def classify_defense(team_name: str) -> str:
    key = _keyword(team_name)
    return TEAM_DEFENSE_MAP.get(key, "TACTICAL")


def analyze_game_script(
    home_team: str,
    away_team: str,
    total: float,
    spread: float,
    game_name: str = "",
) -> GameScript:
    """
    Build a full GameScript object for a matchup.
    spread should be the absolute value of the point spread.
    """
    pace         = classify_pace(total)
    flow         = classify_flow(abs(spread))
    scoring      = classify_scoring(total)
    off_home     = classify_offense(home_team)
    off_away     = classify_offense(away_team)
    def_home     = classify_defense(home_team)
    def_away     = classify_defense(away_team)

    label = f"{flow}_{scoring}"   # e.g. COMPETITIVE_HIGH_SCORING

    return GameScript(
        game         = game_name or f"{away_team} @ {home_team}",
        home_team    = home_team,
        away_team    = away_team,
        total        = total,
        spread       = abs(spread),
        pace         = pace,
        flow         = flow,
        scoring      = scoring,
        offense_home = off_home,
        offense_away = off_away,
        defense_home = def_home,
        defense_away = def_away,
        label        = label,
    )


def assign_role(
    player: str,
    team: str,
    avg_pts: float,
    avg_reb: float,
    avg_ast: float,
    avg_mins: float,
    avg_usage: float,
    game_script: GameScript,
    is_home: bool,
    position: str = "",
    avg_3pa: float = 0.0,
    avg_blk: float = 0.0,
) -> PlayerRole:
    """
    Assign one of 8 roles using real stats + game script.
    Position is a soft tiebreaker only — it never overrides real numbers.

    go_to_scorer   → high usage star, primary scoring option (points, pra)
    floor_general  → high assists, facilitator / pressure env (assists, pra)
    glass_cleaner  → dominant rebounder, physical/halfcourt game (rebounds)
    rim_anchor     → shot blocker / interior defender (rebounds, blocks)
    spot_up_shooter→ off-ball 3PT threat, moderate usage (3pm, points)
    combo_creator  → secondary playmaker in deficit/transition scripts (pra)
    sixth_man      → bench scorer, limited minutes (points)
    utility_player → balanced stats, no dominant category (pra)
    """
    off_style = game_script.offense_home if is_home else game_script.offense_away
    def_style = game_script.defense_away if is_home else game_script.defense_home

    is_star  = avg_usage >= 25 or (avg_pts >= 20 and avg_mins >= 32)
    is_bench = avg_mins < 28

    pos      = position.upper().strip()
    is_guard = any(p in pos for p in ("PG", "SG", "G"))
    is_big   = any(p in pos for p in ("C", "PF", "F"))

    # ── Primary rules (driven entirely by real stats + game script) ────────────

    # go_to_scorer: star usage in STAR_HEAVY offense
    if is_star and off_style == "STAR_HEAVY":
        role   = "go_to_scorer"
        reason = f"Star usage ({avg_usage:.0f}%) in STAR_HEAVY offense"

    # floor_general: elite assist numbers or facilitator/pressure env
    elif avg_ast >= 5.5 or off_style == "FACILITATOR" or def_style == "PRESSURE":
        role   = "floor_general"
        reason = f"High assists ({avg_ast:.1f}) or facilitator/pressure env"

    # rim_anchor: shot blocker / interior defender (blocks before glass_cleaner)
    elif avg_blk >= 1.5 and avg_ast < 2.5:
        role   = "rim_anchor"
        reason = f"Interior anchor ({avg_blk:.1f} blk, {avg_reb:.1f} reb)"

    # glass_cleaner: dominant board presence in physical/halfcourt game
    elif (game_script.pace in ("HALFCOURT", "SLOW_PACED") and avg_reb >= 6) or avg_reb >= 9:
        role   = "glass_cleaner"
        reason = f"{game_script.pace} game + {avg_reb:.1f} avg reb"

    # spot_up_shooter: off-ball 3PT threat, moderate usage
    elif (avg_3pa >= 4 or (avg_3pa >= 2 and avg_usage < 22)) and avg_usage < 24:
        role   = "spot_up_shooter"
        reason = f"Off-ball shooter ({avg_3pa:.1f} 3PA, {avg_usage:.0f}% usage)"

    # combo_creator: secondary playmaker in deficit/transition scripts
    elif avg_mins >= 28 and avg_pts >= 10 and avg_usage < 22 and avg_ast >= 3:
        role   = "combo_creator"
        reason = "Secondary playmaker — activates in deficit/transition scripts"

    # sixth_man: bench scorer, limited minutes
    elif is_bench and avg_pts >= 10:
        role   = "sixth_man"
        reason = f"Bench scorer ({avg_pts:.1f} pts, {avg_mins:.0f} min)"

    # ── Tiebreaker zone ────────────────────────────────────────────────────────

    elif avg_usage >= 18:
        role   = "go_to_scorer"
        reason = f"Moderate-high usage ({avg_usage:.0f}%) — primary option"

    elif avg_reb >= 5 and (is_big or avg_ast < 3.0):
        role   = "glass_cleaner"
        reason = f"Board presence ({avg_reb:.1f} reb)" + (f" [{pos}]" if pos else "")

    elif avg_ast >= 3.5 and (is_guard or avg_ast >= 4.5):
        role   = "floor_general"
        reason = f"Assist producer ({avg_ast:.1f} ast)" + (f" [{pos}]" if pos else "")

    else:
        role   = "utility_player"
        reason = "Balanced stats — no dominant category" + (f" [{pos}]" if pos else "")

    return PlayerRole(
        player       = player,
        team         = team,
        role         = role,
        is_star      = is_star,
        avg_pts      = avg_pts,
        avg_reb      = avg_reb,
        avg_ast      = avg_ast,
        avg_mins     = avg_mins,
        avg_usage    = avg_usage,
        role_reason  = reason,
    )


def find_benefactors(
    faded_player: str,
    faded_stat: str,
    team_players: list,
    game_script: GameScript,
) -> list:
    """
    Given a faded star and their stat type, find which teammates
    will inherit the production.

    team_players: list of PlayerRole objects for the same team.
    Returns: sorted list of (player_name, inherited_stat, reason) tuples.
    """
    benefactors = []

    for p in team_players:
        if p.player == faded_player:
            continue

        # Stats shift logic based on what the star would have produced
        if faded_stat == "points":
            # If star scorer is faded → next scorer on team gets touches
            if p.role in ("go_to_scorer", "combo_creator", "sixth_man") and p.avg_pts >= 10:
                benefactors.append((p.player, "points",
                    f"Inherits scoring from faded {faded_player}"))
            elif p.role == "floor_general":
                benefactors.append((p.player, "assists",
                    f"More ball movement when {faded_player} under-used"))

        elif faded_stat == "rebounds":
            # Star big man faded on boards → other bigs / active rebounders step up
            if p.role in ("glass_cleaner", "rim_anchor") and p.avg_reb >= 5:
                benefactors.append((p.player, "rebounds",
                    f"Board vacated by {faded_player} fade"))

        elif faded_stat == "assists":
            # Fading the primary playmaker → ball handler / secondary PG picks up
            if p.role in ("floor_general", "combo_creator"):
                benefactors.append((p.player, "assists",
                    f"Becomes primary facilitator with {faded_player} limited"))

        # Game script boosts
        if game_script.pace in ("UPTEMPO", "TRANSITION_HEAVY") and p.role == "go_to_scorer" and p.avg_pts >= 12:
            # Up-tempo = more possessions = secondary scorers flourish
            if (p.player, "points", "") not in [(b[0], b[1], b[2]) for b in benefactors]:
                benefactors.append((p.player, "points",
                    f"{game_script.pace} pace boosts secondary scorer usage"))

    # Deduplicate and sort by avg production (most reliable first)
    seen = set()
    unique = []
    for b in benefactors:
        if b[0] not in seen:
            seen.add(b[0])
            unique.append(b)

    return unique[:3]   # max 3 benefactors per faded player


def get_script_summary(gs: GameScript) -> str:
    """Short human-readable summary of the game script for Telegram messages."""
    pace_labels = {
        "HALFCOURT":        "halfcourt grind",
        "SLOW_PACED":       "slow pace",
        "AVERAGE_PACE":     "average pace",
        "UPTEMPO":          "up-tempo",
        "TRANSITION_HEAVY": "transition/run-and-gun",
    }
    flow_labels = {
        "BLOWOUT":          "blowout expected",
        "DOUBLE_DIGIT_LEAD": "double-digit favorite",
        "COMFORTABLE_LEAD": "comfortable lead expected",
        "COMPETITIVE":      "competitive game",
        "TIGHT_GAME":       "tight battle",
    }
    scoring_labels = {
        "DEFENSIVE_BATTLE": "defensive battle",
        "NORMAL_SCORING":   "normal scoring",
        "HIGH_SCORING":     "high scoring",
        "SHOOTOUT":         "shootout",
    }
    return (
        f"{pace_labels.get(gs.pace, gs.pace)} · "
        f"{flow_labels.get(gs.flow, gs.flow)} · "
        f"{scoring_labels.get(gs.scoring, gs.scoring)}"
    )
