"""
SLIP BUILDER
============
Orchestrates the 7-step decision engine across all games and players
to produce the Edge-Fade 7 slip.

Flow:
  1. Get all props for tonight's games
  2. For each game, classify the game script
  3. For each player in each game:
       a. Assign role
       b. Check if fade candidate (public-heavy + juiced)
       c. Run each prop through the 7-step engine
       d. Tag benefactors for each fade
  4. Collect all passing legs
  5. Build and grade the full slip
  6. Return (slip, vip_message, free_message) or None if grade D
"""

import math
import time
from typing import Optional
from bot.game_script import (
    analyze_game_script, assign_role, find_benefactors, GameScript, PlayerRole
)
from bot.decision_engine import (
    run_full_pipeline, build_and_grade_slip,
    SlipLeg, Slip, juice_test, ev_check, compute_stat_std, evaluate_line_value
)
from bot.telegram_formatter import format_vip_slip, format_free_teaser
from bot.shot_state import get_shot_status


# ── Prop line movement cache ──────────────────────────────────────────────────
# Tracks the previous line per "{player}:{stat}" across cycles so we can compute
# how much a line has moved (positive = line went up, negative = line went down).
# A drop ≤ -0.5 while public is heavy on the OVER = reverse line movement signal.
_prop_line_cache: dict = {}


def _compute_prop_line_movement(player: str, stat: str, current_line: float) -> float:
    """Return how much the prop line moved since last cycle (negative = dropped)."""
    key = f"{player}:{stat}"
    prev = _prop_line_cache.get(key)
    _prop_line_cache[key] = current_line
    if prev is None:
        return 0.0
    return round(current_line - prev, 2)


# ── Constants ─────────────────────────────────────────────────────────────────
STAT_STD_MAP = {
    "points":   5.5,    # typical NBA pts std dev game-to-game
    "rebounds": 2.8,
    "assists":  2.2,
    "threes":   1.5,
}

PRED_KEY_MAP = {
    "points":   "pred_pts",
    "rebounds": "pred_reb",
    "assists":  "pred_ast",
    "threes":   "pred_fg3",
}

STAT_HIST_MAP = {
    "points":   "pts",
    "rebounds": "reb",
    "assists":  "ast",
    "threes":   "fg3",
}

# Maps prop_type → stats dict key for the player's season average
STAT_AVG_FIELD = {
    "points":   "avg_pts",
    "rebounds": "avg_reb",
    "assists":  "avg_ast",
    "threes":   "avg_fg3",
}


def pick_best_even_line(player: str, prop_type: str, avg: float, options: list) -> Optional[dict]:
    """
    From multiple book offerings for a player/stat, pick the best line.

    Scoring (line vs player average):
      line ≤ avg       → score 2 — SAFE (line is fair or below, value for us)
      line ≤ avg + 1   → score 1 — borderline
      else             → score 0 — RISK (line is stretched above avg)

    Sorts by (score DESC, odds DESC) so higher value at better price wins.
    Skips options with odds ≤ -250 unless that's all there is.

    Returns a prop dict with {prop_type, line, odds, decision} or None.
    """
    if not options:
        return None

    valid = []
    for opt in options:
        line = opt["line"]
        odds = opt["odds"]
        if odds < -400:
            continue
        if line <= avg:
            score = 2
        elif line <= avg + 1:
            score = 1
        else:
            score = 0
        valid.append({"line": line, "odds": odds, "score": score})

    if not valid:
        # All options are heavily juiced — fall back to least-juiced
        valid = [{"line": o["line"], "odds": o["odds"], "score": 0}
                 for o in sorted(options, key=lambda x: x["odds"], reverse=True)[:1]]

    best = sorted(valid, key=lambda x: (x["score"], x["odds"]), reverse=True)[0]
    decision = "SAFE" if best["line"] <= avg else "RISK"

    if len(options) > 1:
        print(f"  [BestLine] {player} {prop_type}: "
              f"chose {best['line']} @ {best['odds']} "
              f"(score={best['score']} → {decision}) "
              f"from {len(options)} book(s)")

    return {
        "prop_type": prop_type,
        "line":      best["line"],
        "odds":      best["odds"],
        "decision":  decision,
    }


def build_slip_from_props(
    props_data: list,              # raw Odds API game list
    get_player_stats_fn,           # function(player_name) → stats dict
    games_data: dict,              # {game_name: {total, spread}} from existing bot
    checkout_url: str = "",
    admin_alert_fn=None,           # function(msg) to DM admin
    injuries: dict = None,
    injury_boost: dict = None,     # {player_lower: boost_pct} — star is out, teammate inherits
    back_to_back_teams: set = None,# teams on second night in a row — fatigue penalty
    shadow_hit_rates: dict = None, # {"{player}:{stat}": {"rate": 0.xx, "total": n}} from learning
    win_rate_context: dict = None, # all historical win-rate learning from settled bets
    conf_multipliers: dict = None, # {category: multiplier} from nightly learning
    players_bet_today: set = None, # player names already bet today — skip to prevent re-picks
) -> tuple:
    """
    Main entry point for the Edge-Fade 7 slip builder.

    Returns:
      (slip, vip_msg, free_msg) on success
      (None, None, None) if no valid slip can be built
    """
    injuries           = injuries or {}
    injury_boost       = injury_boost or {}
    back_to_back_teams = back_to_back_teams or set()
    _already_bet_today = {p.strip().lower() for p in (players_bet_today or set())}

    # ── Step A: Group props by game ───────────────────────────────────────────
    by_game = {}
    for game_data in props_data:
        home = game_data.get("home_team", "")
        away = game_data.get("away_team", "")
        if not home or not away:
            continue
        game_name = f"{away} @ {home}"
        by_game[game_name] = {
            "home":      home,
            "away":      away,
            "game_data": game_data,
        }

    if not by_game:
        print("[SlipBuilder] No games found in props data")
        return None, None, None

    all_candidates = []   # SlipLeg objects that passed the engine
    primary_script = None

    # ── Step B: Process each game ─────────────────────────────────────────────
    for game_name, game_info in by_game.items():
        home_team = game_info["home"]
        away_team = game_info["away"]
        gd        = games_data.get(game_name, {})

        total  = float(gd.get("total", 220))
        spread = abs(float(gd.get("spread", 5.0)))

        # Classify game script for this matchup
        gs = analyze_game_script(home_team, away_team, total, spread, game_name)
        print(f"[SlipBuilder] {game_name} → {gs.label} | {gs.pace} pace | {gs.flow} flow")

        if primary_script is None:
            primary_script = gs   # use first game as primary script reference

        # ── Step C: Extract props — best line across all books ────────────────
        game_raw = game_info["game_data"]
        props_by_player = {}

        # Collect all available lines per player/stat across every book
        all_book_lines = {}   # (player, prop_type) → list of {line, odds}
        for book in [b for b in game_raw.get("bookmakers", []) if b.get("key") == "fanduel"]:
            for market in book.get("markets", []):
                prop_type = market.get("key", "").replace("player_", "")
                if prop_type not in STAT_HIST_MAP:
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome.get("name") != "Over":
                        continue
                    player = outcome.get("description", "")
                    if not player:
                        continue
                    key = (player, prop_type)
                    all_book_lines.setdefault(key, []).append({
                        "line": float(outcome.get("point", 0)),
                        "odds": float(outcome.get("price", -110)),
                    })

        # props_by_player is populated in Step D after player avg is known
        if not all_book_lines:
            print(f"[SlipBuilder] {game_name}: no props extracted")
            continue

        # ── Step D: Build per-player roles and pick best lines ───────────────
        home_players = []    # PlayerRole list for home team
        away_players = []    # PlayerRole list for away team

        player_role_map = {}    # player_name → PlayerRole

        # Derive the ordered player list from all_book_lines (preserve first-seen order)
        seen_order: list = []
        seen_set: set = set()
        for (p, _) in all_book_lines:
            if p not in seen_set:
                seen_set.add(p)
                seen_order.append(p)

        for player in seen_order[:10]:
            try:
                # Skip players already picked today — prevents hammering the same
                # player across multiple bot cycles on the same night
                if player.strip().lower() in _already_bet_today:
                    print(f"  [SlipBuilder] Skip {player} — already in today's picks")
                    continue

                # Skip injured players
                inj = injuries.get(player.lower(), {})
                if inj.get("status") in ("Out", "Doubtful"):
                    print(f"  [SlipBuilder] Skip {player} — {inj.get('status')}")
                    continue

                time.sleep(1)
                stats = get_player_stats_fn(player)
                if not stats:
                    continue

                _raw_mins = stats.get("avg_mins")
                if _raw_mins:
                    avg_mins = float(_raw_mins)
                elif float(stats.get("avg_pts") or 0) > 10:
                    avg_mins = 22.0   # scoring player, likely rotation — conservative fallback
                else:
                    avg_mins = 0.0    # unknown/fringe player — reject
                avg_usage = float(stats.get("avg_usage", 15))

                if avg_mins < 20 or avg_usage < 8:   # 20 min gate
                    continue

                avg_pts = float(stats.get("pred_pts") or stats.get("avg_pts") or 0)
                avg_reb = float(stats.get("pred_reb") or stats.get("avg_reb") or 0)
                avg_ast = float(stats.get("pred_ast") or stats.get("avg_ast") or 0)
                avg_fg3 = float(stats.get("avg_fg3") or 0)

                avg_by_stat = {
                    "points":   avg_pts,
                    "rebounds": avg_reb,
                    "assists":  avg_ast,
                    "threes":   avg_fg3,
                }

                team_name = stats.get("team", "")
                is_home   = _team_matches(team_name, home_team)

                role = assign_role(
                    player    = player,
                    team      = team_name,
                    avg_pts   = avg_pts,
                    avg_reb   = avg_reb,
                    avg_ast   = avg_ast,
                    avg_mins  = avg_mins,
                    avg_usage = avg_usage,
                    game_script = gs,
                    is_home   = is_home,
                    position  = stats.get("position", ""),
                )
                player_role_map[player] = role

                if is_home:
                    home_players.append(role)
                else:
                    away_players.append(role)

                # ── Score & pick best line for each of this player's props ──
                for (p, prop_type), options in all_book_lines.items():
                    if p != player:
                        continue
                    avg = avg_by_stat.get(prop_type, 0.0)
                    if avg == 0.0:
                        continue   # can't score without avg — skip
                    result = pick_best_even_line(player, prop_type, avg, options)
                    if result:
                        props_by_player.setdefault(player, []).append(result)

            except Exception as e:
                print(f"[SlipBuilder] Player error ({player}): {e}")

        # ── Step E: Identify fades ─────────────────────────────────────────────
        fade_map = {}    # player_name → stat (what we're fading them on)

        for player, player_props in props_by_player.items():
            role = player_role_map.get(player)
            if not role:
                continue
            for prop in player_props:
                prop_type = prop["prop_type"]
                odds      = prop["odds"]
                line      = prop["line"]

                # Hard block — never include a leg at -600 or worse
                if odds <= -600:
                    print(f"  [HardBlock] {player} {prop_type} {odds} — beyond -600 wall, skipped")
                    continue

                # Fade candidate: star + juiced line + primary stat for their role
                FADEABLE_STATS = {
                    "go_to_scorer":   ["points", "player_points"],
                    "combo_creator":  ["points", "player_points", "assists"],
                    "sixth_man":      ["points", "player_points"],
                    "floor_general":  ["assists", "player_assists"],
                    "glass_cleaner":  ["rebounds", "player_rebounds", "total_rebounds"],
                    "rim_anchor":     ["rebounds", "player_rebounds", "total_rebounds"],
                    "spot_up_shooter": ["points", "player_points"],
                    "utility_player": ["points", "player_points"],
                }
                fadeable = FADEABLE_STATS.get(role.role, ["points", "player_points"])
                if (role.is_star
                        and odds <= -130     # juiced line (lowered from -140)
                        and prop_type in fadeable):
                    fade_map[player] = prop_type
                    print(f"  [SlipBuilder] FADE candidate: {player} ({role.role}) {prop_type} (odds {odds})")
                    break

        # ── Step F: Map benefactors ────────────────────────────────────────────
        benefactor_map = {}    # player_name → (fade_player, inherited_stat)

        for fade_player, fade_stat in fade_map.items():
            role = player_role_map.get(fade_player)
            if not role:
                continue
            team_roles = home_players if _team_matches(role.team, home_team) else away_players
            benes = find_benefactors(fade_player, fade_stat, team_roles, gs)
            for bene_name, bene_stat, _ in benes:
                if bene_name not in benefactor_map:
                    benefactor_map[bene_name] = (fade_player, bene_stat)
                    print(f"  [SlipBuilder] BENEFACTOR: {bene_name} {bene_stat} ← {fade_player}")

        # ── Step G: Run all picks through 7-step engine ───────────────────────
        for player, player_props in props_by_player.items():
            role = player_role_map.get(player)
            if not role:
                continue

            is_fade       = player in fade_map
            is_benefactor = player in benefactor_map
            fade_target   = benefactor_map.get(player, (None,))[0] or ""

            for prop in player_props:
                prop_type = prop["prop_type"]
                line      = prop["line"]
                odds      = prop["odds"]

                # For fades: we take the UNDER
                direction = "UNDER" if is_fade else "OVER"
                # For fades: get the under odds (approximate from over odds)
                if is_fade and odds < 0:
                    # The under is typically positive when over is juiced
                    adj_odds = abs(odds) - 20    # rough approximation
                    odds = adj_odds if adj_odds > 0 else 110
                elif is_fade:
                    odds = -odds if odds > 0 else odds

                # Only proceed with stat that matches the fade/benefactor plan
                if is_fade and prop_type != fade_map.get(player):
                    continue
                if is_benefactor and prop_type != benefactor_map.get(player, (None, None))[1]:
                    continue

                # Get prediction and real std dev from stats
                try:
                    stats = get_player_stats_fn(player)
                    if not stats:
                        continue
                    pred_key = PRED_KEY_MAP.get(prop_type, "pred_pts")
                    prediction = float(stats.get(pred_key) or 0)
                    if prediction == 0:
                        continue

                    # Use the player's real game-to-game variance computed from
                    # their actual BDL game log — not a fixed league-wide constant.
                    # Falls back to STAT_STD_MAP only when < 5 games are available.
                    log_key   = STAT_HIST_MAP.get(prop_type, "pts")
                    game_log  = stats.get(log_key, [])
                    fallback  = STAT_STD_MAP.get(prop_type, 4.0)
                    stat_std  = compute_stat_std(game_log, fallback=fallback)
                except Exception:
                    continue

                _line_move = _compute_prop_line_movement(player, prop_type, line)
                _shot_st, _shot_det = get_shot_status(player)

                # ── L1: per-player B2B flag ───────────────────────────────────
                _team_str = (role.team or "").lower()
                _is_b2b = any(kw.lower() in _team_str for kw in back_to_back_teams)

                # ── L3: ML confidence from prediction vs line gap ─────────────
                # Converts the model's numeric edge into a 0-1 confidence score.
                # Sigmoid of z-score: 0.5 = toss-up, >0.5 = strong signal.
                _z = (prediction - line) / stat_std if stat_std > 0 else 0.0
                if direction == "UNDER":
                    _z = -_z
                _ml_prob = round(1.0 / (1.0 + math.exp(-_z * 0.8)), 3)

                leg = run_full_pipeline(
                    player           = player,
                    team             = role.team,
                    game             = game_name,
                    stat             = prop_type,
                    direction        = direction,
                    line             = line,
                    odds             = odds,
                    prediction       = prediction,
                    stat_std         = stat_std,
                    player_stats     = stats,
                    game_script      = gs,
                    is_fade          = is_fade,
                    is_benefactor    = is_benefactor,
                    fade_target      = fade_target,
                    public_pct       = 75.0 if is_fade else 30.0,
                    line_movement    = _line_move,
                    line_decision    = prop.get("decision", "RISK"),
                    shadow_hit_rates = shadow_hit_rates,
                    win_rate_context = win_rate_context,
                    shot_status      = _shot_st,
                    shot_detail      = _shot_det,
                    back_to_back     = _is_b2b,
                    ml_prediction    = _ml_prob,
                )

                if leg:
                    # ── Apply all real API data signals to confidence ──────────
                    # These are all computed from real BDL/ESPN data and were
                    # previously only used in the fallback path. Now every leg
                    # in the main engine benefits from them.

                    adj_log = []

                    # 1. Form score — hot/cold streak vs season average
                    #    Derived from real BDL game logs already fetched above.
                    form_key = {"points": "pts_form", "rebounds": "reb_form",
                                "assists": "ast_form", "threes": "fg3_form"}.get(
                                    prop_type, "pts_form")
                    form_val = float(stats.get(form_key, 0.0))
                    # Scale: ±12% streak → ±5.4 confidence pts (max ±6)
                    form_adj = round(max(-6.0, min(6.0, form_val * 45)), 1)
                    if form_adj != 0:
                        leg.confidence = round(leg.confidence + form_adj, 1)
                        adj_log.append(f"form {form_adj:+.1f}")

                    # 2. Historical accuracy adjustment per player
                    #    Uses stat-specific hit rate (e.g. points vs rebounds).
                    try:
                        from bot.bot import get_player_confidence_adjustment as _gca
                        hist_adj = _gca(player, prop_type)
                    except Exception:
                        hist_adj = float(stats.get("confidence_adj", 0.0))
                    if hist_adj != 0.0:
                        leg.confidence = round(leg.confidence + hist_adj, 1)
                        adj_log.append(f"hist {hist_adj:+.1f}")

                    # 3. Injury boost — benefactor inherits faded star's usage
                    inj_boost = injury_boost.get(player.lower(), 0.0)
                    if inj_boost > 0 and is_benefactor:
                        boost_pts = round(inj_boost * 80, 1)
                        leg.confidence = round(leg.confidence + boost_pts, 1)
                        adj_log.append(f"inj_boost +{boost_pts:.1f}")

                    # 4. Back-to-back fatigue penalty — real ESPN schedule data
                    team_kw = (stats.get("team", "") or "").split()[-1].lower()
                    if team_kw and team_kw in back_to_back_teams:
                        avg_mins = float(stats.get("avg_mins", 0))
                        b2b_pen = -5.0 if avg_mins >= 30 else -2.5 if avg_mins >= 20 else 0
                        if b2b_pen:
                            leg.confidence = round(leg.confidence + b2b_pen, 1)
                            adj_log.append(f"b2b {b2b_pen:.1f}")

                    # 5. Hot/cold flag — is_hot or is_cold from BDL form data
                    if stats.get("is_hot") and form_adj > 0:
                        leg.confidence = round(leg.confidence + 2.0, 1)
                        adj_log.append("is_hot +2.0")
                    elif stats.get("is_cold") and form_adj < 0:
                        leg.confidence = round(leg.confidence - 2.0, 1)
                        adj_log.append("is_cold -2.0")

                    # Clamp final confidence to valid range
                    leg.confidence = max(0.0, min(99.0, leg.confidence))

                    if adj_log:
                        print(f"  [RealData] {player} adjustments: {', '.join(adj_log)} "
                              f"→ conf={leg.confidence:.1f}%")

                    all_candidates.append(leg)
                    print(f"  [SlipBuilder] PASS: {player} {direction} {line} {prop_type} "
                          f"(EV {leg.ev:+.3f}) role={leg.role} conf={leg.confidence:.1f}%")

    # ── Step H: Build and grade the slip ──────────────────────────────────────
    if not all_candidates:
        print("[SlipBuilder] No candidates passed the engine — no slip built")
        return None, None, None

    print(f"[SlipBuilder] {len(all_candidates)} candidates → building slip")

    slip = build_and_grade_slip(all_candidates, game_script=primary_script)

    if slip is None:
        print("[SlipBuilder] Slip graded D or insufficient legs — not sending")
        if admin_alert_fn:
            admin_alert_fn(
                f"⚠️ Engine ran but produced no valid slip.\n"
                f"Candidates: {len(all_candidates)} — all failed validation."
            )
        return None, None, None

    print(f"[SlipBuilder] Slip grade: {slip.grade} | Legs: {len(slip.legs)} | "
          f"Payout: +{slip.estimated_payout:.0f}")

    # Apply learned confidence multipliers to each leg before formatting.
    # These are nightly-updated category multipliers (fade_prop, neutral_prop, etc.)
    # that reflect actual win rates per pick type — directly affects what users see.
    if conf_multipliers:
        for leg in slip.legs:
            if leg.is_fade:
                cat = "fade_prop"
            elif leg.is_benefactor:
                cat = "benefactor_prop"
            else:
                cat = "neutral_prop"
            mult = conf_multipliers.get(cat, 1.0)
            leg.confidence = round(max(40.0, min(99.0, leg.confidence * mult)), 1)

    vip_msg  = format_vip_slip(slip, checkout_url)
    free_msg = format_free_teaser(slip, checkout_url)

    return slip, vip_msg, free_msg


def _team_matches(player_team: str, target_team: str) -> bool:
    """Check if a player's team name matches the target team (fuzzy)."""
    if not player_team or not target_team:
        return False
    pt = player_team.lower().split()
    tt = target_team.lower().split()
    return bool(set(pt) & set(tt))


def get_top_candidates(
    props_data: list,
    get_player_stats_fn,
    games_data: dict,
    injuries: dict = None,
    top_n: int = 5,
    injury_boost: dict = None,
    back_to_back_teams: set = None,
    shadow_hit_rates: dict = None,  # {"{player}:{stat}": {"rate": 0.xx, "total": n}}
    win_rate_context: dict = None,  # all historical win-rate learning from settled bets
) -> list:
    """
    Fallback for when a full 7-leg slip can't be graded.
    Runs every player/prop through the engine and returns the top N
    individual picks that cleared EV + juice + role checks.

    Returns a list of dicts: {player, pick, line, prop_type, ev, confidence}
    """
    injuries       = injuries or {}
    injury_boost   = injury_boost or {}
    back_to_back_teams = back_to_back_teams or set()
    candidates = []

    for game_data in props_data:
        home = game_data.get("home_team", "")
        away = game_data.get("away_team", "")
        if not home or not away:
            continue
        game_name = f"{away} @ {home}"
        gd        = games_data.get(game_name, {})
        total     = float(gd.get("total", 220))
        spread    = abs(float(gd.get("spread", 5.0)))
        gs        = analyze_game_script(home, away, total, spread, game_name)

        seen_keys = set()
        props_by_player = {}
        for book in [b for b in game_data.get("bookmakers", []) if b.get("key") == "fanduel"]:
            for market in book.get("markets", []):
                prop_type = market.get("key", "").replace("player_", "")
                if prop_type not in STAT_HIST_MAP:
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome.get("name") != "Over":
                        continue
                    player = outcome.get("description", "")
                    line   = outcome.get("point", 0)
                    odds   = outcome.get("price", -110)
                    key    = (player, prop_type)
                    if not player or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    props_by_player.setdefault(player, []).append({
                        "prop_type": prop_type,
                        "line":      float(line),
                        "odds":      float(odds),
                    })

        for player, player_props in list(props_by_player.items())[:12]:
            try:
                inj = injuries.get(player.lower(), {})
                if inj.get("status") in ("Out", "Doubtful"):
                    continue
                # Hard block
                for prop in player_props:
                    if prop["odds"] <= -600:
                        continue
                stats = get_player_stats_fn(player)
                if not stats:
                    continue
                _raw_mins = stats.get("avg_mins")
                if _raw_mins:
                    avg_mins = float(_raw_mins)
                elif float(stats.get("avg_pts") or 0) > 10:
                    avg_mins = 22.0   # scoring player, likely rotation — conservative fallback
                else:
                    avg_mins = 0.0    # unknown/fringe player — reject
                avg_usage = float(stats.get("avg_usage", 15))
                if avg_mins < 20 or avg_usage < 8:   # 20 min gate
                    continue

                avg_pts = float(stats.get("pred_pts") or stats.get("avg_pts") or 0)
                avg_reb = float(stats.get("pred_reb") or stats.get("avg_reb") or 0)
                avg_ast = float(stats.get("pred_ast") or stats.get("avg_ast") or 0)
                team    = stats.get("team", "")
                is_home = _team_matches(team, home)

                role = assign_role(
                    player=player, team=team,
                    avg_pts=avg_pts, avg_reb=avg_reb, avg_ast=avg_ast,
                    avg_mins=avg_mins, avg_usage=avg_usage,
                    game_script=gs, is_home=is_home,
                )

                for prop in player_props:
                    if prop["odds"] <= -400:
                        continue
                    pred_key = PRED_KEY_MAP.get(prop["prop_type"], "pred_pts")
                    prediction = float(stats.get(pred_key) or avg_pts)
                    _std_fallback = STAT_STD_MAP.get(prop["prop_type"], 4.0)

                    # ── Real stat std from game log (same as main edge-fade path) ─
                    _log_key2  = STAT_HIST_MAP.get(prop["prop_type"], "pts")
                    _game_log2 = stats.get(_log_key2, [])
                    _stat_std2 = compute_stat_std(_game_log2, fallback=_std_fallback)

                    _elite_line_move = _compute_prop_line_movement(
                        player, prop["prop_type"], prop["line"]
                    )
                    _shot_st2, _shot_det2 = get_shot_status(player)

                    # ── L1: per-player B2B flag ───────────────────────────────
                    _team_str2 = (team or "").lower()
                    _is_b2b2   = any(kw.lower() in _team_str2 for kw in back_to_back_teams)

                    # ── L2: line decision via evaluate_line_value ─────────────
                    _line_dec2 = evaluate_line_value(prop["line"], prediction, prop["odds"])

                    # ── L3: ML confidence from prediction vs line gap ─────────
                    _dir2    = "OVER" if prediction > prop["line"] else "UNDER"
                    _z2      = (prediction - prop["line"]) / _stat_std2 if _stat_std2 > 0 else 0.0
                    if _dir2 == "UNDER":
                        _z2  = -_z2
                    _ml_prob2 = round(1.0 / (1.0 + math.exp(-_z2 * 0.8)), 3)

                    result = run_full_pipeline(
                        player           = player,
                        team             = team,
                        game             = game_name,
                        stat             = prop["prop_type"],
                        direction        = _dir2,
                        line             = prop["line"],
                        odds             = prop["odds"],
                        prediction       = prediction,
                        stat_std         = _stat_std2,
                        player_stats     = stats,
                        game_script      = gs,
                        public_pct       = 50.0,
                        line_movement    = _elite_line_move,
                        line_decision    = _line_dec2,
                        shadow_hit_rates = shadow_hit_rates,
                        win_rate_context = win_rate_context,
                        shot_status      = _shot_st2,
                        shot_detail      = _shot_det2,
                        back_to_back     = _is_b2b2,
                        ml_prediction    = _ml_prob2,
                    )
                    if result is not None:
                        base_conf = result.confidence

                        # ── Fix 2: Apply form score ───────────────────────
                        form_key  = f"{prop['prop_type'][:3]}_form"
                        form_key  = {"poi": "pts_form", "reb": "reb_form",
                                     "ast": "ast_form", "thr": "fg3_form"}.get(
                                         prop["prop_type"][:3], "pts_form")
                        form_val  = float(stats.get(form_key, 0))
                        # Hot streak boosts confidence up to +6, cold drops up to -5
                        form_adj  = round(form_val * 45, 1)
                        form_adj  = max(-5.0, min(6.0, form_adj))
                        base_conf = base_conf + form_adj

                        # ── Fix 3a: Apply injury boost ────────────────────
                        inj_boost = injury_boost.get(player.lower(), 0)
                        if inj_boost > 0:
                            base_conf = base_conf + round(inj_boost * 80, 1)

                        # ── Fix 3b: Back-to-back penalty ─────────────────
                        team_kw = (stats.get("team", "") or "").split()[-1].lower()
                        if team_kw and team_kw in back_to_back_teams:
                            mins_played = float(stats.get("avg_mins", 0))
                            if mins_played >= 30:   # high-minute players feel it more
                                base_conf = base_conf - 5.0
                            elif mins_played >= 20:
                                base_conf = base_conf - 2.5

                        # ── Historical learning adjustment ────────────────
                        # Uses stat-specific hit rate (e.g. points vs rebounds).
                        try:
                            from bot.bot import get_player_confidence_adjustment as _gca2
                            hist_adj = _gca2(player, prop["prop_type"])
                        except Exception:
                            hist_adj = float(stats.get("confidence_adj", 0.0))
                        if hist_adj != 0.0:
                            base_conf = base_conf + hist_adj

                        base_conf = max(0.0, min(99.0, base_conf))

                        candidates.append({
                            "player":     player,
                            "pick":       "OVER" if prediction > prop["line"] else "UNDER",
                            "line":       prop["line"],
                            "prop_type":  prop["prop_type"],
                            "ev":         result.ev,
                            "edge":       round(result.ev, 4),
                            "odds":       prop["odds"],
                            "confidence": base_conf,
                            "form_adj":   form_adj,
                            "inj_boost":  round(inj_boost * 80, 1),
                            "team":       team,
                            "game":       game_name,
                        })
            except Exception:
                continue

    candidates.sort(key=lambda x: x["confidence"], reverse=True)
    return candidates[:top_n]


def slip_to_bet_records(slip: Slip, timestamp: str) -> list:
    """
    Convert a Slip into a list of bet dicts for save_bet() in bot.py.
    Adds new fields: slip_grade, role, is_fade, is_benefactor, fade_target, ev.
    """
    records = []
    for leg in slip.legs:
        records.append({
            "game":            leg.game,
            "player":          leg.player,
            "pick":            f"{leg.direction} {leg.line}",
            "betType":         leg.stat,
            "line":            leg.line,
            "prediction":      leg.prediction,
            "odds":            leg.odds,
            "prob":            leg.true_prob,
            "edge":            round(leg.ev, 4),
            "confidence":      leg.confidence,
            "time":            timestamp,
            "result":          None,
            "script":          leg.game_script_label,
            "slip_grade":      slip.grade,
            "role":            leg.role,
            "is_fade":         leg.is_fade,
            "is_benefactor":   leg.is_benefactor,
            "fade_target":     leg.fade_target,
            "ev":              leg.ev,
            "pick_category":   "EDGE_FADE_7",
            # ── new fields from engine upgrades ──────────────────────────
            "line_rating":     getattr(leg, "line_rating",   "GOOD"),
            "line_decision":   getattr(leg, "line_decision", "RISK"),
            "true_edge":       getattr(leg, "edge",          None),
            "parlay_hit_prob": getattr(slip, "parlay_hit_prob", None),
            "parlay_ev":       getattr(slip, "parlay_ev",       None),
            # ── context fields for pattern engine ────────────────────────
            "game_pace":       getattr(leg, "game_pace",  "AVERAGE_PACE"),
            "game_phase":      getattr(leg, "game_phase", "pregame"),
        })
    return records
