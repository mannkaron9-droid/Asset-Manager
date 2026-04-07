"""
TELEGRAM FORMATTER
==================
Formats the Edge-Fade 7 slip as a clean story-based message.
Tells the WHY behind each pick, not just the stat line.

Format:
  - Header: slip grade + payout target
  - Game script summary (what kind of game this is)
  - Fades section (who we're fading and why)
  - Benefactors section (who inherits the production)
  - Each leg: stat line + role reason + EV signal
  - Footer: validation summary
"""

from bot.decision_engine import Slip, SlipLeg
from bot.game_script import get_script_summary


GRADE_BADGE = {
    "A": "🏆 GRADE A — ELITE SLIP",
    "B": "🎯 GRADE B — STRONG SLIP",
    "C": "⚠️ GRADE C — RISKY SLIP",
    "D": "🚫 GRADE D — DO NOT SEND",
}

GRADE_BADGE_FREE = {
    "A": "🏆 ELITE SLIP (A-Grade)",
    "B": "🎯 STRONG SLIP",
}

STAT_LABEL = {
    "points":   "PTS",
    "rebounds": "REB",
    "assists":  "AST",
    "threes":   "3PM",
}

ROLE_EMOJI = {
    "SCORER":    "🔥",
    "PLAYMAKER": "🎯",
    "REBOUNDER": "💪",
    "TRAIL_PG":  "⚡",
}

STAT_EMOJI = {
    "points":   "🏀",
    "rebounds": "💪",
    "assists":  "🔥",
    "threes":   "🎯",
}

JUICE_DOT = {
    "GREEN":  "🟢",
    "YELLOW": "🟡",
    "RED":    "🔴",
}

LINE_BADGE = {
    "ELITE": "⭐ ELITE LINE",
    "GOOD":  "✅ GOOD LINE",
    "MID":   "〰️ MID LINE",
    "BAD":   "⚠️ TRAP",
}


def _leg_line(leg: SlipLeg, index: int) -> str:
    """Format a single parlay leg — FanDuel-style, no internal model data."""
    stat_lbl = STAT_LABEL.get(leg.stat, leg.stat.upper())
    odds_str = f"+{int(leg.odds)}" if leg.odds > 0 else str(int(leg.odds))

    # Role label — short, no numbers
    if leg.is_fade:
        note = "Public fade — value on the UNDER"
    elif leg.is_benefactor:
        src  = leg.fade_target.split()[-1] if leg.fade_target else "star"
        note = f"Inherits production from {src}"
    else:
        note = "Clean edge pick"

    return (
        f"{index}. *{leg.player}*  {leg.direction} {leg.line} {stat_lbl} ({odds_str})\n"
        f"   _{note}_"
    )


def format_vip_slip(slip: Slip, checkout_url: str = "") -> str:
    """Full VIP message — clean, readable, story-driven."""
    D     = "━━━━━━━━━━━━━━━━━━━━━━"
    badge = GRADE_BADGE.get(slip.grade, f"Grade {slip.grade}")

    payout     = slip.estimated_payout
    payout_str = f"+{int(payout)}" if payout > 0 else str(int(payout))
    hit_pct    = round((slip.parlay_hit_prob or 0) * 100, 1)
    hit_str    = f"{hit_pct}%" if hit_pct > 0 else "—"

    gs = slip.game_script
    script_str = get_script_summary(gs) if gs else ""

    # Sort legs: fades → benefactors → value
    fade_legs  = [l for l in slip.legs if l.is_fade]
    bene_legs  = [l for l in slip.legs if l.is_benefactor]
    value_legs = [l for l in slip.legs if not l.is_fade and not l.is_benefactor]

    i = 1
    sections = []

    if fade_legs:
        lines = [f"❌ *FADES*"]
        for leg in fade_legs:
            lines.append(_leg_line(leg, i)); i += 1
        sections.append("\n".join(lines))

    if bene_legs:
        lines = [f"✅ *BENEFACTORS*"]
        for leg in bene_legs:
            lines.append(_leg_line(leg, i)); i += 1
        sections.append("\n".join(lines))

    if value_legs:
        lines = [f"📊 *VALUE PICKS*"]
        for leg in value_legs:
            lines.append(_leg_line(leg, i)); i += 1
        sections.append("\n".join(lines))

    legs_block = f"\n{D}\n".join(sections)

    # Diversity
    div = slip.stat_diversity
    div_str = "  ·  ".join(
        f"{v}× {STAT_LABEL.get(k, k)}" for k, v in sorted(div.items()) if v > 0
    )

    # Header
    msg  = f"🔥 *EDGE-FADE 7*\n{D}\n"
    msg += f"{badge}\n"
    msg += f"💰 *Payout: {payout_str}*  ·  🎲 Hit rate: *{hit_str}*\n"
    if script_str:
        msg += f"🧠 _{script_str}_\n"
    msg += f"\n{D}\n\n"

    # Legs
    msg += legs_block

    # Footer
    msg += f"\n\n{D}\n"
    msg += f"📊 {div_str}\n"
    msg += f"_{slip.grade_reason}_"

    return msg


def format_free_teaser(slip: Slip, checkout_url: str = "") -> str:
    """
    Free channel teaser — shows the structure but hides the specific lines.
    Builds FOMO, drives subscriptions.
    """
    gs = slip.game_script
    script_line = get_script_summary(gs) if gs else "calculated game script"

    n_fades = len(slip.fades)
    n_bene  = len(slip.benefactors)
    n_total = len(slip.legs)
    payout  = slip.estimated_payout
    payout_str = f"+{int(payout)}" if payout > 0 else str(int(payout))

    badge = GRADE_BADGE_FREE.get(slip.grade, "Strong Slip")

    fade_players  = ", ".join(l.player.split()[-1] for l in slip.fades) if slip.fades else "top stars"
    bene_players  = ", ".join(l.player.split()[-1] for l in slip.benefactors[:3]) if slip.benefactors else "secondary players"

    msg = (
        f"🔥 *TONIGHT'S SYSTEM PLAY* — {badge}\n\n"
        f"🧠 Game script: _{script_line}_\n\n"
        f"❌ We're fading: *{fade_players}* (public trap)\n"
        f"✅ We're backing: *{bene_players}* (inherit the production)\n\n"
        f"📊 *{n_total}-leg parlay · Target {payout_str}*\n\n"
        f"🔒 Full slip (all {n_total} legs + exact lines) in VIP\n"
    )

    if checkout_url:
        msg += f"👉 {checkout_url}\n"

    msg += f"\n_$29/month · 7-day free trial · Cancel anytime_"

    return msg


def format_grade_d_alert(slip_attempt: dict, reason: str) -> str:
    """
    Admin alert when a slip gets graded D and is not sent.
    """
    return (
        f"🚫 *SLIP BLOCKED — Grade D*\n\n"
        f"Reason: {reason}\n\n"
        f"The engine found picks but blocked the slip from sending.\n"
        f"Review the decision log for details."
    )
