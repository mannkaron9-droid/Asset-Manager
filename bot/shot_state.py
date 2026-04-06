"""
Shared shot-tracker state accessible by both bot.py and slip_builder.py
without circular imports.

Name normalization
-----------------
NBA CDN play-by-play uses abbreviated playerNameI format ("L. James"),
while the Odds API / FanDuel props use full names ("LeBron James").
Both sides are normalized to the same canonical key "initial.lastname"
so that update_shot_history (CDN) and get_shot_status (pick pipeline)
always read and write the same bucket.

Examples
  "LeBron James"  → "l.james"
  "L. James"      → "l.james"
  "Nikola Jokic"  → "n.jokic"
  "N. Jokic"      → "n.jokic"
  "Shai Gilgeous-Alexander" → "s.gilgeous-alexander"
  "S. Gilgeous-Alexander"   → "s.gilgeous-alexander"
"""

import time as _time_ss

_shot_history: dict = {}   # {canonical_key: [{"type","made","t"}, ...]}


def _normalize_name(name: str) -> str:
    """
    Map both 'LeBron James' and 'L. James' to the same key 'l.james'.

    Algorithm
    ---------
    1. Strip and split on whitespace.
    2. Last token → last name (lowercased, keeps hyphens as-is).
    3. First token → extract the initial (strip trailing dots, take [0]).
    4. Return  "{initial}.{last}"

    Edge case: single-word name → return lowercased as-is.
    """
    parts = name.strip().split()
    if not parts:
        return name.lower()
    if len(parts) == 1:
        return parts[0].lower()
    last    = parts[-1].lower()
    initial = parts[0].replace(".", "").lower()
    initial = initial[0] if initial else "?"
    return f"{initial}.{last}"


def update_shot_history(player_name: str, shot_type: str, made: bool) -> None:
    """
    Record one shot event for a player. Called by the live CDN tracker in bot.py.
    Keeps the last 15 shots per player so get_shot_status() always has fresh data.

    player_name may be the CDN abbreviated form ("L. James") or the full form
    ("LeBron James") — both normalize to the same key.
    """
    key  = _normalize_name(player_name)
    hist = _shot_history.setdefault(key, [])
    hist.append({"type": shot_type, "made": made, "t": _time_ss.time()})
    if len(hist) > 15:
        _shot_history[key] = hist[-15:]


def get_shot_status(player_name: str) -> tuple:
    """
    Derive HOT / COLD / NEUTRAL from the most recent 15 shots stored for this player.

    player_name may be the full form ("LeBron James") or abbreviated ("L. James") —
    both resolve to the same normalized key so CDN data is always found.

    Returns
    -------
    status : str  — "HOT" | "COLD" | "NEUTRAL"
    detail : str  — human-readable reason, e.g. "3/5 from 3PT last 5 shots"
    """
    key  = _normalize_name(player_name)
    hist = _shot_history.get(key, [])
    if not hist:
        return "NEUTRAL", ""

    last5 = hist[-5:] if len(hist) >= 5 else hist
    last6 = hist[-6:] if len(hist) >= 6 else hist

    made_3pt_last5 = sum(1 for s in last5 if s.get("type") == "3PT" and s.get("made"))
    miss_consec    = len(last5) == 5 and all(not s.get("made") for s in last5)
    made_consec3   = len(last6) >= 3 and all(s.get("made") for s in last6[-3:])

    if made_3pt_last5 >= 3:
        return "HOT", f"{made_3pt_last5}/5 from 3PT last 5 shots"
    if made_consec3:
        return "HOT", "3 consecutive made shots"
    if miss_consec:
        return "COLD", "5 consecutive missed shots"
    return "NEUTRAL", ""
