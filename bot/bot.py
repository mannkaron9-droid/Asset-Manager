import requests, time, json, math, os, sys, pickle, threading
import numpy as np
import zoneinfo as _zi
from datetime import datetime, timedelta, timezone

# Ensure the directory containing bot.py is always on sys.path so that
# sibling modules (decision_engine, adaptive_thresholds, slip_builder, …)
# are importable regardless of how the process is launched (python bot.py,
# python -m bot.bot, gunicorn bot.bot:app, etc.)
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
if _BOT_DIR not in sys.path:
    sys.path.insert(0, _BOT_DIR)

ET = _zi.ZoneInfo("America/New_York")


class _DatetimeEncoder(json.JSONEncoder):
    """JSON encoder that converts datetime/date objects to ISO strings."""
    def default(self, obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def _safe_json_dumps(obj):
    """json.dumps that never crashes on datetime objects."""
    return json.dumps(obj, cls=_DatetimeEncoder)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
FREE_CHANNEL = os.environ.get("FREE_CHANNEL", "")
VIP_CHANNEL = os.environ.get("VIP_CHANNEL", "")
ADMIN_ID = 6723106141  # Admin DM for operational alerts
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

BANKROLL = 1000
EDGE_THRESHOLD = 0.06
line_history = {}
KELLY_FRACTION = 0.5
TOTALS_EDGE_THRESHOLD = 5.0   # points diff to flag a totals edge
SPREAD_EDGE_THRESHOLD = 3.0   # points diff to flag a spread edge

STATUS_FILE    = os.path.join(os.path.dirname(__file__), "..", "bot_status.json")
BETS_FILE      = os.path.join(os.path.dirname(__file__), "..", "bets.json")
LEARNING_FILE  = os.path.join(os.path.dirname(__file__), "..", "learning_data.json")

DATABASE_URL   = os.environ.get("DATABASE_URL", "")

def _get_db_url():
    """Return DATABASE_URL — either from env directly or built from PG* vars."""
    raw = os.environ.get("DATABASE_URL", "")
    if raw and not raw.startswith("${{"):
        print(f"[DB] Using DATABASE_URL env var")
        return raw
    host = os.environ.get("PGHOST", "")
    port = os.environ.get("PGPORT", "5432")
    db   = os.environ.get("PGDATABASE", "")
    user = os.environ.get("PGUSER", "")
    pw   = os.environ.get("PGPASSWORD", "")
    print(f"[DB] PG vars — host={host!r} port={port!r} db={db!r} user={user!r} pw={'SET' if pw else 'MISSING'}")
    if host and db and user:
        built = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
        print(f"[DB] Built URL from PG* vars")
        return built
    print(f"[DB] No usable DB credentials found — using JSON fallback")
    return ""

def _db_conn():
    url = _get_db_url()
    if not url:
        return None
    try:
        import psycopg2
        try:
            conn = psycopg2.connect(url, sslmode="require", connect_timeout=10)
            print(f"[DB] Connected (SSL)")
            return conn
        except Exception as e1:
            print(f"[DB] SSL connect failed: {e1} — retrying without SSL")
            try:
                conn = psycopg2.connect(url, connect_timeout=10)
                print(f"[DB] Connected (no SSL)")
                return conn
            except Exception as e2:
                print(f"[DB] connect error (no SSL): {e2}")
                return None
    except Exception as e:
        print(f"[DB] connect error: {e}")
        return None

def _db_init():
    conn = _db_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                game TEXT, player TEXT, pick TEXT, bet_type TEXT,
                line REAL, prediction REAL, odds REAL, prob REAL,
                edge REAL, confidence REAL, result TEXT,
                bet_time TIMESTAMP, created_at TIMESTAMP DEFAULT NOW(),
                tier VARCHAR(20) DEFAULT 'BALANCED'
            )
        """)
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'BALANCED'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS script VARCHAR(20) DEFAULT 'NORMAL'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS game_total REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS game_spread REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS player_avg_mins REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS player_avg_usage REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS script_combo VARCHAR(60) DEFAULT 'NORMAL'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS pick_category VARCHAR(30) DEFAULT 'INDIVIDUAL'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS actual_value REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS prediction_error REAL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS slip_grade VARCHAR(1) DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS is_fade BOOLEAN DEFAULT FALSE")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS is_benefactor BOOLEAN DEFAULT FALSE")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS fade_target TEXT DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS ev REAL DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS line_rating VARCHAR(10) DEFAULT 'GOOD'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS line_decision VARCHAR(10) DEFAULT 'RISK'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS true_edge REAL DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS parlay_hit_prob REAL DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS parlay_ev REAL DEFAULT NULL")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS game_pace VARCHAR(10) DEFAULT 'MED'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS game_phase VARCHAR(10) DEFAULT 'pregame'")
        cur.execute("""
            UPDATE bets SET pick_category = CASE
                WHEN bet_type = 'VIP_LOCK' THEN 'VIP_LOCK'
                WHEN bet_type = 'SGP'      THEN 'SGP'
                ELSE 'INDIVIDUAL'
            END
            WHERE pick_category IS NULL OR pick_category = 'INDIVIDUAL'
        """)
        cur.execute("""
            UPDATE bets SET tier = CASE
                WHEN confidence >= 80 THEN 'SAFE'
                WHEN confidence >= 65 THEN 'BALANCED'
                ELSE 'AGGRESSIVE'
            END
            WHERE tier IS NULL OR tier = 'BALANCED'
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_data (
                id SERIAL PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                value JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_status (
                id SERIAL PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS feed_picks (
                id SERIAL PRIMARY KEY,
                pick_text TEXT NOT NULL,
                logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                logged_at_et TEXT NOT NULL,
                picked_at_et TEXT NOT NULL,
                is_past BOOLEAN NOT NULL DEFAULT FALSE,
                admin_id BIGINT NOT NULL
            )
        """)
        cur.execute("ALTER TABLE feed_picks ADD COLUMN IF NOT EXISTS picked_at_et TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE feed_picks ADD COLUMN IF NOT EXISTS is_past BOOLEAN NOT NULL DEFAULT FALSE")
        cur.execute("ALTER TABLE feed_picks ADD COLUMN IF NOT EXISTS result TEXT DEFAULT NULL")
        cur.execute("ALTER TABLE feed_picks ADD COLUMN IF NOT EXISTS settled_at_et TEXT DEFAULT NULL")

        # ── Game observer tables ───────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_observations (
                id              SERIAL PRIMARY KEY,
                game_id         INTEGER,
                game_date       TEXT NOT NULL,
                home_team       TEXT NOT NULL,
                away_team       TEXT NOT NULL,
                predicted_script TEXT DEFAULT '',
                actual_script   TEXT DEFAULT '',
                script_match    BOOLEAN DEFAULT NULL,
                home_pts        INTEGER DEFAULT 0,
                away_pts        INTEGER DEFAULT 0,
                actual_total    INTEGER DEFAULT 0,
                projected_total FLOAT DEFAULT 0,
                period          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'scheduled',
                observed_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(game_id, game_date)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_observations (
                id              SERIAL PRIMARY KEY,
                game_id         INTEGER,
                game_date       TEXT NOT NULL,
                player_name     TEXT NOT NULL,
                team            TEXT NOT NULL,
                opponent        TEXT NOT NULL,
                minutes         FLOAT DEFAULT 0,
                pts             INTEGER DEFAULT 0,
                ast             INTEGER DEFAULT 0,
                reb             INTEGER DEFAULT 0,
                fg3m            INTEGER DEFAULT 0,
                stl             INTEGER DEFAULT 0,
                blk             INTEGER DEFAULT 0,
                fg_pct          FLOAT DEFAULT 0,
                ft_pct          FLOAT DEFAULT 0,
                plus_minus      INTEGER DEFAULT 0,
                season_avg_pts  FLOAT DEFAULT 0,
                season_avg_ast  FLOAT DEFAULT 0,
                season_avg_reb  FLOAT DEFAULT 0,
                season_avg_fg3  FLOAT DEFAULT 0,
                is_starter      BOOLEAN DEFAULT FALSE,
                is_benefactor   BOOLEAN DEFAULT FALSE,
                is_fade         BOOLEAN DEFAULT FALSE,
                injury_context  TEXT DEFAULT '',
                observed_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(game_id, player_name, game_date)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shadow_picks (
                id          SERIAL PRIMARY KEY,
                game_id     INTEGER,
                game_date   TEXT NOT NULL,
                home_team   TEXT NOT NULL,
                away_team   TEXT NOT NULL,
                pick_type   TEXT NOT NULL,
                player_name TEXT DEFAULT '',
                stat        TEXT DEFAULT '',
                line        FLOAT DEFAULT 0,
                direction   TEXT DEFAULT '',
                confidence  FLOAT DEFAULT 0,
                edge_score  FLOAT DEFAULT 0,
                game_script TEXT DEFAULT '',
                pick_text   TEXT DEFAULT '',
                blocked_by  TEXT DEFAULT NULL,
                role_tag    TEXT DEFAULT NULL,
                actual_value FLOAT DEFAULT NULL,
                result       TEXT DEFAULT NULL,
                graded_at    TIMESTAMPTZ DEFAULT NULL,
                created_at   TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(game_id, pick_type, player_name, stat)
            )
        """)
        cur.execute("""
            ALTER TABLE game_observations
            ADD COLUMN IF NOT EXISTS shadows_generated BOOLEAN DEFAULT FALSE
        """)
        cur.execute("ALTER TABLE shadow_picks ADD COLUMN IF NOT EXISTS blocked_by   TEXT DEFAULT NULL")
        cur.execute("ALTER TABLE shadow_picks ADD COLUMN IF NOT EXISTS role_tag    TEXT DEFAULT NULL")
        cur.execute("ALTER TABLE shadow_picks ADD COLUMN IF NOT EXISTS prob        FLOAT DEFAULT NULL")
        cur.execute("ALTER TABLE shadow_picks ADD COLUMN IF NOT EXISTS implied_prob FLOAT DEFAULT NULL")
        cur.execute("ALTER TABLE shadow_picks ADD COLUMN IF NOT EXISTS parlay_id   TEXT DEFAULT NULL")

        # ── Causality event log (survives restart/error — feeds self-learning) ─
        cur.execute("""
            CREATE TABLE IF NOT EXISTS causality_log (
                id          SERIAL PRIMARY KEY,
                game_id     INTEGER NOT NULL,
                game_date   TEXT NOT NULL,
                period      INTEGER DEFAULT 0,
                cause_type  TEXT NOT NULL,
                full_cause  TEXT NOT NULL,
                from_script TEXT DEFAULT '',
                to_script   TEXT DEFAULT '',
                home_score  INTEGER DEFAULT 0,
                away_score  INTEGER DEFAULT 0,
                logged_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS causality_log_game_idx
            ON causality_log (game_id, game_date)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id                     SERIAL PRIMARY KEY,
                telegram_id            TEXT NOT NULL UNIQUE,
                stripe_customer_id     TEXT,
                stripe_subscription_id TEXT,
                status                 TEXT DEFAULT 'active',
                created_at             TIMESTAMPTZ DEFAULT NOW(),
                cancelled_at           TIMESTAMPTZ
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS subscribers_telegram_idx
            ON subscribers (telegram_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS subscribers_stripe_sub_idx
            ON subscribers (stripe_subscription_id)
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Tables verified ✓")
    except Exception as e:
        print(f"[DB] init error: {e}")
        try:
            conn.close()
        except Exception:
            pass

_last_injury_bulletin  = None   # date string (YYYY-MM-DD) when injury bulletin last sent
_starters_sent_today   = set()  # "AWAY@HOME" keys already sent this calendar day
_starters_sent_date    = None   # date string when the set was last cleared
_results_recap_sent    = None   # date string when the recap was last sent
_pregame_picks_sent    = set()  # "HOME vs AWAY:TYPE" keys already fired this calendar day
_pregame_picks_date    = None   # date string when the set was last cleared
_full_card_sent_today  = None   # date string when the VIP full card was last sent
_props_sent_today      = set()  # "PLAYER:PROPTYPE" keys already sent this calendar day
_props_sent_date       = None   # date string when the props set was last cleared
_monthly_report_sent   = None   # "YYYY-MM" string when monthly report last sent
_free_preview_sent     = None   # date string when free channel daily preview was sent
_todays_parlay_legs    = []     # picks accumulated this day for system/parlay selection
_cmd_offset            = 0      # Telegram getUpdates offset
_avoid_sent_date       = None   # date string when avoid list was last sent
_vip_lock_desc         = None   # desc of today's VIP LOCK (excluded from SGPs)
_sgp_sent_games        = set()  # game names that already got an SGP post today
_shadow_cgp_dates      = set()  # dates where shadow CGP has already been generated
_elite_props_sent_games = set() # game names that already got Elite Props post today
_cgp_sent_date         = None   # date string when CGP was last sent (once per day)
_games_data            = {}     # game_name -> {total, spread} for script detection

REPLIT_DOMAIN  = os.environ.get("REPLIT_DEV_DOMAIN", "")
RAILWAY_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN",
                                "asset-manager-production-0f7d.up.railway.app")
CHECKOUT_URL   = (
    f"https://{REPLIT_DOMAIN}/join" if REPLIT_DOMAIN
    else f"https://{RAILWAY_DOMAIN}/join"
)

# Odds fetching is game-time-aware (BDL game times drive the schedule — free).
# Seed fetch : 3 hours before the first tip of the day (props fully live by then).
# Cluster fetches: 30 min before each distinct tip-off cluster.
#   Games tipping within 30 min of each other share one fetch (no double-calls).
# 0 calls on off-days.
_odds_cache            = ({}, [])   # (moneyline_dict, odds_games_list)
_odds_cache_hour       = -1         # -1 = never seeded; any other value = seeded
_odds_game_fetch_date  = {"early": None}  # ET date string for the day-seed fetch
_game_cluster_fetched  = set()            # "YYYY-MM-DD HH:MM" keys already fetched

_model_path = os.path.join(os.path.dirname(__file__), "..", "model.pkl")
def _load_model():
    """Load the ML model — DB (most recent retrained) → file (original). Never lost on redeploy."""
    import base64 as _b64
    try:
        # Try to pull the retrained model from DB first
        _conn = _db_conn()
        if _conn:
            try:
                _cur = _conn.cursor()
                _cur.execute("SELECT value FROM learning_data WHERE key='model_b64'")
                _row = _cur.fetchone()
                _cur.close(); _conn.close()
                if _row and _row[0]:
                    _raw = _row[0]
                    _b = _raw if isinstance(_raw, str) else json.dumps(_raw)
                    _b = _b.strip('"')
                    _model_bytes = _b64.b64decode(_b)
                    _m = pickle.loads(_model_bytes)
                    # Cache to file so subsequent loads are fast
                    with open(_model_path, "wb") as _f:
                        _f.write(_model_bytes)
                    print("[Model] Loaded retrained model from DB")
                    return _m
            except Exception as _dbe:
                print(f"[Model] DB load failed ({_dbe}), falling back to file")
                try: _conn.close()
                except Exception: pass
    except Exception:
        pass
    # Fallback — load from file (original model in git repo)
    with open(_model_path, "rb") as _f:
        print("[Model] Loaded base model from file")
        return pickle.load(_f)

model = _load_model()


# ==========================
# 📡 TELEGRAM
# ==========================
def send(msg, chat):
    """Send a Telegram message with up to 3 retry attempts on timeout/error."""
    if not BOT_TOKEN or not chat:
        return
    for attempt in range(1, 4):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat, "text": msg, "parse_mode": "Markdown"},
                timeout=15
            )
            # Telegram responded — message was received, do NOT retry regardless
            # of status code (retrying here would send duplicates)
            if not resp.ok:
                print(f"[Telegram] send HTTP {resp.status_code}: {resp.text[:120]}")
            return
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            # Network/timeout before any response — message definitely not sent, safe to retry
            print(f"[Telegram] send attempt {attempt}/3 network error: {e}")
            if attempt < 3:
                time.sleep(2)
        except Exception as e:
            # Unexpected error — log and stop, don't risk duplicate
            print(f"[Telegram] send unexpected error: {e}")
            return
    print(f"[Telegram] send gave up after 3 attempts (chat={chat})")


def send_telegram(message, chat_id=None):
    """Send a message with retry. Defaults to VIP channel if no chat_id given."""
    send(message, str(chat_id) if chat_id else VIP_CHANNEL)


# ==========================
# 🤖 COMMAND HANDLER
# ==========================
def reply(chat_id, msg):
    """Send a reply to a specific chat."""
    send(msg, str(chat_id))


def send_with_buttons(chat_id, text, buttons):
    """
    Send a message with an inline keyboard.
    buttons: list of lists of {text, callback_data}
    e.g. [[{"text": "✅ Confirm", "callback_data": "confirm_pick"}]]
    """
    if not BOT_TOKEN or not chat_id:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id":    str(chat_id),
                "text":       text,
                "parse_mode": "Markdown",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": b["text"], "callback_data": b["callback_data"]} for b in row]
                        for row in buttons
                    ]
                }
            },
            timeout=10
        )
        return r.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"[Buttons] send error: {e}")
        return None


def edit_message_text(chat_id, message_id, text, buttons=None):
    """Edit an existing message's text (and optionally its inline keyboard)."""
    if not BOT_TOKEN:
        return
    payload = {
        "chat_id":    str(chat_id),
        "message_id": message_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if buttons is not None:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [{"text": b["text"], "callback_data": b["callback_data"]} for b in row]
                for row in buttons
            ]
        }
    else:
        payload["reply_markup"] = {"inline_keyboard": []}
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json=payload,
            timeout=10
        )
    except Exception as e:
        print(f"[EditMsg] error: {e}")


def answer_callback_query(callback_query_id, text=""):
    """Acknowledge a callback query so the spinner disappears."""
    if not BOT_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text},
            timeout=10
        )
    except Exception as e:
        print(f"[Callback] answer error: {e}")


def cmd_picks(chat_id):
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT game, pick, bet_type, result
                FROM bets
                WHERE DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                ORDER BY COALESCE(bet_time, created_at) ASC
            """, (today_et,))
            rows = cur.fetchall()
            cur.close(); conn.close()
        except Exception as e:
            try: conn.close()
            except Exception: pass
    if not rows:
        bets = load_bets()
        today = datetime.now().strftime("%Y-%m-%d")
        fallback = [b for b in bets if str(b.get("time") or "").startswith(today)]
        if fallback:
            lines = []
            for b in fallback:
                icon = "✅" if b.get("result") == "win" else "❌" if b.get("result") == "loss" else "⏳"
                lines.append(f"{icon} *{b['pick']}* ({b.get('betType','ML')}) — {b['game']}")
            reply(chat_id, "🏀 *TODAY'S PICKS*\n\n" + "\n".join(lines))
            return
        reply(chat_id, "📭 No picks sent today yet — check back closer to tip-off.")
        return
    lines = []
    for game, pick, btype, result in rows:
        icon = "✅" if result == "win" else "❌" if result == "loss" else "⏳"
        lines.append(f"{icon} *{pick}* ({btype or 'ML'}) — {game}")
    reply(chat_id, f"🏀 *TODAY'S PICKS* ({len(rows)})\n\n" + "\n".join(lines))


def cmd_record(chat_id):
    bets = load_bets()
    settled = [b for b in bets if b.get("result")]
    wins   = sum(1 for b in settled if b["result"] == "win")
    losses = len(settled) - wins
    pct    = round(wins / len(settled) * 100, 1) if settled else 0
    # Last 10
    last10  = settled[-10:]
    l10w    = sum(1 for b in last10 if b["result"] == "win")
    l10_label = f"Last {len(last10)}" if len(last10) < 10 else "Last 10"
    reply(chat_id, (
        f"📊 *ALL-TIME RECORD*\n\n"
        f"✅ Wins:   {wins}\n"
        f"❌ Losses: {losses}\n"
        f"🎯 Win Rate: *{pct}%*\n\n"
        f"🔥 {l10_label}: *{l10w}W-{len(last10)-l10w}L*"
    ))


def _run_line_monitor_loop(
    chat_id: int,
    refresh_sec: int,
    ev_threshold: float,
    bankroll: float = 1000.0,
    kelly_fraction: float = 0.5,
):
    """
    Background thread: re-fetches props every `refresh_sec` seconds, rebuilds
    the best slip through the full Edge-Fade 7 engine, and sends a compact
    status update to admin DM.

    Alerts fired instantly (separate from the cycle summary) when:
      1. A prop line moves between cycles (line movement alert)
      2. A RISK fill leg was swapped to a SAFE alternative (swap alert)
      3. The rebuilt slip EV exceeds ev_threshold (high-EV alert)
         → sends a full bet ticket with fractional Kelly stake sizing
    """
    import time as _time
    global _line_monitor_active, _bet_history, _session_bankroll_start

    # Reset session log for this run
    _bet_history = []
    _session_bankroll_start = bankroll

    # Tracks last known line per (player, stat) key across cycles
    _prev_lines: dict = {}
    # Deduplication: slips already sent as bet tickets this session
    _placed_bets: set = set()   # frozenset of (player, stat, line) tuples
    # Live bankroll — deducted each time a bet ticket fires
    _current_bankroll: float = bankroll

    def _kelly_stake(broll: float, prob: float, decimal_odds: float, fraction: float) -> float:
        """
        Fractional Kelly criterion stake.
          b = decimal_odds - 1  (net profit per $1 wagered)
          f = (b*p - (1-p)) / b   (full Kelly fraction)
        Returns 0 if the edge is negative (don't bet).
        """
        b = decimal_odds - 1.0
        if b <= 0:
            return 0.0
        f = (b * prob - (1.0 - prob)) / b
        f = max(0.0, f)
        return round(broll * f * fraction, 2)

    while _line_monitor_active:
        try:
            from bot.slip_builder import build_slip_from_props
            _ts  = datetime.now().strftime("%H:%M:%S")
            odds = get_player_props()

            if not odds:
                reply(chat_id, f"🔄 *Line Monitor* [{_ts}] — no props available yet")
            else:
                # Pull real data every cycle — same signals the main engine uses
                try:
                    _lm_injuries = get_espn_injuries()
                except Exception:
                    _lm_injuries = {}
                try:
                    _lm_inj_boost = assess_injury_boost(_lm_injuries, odds)
                except Exception:
                    _lm_inj_boost = {}
                try:
                    _lm_b2b = detect_back_to_back_teams()
                except Exception:
                    _lm_b2b = set()

                _load_and_apply_team_styles()
                _lm_shadow = _load_shadow_hit_rates()
                _lm_wr_ctx = _load_win_rate_context()
                _lm_mults  = _load_conf_multipliers()
                slip, _, _ = build_slip_from_props(
                    props_data          = odds,
                    get_player_stats_fn = get_player_stats,
                    games_data          = _games_data,
                    checkout_url        = CHECKOUT_URL,
                    injuries            = _lm_injuries,
                    injury_boost        = _lm_inj_boost,
                    back_to_back_teams  = _lm_b2b,
                    shadow_hit_rates    = _lm_shadow,
                    win_rate_context    = _lm_wr_ctx,
                    conf_multipliers    = _lm_mults,
                )

                if slip is None:
                    reply(chat_id, f"🔄 *Line Monitor* [{_ts}] — engine found no valid slip this cycle")
                else:
                    # ── Alert 1: Line movement detection ─────────────────────
                    for leg in slip.legs:
                        key      = (leg.player, leg.stat)
                        prev_val = _prev_lines.get(key)
                        if prev_val is not None:
                            prev_line, prev_odds = prev_val
                            if prev_line != leg.line:
                                direction = "▲" if leg.line > prev_line else "▼"
                                reply(
                                    chat_id,
                                    f"⚡ *LINE MOVE* {direction}\n"
                                    f"{leg.player} — {leg.stat.upper()} {leg.direction}\n"
                                    f"`{prev_line}` → `{leg.line}` "
                                    f"(odds {prev_odds} → {leg.odds})\n"
                                    f"_{leg.game}_",
                                )
                        _prev_lines[key] = (leg.line, leg.odds)

                    # ── Alert 2: RISK-to-SAFE swap detection ─────────────────
                    # Any leg marked SAFE that wasn't in prev cycle with same
                    # player key is a swap candidate — detect via line_decision
                    # changes reflected in the slip itself by checking if fill
                    # legs are SAFE (the swap already happened inside the engine)
                    swapped_legs = [
                        l for l in slip.legs
                        if not l.is_fade and not l.is_benefactor
                        and l.line_decision == "SAFE"
                    ]
                    for sleg in swapped_legs:
                        key = (sleg.player, sleg.stat)
                        if key not in _prev_lines:
                            reply(
                                chat_id,
                                f"🔀 *SWAP ALERT*\n"
                                f"New SAFE fill added: {sleg.player}\n"
                                f"{sleg.stat.upper()} {sleg.direction} {sleg.line} ({sleg.odds})\n"
                                f"edge {sleg.edge:+.2f} · _{sleg.game}_",
                            )

                    # ── Alert 3: EV threshold breach + Kelly-sized bet ticket ─
                    slip_id = frozenset(
                        (l.player, l.stat, l.line) for l in slip.legs
                    )
                    if slip.parlay_ev >= ev_threshold:
                        if slip_id not in _placed_bets:
                            # Compute Kelly stake from live bankroll
                            from bot.decision_engine import estimate_payout
                            decimal_odds = slip.estimated_payout / 100 + 1
                            stake = _kelly_stake(
                                _current_bankroll,
                                slip.parlay_hit_prob,
                                decimal_odds,
                                kelly_fraction,
                            )
                            _placed_bets.add(slip_id)
                            _current_bankroll -= stake   # deduct from running bankroll
                            _bet_history.append({
                                "time":     _ts,
                                "legs":     len(slip.legs),
                                "stake":    stake,
                                "ev":       round(slip.parlay_ev, 2),
                                "prob":     round(slip.parlay_hit_prob, 4),
                                "grade":    slip.grade,
                                "bankroll": round(_current_bankroll, 2),
                            })

                            ticket_lines = [
                                f"💰 *AUTO-BET TICKET* 💰",
                                f"EV `${slip.parlay_ev:.2f}` ≥ threshold `${ev_threshold:.0f}` · Grade *{slip.grade}*",
                                f"{len(slip.legs)}-leg · Hit `{slip.parlay_hit_prob:.1%}`",
                                f"",
                                f"🏦 *Kelly Stake: ${stake:.2f}* ({kelly_fraction*100:.0f}% Kelly)",
                                f"Bankroll remaining: `${_current_bankroll:.2f}`",
                                f"",
                                f"📋 *LEGS TO PLACE ON FANDUEL:*",
                            ]
                            for i, leg in enumerate(slip.legs, 1):
                                badge = "FADE" if leg.is_fade else "BENE" if leg.is_benefactor else "FILL"
                                ticket_lines.append(
                                    f"  {i}. [{badge}] {leg.player}\n"
                                    f"     {leg.stat.upper()} {leg.direction} {leg.line} @ {leg.odds}\n"
                                    f"     avg {leg.prediction} · edge {leg.edge:+.2f} · {leg.line_rating}"
                                )
                            ticket_lines += [
                                f"",
                                f"⚠️ _FanDuel has no public API — place this manually._",
                                f"_This slip won't be re-sent unless a line changes._",
                            ]
                            reply(chat_id, "\n".join(ticket_lines))
                        else:
                            # Same slip, EV still high — brief reminder only
                            reply(
                                chat_id,
                                f"🚨 *EV STILL HIGH* `${slip.parlay_ev:.2f}` — ticket already sent · Bankroll `${_current_bankroll:.2f}`",
                            )

                    # ── Cycle summary ─────────────────────────────────────────
                    risk_count = sum(1 for l in slip.legs if l.line_decision == "RISK")
                    safe_count = len(slip.legs) - risk_count
                    msg_lines  = [
                        f"🔄 *Line Monitor* [{_ts}]",
                        f"Grade *{slip.grade}* · {len(slip.legs)}-leg · EV `${slip.parlay_ev:.2f}` · Hit `{slip.parlay_hit_prob:.1%}`",
                        f"SAFE: {safe_count} · RISK: {risk_count} · EV threshold: ${ev_threshold:.0f}",
                        f"🏦 Bankroll: `${_current_bankroll:.2f}` · Kelly: {kelly_fraction*100:.0f}%",
                        "",
                    ]
                    for leg in slip.legs:
                        flag  = "🟢" if leg.line_decision == "SAFE" else "🟡"
                        badge = "FADE" if leg.is_fade else "BENE" if leg.is_benefactor else "FILL"
                        msg_lines.append(
                            f"{flag} [{badge}] {leg.player} — {leg.stat.upper()} "
                            f"{leg.direction} {leg.line} ({leg.odds}) | edge {leg.edge:+.2f}"
                        )
                    reply(chat_id, "\n".join(msg_lines))

        except Exception as _lme:
            reply(chat_id, f"⚠️ Line monitor error: {_lme}")

        _time.sleep(refresh_sec)

    reply(chat_id, "⏹ *Line Monitor stopped.*")


def cmd_line_monitor(chat_id: int, args: str):
    """
    /linemonitor start [seconds] [ev_threshold] [bankroll] [kelly_fraction]
        — start refresh loop with bankroll-managed Kelly stakes
    /linemonitor stop

    Examples:
      /linemonitor start                    → 30s, EV≥$50, $1000 bankroll, 50% Kelly
      /linemonitor start 60                 → 60s refresh
      /linemonitor start 30 75              → EV threshold $75
      /linemonitor start 30 50 500          → $500 bankroll
      /linemonitor start 30 50 1000 0.25    → 25% Kelly (more conservative)
    """
    import threading as _thr
    global _line_monitor_active, _line_monitor_thread

    cmd = args.strip().lower() if args else "start"

    if cmd == "stop":
        if not _line_monitor_active:
            reply(chat_id, "ℹ️ Line monitor is not running.")
        else:
            _line_monitor_active = False
            reply(chat_id, "⏹ Line monitor stopping after current cycle...")
        return

    if cmd.startswith("start"):
        parts = cmd.split()
        try:
            refresh_sec = max(15, min(300, int(parts[1])))
        except (IndexError, ValueError):
            refresh_sec = 30
        try:
            ev_threshold = float(parts[2])
        except (IndexError, ValueError):
            ev_threshold = 50.0
        try:
            bankroll = float(parts[3])
        except (IndexError, ValueError):
            bankroll = 1000.0
        try:
            kelly_fraction = max(0.1, min(1.0, float(parts[4])))
        except (IndexError, ValueError):
            kelly_fraction = 0.5

        if _line_monitor_active:
            reply(chat_id, "⚠️ Already running. Send `/linemonitor stop` first.")
            return

        _line_monitor_active = True
        _line_monitor_thread = _thr.Thread(
            target = _run_line_monitor_loop,
            args   = (chat_id, refresh_sec, ev_threshold, bankroll, kelly_fraction),
            daemon = True,
        )
        _line_monitor_thread.start()
        reply(
            chat_id,
            f"▶️ *Line Monitor started*\n"
            f"Refresh: every *{refresh_sec}s* · EV threshold: *${ev_threshold:.0f}*\n"
            f"🏦 Bankroll: *${bankroll:.0f}* · Kelly: *{kelly_fraction*100:.0f}%*\n\n"
            f"Instant alerts for:\n"
            f"  ⚡ Line movements\n"
            f"  🔀 RISK→SAFE player swaps\n"
            f"  💰 EV ≥ ${ev_threshold:.0f} → full Kelly-sized ticket\n\n"
            f"Send `/linemonitor stop` to halt.",
        )
    else:
        reply(chat_id, "Usage: `/linemonitor start [sec] [ev] [bankroll] [kelly]` or `/linemonitor stop`")


def cmd_bankroll(chat_id: int):
    """
    /bankroll — shows current session bankroll, total staked, bets placed, avg EV.
    Only meaningful while /linemonitor is active or after it has stopped.
    """
    if not _bet_history and _session_bankroll_start == 0.0:
        reply(chat_id, "ℹ️ No monitor session active. Start one with `/linemonitor start`.")
        return

    total_staked = sum(b["stake"] for b in _bet_history)
    avg_ev       = (sum(b["ev"] for b in _bet_history) / len(_bet_history)) if _bet_history else 0.0
    current_br   = _bet_history[-1]["bankroll"] if _bet_history else _session_bankroll_start
    pnl_label    = "up" if current_br >= _session_bankroll_start else "down"
    pnl_amt      = abs(current_br - _session_bankroll_start)
    status       = "▶️ Running" if _line_monitor_active else "⏹ Stopped"

    reply(chat_id, (
        f"🏦 *Session Bankroll Summary* [{status}]\n\n"
        f"Starting bankroll: `${_session_bankroll_start:.2f}`\n"
        f"Current bankroll:  `${current_br:.2f}` ({pnl_label} `${pnl_amt:.2f}`)\n"
        f"Total staked:      `${total_staked:.2f}`\n"
        f"Tickets sent:      `{len(_bet_history)}`\n"
        f"Avg EV per ticket: `${avg_ev:.2f}`"
    ))


def cmd_history_feed(chat_id: int):
    """
    /historyfeed — manual picks logged via /feedpick (feed_picks DB table).
    """
    try:
        conn = _db_conn()
        if not conn:
            reply(chat_id, "⚠️ DB unavailable.")
            return
        cur = conn.cursor()
        cur.execute("""
            SELECT id, pick_text, picked_at_et, result, settled_at_et
            FROM feed_picks
            ORDER BY logged_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        reply(chat_id, f"⚠️ Could not load manual picks: {e}")
        return

    if not rows:
        reply(chat_id, "ℹ️ No manual picks logged yet.")
        return

    lines = [f"📝 *Manual Picks* (last {len(rows)})\n"]
    for r in rows:
        pid, ptxt, pat, result, sat = r
        first_line = ptxt.strip().split("\n")[0][:60]
        if result == "win":
            res_tag = "✅ Win"
        elif result == "loss":
            res_tag = "❌ Loss"
        else:
            res_tag = "⏳ Pending"
        date_tag = f"[{pat}]" if pat else ""
        lines.append(f"  #{pid} {date_tag} {res_tag}\n  _{first_line}_")
    reply(chat_id, "\n".join(lines))


def cmd_history_bot(chat_id: int):
    """
    /historybot — bot engine picks stored in the bets table.
    """
    try:
        conn = _db_conn()
        if not conn:
            reply(chat_id, "⚠️ DB unavailable.")
            return
        cur = conn.cursor()
        cur.execute("""
            SELECT game, player, pick, bet_type, line, odds, result, bet_time, tier
            FROM bets
            ORDER BY created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        reply(chat_id, f"⚠️ Could not load bot picks: {e}")
        return

    if not rows:
        reply(chat_id, "ℹ️ No bot engine picks recorded yet.")
        return

    lines = [f"🤖 *Bot Engine Picks* (last {len(rows)})\n"]
    for r in rows:
        game, player, pick, btype, line, odds, result, btime, tier = r
        if result == "win":
            res_tag = "✅"
        elif result == "loss":
            res_tag = "❌"
        else:
            res_tag = "⏳"
        odds_tag = f" ({odds})" if odds else ""
        time_tag = f"[{str(btime)[:10]}]" if btime else ""
        tier_tag = f" [{tier}]" if tier else ""
        lines.append(
            f"  {res_tag} {time_tag} {player} {pick} {line}{odds_tag}"
            f" — {game}{tier_tag}"
        )
    reply(chat_id, "\n".join(lines))


def cmd_history_live(chat_id: int):
    """
    /historylive — bet tickets from the current /linemonitor session (in-memory).
    """
    if not _bet_history:
        reply(chat_id, "ℹ️ No live session tickets yet. Start /linemonitor first.")
        return

    lines = [f"📡 *Live Session Tickets* ({len(_bet_history)})\n"]
    for i, b in enumerate(_bet_history, 1):
        lines.append(
            f"  {i}. [{b['time']}] Grade *{b['grade']}* · {b['legs']}-leg\n"
            f"     Stake `${b['stake']:.2f}` · EV `${b['ev']:.2f}` · "
            f"Hit `{b['prob']:.1%}` · Bankroll `${b['bankroll']:.2f}`"
        )
    reply(chat_id, "\n".join(lines))


def cmd_check_pending(chat_id: int):
    """
    /checkpending — shows all unsettled (pending) picks grouped by date.
    """
    conn = _db_conn()
    if not conn:
        reply(chat_id, "❌ DB connection failed.")
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                bet_time::date AS day,
                COUNT(*)       AS cnt,
                array_agg(
                    COALESCE(NULLIF(TRIM(player), ''), '[no player]')
                    || ' ' || pick
                    || ' @ ' || line::text
                    ORDER BY bet_time
                ) AS picks
            FROM bets
            WHERE result IS NULL OR result = 'pending'
            GROUP BY bet_time::date
            ORDER BY bet_time::date DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            reply(chat_id, "✅ No unsettled picks — all bets are graded.")
            return

        total = sum(r[1] for r in rows)
        lines = [f"⏳ *{total} Unsettled Pick(s)* across {len(rows)} day(s)\n"]
        for day, cnt, picks in rows:
            lines.append(f"📅 *{day}* — {cnt} pick(s):")
            for p in picks[:10]:
                lines.append(f"  • {p}")
            if cnt > 10:
                lines.append(f"  … and {cnt - 10} more")
            lines.append("")

        lines.append(f"_Use /voidpending YYYY-MM-DD to clear a day, or /voidpending to clear all._")
        reply(chat_id, "\n".join(lines))
    except Exception as e:
        reply(chat_id, f"⚠️ Error: {e}")
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass


def cmd_void_pending(chat_id: int, date_str: str = ""):
    """
    /voidpending [YYYY-MM-DD]
    Marks all pending (ungraded) bets as void.
    Pass an optional date to target only that day, e.g. /voidpending 2026-04-02
    Omit the date to void ALL pending bets.
    """
    conn = _db_conn()
    if not conn:
        reply(chat_id, "❌ DB connection failed.")
        return
    try:
        cur = conn.cursor()

        # ── Always clean up corrupt entries (None/empty/null player) ─────────
        # Exclude neutral/fade/benefactor prop legs — those need BDL settlement.
        cur.execute("""
            SELECT id FROM bets
            WHERE (result IS NULL OR result = 'pending')
              AND pick_category NOT IN ('neutral_prop','fade_prop','benefactor_prop')
              AND (player IS NULL OR TRIM(player) = '' OR TRIM(player) IN ('None','none'))
        """)
        corrupt_ids = [r[0] for r in cur.fetchall()]
        if corrupt_ids:
            cur.execute("UPDATE bets SET result = 'void' WHERE id = ANY(%s)", (corrupt_ids,))

        # ── Build main filter (never touch SGP prop legs) ─────────────
        _excl = "AND pick_category NOT IN ('neutral_prop','fade_prop','benefactor_prop')"
        if date_str:
            cur.execute(
                f"SELECT id, game, player, pick, line, bet_type FROM bets "
                f"WHERE (result IS NULL OR result = 'pending') {_excl} AND bet_time::text LIKE %s "
                f"ORDER BY bet_time",
                (f"{date_str}%",)
            )
        else:
            cur.execute(
                f"SELECT id, game, player, pick, line, bet_type FROM bets "
                f"WHERE (result IS NULL OR result = 'pending') {_excl} "
                f"ORDER BY bet_time"
            )

        rows = cur.fetchall()

        if not rows and not corrupt_ids:
            reply(chat_id, f"✅ No pending bets found{' for ' + date_str if date_str else ''}.")
            return

        # ── Preview ───────────────────────────────────────────────────
        preview_lines = [
            f"🗑 *Voiding {len(rows)} pending bet(s)"
            f"{' for ' + date_str if date_str else ''}*"
        ]
        if corrupt_ids:
            preview_lines.append(f"🧹 Auto-cleaned {len(corrupt_ids)} corrupt entry(s) (no player name)\n")
        else:
            preview_lines.append("")
        for row in rows[:30]:
            rid, game, player, pick, line, btype = row
            preview_lines.append(f"  [{rid}] {player} {pick} {line} — {game}")
        if len(rows) > 30:
            preview_lines.append(f"  … and {len(rows)-30} more")
        reply(chat_id, "\n".join(preview_lines))

        # ── Void main picks ───────────────────────────────────────────
        if rows:
            ids = [r[0] for r in rows]
            cur.execute(
                "UPDATE bets SET result = 'void' WHERE id = ANY(%s)",
                (ids,)
            )
        conn.commit()
        cur.close()
        conn.close()
        total_cleared = len(rows) + len(corrupt_ids)
        reply(chat_id, f"✅ Cleared {total_cleared} pick(s). They will no longer appear as pending.")
    except Exception as e:
        reply(chat_id, f"⚠️ Error: {e}")
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass


def cmd_calibrate(chat_id: int):
    """
    /calibrate — fetches live ESPN season stats for all 30 teams, recomputes
    pace/assist_bias/reb_bias, checks accuracy against graded bets in DB,
    then applies the updated styles in-memory.
    """
    from bot.game_script import (
        fetch_calibrated_team_styles,
        check_style_accuracy,
        TEAM_STYLES,
    )
    import bot.game_script as _gs

    reply(chat_id, "🔄 Fetching live ESPN team stats for all 30 teams…")

    try:
        new_styles = fetch_calibrated_team_styles()
    except Exception as e:
        reply(chat_id, f"⚠️ ESPN fetch failed: {e}")
        return

    # ── Build diff report ──────────────────────────────────────────────
    changes = []
    for team, new in new_styles.items():
        old = TEAM_STYLES.get(team, {})
        diffs = []
        for key in ("pace", "assist_bias", "reb_bias"):
            o = old.get(key)
            n = new.get(key)
            if o is not None and n is not None and abs(n - o) >= (1 if key == "pace" else 0.01):
                arrow = "↑" if n > o else "↓"
                diffs.append(f"{key} {o}→{n}{arrow}")
        if diffs:
            changes.append(f"  {team}: {', '.join(diffs)}")

    # ── Apply new styles in-memory ────────────────────────────────────
    _gs.TEAM_STYLES.update(new_styles)

    # ── Accuracy check against DB ─────────────────────────────────────
    conn = _db_conn()
    accuracy = check_style_accuracy(conn)
    if conn:
        try: conn.close()
        except Exception: pass

    # ── Format reply ──────────────────────────────────────────────────
    lines = ["📊 *Calibration Complete*\n"]

    if changes:
        lines.append(f"*{len(changes)} team(s) updated:*")
        lines.extend(changes[:20])   # cap at 20 to avoid Telegram message limit
        if len(changes) > 20:
            lines.append(f"  …and {len(changes)-20} more")
    else:
        lines.append("✅ All 30 teams already match live data — no changes needed.")

    lines.append("\n*Style Accuracy (graded bets):*")
    for stat in ("assists", "rebounds", "points"):
        acc = accuracy[stat]
        tot = acc["total"]
        if tot == 0:
            lines.append(f"  {stat.capitalize()}: no graded data yet")
        else:
            pct = acc["accuracy"]
            lines.append(
                f"  {stat.capitalize()}: `{acc['correct']}/{tot}` ({pct:.0%}) — {acc['suggestion']}"
            )

    if accuracy["notes"]:
        lines.append("\n*Per-team standouts:*")
        lines.extend(f"  {n}" for n in accuracy["notes"][:10])

    reply(chat_id, "\n".join(lines))


def cmd_schedule(chat_id):
    try:
        today = datetime.now(timezone.utc).date()
        url   = f"{BDL_BASE}/games?dates[]={today}&per_page=15"
        resp  = _bdl_get(url)
        games = resp.get("data", [])
        if not games:
            reply(chat_id, "📅 No games scheduled for today.")
            return
        lines = []
        for g in games:
            away = g["visitor_team"]["full_name"]
            home = g["home_team"]["full_name"]
            tip  = g.get("date", "")
            try:
                t = datetime.fromisoformat(tip.replace("Z","").split(".")[0]).replace(tzinfo=timezone.utc)
                import zoneinfo as _zi
                et = t.astimezone(_zi.ZoneInfo("America/New_York"))
                tip_str = et.strftime("%-I:%M %p ET")
            except Exception:
                tip_str = "TBD"
            lines.append(f"🏀 {away} @ {home} — {tip_str}")
        reply(chat_id, f"📅 *TODAY'S GAMES*\n\n" + "\n".join(lines))
    except Exception as e:
        reply(chat_id, f"Couldn't load schedule: {e}")


def cmd_subscribe(chat_id):
    join_url = f"{CHECKOUT_URL}?tg={chat_id}"
    reply(chat_id, (
        f"🔒 *JOIN ELITE VIP*\n\n"
        f"Get full picks, Starting Five breakdowns, spread & total edges, "
        f"and a 7-day free trial.\n\n"
        f"👉 {join_url}\n\n"
        f"_$29/month — cancel anytime_"
    ))


# ==========================
# 🏀 VS ANALYSIS COMMAND
# ==========================

_NBA_TEAM_LOOKUP = {
    # Full names
    "atlanta hawks": "Atlanta Hawks", "boston celtics": "Boston Celtics",
    "brooklyn nets": "Brooklyn Nets", "charlotte hornets": "Charlotte Hornets",
    "chicago bulls": "Chicago Bulls", "cleveland cavaliers": "Cleveland Cavaliers",
    "dallas mavericks": "Dallas Mavericks", "denver nuggets": "Denver Nuggets",
    "detroit pistons": "Detroit Pistons", "golden state warriors": "Golden State Warriors",
    "houston rockets": "Houston Rockets", "indiana pacers": "Indiana Pacers",
    "los angeles clippers": "Los Angeles Clippers", "los angeles lakers": "Los Angeles Lakers",
    "memphis grizzlies": "Memphis Grizzlies", "miami heat": "Miami Heat",
    "milwaukee bucks": "Milwaukee Bucks", "minnesota timberwolves": "Minnesota Timberwolves",
    "new orleans pelicans": "New Orleans Pelicans", "new york knicks": "New York Knicks",
    "oklahoma city thunder": "Oklahoma City Thunder", "orlando magic": "Orlando Magic",
    "philadelphia 76ers": "Philadelphia 76ers", "phoenix suns": "Phoenix Suns",
    "portland trail blazers": "Portland Trail Blazers", "sacramento kings": "Sacramento Kings",
    "san antonio spurs": "San Antonio Spurs", "toronto raptors": "Toronto Raptors",
    "utah jazz": "Utah Jazz", "washington wizards": "Washington Wizards",
    # Nicknames / city shorthand
    "hawks": "Atlanta Hawks", "celtics": "Boston Celtics", "nets": "Brooklyn Nets",
    "hornets": "Charlotte Hornets", "bulls": "Chicago Bulls", "cavaliers": "Cleveland Cavaliers",
    "cavs": "Cleveland Cavaliers", "mavericks": "Dallas Mavericks", "mavs": "Dallas Mavericks",
    "nuggets": "Denver Nuggets", "pistons": "Detroit Pistons", "warriors": "Golden State Warriors",
    "gsw": "Golden State Warriors", "rockets": "Houston Rockets", "pacers": "Indiana Pacers",
    "clippers": "Los Angeles Clippers", "lac": "Los Angeles Clippers",
    "lakers": "Los Angeles Lakers", "lal": "Los Angeles Lakers",
    "grizzlies": "Memphis Grizzlies", "grizz": "Memphis Grizzlies",
    "heat": "Miami Heat", "bucks": "Milwaukee Bucks", "timberwolves": "Minnesota Timberwolves",
    "wolves": "Minnesota Timberwolves", "twolves": "Minnesota Timberwolves",
    "pelicans": "New Orleans Pelicans", "knicks": "New York Knicks", "ny": "New York Knicks",
    "thunder": "Oklahoma City Thunder", "okc": "Oklahoma City Thunder",
    "magic": "Orlando Magic", "76ers": "Philadelphia 76ers", "sixers": "Philadelphia 76ers",
    "philly": "Philadelphia 76ers", "suns": "Phoenix Suns", "blazers": "Portland Trail Blazers",
    "kings": "Sacramento Kings", "spurs": "San Antonio Spurs",
    "raptors": "Toronto Raptors", "jazz": "Utah Jazz", "wizards": "Washington Wizards",
    # Abbreviations
    "atl": "Atlanta Hawks", "bos": "Boston Celtics", "bkn": "Brooklyn Nets",
    "cha": "Charlotte Hornets", "chi": "Chicago Bulls", "cle": "Cleveland Cavaliers",
    "dal": "Dallas Mavericks", "den": "Denver Nuggets", "det": "Detroit Pistons",
    "hou": "Houston Rockets", "ind": "Indiana Pacers", "mem": "Memphis Grizzlies",
    "mia": "Miami Heat", "mil": "Milwaukee Bucks", "min": "Minnesota Timberwolves",
    "nop": "New Orleans Pelicans", "nyk": "New York Knicks", "orl": "Orlando Magic",
    "phi": "Philadelphia 76ers", "phx": "Phoenix Suns", "por": "Portland Trail Blazers",
    "sac": "Sacramento Kings", "sas": "San Antonio Spurs", "tor": "Toronto Raptors",
    "uta": "Utah Jazz", "was": "Washington Wizards",
}


def resolve_team(raw):
    """Fuzzy-match a raw team string to a full NBA team name."""
    key = raw.strip().lower()
    # Direct lookup
    if key in _NBA_TEAM_LOOKUP:
        return _NBA_TEAM_LOOKUP[key]
    # Partial match — find any lookup key that contains the input or vice versa
    for k, v in _NBA_TEAM_LOOKUP.items():
        if key in k or k in key:
            return v
    return None


# ─────────────────────────────────────────────────────────────
# MULTI-TEAM COMMAND HELPERS
# ─────────────────────────────────────────────────────────────

import re as _re

_SCRIPT_LABEL = {
    # Pace
    "TRANSITION_HEAVY": "🟡 TRANSITION",
    "UPTEMPO":          "🟡 UPTEMPO",
    "AVERAGE_PACE":     "📊 AVG PACE",
    "SLOW_PACED":       "⚫ SLOW",
    "HALFCOURT":        "⚫ HALFCOURT",
    # Flow
    "BLOWOUT":           "🟢 BLOWOUT",
    "DOUBLE_DIGIT_LEAD": "🟢 DBL DIGIT",
    "COMFORTABLE_LEAD":  "🔵 COMF LEAD",
    "COMPETITIVE":       "📊 COMPETITIVE",
    "TIGHT_GAME":        "🔵 TIGHT",
    # Scoring
    "SHOOTOUT":          "🔥 SHOOTOUT",
    "HIGH_SCORING":      "🟡 HIGH SCORE",
    "NORMAL_SCORING":    "📊 NORMAL SCR",
    "DEFENSIVE_BATTLE":  "⚫ DEF BATTLE",
    # Legacy support
    "UPSET":   "🔴 UPSET",
    "INJURY":  "🟣 INJURY",
}

_SCRIPT_NOTE = {
    # Pace
    "TRANSITION_HEAVY": "Run-and-gun · OVERs and scoring props have edge",
    "UPTEMPO":          "Fast pace · Scoring props and OVERs have edge",
    "AVERAGE_PACE":     "Average pace · Run full model",
    "SLOW_PACED":       "Slow pace · Rebound and defensive props shine",
    "HALFCOURT":        "Halfcourt grind · Defensive battle · Under has edge",
    # Flow
    "BLOWOUT":           "Big favourite · Fade public star · Benefactors win",
    "DOUBLE_DIGIT_LEAD": "Double-digit favourite · Star fade likely · Back bench scorers",
    "COMFORTABLE_LEAD":  "Comfortable lead expected · Stars play 3 qtrs · Watch minutes",
    "COMPETITIVE":       "Competitive game · Balanced attack · Trust the model",
    "TIGHT_GAME":        "Tight battle · Playmakers and distributors shine",
    # Scoring
    "SHOOTOUT":          "Scoring explosion · OVERs and high-volume scorers win",
    "HIGH_SCORING":      "High scoring environment · Offensive props have edge",
    "NORMAL_SCORING":    "Normal scoring · No dominant scoring edge",
    "DEFENSIVE_BATTLE":  "Defensive battle · UNDERs and rebound props shine",
    # Legacy
    "UPSET":   "Model disagrees with Vegas · Dog has value",
    "INJURY":  "Key player out · Usage redistributes",
}

D_LINE = "━━━━━━━━━━━━━━━━━━━"


def _et_time_str():
    """Return current time formatted as Eastern Time (handles Railway UTC servers)."""
    try:
        import zoneinfo as _zi
        return datetime.now(_zi.ZoneInfo("America/New_York")).strftime("%-I:%M %p ET")
    except Exception:
        return datetime.utcnow().strftime("%H:%M UTC")


def _parse_teams(raw_text):
    """Split space/comma-separated team tokens and resolve to full NBA names."""
    parts = _re.split(r"[,\s]+", raw_text.strip())
    resolved = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        t = resolve_team(p)
        if t and t not in resolved:
            resolved.append(t)
    return resolved


def _find_game_for_team(team, games_raw):
    """Return the odds-API game object for a team, or None."""
    for g in games_raw:
        if team in (g.get("home_team", ""), g.get("away_team", "")):
            return g
    return None


def _build_game_context(game_obj):
    """Extract vegas total/spread and run game-script detection."""
    vegas_total  = 0.0
    vegas_spread = 0.0
    if game_obj:
        _bks = game_obj.get("bookmakers", [])
        _bk = next((b for b in _bks if b.get("key") == "fanduel"), None)
        if _bk:
            for mkt in _bk.get("markets", []):
                if mkt["key"] == "totals":
                    for o in mkt.get("outcomes", []):
                        if o.get("name") == "Over":
                            vegas_total = float(o.get("point", 0))
                if mkt["key"] == "spreads":
                    for o in mkt.get("outcomes", []):
                        if o.get("name") == game_obj.get("home_team"):
                            vegas_spread = float(o.get("point", 0))
    game_data = {"total": vegas_total, "spread": abs(vegas_spread), "has_key_injury": False}
    script = detect_game_script(game_data)
    return game_data, script


def _filter_props_for_game(ht, at, props_pool):
    """Filter props pool to a specific game."""
    return [
        p for p in props_pool
        if (ht in p.get("game_name", "") or at in p.get("game_name", "")
            or ht in p.get("home_team", "") or at in p.get("home_team", "")
            or ht in p.get("away_team", "") or at in p.get("away_team", ""))
    ]


def _best_by_stat(team_name, props_pool):
    """
    Return the top engine-scored candidate for each stat type for a given team.
    props_pool must be engine candidates (from _get_engine_candidates) which carry
    'player', 'prop_type', 'team', 'confidence', 'edge', 'pick', 'line', 'odds'.
    """
    STAT_KW = {
        "pts":    ["points", "pts"],
        "reb":    ["rebounds", "reb"],
        "ast":    ["assists", "ast"],
        "threes": ["threes", "three", "fg3"],
    }
    team_last = team_name.split()[-1].lower()

    def _is_team(p):
        candidate_team = (p.get("team") or "").lower()
        return (team_last in candidate_team or team_last in p.get("player", "").lower())

    result = {}
    for stat, kws in STAT_KW.items():
        pool = [
            p for p in props_pool
            if _is_team(p) and any(k in p.get("prop_type", "").lower() for k in kws)
        ]
        result[stat] = (
            max(pool, key=lambda x: x.get("confidence", 0))
            if pool else None
        )
    return result


def _get_engine_candidates(filtered_props, all_props, game_data, ht, at, top_n=10):
    """Run Edge-Fade 7 engine and return sorted candidate list."""
    from bot.slip_builder import build_slip_from_props, get_top_candidates

    def _no_alert(msg): pass

    injuries = {}
    try:
        injuries = get_espn_injuries()
    except Exception:
        pass

    inj_boost = {}
    try:
        inj_boost = assess_injury_boost(injuries, filtered_props if filtered_props else all_props)
    except Exception:
        pass

    b2b = set()
    try:
        b2b = detect_back_to_back_teams()
    except Exception:
        pass

    props = filtered_props if filtered_props else all_props
    gkey1 = f"{ht} vs {at}"
    gkey2 = f"{at} vs {ht}"

    _load_and_apply_team_styles()
    _vip_shadow = _load_shadow_hit_rates()
    _vip_wr_ctx = _load_win_rate_context()
    _vip_mults  = _load_conf_multipliers()

    candidates = get_top_candidates(
        props_data=props,
        get_player_stats_fn=get_player_stats,
        games_data={gkey1: game_data, gkey2: game_data},
        injuries=injuries,
        top_n=top_n,
        injury_boost=inj_boost,
        back_to_back_teams=b2b,
        shadow_hit_rates=_vip_shadow,
        win_rate_context=_vip_wr_ctx,
    )

    if not candidates:
        try:
            slip, _, _ = build_slip_from_props(
                props_data=props,
                get_player_stats_fn=get_player_stats,
                games_data={gkey1: game_data, gkey2: game_data},
                checkout_url="",
                admin_alert_fn=_no_alert,
                injuries=injuries,
                injury_boost=inj_boost,
                back_to_back_teams=b2b,
                shadow_hit_rates=_vip_shadow,
                win_rate_context=_vip_wr_ctx,
                conf_multipliers=_vip_mults,
            )
            if slip and slip.legs:
                candidates = [
                    {
                        "player":     getattr(l, "player", ""),
                        "pick":       getattr(l, "pick", ""),
                        "line":       getattr(l, "line", ""),
                        "prop_type":  getattr(l, "prop_type", ""),
                        "confidence": getattr(l, "confidence", 0),
                        "ev":         getattr(l, "ev", 0),
                        "role":       getattr(l, "role", ""),
                    }
                    for l in slip.legs
                ]
        except Exception:
            pass

    # ── ESPN fallback: build from starters when bookmaker props aren't posted yet ──
    # get_team_starters_espn already returns pred_pts/reb/ast/fg3 — no extra API calls.
    if not candidates and (ht or at):
        try:
            for team_name in [t for t in [ht, at] if t]:
                starters = get_team_starters_espn(team_name)
                for s in starters[:5]:
                    pname    = s.get("name", "")
                    avg_mins = float(s.get("avg_mins") or 0)
                    if not pname or avg_mins < 20:
                        continue
                    inj_info = injuries.get(pname.lower(), {})
                    if inj_info.get("status") in ("Out", "Doubtful"):
                        continue
                    avg_pts = float(s.get("pred_pts") or 0)
                    avg_reb = float(s.get("pred_reb") or 0)
                    avg_ast = float(s.get("pred_ast") or 0)
                    avg_fg3 = float(s.get("pred_fg3") or 0)
                    conf    = min(72, max(50, int(55 + avg_mins * 0.4)))
                    for prop_type, avg_val in [
                        ("points",   avg_pts),
                        ("rebounds", avg_reb),
                        ("assists",  avg_ast),
                        ("threes",   avg_fg3),
                    ]:
                        if avg_val < 0.5:
                            continue
                        line = round(avg_val - 0.5, 1)
                        candidates.append({
                            "player":         pname,
                            "pick":           "OVER",
                            "line":           line,
                            "prop_type":      prop_type,
                            "confidence":     conf,
                            "ev":             0.0,
                            "team":           team_name,
                            "game":           gkey1,
                            "odds":           -110,
                            "_from_fallback": True,
                        })
        except Exception as e:
            print(f"[_get_engine_candidates] fallback error: {e}")

    return candidates


def _leg_fmt(c, game_tag=""):
    """Format a single candidate leg for Telegram output."""
    conf     = int(c.get("confidence", 0))
    role     = c.get("role", "")
    role_tag = f" [{role.upper()}]" if role else ""
    desc     = f"{c.get('player','?')} {c.get('pick','?')} {c.get('line','?')} {c.get('prop_type','')}"
    odds_raw = c.get("odds", "")
    odds_tag = f" ({odds_raw})" if odds_raw else ""
    g_tag    = f" [{game_tag}]" if game_tag else ""
    return f"  {desc.strip()}{odds_tag}{role_tag}{g_tag} — {conf}%"


def _collect_unique_games(teams, games_raw):
    """Return list of (home_team, away_team, game_obj) with no duplicates."""
    seen = set()
    entries = []
    for team in teams:
        game_obj = _find_game_for_team(team, games_raw)
        if game_obj:
            ht  = game_obj.get("home_team", "")
            at  = game_obj.get("away_team", "")
            key = f"{ht}:{at}"
            if key not in seen:
                seen.add(key)
                entries.append((ht, at, game_obj))
    return entries


# ─────────────────────────────────────────────────────────────
# /props  — per-team breakdown by pts / reb / ast
# ─────────────────────────────────────────────────────────────

def cmd_props(chat_id, raw_teams_text):
    try:
        teams = _parse_teams(raw_teams_text)
        if not teams:
            reply(chat_id, "❌ No teams recognised.\nExample: /props lakers celtics okc")
            return

        reply(chat_id, f"📋 Pulling props for {len(teams)} team(s) — one moment...")

        _, games_raw = get_odds_cached()
        all_props    = get_player_props()

        lines = [f"📋 *PROPS BREAKDOWN*", D_LINE]

        for team in teams:
            game_obj = _find_game_for_team(team, games_raw)
            if game_obj:
                ht        = game_obj.get("home_team", "")
                at        = game_obj.get("away_team", "")
                opp       = at if team == ht else ht
                game_data, script = _build_game_context(game_obj)
                filtered  = _filter_props_for_game(ht, at, all_props)
            else:
                ht, at    = team, ""
                opp       = "TBD"
                game_data = {"total": 0, "spread": 0, "has_key_injury": False}
                script    = "NORMAL"
                filtered  = all_props

            # Use engine candidates (engine-scored, include team field) for display
            candidates = _get_engine_candidates(filtered, all_props, game_data, ht, at, top_n=40)
            stat_map   = _best_by_stat(team, candidates)

            label     = _SCRIPT_LABEL.get(script, "📊")
            opp_short = opp.split()[-1] if opp != "TBD" else "TBD"
            lines.append(f"\n*{team.split()[-1]}* (vs {opp_short}) {label}")
            lines.append(f"_{_SCRIPT_NOTE.get(script, '')}_")

            is_fallback = any(c.get("_from_fallback") for c in candidates)

            def _fmt(p, icon, label_str):
                if not p:
                    return f"  {icon} *{label_str}* —"
                player   = p.get("player", "?")
                pick     = p.get("pick", "OVER")
                line     = p.get("line", "?")
                odds     = p.get("odds", "")
                odds_tag = f" ({odds})" if odds and not p.get("_from_fallback") else ""
                conf     = int(p.get("confidence", 0))
                proj_tag = " _(proj)_" if p.get("_from_fallback") else ""
                return f"  {icon} *{label_str}* {player} {pick} {line}{odds_tag} — {conf}%{proj_tag}"

            lines.append(_fmt(stat_map.get("pts"),    "🏀", "Points:  "))
            lines.append(_fmt(stat_map.get("reb"),    "💪", "Rebounds:"))
            lines.append(_fmt(stat_map.get("ast"),    "🔥", "Assists: "))
            lines.append(_fmt(stat_map.get("threes"), "🎯", "Threes:  "))

        lines += ["", D_LINE, f"_Requested · {_et_time_str()}_"]
        reply(chat_id, "\n".join(lines))

    except Exception as e:
        print(f"[cmd_props] Error: {e}")
        reply(chat_id, f"⚠️ Error: {e}")


# ─────────────────────────────────────────────────────────────
# /sgp  — same-game parlay per game in 3 / 5 / 7 tiers
# ─────────────────────────────────────────────────────────────

def _save_pick_legs_to_bets(candidates, bet_type, game_label, timestamp=None):
    """
    Persist a list of candidate pick legs to the bets table so they
    get graded, tracked, and fed into the learning loop.
    Deduplicates on (game, player, pick, bet_type) — safe to call multiple times.
    """
    if not candidates:
        return 0
    ts = timestamp or str(datetime.now())
    conn = _db_conn()
    if not conn:
        return 0
    saved = 0
    try:
        cur = conn.cursor()
        for c in candidates:
            # Never persist fallback/synthetic picks — they use prediction-0.5 as
            # the line, not a real FanDuel number, and pollute the DB and results.
            if c.get("_from_fallback"):
                continue

            pname  = c.get("player", "")
            pick   = c.get("pick", "")
            line   = c.get("line", 0)
            ptype  = c.get("prop_type", "")
            conf   = c.get("confidence", 0)
            ev     = c.get("ev", 0)
            is_f   = bool(c.get("is_fade", False))
            is_b   = bool(c.get("is_benefactor", False))
            fade_t = c.get("fade_target", "") or ""
            odds   = c.get("odds", -110)

            if not pname or not pick or str(pname).strip() in ("None", "none", ""):
                continue

            # Deduplication: check before insert (ON CONFLICT DO NOTHING alone is
            # not sufficient because the bets table has no unique constraint on
            # (game, player, pick, bet_type) — only on the serial primary key).
            try:
                cur.execute(
                    "SELECT 1 FROM bets WHERE game=%s AND player=%s AND pick=%s AND bet_type=%s",
                    (game_label, pname, pick, bet_type)
                )
                if cur.fetchone():
                    continue
            except Exception:
                pass

            # category
            if is_f:
                cat = "fade_prop"
            elif is_b:
                cat = "benefactor_prop"
            else:
                cat = "neutral_prop"

            # Embed prop_type into pick so the settlement pass can resolve the stat.
            # Format: "OVER|points", "UNDER|rebounds", etc.  Legacy rows stored
            # bare "OVER"/"UNDER"; the resolver handles both forms.
            pick_stored = f"{pick}|{ptype}" if ptype else pick

            try:
                cur.execute("""
                    INSERT INTO bets
                        (game, player, pick, bet_type, line, odds, confidence,
                         ev, is_fade, is_benefactor, fade_target,
                         pick_category, bet_time, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    game_label, pname, pick_stored, bet_type,
                    line, odds, conf,
                    ev, is_f, is_b, fade_t,
                    cat, ts, ts,
                ))
                saved += 1
            except Exception as ie:
                print(f"[SaveLegs] insert error {pname}: {ie}")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SaveLegs] error: {e}")
        try:
            conn.close()
        except Exception:
            pass
    return saved


def cmd_sgp(chat_id, raw_teams_text):
    try:
        teams = _parse_teams(raw_teams_text)
        if not teams:
            reply(chat_id, "❌ No teams recognised.\nExample: /sgp lakers celtics okc")
            return

        reply(chat_id, f"🎲 Building SGPs for {len(teams)} team(s) — one moment...")

        _, games_raw = get_odds_cached()
        all_props    = get_player_props()
        game_entries = _collect_unique_games(teams, games_raw)

        if not game_entries:
            reply(chat_id, "❌ No tonight's games found for those teams. Try after lines post.")
            return

        lines = [f"🎲 *SGP — {len(game_entries)} GAME(S)*", D_LINE]

        for ht, at, game_obj in game_entries:
            game_data, script = _build_game_context(game_obj)
            label    = _SCRIPT_LABEL.get(script, "📊")
            filtered = _filter_props_for_game(ht, at, all_props)

            lines.append(f"\n*[SGP] {ht.split()[-1]} vs {at.split()[-1]}* {label}")
            lines.append(f"_{_SCRIPT_NOTE.get(script, '')}_")

            try:
                cands = _get_engine_candidates(filtered, all_props, game_data, ht, at, top_n=10)
            except Exception:
                cands = []

            if not cands:
                lines.append("  ⚠️ No picks cleared for this game — props or stats may not be available yet.")
                continue

            for tier_name, icon, n in [("SAFE", "🟢", 3), ("BALANCED", "🟡", 5), ("AGGRESSIVE", "🔴", 7)]:
                legs = cands[:n]
                odds = _parlay_odds(len(legs))
                lines.append(f"\n  {icon} *{tier_name} — {len(legs)} LEG*")
                for c in legs:
                    lines.append(_leg_fmt(c))
                lines.append(f"  📊 Est. +{odds:,}")

        lines += ["", D_LINE, f"_Requested · {_et_time_str()}_"]
        reply(chat_id, "\n".join(lines))

        # ── Persist legs to bets table for grading + learning ────────────────
        try:
            _ts = str(datetime.now())
            for ht2, at2, _go2 in game_entries:
                _filt2  = _filter_props_for_game(ht2, at2, all_props)
                _gd2, _ = _build_game_context(_go2)
                try:
                    _cands2 = _get_engine_candidates(
                        _filt2, all_props, _gd2, ht2, at2, top_n=7
                    )
                    _label2 = f"{ht2.split()[-1]} vs {at2.split()[-1]}"
                    _n = _save_pick_legs_to_bets(_cands2, "SGP", _label2, _ts)
                    print(f"[cmd_sgp] Saved {_n} legs to bets: {_label2}")
                except Exception:
                    pass
        except Exception as _se:
            print(f"[cmd_sgp] save error: {_se}")

    except Exception as e:
        print(f"[cmd_sgp] Error: {e}")
        reply(chat_id, f"⚠️ Error: {e}")


# ─────────────────────────────────────────────────────────────
# /parlay  — cross-game parlay in 3 / 5 / 7 tiers
# ─────────────────────────────────────────────────────────────

def cmd_parlay(chat_id, raw_teams_text):
    try:
        teams = _parse_teams(raw_teams_text)
        if not teams:
            reply(chat_id, "❌ No teams recognised.\nExample: /parlay lakers celtics okc")
            return

        reply(chat_id, f"🎯 Building cross-game parlay for {len(teams)} team(s) — one moment...")

        _, games_raw = get_odds_cached()
        all_props    = get_player_props()
        game_entries = _collect_unique_games(teams, games_raw)

        if not game_entries:
            reply(chat_id, "❌ No tonight's games found for those teams. Try after lines post.")
            return

        lines = [f"🎯 *PARLAY — {len(game_entries)} GAME(S)*", D_LINE]
        for ht, at, game_obj in game_entries:
            _, script = _build_game_context(game_obj)
            label = _SCRIPT_LABEL.get(script, "📊")
            lines.append(f"  {ht.split()[-1]} vs {at.split()[-1]} {label}")
        lines.append("")

        # Gather engine-cleared candidates across all games
        STAT_KW = {
            "pts": ["pts", "points", "player_points"],
            "reb": ["reb", "rebounds", "player_rebounds", "total_rebounds"],
            "ast": ["ast", "assists", "player_assists"],
        }

        all_cands = []
        for ht, at, game_obj in game_entries:
            game_data, _ = _build_game_context(game_obj)
            filtered = _filter_props_for_game(ht, at, all_props)
            try:
                cands = _get_engine_candidates(filtered, all_props, game_data, ht, at, top_n=10)
                for c in cands:
                    c["_game"] = f"{ht.split()[-1]} vs {at.split()[-1]}"
                all_cands.extend(cands)
            except Exception:
                pass

        if not all_cands:
            reply(chat_id, "⚠️ No picks cleared for these games — props or stats may not be available yet.")
            return

        all_cands.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        def _by_stat(pool, stat):
            kws = STAT_KW.get(stat, [])
            return [c for c in pool if any(k in c.get("prop_type", "").lower() for k in kws)]

        pts_pool = _by_stat(all_cands, "pts")
        reb_pool = _by_stat(all_cands, "reb")
        ast_pool = _by_stat(all_cands, "ast")

        safe_legs = pts_pool[:3]
        bal_extra = [c for c in reb_pool if c not in safe_legs][:2]
        bal_legs  = safe_legs + bal_extra

        agg_extra = [c for c in ast_pool if c not in bal_legs][:2]
        if len(bal_legs) + len(agg_extra) < 7:
            remaining = [c for c in all_cands if c not in bal_legs and c not in agg_extra]
            agg_extra += remaining[: 7 - len(bal_legs) - len(agg_extra)]
        agg_legs = bal_legs + agg_extra

        def _tier_block(tier_name, icon, legs):
            if not legs:
                return []
            odds = _parlay_odds(len(legs))
            block = [f"{icon} *{tier_name} — {len(legs)} LEG*"]
            for c in legs:
                block.append(_leg_fmt(c, game_tag=c.get("_game", "")))
            block.append(f"  📊 Est. +{odds:,}")
            block.append("")
            return block

        lines += _tier_block("SAFE", "🟢", safe_legs)
        lines += _tier_block("BALANCED", "🟡", bal_legs)
        lines += _tier_block("AGGRESSIVE", "🔴", agg_legs)
        lines += [D_LINE, f"_Requested · {_et_time_str()}_"]
        reply(chat_id, "\n".join(lines))

        # ── Persist all unique legs to bets for grading + learning ───────────
        try:
            _seen = {}
            for c in agg_legs:
                key = (c.get("player",""), c.get("pick",""), c.get("line",""))
                if key not in _seen:
                    _seen[key] = c
            _unique = list(_seen.values())
            _glabel = " / ".join(
                f"{ht.split()[-1]} vs {at.split()[-1]}"
                for ht, at, _ in game_entries
            )
            _n = _save_pick_legs_to_bets(_unique, "CROSS_SGP", _glabel)
            print(f"[cmd_parlay] Saved {_n} legs to bets")
        except Exception as _pe:
            print(f"[cmd_parlay] save error: {_pe}")

    except Exception as e:
        print(f"[cmd_parlay] Error: {e}")
        reply(chat_id, f"⚠️ Error: {e}")


_FEEDPICK_STAT_WORDS = {
    "points": "PTS", "pts": "PTS", "point": "PTS",
    "assists": "AST", "ast": "AST", "assist": "AST",
    "rebounds": "REB", "reb": "REB", "rebound": "REB", "boards": "REB",
    "threes": "3PM", "three": "3PM", "3s": "3PM", "3pm": "3PM",
    "made threes": "3PM", "made three": "3PM", "3-pointers": "3PM",
    "steals": "STL", "stl": "STL", "steal": "STL",
    "blocks": "BLK", "blk": "BLK", "block": "BLK",
    "minutes": "MIN", "min": "MIN",
}

_STAT_ICON = {
    "PTS": "🏀", "AST": "🔥", "REB": "💪",
    "3PM": "🎯", "STL": "🤝", "BLK": "🛡️", "MIN": "⏱️",
}


def _parse_one_leg(text):
    """
    Parse a single leg: '[player name] [+/-line] [stat] ... [odds]'
    Returns dict: {player, stats: [(line, label), ...], odds, raw}
    """
    import re as _re2

    t = " ".join(text.strip().split())

    # Extract trailing odds (+NNN / -NNN, 3-4 digits)
    odds = None
    odds_m = _re2.search(r'([+-]\d{3,4})$', t)
    if odds_m:
        odds = int(odds_m.group(1))
        t = t[:odds_m.start()].strip()

    # Build stat-keyword regex (longest match first)
    stat_keys = sorted(_FEEDPICK_STAT_WORDS.keys(), key=len, reverse=True)
    stat_pat  = "|".join(_re2.escape(k) for k in stat_keys)
    seg_re    = _re2.compile(
        rf'([+-]?\d+(?:\.\d+)?)\s+(?:(?:over|under)\s+)?({stat_pat})',
        _re2.IGNORECASE
    )

    stats, spans = [], []
    for m in seg_re.finditer(t):
        raw_stat   = m.group(2).lower()
        label      = _FEEDPICK_STAT_WORDS.get(raw_stat, raw_stat.upper())
        stats.append((m.group(1), label))
        spans.append((m.start(), m.end()))

    # Everything before the first stat = player name
    player = ""
    if spans:
        before = t[:spans[0][0]].strip()
        before = _re2.sub(r'^(?:over|under)\s+', '', before, flags=_re2.IGNORECASE).strip()
        if before and _re2.search(r'[A-Za-z]', before):
            player = before.title()

    return {"player": player, "stats": stats, "odds": odds, "raw": text.strip()}


def _parse_pick_text(text):
    """
    Parse a full feedpick text (1–8 legs).

    Leg separators: comma  /  pipe  |  newline
    Examples:
      Single leg:
        Jamal Murray +20 points -280
        Over Jokic 27.5 pts -110
      Multi-leg parlay (2–8 players):
        Murray +20 pts, LeBron +25 pts +8 reb, Curry +4 threes -115
        Murray +20 pts / LeBron +25 pts / SGA +6 ast
      With per-leg odds + overall parlay odds at end:
        Murray +20 pts +3 threes, Jokic 12 reb, LaMelo 6 ast -245, Banchero 20 pts -295, +728

    Returns list of leg dicts: [{player, stats, odds, raw, parlay_odds?}, ...]
    The last leg may carry a parlay_odds key if a standalone odds token was found.
    """
    import re as _re2

    # Split on  ,  /  |  or newline
    parts = _re2.split(r'\s*[,/|\n]\s*', text.strip())
    parts = [p.strip() for p in parts if p.strip()]

    # Check if the very last part is a standalone odds token (+NNN / -NNN, no stat words)
    parlay_odds = None
    if len(parts) > 1:
        last = parts[-1]
        if _re2.fullmatch(r'[+-]\d{3,4}', last):
            parlay_odds = int(last)
            parts = parts[:-1]

    legs = [_parse_one_leg(p) for p in parts]
    legs = [l for l in legs if l["stats"] or l["player"]]

    if not legs:
        legs = [_parse_one_leg(text)]

    # Attach overall parlay odds to the last leg so _fmt_pick_confirmation can use it
    if parlay_odds is not None and legs:
        legs[-1] = dict(legs[-1], parlay_odds=parlay_odds)

    return legs


def _fmt_leg(leg):
    """Single-line summary for one leg."""
    lv_parts = []
    for line_val, stat in leg["stats"]:
        lv = str(line_val)
        if not lv.startswith("+") and not lv.startswith("-"):
            lv = "+" + lv
        lv_parts.append(f"{lv} {stat}")
    stat_str = " · ".join(lv_parts)
    odds_str = ""
    if leg["odds"] is not None:
        sign = "+" if leg["odds"] > 0 else ""
        odds_str = f" ({sign}{leg['odds']})"
    name = leg["player"] or "Pick"
    return f"{name}  {stat_str}{odds_str}".strip()


def _fmt_pick_confirmation(legs):
    """
    Build a full Telegram-formatted breakdown for 1–8 legs.
    legs: list of leg dicts from _parse_pick_text()

    For multi-leg parlays: if the last leg carries odds, those are treated as
    the overall parlay payout odds and shown at the bottom, not pinned to that leg.
    Single-leg picks keep odds on the pick itself.
    """
    if not legs:
        return ""

    is_parlay = len(legs) > 1

    # Pull overall parlay odds off the last leg if present
    parlay_odds = legs[-1].get("parlay_odds") if legs else None

    lines = []
    for i, leg in enumerate(legs, 1):
        prefix = f"*Leg {i}*" if is_parlay else "*Pick*"
        name   = leg["player"] or ""
        header = f"{prefix}{' — ' + name if name else ''}"
        lines.append(header)
        if leg["stats"]:
            for line_val, stat in leg["stats"]:
                lv = str(line_val)
                if not lv.startswith("+") and not lv.startswith("-"):
                    lv = "+" + lv
                icon = _STAT_ICON.get(stat, "📊")
                lines.append(f"  {icon} {lv} {stat}")
        else:
            lines.append(f"  📝 {leg['raw']}")
        if leg.get("odds") is not None:
            sign = "+" if leg["odds"] > 0 else ""
            lines.append(f"  💵 {sign}{leg['odds']}")

    if parlay_odds is not None:
        sign = "+" if parlay_odds > 0 else ""
        lines.append(f"\n💵 *Parlay payout:* {sign}{parlay_odds}")

    return "\n".join(lines)


def cmd_feedpick(chat_id, raw):
    """
    /feedpick <pick>                         → logged at current ET time
    /feedpick @3:30PM <pick>                 → logged as 3:30 PM today ET
    /feedpick @yesterday 9:00PM <pick>       → logged as 9:00 PM yesterday ET

    Accepted pick formats (any combination):
      Jamal Murray +20 points -280
      +20 points +6 assists +4 rebounds +3 made threes
      Jamal Murray +20 points +6 assists -280
      Over Jokic 27.5 pts -110
    """
    import re, zoneinfo as _zi_fp

    raw = raw.strip()
    if not raw:
        reply(chat_id,
            "❌ *Usage examples:*\n"
            "`/feedpick Jamal Murray +20 points -280`\n"
            "`/feedpick +20 points +6 assists +4 rebounds +3 made threes`\n"
            "`/feedpick @3:30PM Jamal Murray +20 points -280`\n"
            "`/feedpick @yesterday 9PM Over Jokic 27.5 pts -110`"
        )
        return

    try:
        tz_et   = _zi_fp.ZoneInfo("America/New_York")
        et_now  = datetime.now(tz_et)

        # ── Parse optional @time prefix ──────────────────────────────────
        picked_dt = et_now
        pick_text = raw

        def _parse_time(time_str, use_date):
            for fmt in ("%I:%M%p", "%I:%M %p", "%I%p", "%I %p", "%H:%M"):
                try:
                    t = datetime.strptime(time_str.upper().replace(" ", ""), fmt.replace(" ", ""))
                    return datetime(use_date.year, use_date.month, use_date.day,
                                    t.hour, t.minute, tzinfo=tz_et)
                except ValueError:
                    continue
            return et_now

        if raw.startswith("@"):
            rest = raw[1:]
            if rest.lower().startswith("yesterday"):
                after_yesterday = rest[9:].strip()
                use_date = (et_now - timedelta(days=1)).date()
                t_m = re.match(r'^(\d{1,2}(?::\d{2})?\s*[APap][Mm])\s+(.*)', after_yesterday, re.DOTALL)
                if t_m:
                    time_str  = t_m.group(1).strip()
                    pick_text = t_m.group(2).strip()
                else:
                    time_str  = "12:00 AM"
                    pick_text = after_yesterday.strip()
                picked_dt = _parse_time(time_str, use_date)
            else:
                m2 = re.match(r'^(\S+)\s+(.*)', rest, re.DOTALL)
                if m2:
                    time_str  = m2.group(1).strip()
                    pick_text = m2.group(2).strip()
                    picked_dt = _parse_time(time_str, et_now.date())

        # ── Parse pick structure (returns list of legs) ──────────────────
        legs    = _parse_pick_text(pick_text)
        summary = _fmt_pick_confirmation(legs)

        # ── Show preview + Confirm / Edit buttons (don't save yet) ──────
        logged_str = et_now.strftime("%b %d, %Y %-I:%M %p ET")
        picked_str = picked_dt.strftime("%b %d, %Y %-I:%M %p ET")
        is_past    = picked_dt <= et_now
        status_tag = "✅ Already played" if is_past else "⏳ Upcoming"
        n_legs     = len(legs)
        parlay_tag = f"🎯 *{n_legs}-Leg Parlay*\n\n" if n_legs > 1 else ""

        preview_text = (
            f"📋 *Pick Preview*\n\n"
            f"{parlay_tag}"
            f"{summary}\n\n"
            f"🕐 *Pick time:* {picked_str}\n"
            f"📌 *Status:* {status_tag}\n\n"
            f"Confirm or edit this pick?"
        )

        _pending_feedpicks[str(chat_id)] = {
            "legs":       legs,
            "pick_text":  pick_text.strip(),
            "picked_dt":  picked_dt,
            "picked_str": picked_str,
            "logged_str": logged_str,
            "is_past":    is_past,
        }

        send_with_buttons(chat_id, preview_text, [
            [
                {"text": "✅ Confirm", "callback_data": "feedpick_confirm"},
                {"text": "✏️ Edit",    "callback_data": "feedpick_edit"},
            ]
        ])
        print(f"[FeedPick] Preview sent to {chat_id} | {n_legs} leg(s) | {pick_text.strip()}")
    except Exception as e:
        print(f"[FeedPick] Error: {e}")
        reply(chat_id, f"⚠️ Could not save pick: {e}")


def cmd_forcesettle(chat_id):
    """
    /forcesettle — Admin: immediately run a full settlement pass.
    Settles JSON picks (ML/totals/spreads) AND DB-stored neutral_prop /
    fade_prop / benefactor_prop legs from SGP/CGP that auto-settle misses.
    """
    reply(chat_id, "⚙️ Running settlement pass now...")

    # Quick DB diagnostic before running — no BDL call needed
    diag_lines = []
    try:
        _dc = _db_conn()
        if _dc:
            _dcur = _dc.cursor()
            _dcur.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE player IS NULL OR player = ''),
                       COUNT(*) FILTER (WHERE result = 'void'),
                       COUNT(*) FILTER (WHERE result IN ('win','loss'))
                FROM bets
                WHERE pick_category IN ('neutral_prop','fade_prop','benefactor_prop')
            """)
            _tot, _empty_player, _voided, _graded = _dcur.fetchone()
            _dcur.execute("""
                SELECT player, pick, line, result,
                       DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York')
                FROM bets
                WHERE pick_category IN ('neutral_prop','fade_prop','benefactor_prop')
                  AND (result IS NULL OR result = 'void')
                  AND player IS NOT NULL AND player != ''
                ORDER BY id LIMIT 3
            """)
            _sample = _dcur.fetchall()
            _dcur.close()
            _dc.close()
            diag_lines.append(f"📊 DB: {_tot} prop rows | {_graded} graded | {_voided} void | {_empty_player} empty player")
            for _sp, _sk, _sl, _sr, _sd in _sample:
                diag_lines.append(f"  • {_sp} | {_sk} | line={_sl} | {_sr} | {_sd}")
    except Exception as _de:
        diag_lines.append(f"⚠️ DB diag error: {_de}")

    try:
        n = update_results()
        try:
            from bot.decision_engine import pe_flush
            pe_flush()
        except Exception:
            pass
        msg = f"✅ Settlement complete — {n or 0} pick(s) newly graded."
        if diag_lines:
            msg += "\n\n" + "\n".join(diag_lines)
        reply(chat_id, msg)
    except Exception as e:
        reply(chat_id, f"❌ Settlement error: {e}\n" + "\n".join(diag_lines))
        print(f"[ForceSettle] error: {e}")


def cmd_debugsettle(chat_id):
    """
    /debugsettle — Admin: show first 5 unsettled neutral_prop rows and
    test BDL lookup for the most recent date found, so we can diagnose
    why /forcesettle returns 0.
    """
    lines = ["🔍 <b>Debug: unsettled prop rows</b>"]
    conn = _db_conn()
    if not conn:
        reply(chat_id, "❌ No DB connection.")
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, player, pick, line, pick_category,
                   DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York'),
                   result
            FROM bets
            WHERE pick_category IN ('neutral_prop','fade_prop','benefactor_prop')
            ORDER BY id
            LIMIT 8
        """)
        rows = cur.fetchall()
        cur.execute("""
            SELECT COUNT(*) FROM bets
            WHERE pick_category IN ('neutral_prop','fade_prop','benefactor_prop')
              AND result IS NULL
              AND player IS NOT NULL AND player != ''
        """)
        total_unsettled = cur.fetchone()[0]
        cur.close()
        conn.close()

        lines.append(f"Total unsettled (player not null): <b>{total_unsettled}</b>")
        lines.append(f"First 8 rows (any result):\n")
        for rid, player, pick, line, pcat, bdate, result in rows:
            lines.append(
                f"  #{rid} | {player or '(empty)'} | {pick} | line={line} | "
                f"{pcat} | {bdate} | result={result}"
            )

        # Now test BDL for the most recent date found
        dates_found = sorted({str(r[5]) for r in rows if r[5]}, reverse=True)
        if dates_found:
            test_date = dates_found[0]
            lines.append(f"\n🌐 BDL test for {test_date}:")
            try:
                url = f"{BDL_BASE}/stats?dates[]={test_date}&per_page=100"
                data = _bdl_get(url).get("data", [])
                final_players = []
                for st in data:
                    p = st.get("player", {})
                    nm = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
                    status = st.get("game", {}).get("status", "")
                    if "final" in status.lower():
                        final_players.append(nm)
                lines.append(f"  {len(final_players)} final-game players from BDL")
                lines.append(f"  First 5: {', '.join(final_players[:5]) or '(none)'}")
            except Exception as bdl_e:
                lines.append(f"  BDL error: {bdl_e}")
    except Exception as e:
        lines.append(f"❌ DB error: {e}")

    try:
        reply(chat_id, "\n".join(lines), parse_mode="HTML")
    except Exception:
        reply(chat_id, "\n".join(lines))


def cmd_settle(chat_id, raw):
    """
    /settle <id> <win|loss|push>
    Marks a feed pick result and announces wins to the free channel.
    """
    import zoneinfo as _zi_s
    raw = raw.strip()
    parts = raw.split()
    if len(parts) < 2:
        reply(chat_id,
            "❌ *Usage:*\n"
            "`/settle 4 win`\n"
            "`/settle 4 loss`\n"
            "`/settle 4 push`"
        )
        return
    try:
        pick_id = int(parts[0])
        result  = parts[1].lower().strip()
        if result not in ("win", "loss", "push"):
            reply(chat_id, "❌ Result must be `win`, `loss`, or `push`.")
            return

        tz_et      = _zi_s.ZoneInfo("America/New_York")
        settled_at = datetime.now(tz_et).strftime("%b %d, %Y %-I:%M %p ET")

        conn = _db_conn()
        if not conn:
            reply(chat_id, "⚠️ Database unavailable — pick not settled.")
            return
        cur  = conn.cursor()
        cur.execute(
            "UPDATE feed_picks SET result=%s, settled_at_et=%s WHERE id=%s RETURNING pick_text, picked_at_et",
            (result, settled_at, pick_id)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if not row:
            reply(chat_id, f"❌ No pick found with ID #{pick_id}.")
            return

        pick_text, picked_at = row
        icon = {"win": "✅", "loss": "❌", "push": "➖"}[result]
        label = {"win": "WON", "loss": "LOST", "push": "PUSH"}[result]

        # ── Confirm to admin ──────────────────────────────────────────────
        reply(chat_id,
            f"{icon} *Pick #{pick_id} settled as {label}*\n\n"
            f"📝 _{pick_text}_\n\n"
            f"🕐 *Pick time:* {picked_at}\n"
            f"📌 *Settled:* {settled_at}"
        )

        # ── Announce wins to free channel ─────────────────────────────────
        if result == "win":
            announcement = (
                f"🏆 *PICK HIT* — VIP members cashed last night\n\n"
                f"📅 {picked_at}\n\n"
                f"✅ _{pick_text}_\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🔒 VIP gets these *before* the game.\n"
                f"Lock in → {CHECKOUT_URL}"
            )
            send(announcement, FREE_CHANNEL)
            reply(chat_id, "📢 Win announced to free channel.")

        # ── Self-learning: closed loop for all settled picks ──────────────
        if result in ("win", "loss"):
            try:
                from decision_engine import (
                    record_channel_outcome, record_kelly_outcome,
                    record_ml_outcome, record_causality_outcome, pe_flush,
                )
                _hit = (result == "win")

                # Channel floor + Kelly + ML learning
                record_channel_outcome("VIP", _hit)
                record_kelly_outcome(1.0 if _hit else -1.0)
                record_ml_outcome(_hit)

                # Causality closed loop — try to match pick to game causality
                # by finding the game in game_observations for that date
                try:
                    _c2 = _db_conn()
                    if _c2:
                        _cu2 = _c2.cursor()
                        _today_str = datetime.now().strftime("%Y-%m-%d")
                        _cu2.execute("""
                            SELECT DISTINCT game_id FROM game_observations
                            WHERE game_date = %s
                            ORDER BY game_id DESC LIMIT 10
                        """, (_today_str,))
                        _game_rows = _cu2.fetchall()
                        _cu2.close()
                        _c2.close()
                        # Aggregate causality events across all games from today
                        _all_causes = []
                        for (_gid,) in _game_rows:
                            _all_causes.extend(_get_game_causality_events(_gid))
                        # Infer stat from pick text (best effort)
                        _txt_lower = pick_text.lower()
                        _inf_stat = (
                            "points"    if "pts" in _txt_lower or "points" in _txt_lower else
                            "rebounds"  if "reb" in _txt_lower else
                            "assists"   if "ast" in _txt_lower else
                            "3pm"       if "3pm" in _txt_lower or "three" in _txt_lower else
                            "pra"       if "pra" in _txt_lower else "points"
                        )
                        record_causality_outcome(result, _inf_stat, "UNKNOWN", _all_causes)
                except Exception:
                    record_causality_outcome(result, "points", "UNKNOWN", [])

                # Persist causality + pattern state immediately so a restart
                # before the nightly cycle doesn't erase today's learning.
                try:
                    pe_flush()
                except Exception:
                    pass
            except Exception as _sle:
                print(f"[Settle] self-learning error: {_sle}")

        print(f"[Settle] #{pick_id} → {result} | {pick_text[:50]}")

    except Exception as e:
        print(f"[Settle] Error: {e}")
        reply(chat_id, f"⚠️ Could not settle pick: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 🔍  AUTO PICK CHECK — BDL box-score result checker
# ─────────────────────────────────────────────────────────────────────────────

_STAT_BDL_FIELD = {
    "PTS": "pts", "REB": "reb", "AST": "ast",
    "3PM": "fg3m", "STL": "stl", "BLK": "blk", "MIN": "min",
}

_nightly_check_sent    = None   # ISO date of last nightly sweep (fallback)
_auto_checked_picks    = set()  # pick IDs fully resolved and reported
_auto_notified_misses  = {}     # {pick_id: set of (player, stat) already miss-alerted}
_live_tracker_cache    = {}     # {pick_id: last alert text} — spam prevention
_parlay_notified       = set()  # {parlay_key} — parlays that already got a result card
_pending_feedpicks     = {}     # {chat_id: {legs, pick_text, picked_dt, picked_str, logged_str, is_past}}
_editing_feedpick      = {}     # {chat_id: True} — user is in edit-mode, next message = new pick text
_auto_adjust_done_date = None   # ISO date auto-adjustment last ran (once per night)


def _fetch_player_boxscore(player_name, date_str):
    """
    Fetch a player's actual stats for a specific game date from ESPN.
    date_str: YYYY-MM-DD
    Returns dict {full_name, pts, reb, ast, fg3m, stl, blk, min} or None.
    """
    try:
        espn_date = date_str.replace("-", "")
        resp = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={espn_date}",
            timeout=10
        ).json()
        events = resp.get("events", [])
    except Exception as e:
        print(f"[checkpick] ESPN scoreboard error for {date_str}: {e}")
        return None

    target      = player_name.lower().strip()
    target_last = target.split()[-1] if target else ""

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue
        pstats = _espn_summary_player_stats(event_id)
        for ps in pstats:
            pname = ps.get("pname", "")
            if not pname:
                continue
            pname_l = pname.lower()
            if pname_l == target or pname_l.split()[-1] == target_last or target in pname_l:
                mins_f   = ps.get("mins", 0.0)
                mins_int = int(mins_f)
                mins_sec = round((mins_f - mins_int) * 60)
                return {
                    "full_name": pname,
                    "pts":       ps["pts"],
                    "reb":       ps["reb"],
                    "ast":       ps["ast"],
                    "fg3m":      ps["fg3m"],
                    "stl":       ps["stl"],
                    "blk":       ps["blk"],
                    "min":       f"{mins_int}:{mins_sec:02d}",
                }
    print(f"[checkpick] ESPN: {player_name} not found in any game on {date_str}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MID-GAME PARLAY TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_player_live_stats(player_name, live_game):
    """
    Fetch a player's live in-game stats from ESPN game summary.
    game_id is the ESPN event ID (from _fetch_bdl_live_games which now uses ESPN).
    Returns dict {pts, reb, ast, fg3m, stl, blk} or None.
    """
    try:
        event_id = live_game.get("game_id") if live_game else None
        if not event_id:
            return None
        summary = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}",
            timeout=10
        ).json()
        boxscore = summary.get("boxscore", {})
        target      = player_name.lower()
        target_last = target.split()[-1]
        for team_data in boxscore.get("players", []):
            for section in team_data.get("statistics", []):
                keys = section.get("keys", [])
                for athlete in section.get("athletes", []):
                    ath_info  = athlete.get("athlete", {})
                    disp_name = ath_info.get("displayName", "").lower()
                    last_name = ath_info.get("lastName", disp_name.split()[-1] if disp_name else "").lower()
                    if disp_name == target or last_name == target_last or target in disp_name:
                        vals = athlete.get("stats", [])
                        def _g(k, default=0):
                            try:
                                idx = keys.index(k)
                                raw = str(vals[idx] or "0")
                                return int(float(raw.split("-")[0]))
                            except Exception:
                                return default
                        # ESPN uses "threePointFieldGoalsMade-threePointFieldGoalsAttempted"
                        fg3_key = next((k for k in keys if "threePoint" in k and "Made" in k), "")
                        return {
                            "pts":  _g("points"),
                            "reb":  _g("rebounds"),
                            "ast":  _g("assists"),
                            "stl":  _g("steals"),
                            "blk":  _g("blocks"),
                            "fg3m": _g(fg3_key) if fg3_key else 0,
                        }
    except Exception as e:
        print(f"[LiveStats] ESPN {player_name}: {e}")
    return None


def _fetch_bdl_live_games():
    """
    Fetch all of today's NBA games with live clock data.
    Uses ESPN scoreboard for real-time in-progress status — BDL free tier
    does not update game state until Final, so ESPN is the reliable source.
    Returns list of dicts: {home, away, status, period, time, game_id}
    where status = 'pre' / 'in' / 'post' (ESPN state) and game_id = ESPN event id.
    """
    try:
        resp = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            timeout=10
        ).json()
        games = []
        for event in resp.get("events", []):
            comp   = event.get("competitions", [{}])[0]
            teams  = comp.get("competitors", [])
            home_c = next((t for t in teams if t.get("homeAway") == "home"), {})
            away_c = next((t for t in teams if t.get("homeAway") == "away"), {})
            home_name  = home_c.get("team", {}).get("displayName", "")
            away_name  = away_c.get("team", {}).get("displayName", "")
            home_score = int(home_c.get("score", 0) or 0)
            away_score = int(away_c.get("score", 0) or 0)
            st     = event.get("status", {})
            state  = st.get("type", {}).get("state", "")   # "pre" / "in" / "post"
            period = st.get("period", 0) or 0
            clock  = st.get("displayClock", "")
            games.append({
                "game_id":    event.get("id"),
                "home":       home_name,
                "away":       away_name,
                "status":     state,
                "period":     period,
                "time":       clock,
                "home_score": home_score,
                "away_score": away_score,
            })
        return games
    except Exception as e:
        print(f"[LiveTracker] ESPN games fetch error: {e}")
        return []


_PLAYER_NICKNAMES = {
    "sga":   "Shai Gilgeous-Alexander",
    "kd":    "Kevin Durant",
    "bron":  "LeBron James",
    "lbj":   "LeBron James",
    "ad":    "Anthony Davis",
    "pg":    "Paul George",
    "pg13":  "Paul George",
    "jt":    "Jayson Tatum",
    "jaylen": "Jaylen Brown",
    "steph": "Stephen Curry",
    "dray":  "Draymond Green",
    "klay":  "Klay Thompson",
    "cp3":   "Chris Paul",
    "russ":  "Russell Westbrook",
    "dame":  "Damian Lillard",
    "cj":    "CJ McCollum",
    "jrue":  "Jrue Holiday",
    "giannis": "Giannis Antetokounmpo",
    "greek freak": "Giannis Antetokounmpo",
    "embiid": "Joel Embiid",
    "jojo":  "Joel Embiid",
    "luka":  "Luka Doncic",
    "shai":  "Shai Gilgeous-Alexander",
    "trae":  "Trae Young",
    "ant":   "Anthony Edwards",
    "kat":   "Karl-Anthony Towns",
    "jok":   "Nikola Jokic",
    "jokic": "Nikola Jokic",
    "zion":  "Zion Williamson",
    "ja":    "Ja Morant",
    "bam":   "Bam Adebayo",
    "herro": "Tyler Herro",
    "max":   "Pascal Siakam",
    "siakam": "Pascal Siakam",
    "lauri": "Lauri Markkanen",
    "spida": "Donovan Mitchell",
    "mitchell": "Donovan Mitchell",
    "sabonis": "Domantas Sabonis",
    "domantas": "Domantas Sabonis",
    "devin": "Devin Booker",
    "booker": "Devin Booker",
    "bbook": "Devin Booker",
    "rj":    "RJ Barrett",
    "barrett": "RJ Barrett",
    "hali":  "Tyrese Haliburton",
    "haliburton": "Tyrese Haliburton",
    "payton": "Payton Pritchard",
}


def _match_live_game(player_name, live_games):
    """
    Find the live game a player is in by scanning ESPN game summaries directly.
    Supports full names, last names only, and common nicknames.
    Returns the game dict or None.
    """
    if not live_games:
        return None

    raw       = player_name.strip()
    raw_lower = raw.lower()

    # Resolve nickname → canonical full name
    canonical   = _PLAYER_NICKNAMES.get(raw_lower, raw)
    target      = canonical.lower()
    target_last = target.split()[-1] if target else ""

    for g in live_games:
        try:
            gid = g.get("game_id") or g.get("id", "")
            if not gid:
                continue
            pstats = _espn_summary_player_stats(gid)
            for ps in pstats:
                pname = ps.get("pname", "").lower()
                if not pname:
                    continue
                if pname == target or pname.split()[-1] == target_last or target in pname:
                    return g
        except Exception as e:
            print(f"[LiveTracker] match game ESPN error for {player_name}: {e}")
            continue

    print(f"[LiveTracker] {player_name} not found in any live ESPN game")
    return None


def _minutes_elapsed(period, time_str):
    """
    Convert BDL period + time-remaining string ("4:32") to total minutes elapsed.
    NBA: 4 quarters × 12 min = 48 min regulation.
    """
    try:
        parts = str(time_str).split(":")
        mins_left = int(parts[0]) + (int(parts[1]) / 60 if len(parts) > 1 else 0)
    except Exception:
        mins_left = 0
    period = max(1, int(period or 1))
    elapsed = (period - 1) * 12 + (12 - mins_left)
    return max(0.0, min(elapsed, 48.0))


def _pace_status(current_val, line_val, period, time_str, pick_dir="OVER"):
    """
    Evaluate whether a prop is on pace using real BDL game clock.
    Returns (status, progress_pct, pace_note).
      status: "GREEN" | "YELLOW" | "RED" | "DONE"
    """
    if period == 0:
        return "WAIT", 0.0, "Game hasn't started"

    elapsed   = _minutes_elapsed(period, time_str)
    remaining = max(0.0, 48.0 - elapsed)
    pace_frac = elapsed / 48.0 if elapsed > 0 else 0.0

    if pace_frac == 0:
        return "WAIT", 0.0, "Pre-game"

    expected_now = line_val * pace_frac
    progress_pct  = (current_val / line_val * 100) if line_val else 0

    q_label = f"Q{period}" if period <= 4 else f"OT{period - 4}"
    mins_left_str = f"{remaining:.0f} min left"

    if pick_dir == "OVER":
        if current_val >= line_val:
            return "DONE", 100.0, f"Already hit ✅ ({current_val:.0f}/{line_val:.0f})"
        if current_val >= expected_now * 0.92:
            return "GREEN", progress_pct, f"{current_val:.0f}/{line_val:.0f} — on pace {q_label} ({mins_left_str})"
        if current_val >= expected_now * 0.72:
            return "YELLOW", progress_pct, f"{current_val:.0f}/{line_val:.0f} — slightly behind {q_label} ({mins_left_str})"
        return "RED", progress_pct, f"{current_val:.0f}/{line_val:.0f} — behind pace {q_label} ({mins_left_str})"
    else:  # UNDER
        safe_ceiling = line_val * pace_frac * 1.1
        if current_val <= safe_ceiling:
            return "GREEN", progress_pct, f"{current_val:.0f}/{line_val:.0f} — under pace {q_label} ({mins_left_str})"
        if current_val <= safe_ceiling * 1.2:
            return "YELLOW", progress_pct, f"{current_val:.0f}/{line_val:.0f} — getting close {q_label} ({mins_left_str})"
        return "RED", progress_pct, f"{current_val:.0f}/{line_val:.0f} — at risk {q_label} ({mins_left_str})"


def _cashout_advice(leg_statuses, parlay_odds_str=None):
    """
    Given list of leg statuses (GREEN/YELLOW/RED/DONE/WAIT),
    return (recommendation, estimated_cashout_pct).
    """
    prob_map = {"GREEN": 0.75, "YELLOW": 0.50, "RED": 0.20, "DONE": 1.0, "WAIT": 0.65}
    leg_probs = [prob_map.get(s, 0.5) for s in leg_statuses]

    from functools import reduce
    import operator
    combined_prob = reduce(operator.mul, leg_probs, 1.0)
    cashout_pct   = combined_prob * 0.80  # book keeps ~20% margin on cashout

    reds    = leg_statuses.count("RED")
    yellows = leg_statuses.count("YELLOW")
    greens  = leg_statuses.count("GREEN") + leg_statuses.count("DONE")
    total   = len(leg_statuses)

    if reds >= 2:
        rec = "🚨 CASH OUT NOW"
    elif reds == 1 and total >= 3:
        rec = "⚠️ ONE LEG FAILING — consider cashing out"
    elif yellows >= 2:
        rec = "👀 MULTIPLE LEGS AT RISK — watch closely"
    elif greens == total:
        rec = "🔥 ALL LEGS ON PACE — RIDE"
    elif greens >= total - 1 and yellows == 1:
        rec = "💪 MOSTLY ON PACE — HOLD"
    else:
        rec = "👀 WATCH CLOSE"

    # Hedge check: if only one non-DONE leg remains
    non_done = [s for s in leg_statuses if s not in ("DONE",)]
    if len(non_done) == 1 and greens >= total - 1:
        rec = "💰 HEDGE LAST LEG — others already hit"

    cashout_display = f"~{cashout_pct*100:.0f}% of payout"
    if parlay_odds_str:
        try:
            odds_val = int(str(parlay_odds_str).replace("+", "").replace(" ", ""))
            if odds_val > 0:
                decimal = 1 + odds_val / 100
                cashout_display += f" (≈ {decimal * cashout_pct:.2f}x stake)"
        except Exception:
            pass

    return rec, cashout_display


_STAT_LABEL_TO_BDL = {
    "PTS": "pts", "REB": "reb", "AST": "ast",
    "3PM": "fg3m", "STL": "stl", "BLK": "blk",
}


def _parse_bet_row_to_leg(player, pick_text, line):
    """
    Convert a single bets-table row into a leg dict for live tracking.
    pick_text examples: 'Anthony Davis OVER 24.5 PTS', 'Jokic UNDER 8.5 AST'
    Falls back to the raw `line` column if parsing fails.
    """
    import re
    if not player:
        return None
    leg = {"player": player, "stats": [], "pick": "OVER"}
    m = re.search(r'\b(OVER|UNDER)\b\s+([\d.]+)\s+([A-Z3]+)', (pick_text or "").upper())
    if m:
        leg["pick"]  = m.group(1)
        leg["stats"] = [{"stat": m.group(3), "line": float(m.group(2))}]
    elif line is not None:
        direction = "UNDER" if "UNDER" in (pick_text or "").upper() else "OVER"
        known_stats = list(_STAT_LABEL_TO_BDL.keys())
        stat = next((s for s in known_stats if s in (pick_text or "").upper()), "PTS")
        leg["pick"]  = direction
        leg["stats"] = [{"stat": stat, "line": float(line)}]
    else:
        return None
    return leg


# ══════════════════════════════════════════════════════════════════════════════
# MANUAL LIVE UPDATE — shared helpers + 6 /update* commands
# ══════════════════════════════════════════════════════════════════════════════

def _check_player_live_pick(player, pick_text, line, live_in_progress):
    """
    Check a single player pick against live box score.
    Returns (formatted_block_str, rec_str).
    """
    leg = _parse_bet_row_to_leg(player, pick_text, line)
    if not leg or not leg.get("stats"):
        return f"  ⚠️ *{player}* — can't parse pick", "UNKNOWN"

    game = _match_live_game(player, live_in_progress)
    box  = _fetch_player_live_stats(player, game) if game else None

    if not box:
        return f"  ⏳ *{player}* — no live data", "WAIT"

    period   = game["period"]
    time_str = game["time"]
    q_label  = f"Q{period}" if period <= 4 else f"OT{period - 4}"
    clock_tag = f" ⏱ {q_label} {time_str}"

    stat_entry  = leg["stats"][0]
    stat        = stat_entry.get("stat", "PTS").upper()
    sline       = float(stat_entry.get("line", 0))
    bdl_f       = _STAT_LABEL_TO_BDL.get(stat, "pts")
    pick_dir    = leg.get("pick", "OVER")
    current_val = float(box.get(bdl_f, 0) or 0)
    si          = _STAT_ICON.get(stat, "📊")

    status, pct, note = _pace_status(current_val, sline, period, time_str, pick_dir)
    color = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "DONE": "✅", "WAIT": "⏳"}.get(status, "⚪")
    rec, cashout_pct = _cashout_advice([status], None)

    block = (
        f"  👤 *{player}*{clock_tag}\n"
        f"    {color} {si} {note}\n"
        f"  👉 *{rec}* · 💸 {cashout_pct}"
    )
    return block, rec


def _run_manual_update(chat_id, title, rows):
    """
    Generic dispatcher for all /update* admin commands.
    rows = list of dicts: {id, player, pick_text, line, label}
    Always bypasses spam cache. Sends to chat_id (admin) only.
    """
    D = "━━━━━━━━━━━━━━━━━━━"
    if not rows:
        reply(chat_id, f"📡 *{title}*\n{D}\n\nNo unsettled picks today.")
        return

    live_games = _fetch_bdl_live_games()
    live_in_progress = [g for g in live_games if g["status"] == "in"]

    blocks = [f"📡 *{title}*\n{D}"]
    for row in rows:
        player    = row.get("player", "")
        pick_text = row.get("pick_text", "")
        line      = row.get("line")
        label     = row.get("label", f"#{row.get('id','?')}")
        block, _  = _check_player_live_pick(player, pick_text, line, live_in_progress)
        blocks.append(f"\n🎯 *{label}*\n{block}")

    if not live_in_progress:
        blocks.append(f"\n_No live games right now_")
    blocks.append(D)
    reply(chat_id, "\n".join(blocks))


def cmd_update_feed(chat_id):
    """Admin /updatefeed — live status of all manual feedpicks."""
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, pick_text FROM feed_picks
                WHERE result IS NULL ORDER BY logged_at DESC
            """)
            for pid, pick_text in cur.fetchall():
                legs = _parse_pick_text(pick_text)
                for i, leg in enumerate(legs):
                    player   = leg.get("player", "")
                    if not player:
                        continue
                    stats    = leg.get("stats", [])
                    line     = float(stats[0][0]) if stats else None
                    pick_dir = leg.get("pick", "OVER")
                    stat_nm  = str(stats[0][1]) if stats else "PTS"
                    synth    = f"{player} {pick_dir} {line} {stat_nm}" if line else pick_text
                    label    = f"Feed #{pid}" + (f" Leg {i+1}" if len(legs) > 1 else "")
                    rows.append({"id": pid, "player": player, "pick_text": synth,
                                 "line": line, "label": label})
            cur.close(); conn.close()
        except Exception as e:
            print(f"[updatefeed] DB error: {e}")
    _run_manual_update(chat_id, "📋 FEED PICKS — Live Status", rows)


def cmd_update_ml(chat_id):
    """Admin /updateml — live status of bot ML/Spread/Total picks."""
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, player, pick, line, bet_type FROM bets
                WHERE result IS NULL
                  AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                  AND (
                      pick_category IN ('INDIVIDUAL', 'VIP_LOCK')
                      OR bet_type IN ('ML', 'TOTAL', 'SPREAD', 'VIP_LOCK')
                  )
                ORDER BY id DESC
            """, (today_et,))
            for bid, player, pick_text, line, bet_type in cur.fetchall():
                type_label = {"ML": "Moneyline", "TOTAL": "Total", "SPREAD": "Spread",
                              "VIP_LOCK": "VIP Lock"}.get(bet_type, bet_type or "Pick")
                label_player = player or pick_text or f"Pick #{bid}"
                rows.append({"id": bid, "player": label_player, "pick_text": pick_text,
                             "line": line, "label": f"{type_label} #{bid} — {label_player}"})
            cur.close(); conn.close()
        except Exception as e:
            print(f"[updateml] DB error: {e}")
    _run_manual_update(chat_id, "📊 ML / SPREAD / TOTAL — Live Status", rows)


def cmd_update_props(chat_id):
    """Admin /updateprops — live status of elite prop picks."""
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, player, pick, line FROM bets
                WHERE result IS NULL
                  AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                  AND player IS NOT NULL AND player != ''
                  AND (
                      pick_category = 'ELITE_PROP'
                      OR bet_type IN ('ELITE_PROP', 'PROP', 'PLAYER_PROP')
                      OR (pick ILIKE '%over%' AND pick_category NOT IN ('SGP','EDGE_FADE','VIP_LOCK'))
                      OR (pick ILIKE '%under%' AND pick_category NOT IN ('SGP','EDGE_FADE','VIP_LOCK'))
                  )
                ORDER BY id DESC
            """, (today_et,))
            for bid, player, pick_text, line in cur.fetchall():
                rows.append({"id": bid, "player": player, "pick_text": pick_text,
                             "line": line, "label": f"Elite Prop #{bid} — {player}"})
            cur.close(); conn.close()
        except Exception as e:
            print(f"[updateprops] DB error: {e}")
    _run_manual_update(chat_id, "🎯 ELITE PROPS — Live Status", rows)


def cmd_update_sgp(chat_id):
    """Admin /updatesgp — live status of today's SGP legs."""
    import re as _re
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, player, pick, line FROM bets
                WHERE result IS NULL AND pick_category = 'SGP'
                  AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                ORDER BY id DESC
            """, (today_et,))
            for bid, player_desc, pick_text, line in cur.fetchall():
                # player field for SGP contains the full desc — extract player name
                m = _re.search(r'\b(OVER|UNDER)\b', (player_desc or "").upper())
                player_name = player_desc[:m.start()].strip() if m else (player_desc or "")
                rows.append({"id": bid, "player": player_name,
                             "pick_text": player_desc or pick_text,
                             "line": line, "label": f"SGP — {player_desc}"})
            cur.close(); conn.close()
        except Exception as e:
            print(f"[updatesgp] DB error: {e}")
    _run_manual_update(chat_id, "🎰 SGP LEGS — Live Status", rows)


def _check_game_total_live(game_name, direction, line, live_games):
    """Return a status block for a game total CGP leg using live/final combined score."""
    matched = None
    gname_lower = game_name.lower()
    for g in live_games:
        home = g.get("home_team", "").lower()
        away = g.get("away_team", "").lower()
        if home in gname_lower or away in gname_lower:
            matched = g
            break
    if not matched:
        return f"  ⏳ *{game_name}* — no live data"

    home_score = int(matched.get("home_score", 0) or 0)
    away_score = int(matched.get("away_score", 0) or 0)
    combined   = home_score + away_score
    status     = matched.get("status", "pre")
    period     = matched.get("period", 0) or 0
    clock      = matched.get("clock", "") or matched.get("time", "") or ""
    away_name  = matched.get("away_team", "")
    home_name  = matched.get("home_team", "")

    if status == "post":
        if direction == "OVER":
            result = "win" if combined > line else "loss"
        else:
            result = "win" if combined < line else "loss"
        icon = "✅" if result == "win" else "❌"
        return (f"  {icon} *{away_name} @ {home_name}* ✅ Final\n"
                f"    Combined: {combined} pts  |  {direction} {line} → *{result.upper()}*")

    elif status == "in":
        q_label   = f"Q{period}" if period <= 4 else f"OT{period - 4}"
        clock_tag = f" ⏱ {q_label} {clock}".rstrip()
        remaining = line - combined
        if direction == "OVER":
            color = "🟢" if combined >= line * 0.55 else "🟡"
            note  = f"{combined} scored — need {round(remaining, 1)} more"
        else:
            color = "🔴" if combined > line else "🟢"
            delta = round(combined - line, 1)
            note  = f"{combined} scored — {abs(delta)} {'over' if delta > 0 else 'under'} the line"
        return (f"  {color} *{away_name} @ {home_name}*{clock_tag}\n"
                f"    {note}  |  {direction} {line}")
    else:
        return f"  ⏳ *{game_name}* — not started yet"


def cmd_update_cgp(chat_id):
    """Admin /updatecgp — live status of today's CGP legs (game totals + player props)."""
    import re as _re
    D = "━━━━━━━━━━━━━━━━━━━"
    if not _todays_parlay_legs:
        reply(chat_id, f"📡 *🔗 CROSS-GAME PARLAY — Live Status*\n{D}\n\nNo CGP legs in today's pool yet.")
        return

    live_games       = _fetch_bdl_live_games()
    live_in_progress = [g for g in live_games if g["status"] == "in"]

    blocks = [f"📡 *🔗 CROSS-GAME PARLAY — Live Status*\n{D}"]
    for leg in _todays_parlay_legs:
        desc     = leg.get("desc", "")
        game     = leg.get("game", "")
        bet_type = leg.get("bet_type", "").upper()
        m        = _re.search(r'(OVER|UNDER)\s+([\d.]+)', desc.upper())
        direction = m.group(1) if m else "OVER"
        line      = float(m.group(2)) if m else 0
        label     = f"CGP — {desc} ({game})"

        is_game_level = (
            bet_type in ("TOTAL", "SPREAD")
            or desc.upper().startswith("GAME TOTAL")
            or desc.upper().startswith("SPREAD")
        )
        if is_game_level:
            block = _check_game_total_live(game, direction, line, live_games)
        else:
            block, _ = _check_player_live_pick(desc, desc, line, live_in_progress)

        blocks.append(f"\n🎯 *{label}*\n{block}")

    if not live_in_progress:
        blocks.append("\n_No live games right now_")
    blocks.append(D)
    reply(chat_id, "\n".join(blocks))


def cmd_update_edge(chat_id):
    """Admin /updateedge — live status of EdgeFade7 picks."""
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, player, pick, line FROM bets
                WHERE result IS NULL
                  AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                  AND (pick_category = 'EDGE_FADE' OR bet_type IN ('EDGE_FADE', 'FADE'))
                ORDER BY id DESC
            """, (today_et,))
            for bid, player, pick_text, line in cur.fetchall():
                label_player = player or pick_text or f"Pick #{bid}"
                rows.append({"id": bid, "player": label_player, "pick_text": pick_text,
                             "line": line, "label": f"EdgeFade #{bid} — {label_player}"})
            cur.close(); conn.close()
        except Exception as e:
            print(f"[updateedge] DB error: {e}")
    _run_manual_update(chat_id, "⚡ EDGE-FADE 7 — Live Status", rows)


def cmd_admins(chat_id):
    """Admin /admins — full system internals health panel."""
    import time as _time
    import json as _j

    def _md(s):
        """Escape Telegram Markdown v1 special chars in dynamic values."""
        return str(s).replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    D = "━━━━━━━━━━━━━━━━━━━"
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    lines = [f"🔬 *SYSTEM HEALTH PANEL*\n{D}\n📅 {today_et}"]

    conn = _db_conn()
    db_ok = conn is not None

    # ── 1. ELO RATINGS ────────────────────────────────────────────────────────
    lines.append(f"\n🏀 *ELO RATINGS*")
    try:
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM learning_data WHERE key = 'elo_ratings'")
            row = cur.fetchone()
            if row:
                elo_data = _j.loads(row[0]) if isinstance(row[0], str) else row[0]
                sorted_elos = sorted(elo_data.items(), key=lambda x: float(x[1]), reverse=True)
                top5 = sorted_elos[:5]
                bot5 = list(reversed(sorted_elos[-5:]))
                lines.append("  📈 Top 5:")
                for team, rating in top5:
                    lines.append(f"    {_md(team)}: {float(rating):.0f}")
                lines.append("  📉 Bottom 5:")
                for team, rating in bot5:
                    lines.append(f"    {_md(team)}: {float(rating):.0f}")
            else:
                lines.append("  No ELO data yet")
    except Exception as _e:
        lines.append(f"  ⚠️ {_md(str(_e))}")

    # ── 2. CAUSALITY HIT RATES ────────────────────────────────────────────────
    lines.append(f"\n⚡ *CAUSALITY HIT RATES* (top 5 by sample size)")
    try:
        from decision_engine import _causality_hit_rates
        if _causality_hit_rates:
            sorted_c = sorted(
                _causality_hit_rates.items(),
                key=lambda x: x[1].get("total", 0), reverse=True
            )[:5]
            for key, data in sorted_c:
                total = data.get("total", 0)
                wins  = data.get("wins", 0)
                rate  = round(wins / total * 100, 1) if total > 0.5 else 0.0
                parts = key.split("|")
                event = parts[0] if parts else key
                stat  = parts[1] if len(parts) > 1 else "?"
                lines.append(f"  {_md(event)} [{_md(stat)}]: {rate}% ({total:.0f} samples)")
        else:
            lines.append("  No data yet — needs live game cycles to accumulate")
    except Exception as _e:
        lines.append(f"  ⚠️ {_md(str(_e))}")

    # ── 3. LAYER BLOCK RATES (last 7 days of shadow picks) ───────────────────
    lines.append(f"\n🔬 *LAYER BLOCK RATES* (last 7 days)")
    try:
        if conn:
            cur = conn.cursor()
            # game_date is stored as TEXT ('YYYY-MM-DD'), cast explicitly
            _seven_days_ago = (datetime.now(ET) - timedelta(days=7)).strftime("%Y-%m-%d")
            cur.execute("""
                SELECT COUNT(*) FROM shadow_picks
                WHERE game_date >= %s
            """, (_seven_days_ago,))
            total_sh = (cur.fetchone() or [1])[0] or 1
            cur.execute("""
                SELECT blocked_by, COUNT(*) AS cnt
                FROM shadow_picks
                WHERE blocked_by IS NOT NULL
                  AND game_date >= %s
                GROUP BY blocked_by
                ORDER BY cnt DESC
                LIMIT 11
            """, (_seven_days_ago,))
            block_rows = cur.fetchall()
            if block_rows:
                for blocked_by, cnt in block_rows:
                    pct = round(cnt / total_sh * 100, 1)
                    lines.append(f"  {_md(blocked_by)}: {cnt} blocks ({pct}%)")
                lines.append(f"  Total evaluated: {total_sh}")
            else:
                lines.append("  No block data in last 7 days")
    except Exception as _e:
        lines.append(f"  ⚠️ {_md(str(_e))}")
        try: conn.rollback()
        except Exception: pass

    # ── 4. LEARNING DATA FRESHNESS ────────────────────────────────────────────
    lines.append(f"\n🧠 *LEARNING FRESHNESS*")
    try:
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT value, updated_at FROM learning_data WHERE key = 'last_auto_adjust_date'")
            adj = cur.fetchone()
            if adj:
                raw = adj[0]
                if isinstance(raw, str):
                    try:
                        adj_val = _j.loads(raw)
                    except Exception:
                        adj_val = raw
                else:
                    adj_val = raw
                lines.append(f"  Last auto-adjust: {_md(str(adj_val))} @ {str(adj[1])[:16]}")
            else:
                lines.append("  Last auto-adjust: Never")

            cur.execute("SELECT updated_at FROM learning_data WHERE key = 'model_b64'")
            mdl = cur.fetchone()
            lines.append(f"  Last model retrain: {str(mdl[0])[:16] if mdl else 'Never'}")

            cur.execute("SELECT updated_at FROM learning_data WHERE key = 'elo_ratings'")
            elo_ts = cur.fetchone()
            lines.append(f"  ELO last updated: {str(elo_ts[0])[:16] if elo_ts else 'Never'}")
    except Exception as _e:
        lines.append(f"  ⚠️ {_md(str(_e))}")

    # ── 5. API HEALTH ─────────────────────────────────────────────────────────
    lines.append(f"\n📡 *API HEALTH*")
    # BDL
    try:
        t0 = _time.time()
        _bdl_get(f"{BDL_BASE}/teams?per_page=1")
        lines.append(f"  BDL: ✅ {int((_time.time()-t0)*1000)}ms")
    except Exception:
        lines.append("  BDL: ❌ Unreachable")
    # ESPN CDN
    try:
        t0 = _time.time()
        _espn_get("https://cdn.espn.com/core/nba/scoreboard?xhr=1&limit=1")
        lines.append(f"  ESPN CDN: ✅ {int((_time.time()-t0)*1000)}ms")
    except Exception:
        lines.append("  ESPN CDN: ❌ Unreachable")
    # Odds API
    _odds_key = os.getenv("ODDS_API_KEY", "")
    if _odds_key:
        try:
            t0 = _time.time()
            _or = requests.get(
                "https://api.the-odds-api.com/v4/sports",
                params={"apiKey": _odds_key}, timeout=5
            )
            _rem = _or.headers.get("x-requests-remaining", "?")
            _used = _or.headers.get("x-requests-used", "?")
            lines.append(
                f"  Odds API: ✅ {int((_time.time()-t0)*1000)}ms "
                f"— {_rem} calls left ({_used} used)"
            )
        except Exception:
            lines.append("  Odds API: ❌ Unreachable")
    else:
        lines.append("  Odds API: ⚠️ Key not set")
    # DB
    lines.append(f"  Database: {'✅ Connected' if db_ok else '❌ Unreachable'}")

    lines.append(f"\n{D}")
    if conn:
        try: conn.close()
        except Exception: pass

    # ── Split into ≤4000-char chunks (Telegram hard cap is 4096) ──────────────
    full_msg = "\n".join(lines)
    chunk_limit = 4000
    while full_msg:
        if len(full_msg) <= chunk_limit:
            reply(chat_id, full_msg)
            break
        cut = full_msg.rfind("\n", 0, chunk_limit)
        if cut == -1:
            cut = chunk_limit
        reply(chat_id, full_msg[:cut])
        full_msg = full_msg[cut:].lstrip("\n")


def cmd_dbstatus(chat_id):
    """Admin /dbstatus — live DB snapshot, two messages."""
    import json as _dbjson
    send("🗄 Pulling DB snapshot...", str(chat_id))
    conn = None
    try:
        conn = _db_conn()
        if not conn:
            send("❌ DB connection failed.", str(chat_id))
            return
        cur = conn.cursor()

        # ── Message 1: table counts + bets breakdown ─────────────────────
        out1 = ["🗄 *DB STATUS — Tables*", ""]
        for tbl in ("bets", "shadow_picks", "game_observations",
                    "player_observations", "learning_data",
                    "bot_status", "causality_log", "feed_picks"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                n = cur.fetchone()[0]
            except Exception:
                n = "—"
            out1.append(f"  `{tbl}`: {n}")

        out1 += ["", "*🎯 Bets by Category*"]
        try:
            cur.execute("""
                SELECT pick_category,
                       COUNT(*) AS total,
                       SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins,
                       COUNT(result) AS settled
                FROM bets GROUP BY pick_category ORDER BY total DESC
            """)
            brows = cur.fetchall()
            if brows:
                for cat, total, wins, settled in brows:
                    out1.append(f"  `{cat or 'other'}`: {total} total  {wins}/{settled} W/settled")
            else:
                out1.append("  No bets in DB yet")
        except Exception as ex:
            out1.append(f"  error: {ex}")

        # ── Message 2: learning data + thresholds + status ───────────────
        out2 = ["🧠 *Learning Data*"]
        try:
            cur.execute("SELECT key, updated_at FROM learning_data ORDER BY updated_at DESC")
            ld = cur.fetchall()
            if ld:
                for k, ts in ld:
                    out2.append(f"  `{k}` · {str(ts)[:16]}")
            else:
                out2.append("  Empty — no training data yet")
        except Exception as ex:
            out2.append(f"  error: {ex}")

        out2 += ["", "*⚙️ Thresholds*"]
        try:
            cur.execute("SELECT value FROM learning_data WHERE key='script_thresholds'")
            trow = cur.fetchone()
            if trow:
                thr = _dbjson.loads(trow[0]) if isinstance(trow[0], str) else trow[0]
                for k, v in thr.items():
                    out2.append(f"  `{k}`: {v}")
            else:
                out2.append("  All defaults — nothing learned yet")
                out2.append("  Minutes gate : 30 min/game")
                out2.append("  Usage gate   : 10 possessions")
                out2.append("  Prop edge    : 3.0 pts")
                out2.append("  ML edge      : 6%")
                out2.append("  Spread edge  : 3.0 pts")
                out2.append("  Total edge   : 5.0 pts")
        except Exception as ex:
            out2.append(f"  error: {ex}")

        out2 += ["", "*🔧 Bot Status*"]
        try:
            cur.execute("SELECT key, value FROM bot_status ORDER BY updated_at DESC")
            for k, v in cur.fetchall():
                short = str(v)[:45] + ("…" if len(str(v)) > 45 else "")
                out2.append(f"  `{k}`: {short}")
        except Exception as ex:
            out2.append(f"  error: {ex}")

        cur.close()
        conn.close()
        send("\n".join(out1), str(chat_id))
        send("\n".join(out2), str(chat_id))

    except Exception as e:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        send(f"❌ DB error: {e}", str(chat_id))


def cmd_today_picks(chat_id):
    """Admin /todaypicks — all picks sent today across every category."""
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    conn = _db_conn()
    rows = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT game, player, pick, bet_type, pick_category, line, odds, result, confidence
                FROM bets
                WHERE DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                ORDER BY COALESCE(bet_time, created_at) ASC
            """, (today_et,))
            rows = cur.fetchall()
            cur.close(); conn.close()
        except Exception as e:
            print(f"[todaypicks] DB error: {e}")
            try: conn.close()
            except Exception: pass
    if not rows:
        reply(chat_id, f"📭 No picks recorded for {today_et} yet.")
        return
    D = "━━━━━━━━━━━━━━━━━━━"
    lines = [f"📋 *TODAY'S FULL CARD* — {today_et}\n{D}"]
    cat_icons = {
        "ELITE_PROP": "🎯", "VIP_LOCK": "🔒", "SGP": "🎰",
        "EDGE_FADE": "⚡", "INDIVIDUAL": "📊", "CROSS_GAME_PARLAY": "🔗",
    }
    for game, player, pick, btype, cat, line, odds, result, conf in rows:
        icon = "✅" if result == "win" else "❌" if result == "loss" else "⏳"
        cat_icon = cat_icons.get(cat or "", "📋")
        odds_tag = f" @ {odds}" if odds else ""
        line_tag = f" {line}" if line else ""
        conf_tag = f" [{conf}%]" if conf else ""
        player_tag = f" — *{player}*" if player else ""
        lines.append(
            f"{icon} {cat_icon} *{pick}*{line_tag}{odds_tag}{conf_tag}\n"
            f"   {btype or 'PICK'}{player_tag} | _{game}_"
        )
    lines.append(D)
    reply(chat_id, "\n\n".join(lines))


def cmd_edit_feedpick(chat_id, raw):
    """
    /editfeedpick <id> <new pick text>
    Updates the pick_text of a feed pick in the DB.
    """
    parts = raw.strip().split(None, 1)
    if len(parts) < 2:
        reply(chat_id,
              "❌ *Usage:* `/editfeedpick <id> <new pick text>`\n"
              "Example: `/editfeedpick 4 Jamal Murray OVER 25.5 PTS -110`")
        return
    try:
        pick_id = int(parts[0])
    except ValueError:
        reply(chat_id, "❌ Pick ID must be a number.")
        return
    new_text = parts[1].strip()
    conn = _db_conn()
    if not conn:
        reply(chat_id, "⚠️ DB unavailable.")
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, pick_text FROM feed_picks WHERE id = %s", (pick_id,))
        row = cur.fetchone()
        if not row:
            reply(chat_id, f"❌ Feed pick #{pick_id} not found.")
            cur.close(); conn.close()
            return
        cur.execute("UPDATE feed_picks SET pick_text = %s WHERE id = %s", (new_text, pick_id))
        conn.commit()
        cur.close(); conn.close()
        reply(chat_id,
              f"✅ *Feed Pick #{pick_id} Updated*\n\n"
              f"*Old:* {row[1]}\n"
              f"*New:* {new_text}")
    except Exception as e:
        try: conn.close()
        except Exception: pass
        reply(chat_id, f"⚠️ Could not update pick: {e}")


def cmd_delete_feedpick(chat_id, raw):
    """
    /deletefeedpick <id>
    Deletes a feed pick after showing a confirm prompt.
    """
    parts = raw.strip().split()
    if not parts:
        reply(chat_id, "❌ *Usage:* `/deletefeedpick <id>`")
        return
    try:
        pick_id = int(parts[0])
    except ValueError:
        reply(chat_id, "❌ Pick ID must be a number.")
        return
    conn = _db_conn()
    if not conn:
        reply(chat_id, "⚠️ DB unavailable.")
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, pick_text, result FROM feed_picks WHERE id = %s", (pick_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            reply(chat_id, f"❌ Feed pick #{pick_id} not found.")
            return
        result_tag = f" (settled: {row[2]})" if row[2] else " (unsettled)"
        send_with_buttons(chat_id,
            f"🗑 *Delete Feed Pick #{pick_id}?*\n\n"
            f"*Pick:* {row[1]}{result_tag}\n\n"
            f"This cannot be undone.",
            [[
                {"text": "🗑 Delete", "callback_data": f"delfeed_{pick_id}"},
                {"text": "❌ Cancel", "callback_data": "delfeed_cancel"},
            ]]
        )
    except Exception as e:
        try: conn.close()
        except Exception: pass
        reply(chat_id, f"⚠️ Could not fetch pick: {e}")


def _live_pick_tracker():
    """
    Mid-game live tracker. Runs every 10 minutes during game hours (7 PM – 1 AM ET).
    Tracks ALL unsettled picks: manual feedpicks AND bot-generated bets table picks.
    Evaluates live pace per leg and alerts admin + VIP channel when status changes.
    Spam prevention via _live_tracker_cache (only fires on status change).
    """
    global _live_tracker_cache
    print("[LiveTracker] cycle start")

    # ── Fetch live games once ──────────────────────────────────────────────
    live_games = _fetch_bdl_live_games()
    live_in_progress = [g for g in live_games if g["status"] == "in"]
    if not live_in_progress:
        print("[LiveTracker] no live games right now")
        return

    conn = _db_conn()
    if not conn:
        return
    cur = conn.cursor()

    # ── Section 1: feedpick picks ──────────────────────────────────────────
    try:
        cur.execute("""
            SELECT id, pick_text, picked_at_et
            FROM feed_picks
            WHERE result IS NULL
              AND logged_at >= NOW() - INTERVAL '48 hours'
            ORDER BY logged_at DESC
        """)
        feed_rows = cur.fetchall()
    except Exception as e:
        print(f"[LiveTracker] feed_picks DB error: {e}")
        feed_rows = []

    for pid, pick_text, picked_at in feed_rows:
        try:
            legs = _parse_pick_text(pick_text)
            if not legs:
                continue

            parlay_odds = None
            last_leg = legs[-1] if legs else {}
            if last_leg.get("parlay_odds"):
                parlay_odds = last_leg["parlay_odds"]

            leg_lines    = []
            leg_statuses = []
            game_clock_shown = False

            for leg in legs:
                player   = leg.get("player", "")
                pick_dir = leg.get("pick", "OVER").upper()
                stats_list = leg.get("stats", [])
                if not player or not stats_list:
                    continue

                game = _match_live_game(player, live_in_progress)
                box  = _fetch_player_live_stats(player, game) if game else None
                if not box:
                    leg_statuses.append("WAIT")
                    leg_lines.append(f"  ⏳ *{player}* — no live data yet")
                    continue

                period   = game["period"] if game else 0
                time_str = game["time"]   if game else ""

                clock_tag = ""
                if game and not game_clock_shown:
                    q_label   = f"Q{game['period']}" if game["period"] <= 4 else f"OT{game['period']-4}"
                    clock_tag = f" ⏱ {q_label} {game['time']} remaining"
                    game_clock_shown = True

                leg_status_list = []
                stat_lines = []
                for stat_entry in stats_list:
                    line  = float(stat_entry[0])
                    stat  = str(stat_entry[1]).upper()
                    bdl_f = _STAT_LABEL_TO_BDL.get(stat, "pts")
                    current_val = float(box.get(bdl_f, 0) or 0)
                    si = _STAT_ICON.get(stat, "📊")
                    status, pct, note = _pace_status(current_val, line, period, time_str, pick_dir)
                    leg_status_list.append(status)
                    color = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴",
                             "DONE": "✅", "WAIT": "⏳"}.get(status, "⚪")
                    stat_lines.append(f"    {color} {si} {note}")

                priority = ["RED", "YELLOW", "WAIT", "GREEN", "DONE"]
                leg_overall = next((s for s in priority if s in leg_status_list), "WAIT")
                leg_statuses.append(leg_overall)
                leg_lines.append(f"  👤 *{player}*{clock_tag}\n" + "\n".join(stat_lines))

            if not leg_statuses:
                continue

            rec, cashout_pct = _cashout_advice(leg_statuses, parlay_odds)
            n_legs = len(leg_statuses)
            alert_lines = [
                f"📡 *Live Feedpick #{pid}*{' — ' + picked_at if picked_at else ''}\n"
                f"{'🎯 ' + str(n_legs) + '-Leg Parlay' if n_legs > 1 else '📋 Single Bet'}\n",
            ]
            alert_lines.extend(leg_lines)
            alert_lines.append(f"\n👉 *{rec}*")
            alert_lines.append(f"💸 Estimated cashout: {cashout_pct}")
            alert_text = "\n".join(alert_lines)

            cache_key = f"feed_{pid}"
            if _live_tracker_cache.get(cache_key) != rec:
                _live_tracker_cache[cache_key] = rec
                reply(ADMIN_ID, alert_text)
                print(f"[LiveTracker] Feedpick #{pid} — {rec}")

        except Exception as e:
            print(f"[LiveTracker] Error on feedpick #{pid}: {e}")

    # ── Section 2: bot-generated bets table picks ──────────────────────────
    today_et = datetime.now(ET).strftime("%Y-%m-%d")
    try:
        cur.execute("""
            SELECT id, player, pick, line, bet_type, pick_category,
                   prob, edge, confidence, odds
            FROM bets
            WHERE result IS NULL
              AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
              AND player IS NOT NULL AND player != ''
        """, (today_et,))
        bet_rows = cur.fetchall()
    except Exception as e:
        print(f"[LiveTracker] bets DB error: {e}")
        bet_rows = []

    cur.close()
    conn.close()

    import re as _re_lt
    for bid, player_raw, pick_text, line, bet_type, pick_category, _b_prob, _b_edge, _b_conf, _b_odds in (bet_rows or []):
        try:
            # SGP stores full desc in player field — extract real player name
            if pick_category == "SGP":
                _m = _re_lt.search(r'\b(OVER|UNDER)\b', (player_raw or "").upper())
                player = player_raw[:_m.start()].strip() if _m else player_raw
            else:
                player = player_raw

            leg = _parse_bet_row_to_leg(player, pick_text or player_raw, line)
            if not leg or not leg.get("stats"):
                continue

            game = _match_live_game(player, live_in_progress)
            box  = _fetch_player_live_stats(player, game) if game else None

            if not box:
                cache_key = f"bet_{bid}"
                if _live_tracker_cache.get(cache_key) != "WAIT":
                    _live_tracker_cache[cache_key] = "WAIT"
                    print(f"[LiveTracker] Bet #{bid} ({player}) — no live data")
                continue

            period   = game["period"] if game else 0
            time_str = game["time"]   if game else ""
            q_label  = f"Q{period}" if period <= 4 else f"OT{period - 4}"
            clock_tag = f" ⏱ {q_label} {time_str} remaining"

            pick_dir   = leg.get("pick", "OVER").upper()
            stat_entry = leg["stats"][0]
            stat       = stat_entry.get("stat", "PTS").upper()
            pick_line  = float(stat_entry.get("line", 0))
            bdl_f      = _STAT_LABEL_TO_BDL.get(stat, "pts")
            current_val = float(box.get(bdl_f, 0) or 0)
            si = _STAT_ICON.get(stat, "📊")

            status, pct, note = _pace_status(current_val, pick_line, period, time_str, pick_dir)
            color = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴",
                     "DONE": "✅", "WAIT": "⏳"}.get(status, "⚪")

            cat_label = {
                "VIP_LOCK":   "🔒 VIP Lock",
                "EDGE_FADE":  "⚡ Edge-Fade 7",
                "INDIVIDUAL": "📋 Pick",
                "SGP":        "🎰 SGP Leg",
                "ELITE_PROP": "🎯 Elite Prop",
            }.get(pick_category or bet_type, f"📋 {bet_type or 'Pick'}")

            rec, cashout_pct = _cashout_advice([status], None)

            # Build model-stats line from the stored DB values
            _model_parts = []
            if _b_conf is not None:
                _model_parts.append(f"Conf {round(float(_b_conf), 1)}%")
            if _b_prob is not None:
                _model_parts.append(f"Prob {round(float(_b_prob)*100, 1)}%")
            if _b_edge is not None:
                _e_pct = round(float(_b_edge) * 100, 1)
                _model_parts.append(f"Edge {'+' if _e_pct >= 0 else ''}{_e_pct}%")
            if _b_odds is not None and _b_odds != 0:
                _model_parts.append(f"Odds {int(_b_odds):+d}")
            _model_line = " · ".join(_model_parts) if _model_parts else ""

            alert_text = (
                f"📡 *Live {cat_label}* — Bet #{bid}\n"
                f"👤 *{player}*{clock_tag}\n"
                f"  {color} {si} {note}\n"
                + (f"  📊 _{_model_line}_\n" if _model_line else "")
                + f"\n👉 *{rec}*\n"
                f"💸 Estimated cashout: {cashout_pct}"
            )

            cache_key = f"bet_{bid}"
            if _live_tracker_cache.get(cache_key) != rec:
                _live_tracker_cache[cache_key] = rec
                # SGP bust: fire immediately with urgent header
                if pick_category == "SGP" and status == "RED":
                    bust_key = f"sgp_bust_{bid}"
                    if _live_tracker_cache.get(bust_key) != "BUSTED":
                        _live_tracker_cache[bust_key] = "BUSTED"
                        reply(ADMIN_ID, f"🚨 *SGP LEG BUSTING* — Act now\n\n{alert_text}")
                        print(f"[LiveTracker] SGP BUST alert — Bet #{bid} ({player})")
                else:
                    # Admin only — VIP gets ✅/❌ on settlement, not mid-game pace alerts
                    reply(ADMIN_ID, alert_text)
                print(f"[LiveTracker] Bet #{bid} ({player}) — {rec}")

        except Exception as e:
            print(f"[LiveTracker] Error on bet #{bid}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# FULL GAME OBSERVER + AUTO-ADJUSTMENT
# ══════════════════════════════════════════════════════════════════════════════

def _get_predicted_script(home_team, away_team):
    """Look up pre-game pace prediction from TEAM_STYLES numeric pace values."""
    try:
        from bot.game_script import TEAM_STYLES, classify_pace
        h_pace = TEAM_STYLES.get(home_team, {}).get("pace", 100)
        a_pace = TEAM_STYLES.get(away_team, {}).get("pace", 100)
        avg_pace_total = (float(h_pace) + float(a_pace)) * 2.2  # rough total proxy
        return classify_pace(avg_pace_total)
    except Exception:
        return "AVERAGE_PACE"


def _classify_actual_script(projected_total):
    """Classify actual game script from projected final total."""
    try:
        from bot.game_script import classify_pace, classify_scoring
        return f"{classify_pace(projected_total)}_{classify_scoring(projected_total)}"
    except Exception:
        return "AVERAGE_PACE_NORMAL_SCORING"


def _parse_bdl_min(min_str):
    """Convert BDL/ESPN min string ('32:14' or '32') to float minutes."""
    try:
        parts = str(min_str or "0").split(":")
        return float(parts[0]) + (float(parts[1]) / 60 if len(parts) > 1 else 0)
    except Exception:
        return 0.0


def _espn_summary_player_stats(event_id):
    """
    Fetch player box scores from ESPN game summary (works live AND final).
    Returns a list of normalized dicts:
      {pname, team_name, mins, pts, ast, reb, fg3m, stl, blk, fgm, fga, ftm, fta}
    """
    try:
        summary = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}",
            timeout=10
        ).json()
        boxscore = summary.get("boxscore", {})
        out = []
        for team_data in boxscore.get("players", []):
            team_name = team_data.get("team", {}).get("displayName", "")
            for section in team_data.get("statistics", []):
                keys = section.get("keys", [])

                def _idx(k):
                    try:
                        return keys.index(k)
                    except ValueError:
                        return -1

                def _find_key(*fragments):
                    for k in keys:
                        if all(f in k for f in fragments):
                            return k
                    return ""

                fg_key  = _find_key("fieldGoal",       "Made-")
                ft_key  = _find_key("freeThrow",       "Made-")
                fg3_key = _find_key("threePointField",  "Made-")

                for athlete in section.get("athletes", []):
                    ath    = athlete.get("athlete", {})
                    pname  = ath.get("displayName", "")
                    if not pname:
                        continue
                    vals = athlete.get("stats", [])

                    def _sv(k, default="0"):
                        i = _idx(k)
                        return str(vals[i]) if 0 <= i < len(vals) else default

                    def _int_sv(k):
                        try:
                            return int(float(_sv(k, "0").split("-")[0] or "0"))
                        except Exception:
                            return 0

                    def _split(k):
                        raw = _sv(k, "0-0")
                        try:
                            p = str(raw).split("-")
                            return int(p[0]), int(p[1])
                        except Exception:
                            return 0, 0

                    fgm, fga   = _split(fg_key)  if fg_key  else (0, 0)
                    ftm, fta   = _split(ft_key)  if ft_key  else (0, 0)
                    fg3m, _    = _split(fg3_key) if fg3_key else (0, 0)
                    mins       = _parse_bdl_min(_sv("minutes", "0:00"))

                    out.append({
                        "pname":     pname,
                        "team_name": team_name,
                        "mins":      round(mins, 1),
                        "pts":       _int_sv("points"),
                        "ast":       _int_sv("assists"),
                        "reb":       _int_sv("rebounds"),
                        "fg3m":      fg3m,
                        "stl":       _int_sv("steals"),
                        "blk":       _int_sv("blocks"),
                        "fgm":       fgm,  "fga": fga,
                        "ftm":       ftm,  "fta": fta,
                    })
        return out
    except Exception as e:
        print(f"[ESPNStats] summary error event {event_id}: {e}")
        return []


def _watch_all_live_games():
    """
    Observe ALL scheduled NBA games live — with or without picks logged.
    Every cycle pulls ESPN game state + all player box scores.
    Stores to game_observations + player_observations tables.
    Fires on schedule during game hours (7 PM – 1 AM ET).
    """
    today = datetime.now(ET).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            timeout=10
        ).json()
        raw_events = resp.get("events", [])
    except Exception as e:
        print(f"[Observer] ESPN scoreboard fetch error: {e}")
        return

    # Normalise ESPN events into the same shape the rest of the loop expects
    games_data = []
    for ev in raw_events:
        comp   = ev.get("competitions", [{}])[0]
        teams  = comp.get("competitors", [])
        home_c = next((t for t in teams if t.get("homeAway") == "home"), {})
        away_c = next((t for t in teams if t.get("homeAway") == "away"), {})
        st     = ev.get("status", {})
        state  = st.get("type", {}).get("state", "")   # "pre" / "in" / "post"
        games_data.append({
            "id":        ev.get("id"),
            "status":    state,
            "period":    st.get("period", 0) or 0,
            "time":      st.get("displayClock", ""),
            "home_team": home_c.get("team", {}).get("displayName", ""),
            "away_team": away_c.get("team", {}).get("displayName", ""),
            "home_pts":  int(home_c.get("score", 0) or 0),
            "away_pts":  int(away_c.get("score", 0) or 0),
        })

    if not games_data:
        return

    conn = _db_conn()
    if not conn:
        return

    for g in games_data:
        try:
            game_id   = g["id"]
            status    = g["status"]          # "pre" / "in" / "post"
            period    = g["period"]
            time_str  = g["time"]
            home_team = g["home_team"]
            away_team = g["away_team"]
            home_pts  = g["home_pts"]
            away_pts  = g["away_pts"]

            if not home_team or not away_team:
                continue

            # ESPN state: "in" = live, "post" = final, "pre" = not started
            is_live  = status == "in"
            is_final = status == "post"
            if not is_live and not is_final:
                continue

            # ── ContextTracker: live script re-evaluation + causality ─────────
            if is_live:
                try:
                    from decision_engine import get_context_tracker
                    _pre_script = _get_predicted_script(home_team, away_team)
                    _ct = get_context_tracker(game_id, home_team, away_team)
                    _ct_evts = _ct.update(
                        period, time_str, home_pts, away_pts,
                        player_stats=None,
                        injuries=None,
                    )
                    for _ct_evt in (_ct_evts or []):
                        print(f"  [ContextTracker] {_ct_evt['type'].upper()} — "
                              f"{_ct_evt['description']}")
                        if _ct_evt.get("type") == "script_change":
                            print(f"  [ContextTracker] ⚠️ SCRIPT CHANGE — "
                                  f"Causes: {' | '.join(_ct_evt.get('causes', []))}")
                    # Persist all causality events to DB so nothing is lost
                    # on restart or error — used for post-game self-learning
                    _save_causality_events_to_db(
                        game_id, today, _ct.get_causality_log()
                    )
                except Exception as _ct_err:
                    print(f"[ContextTracker] error game {game_id}: {_ct_err}")

            # ── Projected total ──────────────────────────────────────────────
            actual_total   = home_pts + away_pts
            elapsed        = _minutes_elapsed(period, time_str) if is_live else 48.0
            projected_total = (actual_total / elapsed * 48) if elapsed > 0 else 0.0

            predicted_script = _get_predicted_script(home_team, away_team)
            actual_script    = _classify_actual_script(
                actual_total if is_final else projected_total
            )
            script_match = (predicted_script == actual_script)

            # ── Upsert game_observations ─────────────────────────────────────
            cur = conn.cursor()

            # Read existing row to get shadows_generated flag
            cur.execute("""
                SELECT shadows_generated FROM game_observations
                WHERE game_id = %s AND game_date = %s
            """, (game_id, today))
            existing_obs = cur.fetchone()
            already_shadowed = (existing_obs[0] if existing_obs else False) or False

            cur.execute("""
                INSERT INTO game_observations
                    (game_id, game_date, home_team, away_team, predicted_script,
                     actual_script, script_match, home_pts, away_pts,
                     actual_total, projected_total, period, status, observed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (game_id, game_date) DO UPDATE SET
                    actual_script   = EXCLUDED.actual_script,
                    script_match    = EXCLUDED.script_match,
                    home_pts        = EXCLUDED.home_pts,
                    away_pts        = EXCLUDED.away_pts,
                    actual_total    = EXCLUDED.actual_total,
                    projected_total = EXCLUDED.projected_total,
                    period          = EXCLUDED.period,
                    status          = EXCLUDED.status,
                    observed_at     = NOW()
            """, (game_id, today, home_team, away_team, predicted_script,
                  actual_script, script_match, home_pts, away_pts,
                  actual_total, round(projected_total, 1), period, status))
            conn.commit()
            cur.close()

            # ── Generate shadow picks on tip-off (first live cycle) ──────────
            if is_live and not already_shadowed:
                try:
                    _generate_shadow_picks(game_id, home_team, away_team, today)
                except Exception as _sg_err:
                    print(f"[Shadow] generate error game {game_id}: {_sg_err}")

            # ── Grade shadow picks when game goes Final ───────────────────────
            if is_final:
                try:
                    _grade_shadow_picks_for_game(game_id, today)
                except Exception as _gg_err:
                    print(f"[Shadow] grade error game {game_id}: {_gg_err}")
                # Shadow CGP — build once per day on first Final (cross-game pool ready)
                try:
                    _generate_shadow_cgp(today)
                except Exception as _scgp_err:
                    print(f"[ShadowCGP] trigger error: {_scgp_err}")

            # ── Player box scores for this game (ESPN summary) ───────────────
            pstats = _espn_summary_player_stats(game_id)

            # Get current injuries for context
            try:
                injuries_now = get_espn_injuries()
            except Exception:
                injuries_now = {}

            for ps in pstats:
                try:
                    pname     = ps["pname"]
                    team_name = ps["team_name"]
                    if not pname or not team_name:
                        continue

                    mins  = ps["mins"]
                    pts   = ps["pts"]
                    ast   = ps["ast"]
                    reb   = ps["reb"]
                    fg3m  = ps["fg3m"]
                    stl   = ps["stl"]
                    blk   = ps["blk"]
                    fgm   = ps["fgm"]
                    fga   = ps["fga"]
                    ftm   = ps["ftm"]
                    fta   = ps["fta"]

                    fg_pct = round(fgm / fga, 3) if fga > 0 else 0.0
                    ft_pct = round(ftm / fta, 3) if fta > 0 else 0.0

                    opponent = away_team if team_name == home_team else home_team

                    # ── Pull season avgs from learning_data cache ────────────
                    avg_pts = avg_ast = avg_reb = avg_fg3 = avg_mins_s = 0.0
                    try:
                        ld_conn = _db_conn()
                        if ld_conn:
                            lc = ld_conn.cursor()
                            lc.execute(
                                "SELECT value FROM learning_data WHERE key=%s",
                                (f"player_baseline:{pname}",)
                            )
                            row = lc.fetchone()
                            lc.close()
                            ld_conn.close()
                            if row:
                                import json as _json
                                bl = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
                                avg_pts    = float(bl.get("avg_pts", 0))
                                avg_ast    = float(bl.get("avg_ast", 0))
                                avg_reb    = float(bl.get("avg_reb", 0))
                                avg_fg3    = float(bl.get("avg_fg3", 0))
                                avg_mins_s = float(bl.get("avg_mins", 0))
                    except Exception:
                        pass

                    # ── Flags ────────────────────────────────────────────────
                    is_starter    = mins >= 28
                    is_benefactor = False
                    is_fade       = False
                    inj_context   = ""

                    if avg_mins_s > 0:
                        if mins >= avg_mins_s * 1.3 and (
                            pts >= avg_pts * 1.2 or ast >= avg_ast * 1.2
                        ):
                            is_benefactor = True
                        if pts < avg_pts * 0.55 and mins >= 15:
                            is_fade = True

                    # Skip players under 15 min unless flagged
                    if mins < 15 and not is_benefactor and not is_fade:
                        continue

                    # Injury context — was a key teammate out?
                    try:
                        for inj_name, inj_info in injuries_now.items():
                            if (inj_info.get("team") == team_name
                                    and inj_info.get("status") in ("Out", "Doubtful")
                                    and inj_name.lower() != pname.lower()):
                                inj_context = f"{inj_name} out"
                                if is_benefactor:
                                    break
                    except Exception:
                        pass

                    # ── Upsert player_observations ───────────────────────────
                    cur2 = conn.cursor()
                    cur2.execute("""
                        INSERT INTO player_observations
                            (game_id, game_date, player_name, team, opponent,
                             minutes, pts, ast, reb, fg3m, stl, blk,
                             fg_pct, ft_pct, plus_minus,
                             season_avg_pts, season_avg_ast,
                             season_avg_reb, season_avg_fg3,
                             is_starter, is_benefactor, is_fade,
                             injury_context, observed_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (game_id, player_name, game_date) DO UPDATE SET
                            minutes      = EXCLUDED.minutes,
                            pts          = EXCLUDED.pts,
                            ast          = EXCLUDED.ast,
                            reb          = EXCLUDED.reb,
                            fg3m         = EXCLUDED.fg3m,
                            stl          = EXCLUDED.stl,
                            blk          = EXCLUDED.blk,
                            fg_pct       = EXCLUDED.fg_pct,
                            ft_pct       = EXCLUDED.ft_pct,
                            is_starter   = EXCLUDED.is_starter,
                            is_benefactor= EXCLUDED.is_benefactor,
                            is_fade      = EXCLUDED.is_fade,
                            injury_context = EXCLUDED.injury_context,
                            observed_at  = NOW()
                    """, (game_id, today, pname, team_name, opponent,
                          round(mins, 1), pts, ast, reb, fg3m, stl, blk,
                          fg_pct, ft_pct, 0,
                          avg_pts, avg_ast, avg_reb, avg_fg3,
                          is_starter, is_benefactor, is_fade, inj_context))
                    conn.commit()
                    cur2.close()

                except Exception as pe:
                    print(f"[Observer] player row error: {pe}")

        except Exception as ge:
            print(f"[Observer] game error {g.get('id')}: {ge}")

    conn.close()
    print(f"[Observer] cycle complete — {len(games_data)} games checked")

    # Purge ContextTrackers for games no longer in the live feed
    try:
        from decision_engine import purge_context_trackers
        active_ids = [g.get("id") for g in games_data if g.get("id")]
        purge_context_trackers(active_ids)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# NBA CDN LIVE PLAY-BY-PLAY TRACKING — shot type detection + hot/cold alerts
# ══════════════════════════════════════════════════════════════════════════════

_SHOT_COOLDOWN_SEC  = 60
_shot_alerts_sent   = {}   # {player_name: last_alert_epoch}
_shot_history       = {}   # {player_name: list of {"type","made","t"} dicts}
_pbp_last_action    = {}   # {nba_game_id: last action_number seen}


def _cdn_scoreboard():
    """Fetch today's games from NBA CDN (free, no key required)."""
    try:
        r = requests.get(
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
            timeout=8, headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code == 200:
            return r.json().get("scoreboard", {}).get("games", [])
    except Exception as e:
        print(f"[CDN] scoreboard error: {e}")
    return []


def _cdn_pbp(game_id):
    """Fetch play-by-play actions for one NBA CDN game id."""
    try:
        r = requests.get(
            f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json",
            timeout=8, headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code == 200:
            return r.json().get("game", {}).get("actions", [])
    except Exception as e:
        print(f"[CDN] pbp error {game_id}: {e}")
    return []


def _classify_shot_action(action):
    """
    Returns (shot_type, made) or (None, None) for non-shot events.
    shot_type: '3PT' | 'LAYUP' | 'MID'
    """
    atype = (action.get("actionType") or "").lower()
    if atype not in ("2pt", "3pt"):
        return None, None
    made   = (action.get("shotResult") or "").lower() == "made"
    sub    = (action.get("subType")    or "").lower()
    desc   = (action.get("description") or "").lower()
    if atype == "3pt":
        return "3PT", made
    if any(k in sub or k in desc for k in ("layup", "dunk", "alley", "hook", "tip")):
        return "LAYUP", made
    return "MID", made


def _process_cdn_shot(player_name, shot_type, made, game_label):
    """
    Update shot history and return a hot/cold alert string if one should fire.
    Returns None if no alert or still within cooldown.
    Caller is responsible for batching and sending — no direct send here.
    """
    from bot.shot_state import update_shot_history as _ush, _shot_history as _ssh, _normalize_name as _nn_ss
    now  = time.time()
    _ush(player_name, shot_type, made)
    _cdn_key = _nn_ss(player_name)
    hist = _ssh.get(_cdn_key, [])

    if now - _shot_alerts_sent.get(_cdn_key, 0) < _SHOT_COOLDOWN_SEC:
        return None

    last6  = hist[-6:] if len(hist) >= 6 else hist
    last5  = hist[-5:] if len(hist) >= 5 else hist

    made_3pt_last5 = sum(1 for s in last5 if s["type"] == "3PT" and s["made"])
    miss_consec    = all(not s["made"] for s in last5) if len(last5) == 5 else False
    made_consec3   = all(s["made"] for s in last6[-3:]) if len(last6) >= 3 else False

    alert = None
    if made_3pt_last5 >= 3:
        alert = f"🔥 *HOT — {player_name}* · {made_3pt_last5}/5 from 3PT range"
    elif made_consec3:
        alert = f"🔥 *HOT — {player_name}* · 3 consecutive made shots"
    elif miss_consec:
        alert = f"🥶 *COLD — {player_name}* · 5 consecutive misses"

    if alert:
        _shot_alerts_sent[_cdn_key] = now
        print(f"[CDN] Shot alert queued for {player_name}")
    return alert


def _cdn_live_tracker():
    """
    Poll NBA CDN play-by-play for all live games.
    Batches all hot/cold alerts per game into ONE message instead of one per player.
    """
    games = _cdn_scoreboard()
    live  = [g for g in games if g.get("gameStatus") == 2]   # 2 = in-progress
    if not live:
        return

    for g in live:
        game_id    = g.get("gameId", "")
        home       = g.get("homeTeam", {}).get("teamName", "")
        away       = g.get("awayTeam", {}).get("teamName", "")
        game_label = f"{away} @ {home}"
        if not game_id:
            continue

        actions   = _cdn_pbp(game_id)
        last_seen = _pbp_last_action.get(game_id, 0)
        new_acts  = [a for a in actions if (a.get("actionNumber") or 0) > last_seen]
        if not new_acts:
            continue
        _pbp_last_action[game_id] = max(a.get("actionNumber", 0) for a in new_acts)

        # Collect all alerts for this game — send as one batched message
        game_alerts = []
        for action in new_acts:
            player = action.get("playerNameI", "").strip()
            if not player:
                continue
            shot_type, made = _classify_shot_action(action)
            if shot_type is None:
                continue
            alert = _process_cdn_shot(player, shot_type, made, game_label)
            if alert:
                game_alerts.append(alert)

        if game_alerts:
            batch = (
                f"🏀 *SHOT TRACKER — {game_label}*\n\n"
                + "\n".join(game_alerts)
            )
            send(batch, str(ADMIN_ID))
            print(f"[CDN] Sent {len(game_alerts)} alerts for {game_label}")


def _load_conf_multipliers():
    """
    Load category confidence multipliers from learning_data.
    Returns dict covering both pick products and prop ingredient categories.
    """
    import json as _json
    defaults = {
        # Pick product categories (full slip types)
        "VIP_LOCK":          1.0,
        "EDGE_FADE":         1.0,
        "CROSS_GAME_PARLAY": 1.0,
        "SGP":               1.0,
        "INDIVIDUAL":        1.0,
        # Prop ingredient categories (leg-level)
        "fade_prop":         1.0,
        "benefactor_prop":   1.0,
        "neutral_prop":      1.0,
        "game_total":        1.0,
    }
    try:
        conn = _db_conn()
        if not conn:
            return defaults
        cur = conn.cursor()
        cur.execute(
            "SELECT key, value FROM learning_data WHERE key LIKE 'conf_multiplier:%%'"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for key, val in rows:
            cat = key.replace("conf_multiplier:", "")
            try:
                defaults[cat] = float(val) if not isinstance(val, dict) else float(_json.dumps(val))
            except Exception:
                pass
    except Exception as e:
        print(f"[ConfMult] load error: {e}")
    return defaults


def _load_shadow_hit_rates():
    """
    Load per-player-stat shadow hit rates from learning_data.
    Returns dict keyed by "{player}:{stat}" → {"rate": 0.xx, "total": n, "wins": n}.
    Stored nightly by _auto_adjust_model section 7.
    """
    import json as _json
    rates = {}
    try:
        conn = _db_conn()
        if not conn:
            return rates
        cur = conn.cursor()
        cur.execute(
            "SELECT key, value FROM learning_data WHERE key LIKE 'shadow_hit:%%'"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for key, val in rows:
            player_stat = key.replace("shadow_hit:", "")
            try:
                data = val if isinstance(val, dict) else _json.loads(val)
                rates[player_stat] = data
            except Exception:
                pass
    except Exception as e:
        print(f"[ShadowHit] load error: {e}")
    print(f"[ShadowHit] loaded {len(rates)} player-stat hit rates from learning")
    return rates


def _load_win_rate_context():
    """
    Load ALL historical win-rate learning from learning_data in a single DB read.
    Returns a dict the engine uses to boost/penalize every pick:
      • by_type   — win rate per prop type (points, rebounds, assists, threes)
      • by_script — win rate per game script label (PACE_ADVANTAGE, etc.)
      • fade_roles — win rates for fade / beneficiary / neutral roles
      • by_category — win rate per pick product (VIP_LOCK, EDGE_FADE, etc.)
    This is the master learning signal — everything the bot has observed about
    what actually works gets applied to every pick it evaluates.
    """
    import json as _json
    context = {"by_type": {}, "by_script": {}, "fade_roles": {}, "by_category": {}}
    keys_needed = {
        "win_rate_by_type",
        "win_rate_by_script",
        "edge_fade_role_win_rates",
        "win_rate_by_category",
    }
    try:
        conn = _db_conn()
        if not conn:
            return context
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(keys_needed))
        cur.execute(
            f"SELECT key, value FROM learning_data WHERE key IN ({placeholders})",
            list(keys_needed),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for key, val in rows:
            try:
                data = val if isinstance(val, dict) else _json.loads(val)
                if key == "win_rate_by_type":
                    context["by_type"] = data
                elif key == "win_rate_by_script":
                    context["by_script"] = data
                elif key == "edge_fade_role_win_rates":
                    context["fade_roles"] = data
                elif key == "win_rate_by_category":
                    context["by_category"] = data
            except Exception:
                pass
    except Exception as e:
        print(f"[WinRateCtx] load error: {e}")

    loaded = sum(len(v) for v in context.values())
    print(f"[WinRateCtx] loaded — by_type:{len(context['by_type'])} "
          f"by_script:{len(context['by_script'])} "
          f"fade_roles:{len(context['fade_roles'])} "
          f"by_category:{len(context['by_category'])} "
          f"({loaded} total records)")
    return context


def _load_and_apply_team_styles():
    """
    Load calibrated team styles saved by _auto_adjust_model and apply
    them to the in-memory TEAM_STYLES dict used by game_script_fit.
    Called at pick time so learned styles survive bot restarts.
    """
    import json as _json
    import bot.game_script as _gs
    try:
        conn = _db_conn()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM learning_data WHERE key = 'calibrated_team_styles'"
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            saved = row[0] if isinstance(row[0], dict) else _json.loads(row[0])
            if saved:
                _gs.TEAM_STYLES.update(saved)
                print(f"[TeamStyles] loaded {len(saved)} calibrated team styles from DB")
    except Exception as e:
        print(f"[TeamStyles] load error: {e}")


def _apply_conf_multipliers(candidates, multipliers):
    """
    Apply category multipliers to a list of candidate pick dicts.
    Checks pick_category (VIP_LOCK, EDGE_FADE, SGP etc.) first,
    then falls back to leg-level category (fade_prop, neutral_prop etc.).
    Adjusts confidence in-place. Returns the same list (modified).
    """
    for c in candidates:
        # Pick product multiplier takes priority
        pick_cat = c.get("pick_category") or c.get("_pick_category", "")
        if pick_cat and pick_cat in multipliers:
            cat = pick_cat
        else:
            # Fall back to leg-level ingredient category
            is_fade  = c.get("is_fade", False)
            is_benef = c.get("is_benefactor", False)
            ptype    = str(c.get("prop_type", "")).lower()
            if "total" in ptype:
                cat = "game_total"
            elif is_fade:
                cat = "fade_prop"
            elif is_benef:
                cat = "benefactor_prop"
            else:
                cat = "neutral_prop"

        mult = multipliers.get(cat, 1.0)
        old_conf = c.get("confidence", 70)
        c["confidence"] = round(max(40, min(99, old_conf * mult)), 1)
        c["_conf_category"] = cat
        c["_conf_mult"] = mult
    return candidates


def _generate_shadow_picks(game_id, home_team, away_team, game_date):
    """
    Full VIP pipeline — mirrors run_full_system exactly.
    Every candidate goes through the same layers as a real VIP pick.
    Passes stored with blocked_by=NULL. Failures stored with blocked_by='L{N}_REASON'.
    Never sends to any channel.
    """
    from bot.game_script import TEAM_STYLES, analyze_game_script as _ags_shd, assign_role as _ar_shd

    # ── Prop type mappings (same as run_full_system) ──────────────────────────
    _PRED_KEY  = {"points": "pred_pts", "rebounds": "pred_reb",
                  "assists": "pred_ast", "threes":   "pred_fg3"}
    _STAT_HIST = {"points": "pts",      "rebounds": "reb",
                  "assists": "ast",     "threes":   "fg3"}
    # Combo props: map prop_type → tuple of pred_keys to sum
    _COMBO_PRED = {
        "points_rebounds_assists": ("pred_pts", "pred_reb", "pred_ast"),
        "points_rebounds":         ("pred_pts", "pred_reb"),
        "points_assists":          ("pred_pts", "pred_ast"),
    }
    _ROLE_MISMATCH = {
        "go_to_scorer":    ["rebounds"],
        "floor_general":   ["rebounds"],
        "glass_cleaner":   ["points", "assists", "threes"],
        "rim_anchor":      ["points", "assists", "threes"],
        "spot_up_shooter": ["rebounds", "assists"],
        "combo_creator":   ["rebounds"],
        "sixth_man":       ["rebounds"],
        "utility_player":  [],
    }

    # ── Real bookmaker prop lines (uses cache — no extra API cost) ────────────
    odds_data = get_player_props()
    props_by_player = {}   # {player_name_lower: [{prop_type, line, odds}]}

    # ── Real FanDuel game odds (for total line + odds) ─────────────────────────
    try:
        _, _shd_game_list = get_odds_full()
    except Exception:
        _shd_game_list = []
    _shd_game_obj = next(
        (g for g in _shd_game_list
         if g.get("home_team") == home_team and g.get("away_team") == away_team),
        None
    )
    _shd_vegas_total = 0.0
    _shd_total_odds  = -110
    if _shd_game_obj:
        _bks = _shd_game_obj.get("bookmakers", [])
        _bk  = next((b for b in _bks if b.get("key") == "fanduel"), None)
        if _bk:
            for _mkt in _bk.get("markets", []):
                if _mkt["key"] == "totals":
                    for _o in _mkt.get("outcomes", []):
                        if _o.get("name") == "Over":
                            _shd_vegas_total = float(_o.get("point", 0))
                            _shd_total_odds  = float(_o.get("price", -110))
    if odds_data:
        for p in extract_props(odds_data):
            key = p.get("player", "").lower()
            if key:
                props_by_player.setdefault(key, []).append(p)

    # ── Contextual signals ────────────────────────────────────────────────────
    try:
        injuries = get_espn_injuries()
    except Exception:
        injuries = {}
    try:
        inj_boost = assess_injury_boost(injuries, odds_data or [])
    except Exception:
        inj_boost = {}
    try:
        b2b_teams = detect_back_to_back_teams()
    except Exception:
        b2b_teams = set()

    fade_candidates = set(inj_boost.get("fade_candidates", []))
    benefactors     = set(inj_boost.get("benefactors", {}).keys())

    # ── Game script (same as run_full_system) ─────────────────────────────────
    h_style    = TEAM_STYLES.get(home_team, {})
    a_style    = TEAM_STYLES.get(away_team, {})
    off_avg    = (h_style.get("off_rating", 112) + a_style.get("off_rating", 112)) / 2
    def_avg    = (h_style.get("def_strength", 68) + a_style.get("def_strength", 68)) / 2
    pred_total = max(195.0, min(240.0, off_avg * 2 - def_avg))
    gs_label   = _classify_actual_script(pred_total)

    try:
        gs_obj = _ags_shd(home_team, away_team, pred_total, 5)
    except Exception:
        gs_obj = None

    _mults   = _load_conf_multipliers()
    conn     = _db_conn()
    if not conn:
        return
    cur      = conn.cursor()
    picks_stored = 0
    passed   = 0
    blocked  = 0
    _shadow_pass_legs = []   # legs that passed all layers — used for shadow SGP

    # ── Starters ──────────────────────────────────────────────────────────────
    home_starters = get_team_starters_espn(home_team)
    away_starters = get_team_starters_espn(away_team)
    all_players   = (
        [(p, home_team, away_team, True)  for p in home_starters] +
        [(p, away_team, home_team, False) for p in away_starters]
    )

    for player_data, team, opp, is_home in all_players:
        pname    = player_data.get("name", "")
        avg_mins = player_data.get("avg_mins", 0)
        if not pname or avg_mins < 15:
            continue

        # ── Injury gate: skip Out / Doubtful ──────────────────────────────────
        inj_status = (injuries.get(pname.lower()) or {}).get("status", "")
        if inj_status in ("Out", "Doubtful"):
            continue

        # ── Real stats ────────────────────────────────────────────────────────
        try:
            stats = get_player_stats(pname)
            if not stats:
                continue
        except Exception:
            continue

        avg_usage = stats.get("avg_usage", 10)

        # ── Layer 5: assign role once per player ──────────────────────────────
        try:
            role_obj = _ar_shd(
                player    = pname,
                team      = team,
                avg_pts   = float(stats.get("pred_pts") or 0),
                avg_reb   = float(stats.get("pred_reb") or 0),
                avg_ast   = float(stats.get("pred_ast") or 0),
                avg_mins  = float(avg_mins or 28),
                avg_usage = float(avg_usage or 15),
                game_script = gs_obj,
                is_home   = is_home,
            )
            role_tag = role_obj.role
        except Exception:
            role_tag = ""

        is_fade_sig  = pname in fade_candidates or team in b2b_teams
        is_benef_sig = pname in benefactors
        cat_key      = "fade_prop" if is_fade_sig else ("benefactor_prop" if is_benef_sig else "neutral_prop")
        mult         = _mults.get(cat_key, 1.0)
        emoji        = "📈" if is_benef_sig else ("📉" if is_fade_sig else "🏀")

        # ── Fuzzy odds lookup for this player ─────────────────────────────────
        pname_lower = pname.lower()
        player_lines = props_by_player.get(pname_lower)
        if not player_lines:
            last = pname_lower.split()[-1]
            for k, v in props_by_player.items():
                if k.split()[-1] == last:
                    player_lines = v
                    break
        player_lines = player_lines or []

        # ── Per-stat pipeline ─────────────────────────────────────────────────
        for prop_type, pred_key in _PRED_KEY.items():
            prediction = float(stats.get(pred_key) or 0)
            if prediction < 1.0:
                continue

            # Find real bookmaker line for this prop
            stat_entries = [x for x in player_lines if x.get("prop_type") == prop_type]
            if not stat_entries:
                continue   # no real line posted — skip (same as VIP pipeline)

            real_line = float(stat_entries[0].get("line", 0))
            real_odds = float(stat_entries[0].get("odds", -110))
            if real_line <= 0:
                continue

            # ── Edge + confidence (mirrors run_full_system exactly) ────────────
            edge      = prediction - real_line
            pick_dir  = "over" if edge >= 0 else "under"
            stat_hist = _STAT_HIST[prop_type]
            stat_vals = [x for x in (stats.get(stat_hist) or []) if x is not None]
            variance  = float(np.std(stat_vals[:5])) if len(stat_vals) >= 2 else 3.0
            base_conf = calculate_confidence(
                edge, variance,
                history=stat_vals[:20], line=real_line, direction=pick_dir.upper()
            )
            confidence = calibrated_confidence(prop_type, base_conf)

            blocked_by = None   # None = passed all layers so far

            # ── L0: is_elite_pick (edge threshold + 65% conf floor) ───────────
            if not is_elite_pick(edge, confidence, prop_type=prop_type):
                blocked_by = "L0_NO_EDGE_OR_CONF"

            # ── L5: role alignment ────────────────────────────────────────────
            if blocked_by is None and role_tag:
                if prop_type in _ROLE_MISMATCH.get(role_tag, []):
                    blocked_by = "L5_ROLE_MISMATCH"

            # ── L2: juice trap ────────────────────────────────────────────────
            if blocked_by is None:
                try:
                    from bot.decision_engine import juice_test as _jt_shd2, implied_probability as _ip_shd2
                    _jt = _jt_shd2(real_odds)
                    if _jt.flag == "RED":
                        _imp       = _ip_shd2(real_odds)
                        _true_prob = confidence / 100.0
                        if _true_prob < _imp + 0.05:
                            blocked_by = "L2_JUICE_TRAP"
                except Exception:
                    pass

            # ── L4: script alignment ──────────────────────────────────────────
            if blocked_by is None:
                try:
                    _gs_up        = gs_label.upper()
                    _is_defensive = any(w in _gs_up for w in ("DEFENSIVE", "SLOW", "HALFCOURT"))
                    _is_highscr   = any(w in _gs_up for w in ("HIGH_SCORING", "SHOOTOUT", "UPTEMPO"))
                    if (role_tag in ("go_to_scorer", "combo_creator")
                            and _is_defensive and prop_type == "points"):
                        blocked_by = "L4_SCRIPT_MISMATCH"
                    elif role_tag == "floor_general" and _is_highscr and prop_type == "assists":
                        blocked_by = "L4_SCRIPT_MISMATCH"
                except Exception:
                    pass

            # ── Gate: pattern engine ──────────────────────────────────────────
            if blocked_by is None:
                try:
                    from bot.decision_engine import gate_pick as _gate_shd2
                    _bet_dict = {
                        "player":     pname,
                        "pick":       pick_dir.upper(),
                        "betType":    prop_type,
                        "line":       real_line,
                        "confidence": confidence,
                        "edge":       edge,
                        "game":       f"{away_team} @ {home_team}",
                    }
                    if not _gate_shd2(_bet_dict):
                        blocked_by = "GATE_HOLD"
                except Exception:
                    pass

            # ── Final confidence with pick-record multiplier ───────────────────
            final_conf = round(min(99, max(40, confidence * mult)), 1)

            # ── Real probability + edge (same formula as live picks) ───────────
            from decision_engine import implied_probability as _ip_shd
            _shd_sf       = _norm_sf(real_line, prediction, _PROP_STD.get(prop_type, 5.0))
            shd_prob      = round(_shd_sf if pick_dir == "over" else 1.0 - _shd_sf, 4)
            shd_implied   = round(_ip_shd(real_odds), 4)
            shd_edge      = round(shd_prob - shd_implied, 4)

            block_tag  = f"BLOCKED:{blocked_by}" if blocked_by else "PASS"
            pick_text  = (
                f"{emoji} *{pname}* — {pick_dir.upper()} {real_line} {prop_type.upper()}"
                f" [{block_tag}]\n"
                f"  Prob: {shd_prob:.1%} · Edge: {shd_edge:+.3f} · EV implied: {shd_implied:.1%}\n"
                f"  Conf: {final_conf:.0f}% · Role: {role_tag or 'unknown'}\n"
                f"  Script: {gs_label} · {team} vs {opp}"
            )

            try:
                cur.execute("""
                    INSERT INTO shadow_picks
                        (game_id, game_date, home_team, away_team,
                         pick_type, player_name, stat, line,
                         direction, confidence, edge_score,
                         prob, implied_prob,
                         game_script, pick_text, blocked_by, role_tag, created_at)
                    VALUES (%s,%s,%s,%s,'prop',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                        SET blocked_by  = EXCLUDED.blocked_by,
                            role_tag    = EXCLUDED.role_tag,
                            confidence  = EXCLUDED.confidence,
                            edge_score  = EXCLUDED.edge_score,
                            prob        = EXCLUDED.prob,
                            implied_prob = EXCLUDED.implied_prob,
                            pick_text   = EXCLUDED.pick_text
                """, (game_id, game_date, home_team, away_team,
                      pname, prop_type, real_line, pick_dir,
                      final_conf, shd_edge, gs_label,
                      shd_prob, shd_implied,
                      pick_text, blocked_by, role_tag or None))
                picks_stored += 1
                if blocked_by:
                    blocked += 1
                else:
                    passed += 1
                    _shadow_pass_legs.append({
                        "player":   pname,
                        "bet_type": prop_type,
                        "line":     real_line,
                        "odds":     real_odds,
                        "pick":     pick_dir.upper(),
                        "edge":     shd_edge,
                        "prob":     shd_prob,
                        "position": role_tag or "",
                        "game":     f"{away_team} @ {home_team}",
                    })
            except Exception as e:
                print(f"[Shadow] prop insert error {pname}/{prop_type}: {e}")

        # ── Combo props (PRA, PR, PA) — summed prediction vs combined line ────
        for combo_type, pred_keys in _COMBO_PRED.items():
            try:
                combo_pred = sum(float(stats.get(pk) or 0) for pk in pred_keys)
                if combo_pred < 2.0:
                    continue
                combo_entries = [x for x in player_lines if x.get("prop_type") == combo_type]
                if not combo_entries:
                    continue
                real_line = float(combo_entries[0].get("line", 0))
                real_odds = float(combo_entries[0].get("odds", -110))
                if real_line <= 0:
                    continue
                edge     = combo_pred - real_line
                pick_dir = "over" if edge >= 0 else "under"
                base_conf = calculate_confidence(
                    edge, _PROP_STD.get(combo_type, 8.0),
                    history=[], line=real_line, direction=pick_dir.upper()
                )
                confidence = calibrated_confidence("points", base_conf)
                blocked_by = None
                if not is_elite_pick(edge, confidence, prop_type="points"):
                    blocked_by = "L0_NO_EDGE_OR_CONF"
                final_conf = round(min(99, max(40, confidence * mult)), 1)
                combo_label = combo_type.replace("_", "+").upper()
                pick_text = (
                    f"🎯 *{pname}* — {pick_dir.upper()} {real_line} {combo_label}"
                    f" [{'BLOCKED:' + blocked_by if blocked_by else 'PASS'}]\n"
                    f"  Combo pred: {combo_pred:.1f} · Edge: {edge:+.1f} · Conf: {final_conf:.0f}%"
                )
                try:
                    cur.execute("""
                        INSERT INTO shadow_picks
                            (game_id, game_date, home_team, away_team,
                             pick_type, player_name, stat, line,
                             direction, confidence, edge_score,
                             prob, implied_prob,
                             game_script, pick_text, blocked_by, role_tag, created_at)
                        VALUES (%s,%s,%s,%s,'prop',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                            SET blocked_by   = EXCLUDED.blocked_by,
                                confidence   = EXCLUDED.confidence,
                                edge_score   = EXCLUDED.edge_score,
                                pick_text    = EXCLUDED.pick_text
                    """, (game_id, game_date, home_team, away_team,
                          pname, combo_type, real_line, pick_dir,
                          final_conf, edge, gs_label,
                          min(1.0, combo_pred / max(real_line, 1)),
                          0.5, pick_text, blocked_by, role_tag or None))
                    picks_stored += 1
                    if blocked_by:
                        blocked += 1
                    else:
                        passed += 1
                except Exception as _ce:
                    print(f"[Shadow] combo insert error {pname}/{combo_type}: {_ce}")
            except Exception as _cx:
                print(f"[Shadow] combo score error {pname}/{combo_type}: {_cx}")

        # ── First basket — value based on implied probability vs role ──────────
        try:
            fb_entries = [x for x in player_lines if x.get("prop_type") == "first_basket"]
            if fb_entries and role_tag in ("go_to_scorer", "combo_creator", "sixth_man"):
                fb_odds    = float(fb_entries[0].get("odds", 0))
                fb_implied = implied_prob(fb_odds) if fb_odds != 0 else 0
                # Value play if odds > +400 (implied < ~20%) and player is primary scorer
                if fb_odds >= 400 and fb_implied < 0.20:
                    fb_conf = round(min(72, max(55, (0.20 - fb_implied) * 400 + 55)), 1)
                    fb_text = (
                        f"🥇 *{pname}* FIRST BASKET [PASS]\n"
                        f"  Odds: +{int(fb_odds)} · Implied: {fb_implied:.1%} · Conf: {fb_conf:.0f}%"
                    )
                    try:
                        cur.execute("""
                            INSERT INTO shadow_picks
                                (game_id, game_date, home_team, away_team,
                                 pick_type, player_name, stat, line,
                                 direction, confidence, edge_score,
                                 prob, implied_prob,
                                 game_script, pick_text, blocked_by, role_tag, created_at)
                            VALUES (%s,%s,%s,%s,'prop',%s,'first_basket',%s,'over',%s,%s,%s,%s,%s,%s,%s,NULL,%s,NOW())
                            ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                                SET confidence   = EXCLUDED.confidence,
                                    pick_text    = EXCLUDED.pick_text
                        """, (game_id, game_date, home_team, away_team,
                              pname, fb_odds, fb_conf,
                              0.0, 0.20 - fb_implied,
                              gs_label, fb_text, 0.20, role_tag or None))
                        picks_stored += 1
                        passed += 1
                    except Exception as _fbe:
                        print(f"[Shadow] first_basket insert error {pname}: {_fbe}")
        except Exception as _fbx:
            print(f"[Shadow] first_basket score error {pname}: {_fbx}")

    # ── Game total shadow pick ────────────────────────────────────────────────
    _defensive_scripts = ("HALFCOURT", "HALFCOURT_DEFENSIVE_BATTLE", "SLOW_PACED_DEFENSIVE_BATTLE")
    total_dir      = "under" if gs_label in _defensive_scripts else "over"
    total_blocked  = None

    # ── Real prob + edge for total shadow pick ────────────────────────────────
    from decision_engine import implied_probability as _ip_shd_tot
    _shd_use_total = _shd_vegas_total if _shd_vegas_total > 0 else pred_total
    _shd_tot_sf    = _norm_sf(_shd_use_total, pred_total, _NBA_TOTAL_STD)
    shd_tot_prob   = round(_shd_tot_sf if total_dir == "over" else 1.0 - _shd_tot_sf, 4)
    shd_tot_implied = round(_ip_shd_tot(_shd_total_odds), 4)
    shd_tot_edge   = round(shd_tot_prob - shd_tot_implied, 4)
    _shd_tot_diff  = abs(pred_total - _shd_use_total)
    shd_tot_conf   = round(calibrated_confidence("TOTAL", min(55 + _shd_tot_diff * 5, 90)), 1)

    try:
        from bot.decision_engine import gate_pick as _gate_total
        _total_bet = {
            "player": "", "pick": total_dir.upper(), "betType": "TOTAL",
            "line": _shd_use_total, "confidence": shd_tot_conf, "edge": shd_tot_edge,
            "game": f"{away_team} @ {home_team}",
        }
        if not _gate_total(_total_bet):
            total_blocked = "GATE_HOLD"
    except Exception:
        pass

    block_tag  = f"BLOCKED:{total_blocked}" if total_blocked else "PASS"
    total_text = (
        f"📊 *GAME TOTAL* — {total_dir.upper()} {_shd_use_total:.1f} [{block_tag}]\n"
        f"  Prob: {shd_tot_prob:.1%} · Edge: {shd_tot_edge:+.3f} · EV implied: {shd_tot_implied:.1%}\n"
        f"  Conf: {shd_tot_conf:.0f}% · Script: {gs_label} · {home_team} vs {away_team}"
    )
    try:
        cur.execute("""
            INSERT INTO shadow_picks
                (game_id, game_date, home_team, away_team,
                 pick_type, player_name, stat, line,
                 direction, confidence, edge_score,
                 prob, implied_prob,
                 game_script, pick_text, blocked_by, role_tag, created_at)
            VALUES (%s,%s,%s,%s,'total','','total',%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NOW())
            ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                SET blocked_by   = EXCLUDED.blocked_by,
                    confidence   = EXCLUDED.confidence,
                    edge_score   = EXCLUDED.edge_score,
                    prob         = EXCLUDED.prob,
                    implied_prob = EXCLUDED.implied_prob,
                    pick_text    = EXCLUDED.pick_text
        """, (game_id, game_date, home_team, away_team,
              round(_shd_use_total, 1), total_dir,
              shd_tot_conf, shd_tot_edge,
              shd_tot_prob, shd_tot_implied,
              gs_label, total_text, total_blocked))
        picks_stored += 1
    except Exception as e:
        print(f"[Shadow] total insert error: {e}")

    # ── Shadow SGP — greedy selection on passing legs ─────────────────────────
    try:
        if len(_shadow_pass_legs) >= 2:
            import random as _sr

            # Script filter — same as send_sgp_for_game
            _shd_scripts = detect_all_game_scripts({
                "home": home_team, "away": away_team,
                "total": _shd_vegas_total or pred_total,
                "spread": 0,
            })
            _shd_dom = max(_shd_scripts, key=lambda sc: {
                "INJURY": 100, "TRANSITION_HEAVY": (_shd_vegas_total or pred_total) * 0.45,
                "UPTEMPO": (_shd_vegas_total or pred_total) * 0.40,
            }.get(sc, 0)) if _shd_scripts else "COMPETITIVE"

            _shd_script_pool = [l for l in _shadow_pass_legs if fits_script(l, _shd_dom)]
            if len(_shd_script_pool) < 2:
                _shd_script_pool = _shadow_pass_legs

            def _shd_leg_score(leg, selected, role_filter):
                bt  = (leg.get("bet_type") or "").lower()
                pos = _normalize_pos(leg.get("position", ""))
                rf  = _role_fit_score(bt, pos)
                if role_filter == "primary" and rf < 1.0:
                    return -1.0
                if role_filter == "primary+secondary" and rf == 0.0:
                    return -1.0
                sel_types = [(s.get("bet_type") or "").lower() for s in selected]
                dep       = _dep_bonus(bt, sel_types)
                edge      = max(leg.get("edge", 0), 0)
                return edge * max(rf, 0.1) + dep

            def _shd_greedy(candidates, size, role_filter):
                selected  = []
                remaining = list(candidates)
                for _ in range(size):
                    if not remaining:
                        break
                    scores = [(l, _shd_leg_score(l, selected, role_filter)) for l in remaining]
                    scores = [(l, s) for l, s in scores if s >= 0]
                    if not scores:
                        break
                    best = max(scores, key=lambda x: x[1])[0]
                    selected.append(best)
                    remaining.remove(best)
                return selected

            for tier, role_filter, size_range in [
                ("sgp_safe",       "primary",           (2, 4)),
                ("sgp_balanced",   "primary+secondary", (4, 6)),
                ("sgp_aggressive", "all",               (6, 8)),
            ]:
                _sz   = _sr.randint(*size_range)
                _legs = _shd_greedy(_shd_script_pool, _sz, role_filter)
                if len(_legs) < 2:
                    continue
                _pid = f"{tier}_{game_id}"
                for _sl in _legs:
                    _sl_stat = (_sl.get("bet_type") or "").lower()
                    _sl_pick = (_sl.get("pick") or "OVER").upper()
                    _sl_line = _sl.get("line", 0)
                    _sl_txt  = (
                        f"[SHADOW {tier.upper()}] {_sl.get('player','')} — "
                        f"{_sl_pick} {_sl_line} {_sl_stat.upper()}  "
                        f"edge:{_sl.get('edge',0):+.3f} parlay:{_pid}"
                    )
                    try:
                        cur.execute("""
                            INSERT INTO shadow_picks
                                (game_id, game_date, home_team, away_team,
                                 pick_type, player_name, stat, line,
                                 direction, confidence, edge_score,
                                 prob, implied_prob,
                                 game_script, pick_text, blocked_by, role_tag,
                                 parlay_id, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,NOW())
                            ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                                SET parlay_id  = EXCLUDED.parlay_id,
                                    edge_score = EXCLUDED.edge_score,
                                    pick_text  = EXCLUDED.pick_text
                        """, (
                            game_id, game_date, home_team, away_team,
                            tier,
                            _sl.get("player", ""), _sl_stat, _sl_line,
                            _sl_pick.lower(),
                            round(_sl.get("prob", 0.5) * 100, 1),
                            _sl.get("edge", 0),
                            _sl.get("prob", 0.5), 0.5,
                            gs_label, _sl_txt, _sl.get("position", ""),
                            _pid,
                        ))
                        picks_stored += 1
                    except Exception as _sge:
                        print(f"[Shadow] SGP leg insert error {tier}: {_sge}")
            print(f"[Shadow] Shadow SGP stored for {home_team} vs {away_team} "
                  f"({len(_shadow_pass_legs)} pass legs used)")
    except Exception as _sgp_err:
        print(f"[Shadow] SGP block error: {_sgp_err}")

    # ── Mark shadows_generated ────────────────────────────────────────────────
    try:
        cur.execute("""
            UPDATE game_observations SET shadows_generated = TRUE
            WHERE game_id = %s AND game_date = %s
        """, (game_id, game_date))
    except Exception:
        pass

    conn.commit()
    cur.close()
    conn.close()
    print(
        f"[Shadow] {picks_stored} candidates stored ({passed} PASS / {blocked} BLOCKED)"
        f" — {home_team} vs {away_team}"
    )


def _generate_shadow_cgp(game_date):
    """
    Cross-game shadow parlay — mirrors _build_cross_game_parlay exactly.
    Pulls all PASS prop shadow picks from today across all games, runs the
    same greedy CGP algorithm, stores SAFE / BALANCED / AGGRESSIVE tiers.
    Called once per day when the first game goes Final.
    Never sends to any channel.
    """
    global _shadow_cgp_dates
    _date_str = str(game_date)
    if _date_str in _shadow_cgp_dates:
        return
    _shadow_cgp_dates.add(_date_str)

    try:
        conn = _db_conn()
        if not conn:
            return
        cur = conn.cursor()

        # Pull all PASS prop shadow picks for today (blocked_by IS NULL)
        cur.execute("""
            SELECT game_id, home_team, away_team, player_name, stat, line,
                   direction, edge_score, prob, game_script, role_tag
            FROM shadow_picks
            WHERE game_date = %s
              AND pick_type = 'prop'
              AND blocked_by IS NULL
              AND player_name != ''
        """, (_date_str,))
        rows = cur.fetchall()

        if len(rows) < 4:
            cur.close()
            conn.close()
            print(f"[ShadowCGP] Not enough PASS legs ({len(rows)}) for {_date_str}")
            return

        # Build pool in the format _build_cross_game_parlay expects
        pool = []
        for (gid, ht, at, pname, stat, line, direction, edge, prob,
             gs_label, role_tag) in rows:
            gname = f"{at} @ {ht}"
            pool.append({
                "player":   pname,
                "bet_type": stat,
                "line":     float(line or 0),
                "odds":     -110,
                "pick":     (direction or "over").upper(),
                "edge":     float(edge or 0),
                "prob":     float(prob or 0.5),
                "position": role_tag or "",
                "game":     gname,
                "game_id":  gid,
                "home_team": ht,
                "game_date": _date_str,
            })

        # Reuse exact CGP greedy algorithm
        tiers = _build_cross_game_parlay(pool)
        import random as _cgp_r

        stored_legs = 0
        for tier_name, legs in [
            ("cgp_safe",       tiers.get("safe",       [])),
            ("cgp_balanced",   tiers.get("balanced",   [])),
            ("cgp_aggressive", tiers.get("aggressive", [])),
        ]:
            if len(legs) < 2:
                continue
            _pid = f"{tier_name}_{_date_str}"
            for leg in legs:
                _stat  = (leg.get("bet_type") or "").lower()
                _pick  = (leg.get("pick") or "OVER").upper()
                _line  = leg.get("line", 0)
                _gname = leg.get("game", "")
                _ht    = leg.get("home_team") or (_gname.split(" @ ")[1].strip() if " @ " in _gname else "")
                _at    = leg.get("away_team") or (_gname.split(" @ ")[0].strip() if " @ " in _gname else "")
                _gid   = leg.get("game_id", _gname)
                _txt   = (
                    f"[SHADOW {tier_name.upper()}] {leg.get('player','')} — "
                    f"{_pick} {_line} {_stat.upper()}  "
                    f"edge:{leg.get('edge',0):+.3f}  parlay:{_pid}  game:{_gname}"
                )
                try:
                    cur.execute("""
                        INSERT INTO shadow_picks
                            (game_id, game_date, home_team, away_team,
                             pick_type, player_name, stat, line,
                             direction, confidence, edge_score,
                             prob, implied_prob,
                             game_script, pick_text, blocked_by, role_tag,
                             parlay_id, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,NOW())
                        ON CONFLICT (game_id, pick_type, player_name, stat) DO UPDATE
                            SET parlay_id  = EXCLUDED.parlay_id,
                                edge_score = EXCLUDED.edge_score,
                                pick_text  = EXCLUDED.pick_text
                    """, (
                        _gid, _date_str, _ht, _at,
                        tier_name,
                        leg.get("player", ""), _stat, _line,
                        _pick.lower(),
                        round(leg.get("prob", 0.5) * 100, 1),
                        leg.get("edge", 0),
                        leg.get("prob", 0.5), 0.5,
                        leg.get("game_script", ""),
                        _txt, leg.get("position", ""),
                        _pid,
                    ))
                    stored_legs += 1
                except Exception as _cge:
                    print(f"[ShadowCGP] leg insert error {tier_name}: {_cge}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"[ShadowCGP] {stored_legs} legs stored across 3 tiers for {_date_str}")

    except Exception as _cgp_err:
        print(f"[ShadowCGP] error: {_cgp_err}")


def _save_causality_events_to_db(game_id, game_date, causality_log):
    """
    Persist ContextTracker causality events to the causality_log table.
    Call this after every ContextTracker.update() that returns events.
    Nothing is lost on restart because DB is the source of truth.
    """
    if not causality_log:
        return
    try:
        conn = _db_conn()
        if not conn:
            return
        cur = conn.cursor()
        for entry in causality_log:
            for cause_str in (entry.get("causes") or []):
                cause_type = str(cause_str).split("—")[0].strip().split()[0].strip()
                cur.execute("""
                    INSERT INTO causality_log
                        (game_id, game_date, period, cause_type, full_cause,
                         from_script, to_script, home_score, away_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    game_id, game_date,
                    entry.get("period", 0),
                    cause_type,
                    cause_str,
                    entry.get("from_script", ""),
                    entry.get("to_script", ""),
                    entry.get("home_score", 0),
                    entry.get("away_score", 0),
                ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as _e:
        print(f"[CausalityDB] save error game {game_id}: {_e}")


def _get_game_causality_events(game_id) -> list:
    """
    Load all persisted causality cause strings for a game from DB.
    Returns a flat list of full_cause strings — same format as
    ContextTracker._causality_log causes.
    """
    try:
        conn = _db_conn()
        if not conn:
            return []
        cur = conn.cursor()
        cur.execute("""
            SELECT full_cause FROM causality_log
            WHERE game_id = %s
            ORDER BY period ASC, logged_at ASC
        """, (game_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception as _e:
        print(f"[CausalityDB] load error game {game_id}: {_e}")
        return []


def _grade_shadow_picks_for_game(game_id, game_date):
    """
    Grade all shadow picks for a completed game vs BDL final box scores.
    Compares each pick direction vs actual stat — stores win/loss + actual value.
    """
    conn = _db_conn()
    if not conn:
        return

    cur = conn.cursor()

    # Pull ungraded shadow picks for this game
    cur.execute("""
        SELECT id, player_name, stat, line, direction, pick_type
        FROM shadow_picks
        WHERE game_id = %s AND game_date = %s AND result IS NULL
    """, (game_id, game_date))
    picks = cur.fetchall()

    if not picks:
        cur.close()
        conn.close()
        return

    # ESPN final box scores
    pstats = _espn_summary_player_stats(game_id)
    if not pstats:
        print(f"[Shadow] ESPN box score empty for event {game_id}")
        cur.close()
        conn.close()
        return

    # Build player stat map
    stat_map = {}
    for ps in pstats:
        pname = ps.get("pname", "")
        if pname:
            stat_map[pname] = {
                "pts":  ps.get("pts",  0),
                "reb":  ps.get("reb",  0),
                "ast":  ps.get("ast",  0),
                "fg3m": ps.get("fg3m", 0),
            }

    # Actual game total from game_observations
    cur.execute("""
        SELECT actual_total FROM game_observations
        WHERE game_id = %s AND game_date = %s
    """, (game_id, game_date))
    g_row = cur.fetchone()
    actual_total = g_row[0] if g_row else None

    _SGP_CGP_TYPES = {
        "sgp_safe", "sgp_balanced", "sgp_aggressive",
        "cgp_safe", "cgp_balanced", "cgp_aggressive",
    }

    graded = 0
    for pick_id, pname, stat, line, direction, pick_type in picks:
        actual_val = None
        result     = None
        try:
            _is_prop_like = (pick_type == "prop") or (pick_type in _SGP_CGP_TYPES)
            if _is_prop_like and pname in stat_map:
                _stat_key = {
                    "points": "pts", "rebounds": "reb",
                    "assists": "ast", "threes": "fg3m",
                    "pts": "pts", "reb": "reb", "ast": "ast",
                }.get(stat, stat)
                actual_val = stat_map[pname].get(_stat_key, 0)
                result = "win" if (
                    (direction == "over" and actual_val > line) or
                    (direction == "under" and actual_val < line)
                ) else "loss"
            elif pick_type == "total" and actual_total is not None:
                actual_val = float(actual_total)
                result = "win" if (
                    (direction == "over" and actual_val > line) or
                    (direction == "under" and actual_val < line)
                ) else "loss"
        except Exception:
            pass

        if result:
            cur.execute("""
                UPDATE shadow_picks
                SET actual_value = %s, result = %s, graded_at = NOW()
                WHERE id = %s
            """, (actual_val, result, pick_id))
            graded += 1

            # ── Self-learning: feed outcome back into all learning layers ──────
            try:
                from decision_engine import (
                    record_channel_outcome, record_kelly_outcome,
                    record_ml_outcome, update_role_threshold,
                    record_causality_outcome,
                )
                hit = (result == "win")

                # Fetch confidence + role from this pick for learning routing
                cur.execute("""
                    SELECT confidence, game_script FROM shadow_picks WHERE id = %s
                """, (pick_id,))
                _sh = cur.fetchone()
                _conf     = float(_sh[0]) if _sh and _sh[0] else 0.0
                _role_str = str(_sh[1]) if _sh and _sh[1] else "UNKNOWN"

                # Layer 11 — channel floor learning
                _ch = "VIP" if _conf >= 72.0 else "FREE"
                record_channel_outcome(_ch, hit)

                # Layer 10 — Kelly fraction learning (simple +1/-1 ROI proxy)
                record_kelly_outcome(1.0 if hit else -1.0)

                # Layer 3 — ML weight learning
                record_ml_outcome(hit)

                # Causality closed loop — load persisted events, feed back
                _causal_events = _get_game_causality_events(game_id)
                record_causality_outcome(result, stat, _role_str, _causal_events)

            except Exception as _le:
                print(f"[Shadow] self-learning record error: {_le}")

    # ── Log parlay results for any fully-graded SGP/CGP groups ─────────────────
    try:
        cur.execute("""
            SELECT parlay_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
                   SUM(CASE WHEN result IS NULL  THEN 1 ELSE 0 END) AS pending
            FROM shadow_picks
            WHERE parlay_id LIKE 'sgp_%%' || %s
               OR parlay_id LIKE 'cgp_%%'
            GROUP BY parlay_id
            HAVING SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) = 0
        """, (str(game_id),))
        _parlay_rows = cur.fetchall()
        for _pid, _tot, _w, _l, _pend in (_parlay_rows or []):
            _parlay_hit = (_l == 0 and _w == _tot)
            _tag        = "WIN" if _parlay_hit else "LOSS"
            print(
                f"[Shadow] Parlay {_pid}: {_w}W/{_l}L/{_pend}P → {_tag}"
            )
    except Exception as _pre:
        print(f"[Shadow] parlay result log error: {_pre}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"[Shadow] Graded {graded} shadow picks for game {game_id}")

    # Persist causality + pattern state immediately after grading this game's
    # shadow picks — so a restart before the nightly cycle doesn't lose signal.
    try:
        from decision_engine import pe_flush as _pef
        _pef()
    except Exception as _pfe:
        print(f"[Shadow] pe_flush error (non-fatal): {_pfe}")


def _auto_adjust_model():
    """
    Auto-adjustment: runs at end of last game each night.
    Reads game_observations + player_observations from last 7 days.
    Adjusts team TEAM_STYLES (30% new data, 70% historical).
    Updates player baselines in learning_data table.
    DMs admin a full breakdown of every change made.
    """
    import json as _json
    from bot.game_script import TEAM_STYLES
    import bot.game_script as _gs

    today = datetime.now(ET).strftime("%Y-%m-%d")
    cutoff = (datetime.now(ET) - timedelta(days=7)).strftime("%Y-%m-%d")

    conn = _db_conn()
    if not conn:
        print("[AutoAdjust] DB unavailable")
        return

    report_lines = ["🔄 *Nightly Auto-Adjustment*\n"]

    try:
        cur = conn.cursor()

        # ── 1. Team script accuracy → adjust TEAM_STYLES pace ────────────────
        cur.execute("""
            SELECT home_team, away_team, predicted_script, actual_script, script_match
            FROM game_observations
            WHERE game_date >= %s AND status ILIKE '%%final%%'
        """, (cutoff,))
        game_rows = cur.fetchall()

        # Build per-team actual script counts — migrate old labels on read
        _SCRIPT_MIGRATE = {
            "HIGH": "UPTEMPO", "GRIND": "HALFCOURT", "NORMAL": "AVERAGE_PACE",
            "MID": "AVERAGE_PACE",
        }
        team_actuals = {}   # team → {script_label: count}
        for home, away, pred, actual, match in game_rows:
            for team in [home, away]:
                if team not in team_actuals:
                    team_actuals[team] = {}
                label = _SCRIPT_MIGRATE.get(str(actual).upper(), actual) if actual else "AVERAGE_PACE"
                team_actuals[team][label] = team_actuals[team].get(label, 0) + 1

        team_changes = []
        BLEND = 0.30  # 30% new data, 70% historical
        new_styles = {}

        for team, counts in team_actuals.items():
            if team not in TEAM_STYLES:
                continue
            total = sum(counts.values())
            if total < 2:
                continue
            dominant = max(counts, key=counts.get)
            current_pace = TEAM_STYLES[team].get("pace", "NORMAL")
            if dominant != current_pace:
                # Blend — only flip if dominant seen >50% of recent games
                if counts[dominant] / total >= 0.55:
                    new_styles[team] = {**TEAM_STYLES[team], "pace": dominant}
                    team_changes.append(
                        f"  {team}: {current_pace} → {dominant} "
                        f"({counts[dominant]}/{total} games)"
                    )
                else:
                    new_styles[team] = TEAM_STYLES[team].copy()
            else:
                new_styles[team] = TEAM_STYLES[team].copy()

        # Apply in-memory
        for team, style in new_styles.items():
            _gs.TEAM_STYLES[team] = style

        # Persist calibrated styles to DB so they survive restarts
        try:
            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES ('calibrated_team_styles', %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (_json.dumps(new_styles),))
            conn.commit()
            print(f"[AutoAdjust] saved {len(new_styles)} calibrated team styles to DB")
        except Exception as _tse:
            print(f"[AutoAdjust] team styles DB save skipped: {_tse}")

        if team_changes:
            report_lines.append("*Team Script Adjustments:*")
            report_lines.extend(team_changes)
        else:
            report_lines.append("*Teams:* No script changes needed")

        # ── 2. Player baselines → update learning_data ───────────────────────
        cur.execute("""
            SELECT player_name, team,
                   AVG(pts)::FLOAT, AVG(ast)::FLOAT,
                   AVG(reb)::FLOAT, AVG(fg3m)::FLOAT,
                   AVG(minutes)::FLOAT,
                   COUNT(*) AS games
            FROM player_observations
            WHERE game_date >= %s
            GROUP BY player_name, team
            HAVING COUNT(*) >= 2
        """, (cutoff,))
        player_rows = cur.fetchall()

        player_changes = []
        for pname, team, avg_pts, avg_ast, avg_reb, avg_fg3, avg_mins, games in player_rows:
            # Load existing baseline
            cur.execute(
                "SELECT value FROM learning_data WHERE key=%s",
                (f"player_baseline:{pname}",)
            )
            existing = cur.fetchone()
            old_bl = {}
            if existing:
                try:
                    old_bl = _json.loads(existing[0]) if isinstance(existing[0], str) else existing[0]
                except Exception:
                    old_bl = {}

            old_pts = float(old_bl.get("avg_pts", avg_pts))

            # Weighted blend
            new_bl = {
                "avg_pts":  round(old_pts * (1 - BLEND) + avg_pts * BLEND, 1),
                "avg_ast":  round(float(old_bl.get("avg_ast", avg_ast)) * (1 - BLEND) + avg_ast * BLEND, 1),
                "avg_reb":  round(float(old_bl.get("avg_reb", avg_reb)) * (1 - BLEND) + avg_reb * BLEND, 1),
                "avg_fg3":  round(float(old_bl.get("avg_fg3", avg_fg3)) * (1 - BLEND) + avg_fg3 * BLEND, 1),
                "avg_mins": round(float(old_bl.get("avg_mins", avg_mins)) * (1 - BLEND) + avg_mins * BLEND, 1),
                "team":     team,
                "obs_games": int(games),
            }

            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (f"player_baseline:{pname}", _json.dumps(new_bl)))

            # Only report meaningful changes
            pts_delta = abs(new_bl["avg_pts"] - old_pts)
            if pts_delta >= 1.0:
                direction = "↑" if new_bl["avg_pts"] > old_pts else "↓"
                player_changes.append(
                    f"  {pname} pts avg: {old_pts:.1f} → {new_bl['avg_pts']:.1f} {direction}"
                )

        conn.commit()

        if player_changes:
            report_lines.append(f"\n*Player Baselines Updated ({len(player_rows)} players):*")
            report_lines.extend(player_changes[:20])  # cap at 20 for readability
            if len(player_changes) > 20:
                report_lines.append(f"  ...and {len(player_changes)-20} more")
        else:
            report_lines.append("\n*Players:* All baselines stable")

        # ── 3. Benefactor / fade flags summary ───────────────────────────────
        cur.execute("""
            SELECT player_name, team, injury_context
            FROM player_observations
            WHERE game_date = %s AND is_benefactor = TRUE
        """, (today,))
        benefactors = cur.fetchall()

        cur.execute("""
            SELECT player_name, team
            FROM player_observations
            WHERE game_date = %s AND is_fade = TRUE
        """, (today,))
        fades = cur.fetchall()

        if benefactors:
            report_lines.append("\n*Benefactors Tonight:*")
            for pname, team, inj in benefactors:
                ctx = f" ({inj})" if inj else ""
                report_lines.append(f"  📈 {pname} — {team}{ctx}")

        if fades:
            report_lines.append("\n*Fades Tonight:*")
            for pname, team in fades:
                report_lines.append(f"  📉 {pname} — {team}")

        # ── 4. Shadow pick accuracy for tonight ───────────────────────────────
        cur.execute("""
            SELECT
                pick_type,
                COUNT(*) FILTER (WHERE result = 'win')  AS wins,
                COUNT(*) FILTER (WHERE result = 'loss') AS losses,
                COUNT(*) FILTER (WHERE result IS NULL)  AS pending
            FROM shadow_picks
            WHERE game_date = %s
            GROUP BY pick_type
        """, (today,))
        shadow_rows = cur.fetchall()

        if shadow_rows:
            report_lines.append("\n*Shadow Pick Accuracy Tonight:*")
            total_w = total_l = 0
            for ptype, wins, losses, pending in shadow_rows:
                total = (wins or 0) + (losses or 0)
                total_w += (wins or 0)
                total_l += (losses or 0)
                pct = f"{wins/total*100:.0f}%" if total > 0 else "—"
                label = "Props" if ptype == "prop" else "Totals"
                report_lines.append(
                    f"  {label}: {wins}W-{losses}L ({pct})"
                    + (f" · {pending} pending" if pending else "")
                )
            total_all = total_w + total_l
            overall = f"{total_w/total_all*100:.0f}%" if total_all > 0 else "—"
            report_lines.append(f"  *Overall: {total_w}W-{total_l}L ({overall})*")

            # Worst misses — biggest actual vs line gap
            cur.execute("""
                SELECT player_name, stat, line, actual_value,
                       ABS(actual_value - line) AS miss_gap
                FROM shadow_picks
                WHERE game_date = %s
                  AND result = 'loss'
                  AND actual_value IS NOT NULL
                ORDER BY miss_gap DESC
                LIMIT 5
            """, (today,))
            misses = cur.fetchall()
            if misses:
                report_lines.append("\n*Biggest Shadow Misses:*")
                for pname, stat, line, actual, gap in misses:
                    label = pname if pname else "Game Total"
                    report_lines.append(
                        f"  {label} {stat}: line {line:.1f} → actual {actual:.0f} "
                        f"(gap {gap:.1f})"
                    )

        # ── 5. Pick record feedback — adjust confidence multipliers ───────────
        # Read real pick results from bets table (last 30 days), grouped by
        # pick_category (VIP_LOCK, EDGE_FADE, SGP, CROSS_GAME_PARLAY, INDIVIDUAL)
        # so each pick product has its own multiplier.
        cutoff_30 = (datetime.now(ET) - timedelta(days=30)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT
                COALESCE(NULLIF(pick_category, ''), 'INDIVIDUAL') AS category,
                COUNT(*) FILTER (WHERE result = 'win')  AS wins,
                COUNT(*) FILTER (WHERE result = 'loss') AS losses
            FROM bets
            WHERE created_at >= %s
              AND result IN ('win', 'loss')
            GROUP BY category
        """, (cutoff_30,))
        cat_rows = cur.fetchall()

        BLEND_MULT   = 0.10   # slow — 10% new signal
        MULT_MIN     = 0.80   # never below 80%
        MULT_MAX     = 1.15   # never above 115%
        TARGET_RATE  = 0.55   # 55% = neutral (1.0x)
        MIN_SAMPLES  = 15

        mult_report = []
        for category, wins, losses in cat_rows:
            total = (wins or 0) + (losses or 0)
            if total < MIN_SAMPLES:
                mult_report.append(
                    f"  {category}: {wins}W-{losses}L — skip (need {MIN_SAMPLES})"
                )
                continue

            win_rate = wins / total
            # Distance from target: +0.10 above target → +0.05 boost
            rate_delta = (win_rate - TARGET_RATE) * 0.5

            cur.execute(
                "SELECT value FROM learning_data WHERE key = %s",
                (f"conf_multiplier:{category}",)
            )
            existing = cur.fetchone()
            old_mult = 1.0
            if existing:
                try:
                    old_mult = float(existing[0])
                except Exception:
                    old_mult = 1.0

            new_mult = round(
                max(MULT_MIN, min(MULT_MAX,
                    old_mult * (1 - BLEND_MULT) + (1.0 + rate_delta) * BLEND_MULT
                )), 4
            )

            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (f"conf_multiplier:{category}", str(new_mult)))

            direction = "↑" if new_mult > old_mult else ("↓" if new_mult < old_mult else "→")
            mult_report.append(
                f"  {category}: {wins}W-{losses}L "
                f"({win_rate*100:.0f}%) → mult "
                f"{old_mult:.3f} {direction} {new_mult:.3f}"
            )

        conn.commit()

        if mult_report:
            report_lines.append("\n*Pick Record Feedback (Confidence Multipliers):*")
            report_lines.extend(mult_report)
        else:
            report_lines.append("\n*Pick Record Feedback:* Not enough samples yet")

        # ── 6. Shadow pick accuracy → feed into conf_multipliers ─────────────
        # Shadow picks are the bot's own virtual bets. Their win rate adjusts
        # the same confidence multipliers the engine uses to select real picks.
        cur.execute("""
            SELECT
                pick_type,
                COUNT(*) FILTER (WHERE result = 'win')  AS wins,
                COUNT(*) FILTER (WHERE result = 'loss') AS losses
            FROM shadow_picks
            WHERE game_date >= %s AND result IS NOT NULL
            GROUP BY pick_type
        """, (cutoff,))
        shadow_acc_rows = cur.fetchall()

        SHADOW_BLEND   = 0.15   # shadow signal is softer than real bets
        shadow_report  = []

        for pick_type, wins, losses in shadow_acc_rows:
            total = (wins or 0) + (losses or 0)
            if total < 10:
                shadow_report.append(
                    f"  shadow {pick_type}: {wins}W-{losses}L — skip (need 10)"
                )
                continue

            win_rate   = wins / total
            rate_delta = (win_rate - TARGET_RATE) * 0.5

            # Map shadow pick_type to conf_multiplier categories
            if "total" in pick_type:
                cats = ["game_total"]
            else:
                cats = ["fade_prop", "benefactor_prop", "neutral_prop"]

            for cat in cats:
                cur.execute(
                    "SELECT value FROM learning_data WHERE key = %s",
                    (f"conf_multiplier:{cat}",)
                )
                ex = cur.fetchone()
                old_m = 1.0
                if ex:
                    try:
                        old_m = float(ex[0])
                    except Exception:
                        old_m = 1.0

                new_m = round(
                    max(MULT_MIN, min(MULT_MAX,
                        old_m * (1 - SHADOW_BLEND) + (1.0 + rate_delta) * SHADOW_BLEND
                    )), 4
                )
                cur.execute("""
                    INSERT INTO learning_data (key, value, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = NOW()
                """, (f"conf_multiplier:{cat}", str(new_m)))

                direction = "↑" if new_m > old_m else ("↓" if new_m < old_m else "→")
                shadow_report.append(
                    f"  shadow {pick_type} → {cat}: "
                    f"{wins}W-{losses}L ({win_rate*100:.0f}%) "
                    f"mult {old_m:.3f} {direction} {new_m:.3f}"
                )

        conn.commit()

        if shadow_report:
            report_lines.append("\n*Shadow Pick Feedback (Confidence Multipliers):*")
            report_lines.extend(shadow_report)
        else:
            report_lines.append("\n*Shadow Feedback:* Not enough graded picks yet")

        # ── 7. Per-player-stat shadow hit rates → learning_data ──────────────
        # Store each player+stat hit rate so the engine can boost/penalize
        # individual picks based on what the bot has actually been right about.
        cur.execute("""
            SELECT player_name, stat,
                   COUNT(*) FILTER (WHERE result = 'win')         AS wins,
                   COUNT(*) FILTER (WHERE result IS NOT NULL)     AS total
            FROM shadow_picks
            WHERE game_date >= %s
              AND pick_type = 'prop'
              AND result IS NOT NULL
              AND player_name IS NOT NULL
            GROUP BY player_name, stat
            HAVING COUNT(*) FILTER (WHERE result IS NOT NULL) >= 5
        """, (cutoff,))
        player_stat_rows = cur.fetchall()

        player_stat_report = []
        for pname, stat, wins, total in player_stat_rows:
            rate   = round(wins / total, 3)
            key    = f"shadow_hit:{pname.lower()}:{stat.lower()}"
            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (key, _json.dumps({"wins": int(wins), "total": int(total), "rate": rate})))

            icon = "🟢" if rate >= 0.65 else ("🔴" if rate <= 0.40 else "🟡")
            player_stat_report.append(
                f"  {icon} {pname} {stat}: {wins}/{total} ({rate*100:.0f}%)"
            )

        conn.commit()

        if player_stat_report:
            report_lines.append(
                f"\n*Per-Player Shadow Hit Rates ({len(player_stat_rows)} tracked):*"
            )
            report_lines.extend(player_stat_report[:15])
            if len(player_stat_report) > 15:
                report_lines.append(f"  ...and {len(player_stat_report)-15} more")
        else:
            report_lines.append("\n*Player Hit Rates:* Not enough data yet (need 5+ graded)")

        cur.close()
        conn.close()

        # ── 8. Context-aware pattern learning cycle ───────────────────────────
        try:
            from bot.decision_engine import run_learning_cycle as _rlc
            _lc_conn = _db_conn()
            _pattern_lines = _rlc(_lc_conn)
            if _lc_conn:
                try: _lc_conn.close()
                except Exception: pass
            report_lines.append("\n" + "\n".join(_pattern_lines))
        except Exception as _pe_err:
            print(f"[AutoAdjust] pattern cycle error: {_pe_err}")
            report_lines.append(f"\n⚠️ Pattern cycle error: {_pe_err}")

        report_lines.append(f"\n_Blend: 30% recent / 70% historical · {today}_")
        reply(ADMIN_ID, "\n".join(report_lines))
        print("[AutoAdjust] complete")

    except Exception as e:
        print(f"[AutoAdjust] error: {e}")
        try:
            conn.close()
        except Exception:
            pass


def _check_pick_result(pick_id):
    """
    Load pick from DB, parse legs, fetch BDL box scores, compare each leg.
    Returns (result_dict, error_string). Does NOT touch win/loss record.
    """
    conn = _db_conn()
    if not conn:
        return None, "Database unavailable"
    cur = conn.cursor()
    cur.execute(
        "SELECT id, pick_text, picked_at_et, result FROM feed_picks WHERE id=%s",
        (pick_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None, f"Pick #{pick_id} not found"

    _, pick_text, picked_at_et, existing_result = row

    # Parse game date from "Mar 29, 2026 8:00 PM ET"
    date_str = None
    clean = picked_at_et.replace(" ET", "").strip()
    for fmt in ("%b %d, %Y %I:%M %p", "%b %-d, %Y %I:%M %p",
                "%b %d, %Y %-I:%M %p", "%b %-d, %Y %-I:%M %p"):
        try:
            from datetime import datetime as _dt2
            d = _dt2.strptime(clean, fmt)
            date_str = d.strftime("%Y-%m-%d")
            break
        except ValueError:
            continue
    if not date_str:
        # Fallback: grab first 3 tokens and try
        try:
            from datetime import datetime as _dt2
            date_str = _dt2.strptime(clean[:12].strip(), "%b %d, %Y").strftime("%Y-%m-%d")
        except Exception:
            return None, f"Couldn't parse pick date from: {picked_at_et}"

    legs = _parse_pick_text(pick_text)
    if not legs:
        return None, "No legs found in pick"

    leg_results = []
    for leg in legs:
        player = leg.get("player", "")
        if not player or not leg.get("stats"):
            continue
        actual = _fetch_player_boxscore(player, date_str)
        lr = {"player": player, "actual": actual, "hits": [], "misses": []}
        if actual:
            for line_val, stat_label in leg["stats"]:
                bdl_field = _STAT_BDL_FIELD.get(stat_label)
                if not bdl_field:
                    continue
                try:
                    line_float = float(str(line_val).lstrip("+"))
                except ValueError:
                    continue
                if bdl_field == "min":
                    min_str = str(actual.get("min", "0"))
                    try:
                        p = min_str.split(":")
                        actual_val = float(p[0]) + float(p[1]) / 60 if len(p) == 2 else float(p[0])
                    except Exception:
                        actual_val = 0.0
                else:
                    actual_val = float(actual.get(bdl_field, 0) or 0)
                entry = {"stat": stat_label, "line": line_float,
                         "actual": actual_val, "hit": actual_val >= line_float}
                (lr["hits"] if entry["hit"] else lr["misses"]).append(entry)
        leg_results.append(lr)

    all_hit  = bool(leg_results) and all(
        lr["actual"] and len(lr["misses"]) == 0 and len(lr["hits"]) > 0
        for lr in leg_results
    )
    has_data = any(lr["actual"] is not None for lr in leg_results)

    return {
        "pick_id":         pick_id,
        "pick_text":       pick_text,
        "picked_at_et":    picked_at_et,
        "date_str":        date_str,
        "legs":            leg_results,
        "all_hit":         all_hit,
        "has_data":        has_data,
        "existing_result": existing_result,
    }, None


_STAT_AVG_KEY = {
    "PTS":  ("avg_pts",  "pred_pts"),
    "REB":  ("avg_reb",  "pred_reb"),
    "AST":  ("avg_ast",  "pred_ast"),
    "3PM":  ("avg_fg3",  "pred_fg3"),
    "STL":  ("avg_stl",  None),
    "BLK":  ("avg_blk",  None),
}

def _grade_miss(player, stat, line_val, actual_val, bdl_actual):
    """
    Analyze WHY a prop missed. Returns a list of short grade notes (strings).
    bdl_actual: the dict returned by _fetch_player_boxscore (has mins, pts, reb, ast, fg3m, etc.)
    """
    notes = []
    try:
        pstats  = get_player_stats(player) or {}
        avg_key, pred_key = _STAT_AVG_KEY.get(stat, (None, None))
        avg_val = None
        if avg_key:
            raw = pstats.get(pred_key) if pred_key else None
            if raw is None:
                raw = pstats.get(avg_key)
            if raw is not None:
                avg_val = float(raw)

        # ── Minutes check ──────────────────────────────────────────
        mins_played = float(bdl_actual.get("min") or bdl_actual.get("mins") or 0)
        avg_mins    = float(pstats.get("avg_mins") or 0)
        if mins_played > 0 and mins_played < 22:
            notes.append(f"⏱ Only {mins_played:.0f} min played — foul trouble or early DNP")
        elif avg_mins > 0 and mins_played > 0 and mins_played < avg_mins - 6:
            notes.append(f"⏱ Short night: {mins_played:.0f} min vs {avg_mins:.0f} avg — limited role")

        # ── Line vs season average ─────────────────────────────────
        if avg_val is not None and avg_val > 0:
            if line_val > avg_val * 1.12:
                notes.append(
                    f"📌 Sharp line: {line_val:.1f} set {((line_val/avg_val)-1)*100:.0f}% "
                    f"above season avg ({avg_val:.1f}) — bookmakers inflated this"
                )
            elif line_val <= avg_val * 0.92:
                notes.append(
                    f"📌 Line was fair: {line_val:.1f} vs {avg_val:.1f} avg — "
                    f"variance miss, not a bad bet"
                )
            else:
                notes.append(f"📌 Line near avg ({avg_val:.1f}) — marginal spot")

        # ── Off-night check ────────────────────────────────────────
        if avg_val is not None and avg_val > 0 and actual_val < avg_val * 0.75:
            notes.append(
                f"📉 Off-night: {actual_val:.0f} vs {avg_val:.1f} avg — "
                f"cold game, not a model failure"
            )

        # ── Miss margin ────────────────────────────────────────────
        gap = line_val - actual_val
        if 0 < gap <= 2:
            notes.append(f"📏 Close miss by {gap:.0f} — would've hit on most nights")
        elif gap > 8:
            notes.append(f"🚫 Blowout miss ({gap:.0f} short) — consider fading this spot")

        # ── Default verdict if no notes ────────────────────────────
        if not notes:
            notes.append("📊 Standard variance miss — no red flags")
    except Exception as e:
        notes.append(f"_(grade error: {e})_")
    return notes


def _format_check_result(res, include_grades=True):
    """Format pick check result for Telegram."""
    n_legs    = len(res["legs"])
    is_parlay = n_legs > 1
    lines     = [f"📋 *Pick #{res['pick_id']}* — {res['picked_at_et']}"]
    if is_parlay:
        lines.append(f"🎯 {n_legs}-Leg Parlay\n")

    hits_total = 0
    legs_with_data = 0
    for lr in res["legs"]:
        actual = lr["actual"]
        if not actual:
            lines.append(f"⚠️ *{lr['player']}* — no game data found")
            continue
        legs_with_data += 1
        leg_hit = len(lr["misses"]) == 0 and len(lr["hits"]) > 0
        if leg_hit:
            hits_total += 1
        icon = "✅" if leg_hit else "❌"
        lines.append(f"{icon} *{actual['full_name']}*")
        for entry in lr["hits"] + lr["misses"]:
            hi   = "✅" if entry["hit"] else "❌"
            si   = _STAT_ICON.get(entry["stat"], "📊")
            av   = entry["actual"]
            disp = int(av) if av == int(av) else round(av, 1)
            lines.append(f"  {hi} {si} {disp} {entry['stat']} (needed {entry['line']:.0f}+)")
            # Grade missed legs
            if include_grades and not entry["hit"]:
                grade_notes = _grade_miss(
                    lr["player"],
                    entry["stat"],
                    float(entry["line"]),
                    float(entry["actual"]),
                    actual,
                )
                for note in grade_notes:
                    lines.append(f"    {note}")

    if legs_with_data:
        lines.append("")
        if is_parlay:
            if hits_total == legs_with_data:
                lines.append("✅ *PARLAY HIT*")
            else:
                lines.append(f"❌ *PARLAY LOSS* — {hits_total}/{legs_with_data} legs hit")
        else:
            lines.append("✅ *WIN*" if res["all_hit"] else "❌ *LOSS*")
    else:
        lines.append("\n⚠️ No game data yet — game may not have started")

    if res.get("existing_result"):
        lines.append(f"_Already settled: {res['existing_result']}_")
    else:
        lines.append("_(Not recorded to win/loss record)_")
    return "\n".join(lines)


def cmd_checkpick(chat_id, raw):
    """
    /checkpick <id>
    Pulls real BDL box scores for the game date, checks each leg, reports result.
    Does NOT update win/loss record.
    """
    raw = raw.strip()
    if not raw:
        reply(chat_id,
            "❌ *Usage:* `/checkpick <id>`\n"
            "Example: `/checkpick 5`\n\n"
            "Fetches real stats from the API and checks every leg of your pick."
        )
        return
    try:
        pick_id = int(raw.split()[0])
    except ValueError:
        reply(chat_id, "❌ Pick ID must be a number. Example: `/checkpick 5`")
        return

    reply(chat_id, f"🔍 Checking pick #{pick_id} against real box scores...")
    res, err = _check_pick_result(pick_id)
    if err:
        reply(chat_id, f"⚠️ {err}")
        return
    reply(chat_id, _format_check_result(res))


def _nightly_pick_check():
    """
    Per-game auto-check: runs every bot cycle (every ~10 min).
    As soon as BDL box scores appear for ALL legs of a pick, sends result to admin.
    Skips picks already reported. Does NOT update win/loss record.

    Fallback sweep: also runs once at 1–3 AM ET for any picks still unresolved.
    """
    global _auto_checked_picks, _nightly_check_sent
    import zoneinfo as _zi_npc
    try:
        et_now = datetime.now(_zi_npc.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    # Only run between 4 PM and 4 AM ET (game window)
    if not (16 <= et_now.hour or et_now.hour < 4):
        return

    # Load all unsettled picks from the last 2 days
    try:
        conn = _db_conn()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """SELECT id FROM feed_picks
               WHERE result IS NULL
               ORDER BY id DESC LIMIT 30"""
        )
        ids = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[AutoCheck] DB error: {e}")
        return

    # Filter out already-reported picks
    ids = [pid for pid in ids if pid not in _auto_checked_picks]
    if not ids:
        return

    for pid in ids:
        try:
            res, err = _check_pick_result(pid)
            if err or not res:
                continue

            legs_with_player = [lr for lr in res["legs"] if lr["player"]]
            if not legs_with_player:
                continue

            notified = _auto_notified_misses.setdefault(pid, set())
            is_parlay = len(legs_with_player) > 1

            # ── Fire miss alerts as soon as each leg's game ends ─────────────
            for lr in legs_with_player:
                if lr["actual"] is None:
                    continue  # game not done yet
                for entry in lr["misses"]:
                    key = (lr["player"], entry["stat"])
                    if key in notified:
                        continue  # already alerted
                    notified.add(key)
                    av   = entry["actual"]
                    disp = int(av) if av == int(av) else round(av, 1)
                    si   = _STAT_ICON.get(entry["stat"], "📊")
                    msg  = (
                        f"❌ *Leg missed — Pick #{pid}*\n\n"
                        f"👤 {lr['actual']['full_name']}\n"
                        f"  {si} {disp} {entry['stat']} "
                        f"(needed {entry['line']:.0f}+)\n"
                    )
                    # Add grading analysis
                    try:
                        grade_notes = _grade_miss(
                            lr["player"],
                            entry["stat"],
                            float(entry["line"]),
                            float(entry["actual"]),
                            lr["actual"],
                        )
                        if grade_notes:
                            msg += "\n*Why it missed:*\n"
                            msg += "\n".join(f"  {n}" for n in grade_notes)
                    except Exception:
                        pass
                    if is_parlay:
                        msg += "\n\n🚫 *Parlay is dead*"
                    reply(ADMIN_ID, msg)
                    time.sleep(0.3)
                    print(f"[AutoCheck] Pick #{pid} — miss: {lr['player']} {entry['stat']}")

            # ── Final summary once ALL legs have data ─────────────────────────
            all_have_data = all(lr["actual"] is not None for lr in legs_with_player)
            if all_have_data and pid not in _auto_checked_picks:
                _auto_checked_picks.add(pid)
                if res["all_hit"]:
                    reply(ADMIN_ID,
                        f"✅ *Pick #{pid} — ALL LEGS HIT*\n\n"
                        + _format_check_result(res, include_grades=False)
                    )
                    print(f"[AutoCheck] Pick #{pid} — WIN")
                else:
                    reply(ADMIN_ID,
                        f"📋 *Pick #{pid} — Final Grade*\n\n"
                        + _format_check_result(res, include_grades=True)
                    )
                    print(f"[AutoCheck] Pick #{pid} — LOSS (graded)")
                time.sleep(0.3)

        except Exception as e:
            print(f"[AutoCheck] Error on pick #{pid}: {e}")


def handle_commands():
    """Background thread: polls Telegram for commands and replies."""
    global _cmd_offset
    if not BOT_TOKEN:
        return
    print("[commands] Listener started")
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": _cmd_offset, "timeout": 30, "allowed_updates": ["message", "callback_query"]},
                timeout=40,
            )
            updates = r.json().get("result", [])
            for upd in updates:
                _cmd_offset = upd["update_id"] + 1

                # ── Callback query (inline keyboard button taps) ──────────
                cbq = upd.get("callback_query")
                if cbq:
                    cbq_id   = cbq.get("id")
                    cbq_data = cbq.get("data", "")
                    cbq_chat = cbq.get("from", {}).get("id")
                    cbq_msg  = cbq.get("message", {})
                    cbq_mid  = cbq_msg.get("message_id")

                    if cbq_data == "feedpick_confirm" and str(cbq_chat) == str(ADMIN_ID):
                        pending = _pending_feedpicks.pop(str(cbq_chat), None)
                        if not pending:
                            answer_callback_query(cbq_id, "⚠️ Pick expired — please re-send.")
                        else:
                            try:
                                conn = _db_conn()
                                cur  = conn.cursor()
                                cur.execute(
                                    """INSERT INTO feed_picks
                                       (pick_text, logged_at_et, picked_at_et, is_past, admin_id)
                                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                                    (pending["pick_text"], pending["logged_str"],
                                     pending["picked_str"], pending["is_past"], int(cbq_chat))
                                )
                                pick_id = cur.fetchone()[0]
                                conn.commit(); cur.close(); conn.close()
                                n_legs = len(pending["legs"])
                                edit_message_text(cbq_chat, cbq_mid,
                                    f"✅ *Pick #{pick_id} logged*\n\n"
                                    f"{'🎯 *' + str(n_legs) + '-Leg Parlay*' + chr(10) + chr(10) if n_legs > 1 else ''}"
                                    f"🕐 *Pick time:* {pending['picked_str']}\n"
                                    f"📌 *Status:* {'✅ Already played' if pending['is_past'] else '⏳ Upcoming'}"
                                )
                                answer_callback_query(cbq_id, f"Pick #{pick_id} saved!")
                                print(f"[FeedPick] Confirmed #{pick_id} by {cbq_chat}")
                            except Exception as _ce:
                                answer_callback_query(cbq_id, "⚠️ DB error — try again.")
                                print(f"[FeedPick] confirm error: {_ce}")

                    elif cbq_data == "feedpick_edit" and str(cbq_chat) == str(ADMIN_ID):
                        _editing_feedpick[str(cbq_chat)] = True
                        edit_message_text(cbq_chat, cbq_mid,
                            "✏️ *Edit Pick*\n\nSend me the corrected pick text now:"
                        )
                        answer_callback_query(cbq_id)

                    elif cbq_data == "delfeed_cancel":
                        answer_callback_query(cbq_id, "Cancelled.")
                        try:
                            edit_message_text(cbq_chat, cbq_mid, "❌ Delete cancelled.")
                        except Exception:
                            pass

                    elif cbq_data.startswith("delfeed_") and str(cbq_chat) == str(ADMIN_ID):
                        try:
                            del_id = int(cbq_data.split("_")[1])
                            conn   = _db_conn()
                            if conn:
                                cur = conn.cursor()
                                cur.execute("DELETE FROM feed_picks WHERE id = %s", (del_id,))
                                conn.commit(); cur.close(); conn.close()
                                answer_callback_query(cbq_id, f"Pick #{del_id} deleted.")
                                edit_message_text(cbq_chat, cbq_mid,
                                    f"🗑 *Feed Pick #{del_id} deleted.*")
                            else:
                                answer_callback_query(cbq_id, "⚠️ DB unavailable.")
                        except Exception as _de:
                            answer_callback_query(cbq_id, "⚠️ Error deleting pick.")
                            print(f"[delfeed] error: {_de}")
                    continue

                # ── Regular message ───────────────────────────────────────
                msg = upd.get("message", {})
                raw_text = msg.get("text", "").strip()
                chat_id  = msg.get("chat", {}).get("id")
                if not chat_id or not raw_text:
                    continue

                # If admin is in edit mode, treat next message as new feedpick
                if _editing_feedpick.pop(str(chat_id), False) and str(chat_id) == str(ADMIN_ID):
                    cmd_feedpick(chat_id, raw_text)
                    continue

                # Strip bot username only from the command token (e.g. /feedpick@BotName → /feedpick)
                # but preserve @yesterday, @3:30PM etc. in the rest of the message
                if raw_text.startswith("/"):
                    parts = raw_text.split(" ", 1)
                    cmd_token = parts[0].split("@")[0].lower()
                    text = (cmd_token + " " + parts[1]) if len(parts) > 1 else cmd_token
                else:
                    text = raw_text.lower()
                if not text.startswith("/"):
                    continue
                print(f"[commands] {text} from {chat_id}")
                if text == "/picks":
                    cmd_picks(chat_id)
                elif text == "/record":
                    cmd_record(chat_id)
                elif text == "/thresholds":
                    if str(chat_id) == str(ADMIN_ID):
                        try:
                            from bot.adaptive_thresholds import get_threshold_status
                            _tc = _db_conn()
                            _ts = get_threshold_status(_tc) if _tc else "DB unavailable"
                            if _tc:
                                try: _tc.close()
                                except Exception: pass
                            reply(chat_id, _ts)
                        except Exception as _te:
                            reply(chat_id, f"⚠️ Threshold status error: {_te}")
                    else:
                        reply(chat_id, "❌ Admin only.")
                elif text.startswith("/linemonitor") and str(chat_id) == str(ADMIN_ID):
                    _lm_args = text[len("/linemonitor"):].strip()
                    cmd_line_monitor(chat_id, _lm_args)
                elif text.startswith("/linemonitor") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text == "/bankroll" and str(chat_id) == str(ADMIN_ID):
                    cmd_bankroll(chat_id)
                elif text == "/historyfeed" and str(chat_id) == str(ADMIN_ID):
                    cmd_history_feed(chat_id)
                elif text == "/historybot" and str(chat_id) == str(ADMIN_ID):
                    cmd_history_bot(chat_id)
                elif text == "/historylive" and str(chat_id) == str(ADMIN_ID):
                    cmd_history_live(chat_id)
                elif text == "/calibrate" and str(chat_id) == str(ADMIN_ID):
                    cmd_calibrate(chat_id)
                elif text == "/checkpending" and str(chat_id) == str(ADMIN_ID):
                    cmd_check_pending(chat_id)
                elif text.startswith("/voidpending") and str(chat_id) == str(ADMIN_ID):
                    _vp_arg = text[len("/voidpending"):].strip()
                    cmd_void_pending(chat_id, _vp_arg)
                elif text in ("/bankroll", "/historyfeed", "/historybot", "/historylive",
                              "/calibrate", "/checkpending", "/voidpending") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text == "/schedule":
                    cmd_schedule(chat_id)
                elif text in ("/subscribe", "/vip", "/join"):
                    cmd_subscribe(chat_id)
                elif text == "/updatefeed" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_feed(chat_id)
                elif text == "/updateml" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_ml(chat_id)
                elif text == "/updateprops" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_props(chat_id)
                elif text == "/updatesgp" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_sgp(chat_id)
                elif text == "/updatecgp" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_cgp(chat_id)
                elif text == "/updateedge" and str(chat_id) == str(ADMIN_ID):
                    cmd_update_edge(chat_id)
                elif text in ("/updatefeed", "/updateml", "/updateprops",
                              "/updatesgp", "/updatecgp", "/updateedge"):
                    reply(chat_id, "❌ Admin only.")
                elif text == "/admins" and str(chat_id) == str(ADMIN_ID):
                    cmd_admins(chat_id)
                elif text == "/admins":
                    reply(chat_id, "❌ Admin only.")
                elif text == "/dbstatus" and str(chat_id) == str(ADMIN_ID):
                    cmd_dbstatus(chat_id)
                elif text == "/dbstatus":
                    reply(chat_id, "❌ Admin only.")
                elif text == "/resendall" and str(chat_id) == str(ADMIN_ID):
                    reply(chat_id, "♻️ Resetting today's sent flags and refiring all picks...")
                    try:
                        global _system_sent_date, _sgp_sent_games, _elite_props_sent_games, _cgp_sent_date
                        _system_sent_date       = None
                        _sgp_sent_games         = set()
                        _elite_props_sent_games = set()
                        _cgp_sent_date          = None
                        save_status(0, {
                            "_mem_system_sent_date": "",
                            "_sgp_sent_games":       "",
                            "_elite_props_sent_games": "",
                            "_cgp_sent_date":        "",
                        })
                        reply(chat_id, "✅ Flags cleared — firing run_full_system now...")
                        run_full_system()
                        reply(chat_id, "✅ Resend complete.")
                    except Exception as _rsa_err:
                        reply(chat_id, f"❌ Resend error: {_rsa_err}")
                elif text == "/resendall":
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/analyzedrop") and str(chat_id) == str(ADMIN_ID):
                    reply(chat_id, "🔍 Analyzing bets...")
                    try:
                        _arg = text[len("/analyzedrop"):].strip()
                        _aconn = _db_conn()
                        _acur  = _aconn.cursor()
                        # Get total count first
                        _acur.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM bets")
                        _total, _min_id, _max_id = _acur.fetchone()
                        # Parse range — default to last 70 bets
                        try:
                            if "-" in _arg:
                                _parts = _arg.split("-")
                                _lo, _hi = int(_parts[0]), int(_parts[1])
                            elif _arg.isdigit():
                                _hi = _max_id
                                _lo = _max_id - int(_arg)
                            else:
                                _hi = _max_id
                                _lo = _max_id - 70
                        except Exception:
                            _hi = _max_id
                            _lo = _max_id - 70
                        _acur.execute("""
                            SELECT id, DATE(bet_time), bet_type, pick_category,
                                   player, pick, line, odds, result, confidence
                            FROM bets WHERE id BETWEEN %s AND %s ORDER BY id ASC
                        """, (_lo, _hi))
                        _arows = _acur.fetchall()
                        _acur.close(); _aconn.close()
                        if not _arows:
                            reply(chat_id, f"No bets found (id {_lo}–{_hi}). Total bets: {_total}, ID range: {_min_id}–{_max_id}")
                        else:
                            _wins   = sum(1 for r in _arows if (r[8] or "").lower() == "win")
                            _losses = sum(1 for r in _arows if (r[8] or "").lower() == "loss")
                            _voids  = len(_arows) - _wins - _losses
                            _by_type = {}
                            for r in _arows:
                                _k = r[2] or r[3] or "unknown"
                                _by_type.setdefault(_k, {"w": 0, "l": 0})
                                if (r[8] or "").lower() == "win":  _by_type[_k]["w"] += 1
                                if (r[8] or "").lower() == "loss": _by_type[_k]["l"] += 1
                            _out = [
                                f"📉 Bets #{_lo}-#{_hi} Analysis",
                                f"Total: {len(_arows)} | {_wins}W / {_losses}L / {_voids} void",
                                f"Win rate: {_wins/max(1,_wins+_losses)*100:.1f}%",
                                "", "By type:"
                            ]
                            for _k, _v in sorted(_by_type.items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
                                _pct = _v["w"] / max(1, _v["w"] + _v["l"]) * 100
                                _out.append(f"  {_k}: {_v['w']}W/{_v['l']}L ({_pct:.0f}%)")
                            _out += ["", "Last 15 settled:"]
                            _settled = [r for r in _arows if (r[8] or "").lower() in ("win", "loss")][-15:]
                            for r in _settled:
                                _ico = "✅" if (r[8] or "").lower() == "win" else "❌"
                                _nm = (r[4] or r[5] or "")[:22]
                                _out.append(f"  {_ico} #{r[0]} {_nm} | {r[2] or r[3] or ''}")
                            reply(chat_id, "\n".join(_out))
                    except Exception as _ae:
                        reply(chat_id, f"❌ analyzedrop error: {_ae}")
                elif text == "/todaypicks" and str(chat_id) == str(ADMIN_ID):
                    cmd_today_picks(chat_id)
                elif text == "/todaypicks":
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/editfeedpick") and str(chat_id) == str(ADMIN_ID):
                    raw_arg = text[len("/editfeedpick"):].strip()
                    cmd_edit_feedpick(chat_id, raw_arg)
                elif text.startswith("/editfeedpick"):
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/deletefeedpick") and str(chat_id) == str(ADMIN_ID):
                    raw_arg = text[len("/deletefeedpick"):].strip()
                    cmd_delete_feedpick(chat_id, raw_arg)
                elif text.startswith("/deletefeedpick"):
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/props ") and str(chat_id) == str(ADMIN_ID):
                    cmd_props(chat_id, text[7:].strip())
                elif (text == "/sgp" or text.startswith("/sgp ")) and str(chat_id) == str(ADMIN_ID):
                    cmd_sgp(chat_id, text[4:].strip())
                elif (text == "/parlay" or text.startswith("/parlay ")) and str(chat_id) == str(ADMIN_ID):
                    cmd_parlay(chat_id, text[7:].strip())
                elif text.startswith("/feedpick") and str(chat_id) == str(ADMIN_ID):
                    print(f"[FeedPick] Received from {chat_id}: {text}")
                    raw_arg = text[9:].strip()
                    cmd_feedpick(chat_id, raw_arg)
                elif text.startswith("/feedpick") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text == "/forcesettle" and str(chat_id) == str(ADMIN_ID):
                    cmd_forcesettle(chat_id)
                elif text == "/forcesettle" and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/debugsettle") and str(chat_id) == str(ADMIN_ID):
                    cmd_debugsettle(chat_id)
                elif text.startswith("/debugsettle") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/settle") and str(chat_id) == str(ADMIN_ID):
                    raw_arg = text[7:].strip()
                    cmd_settle(chat_id, raw_arg)
                elif text.startswith("/settle") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text.startswith("/checkpick") and str(chat_id) == str(ADMIN_ID):
                    raw_arg = text[10:].strip()
                    cmd_checkpick(chat_id, raw_arg)
                elif text.startswith("/checkpick") and str(chat_id) != str(ADMIN_ID):
                    reply(chat_id, "❌ Admin only.")
                elif text == "/help" or text == "/start":
                    if str(chat_id) == str(ADMIN_ID):
                        reply(chat_id, (
                            "🤖 *Elite Betting Bot — Admin Panel*\n\n"
                            "👤 *Member Commands*\n"
                            "/picks — Today's picks\n"
                            "/record — All-time win/loss record\n"
                            "/schedule — Tonight's NBA games\n"
                            "/subscribe — Join VIP ($29/mo, 7-day trial)\n\n"
                            "🔐 *Admin Commands*\n"
                            "/admins — System health + full command reference\n"
                            "/todaypicks — Full detailed card of today's picks\n"
                            "/props lakers celtics okc — Prop breakdown per team\n"
                            "/sgp lakers celtics okc — SGP per game (3/5/7 tiers)\n"
                            "/parlay lakers celtics okc — Cross-game parlay (3/5/7 tiers)\n"
                            "/feedpick Over Jokic 27.5 pts -110 — Log a manual pick\n"
                            "/editfeedpick 4 New text here — Edit a feed pick\n"
                            "/deletefeedpick 4 — Delete a feed pick\n"
                            "/settle 4 win — Mark pick #4 as won\n"
                            "/settle 4 loss — Mark pick #4 as lost\n"
                            "/linemonitor start — Live EV line scanner\n"
                            "/bankroll — Current bankroll status\n"
                            "/historyfeed — Manual picks logged via /feedpick\n"
                            "/historybot — Bot engine auto-generated picks\n"
                            "/historylive — Current linemonitor session tickets\n"
                            "/checkpending — All unsettled picks grouped by date\n"
                            "/voidpending 2026-04-02 — Void bad picks for a date\n"
                            "/voidpending — Void ALL pending picks + corrupt entries\n"
                            "/calibrate — Force model recalibration\n"
                            "/admins — System health panel\n\n"
                            "📡 *Live Update Commands (Admin)*\n"
                            "/updatefeed — Live status of manual feedpicks\n"
                            "/updateml — Live status of bot ML/Spread/Total picks\n"
                            "/updateprops — Live status of elite prop picks\n"
                            "/updatesgp — Live status of SGP legs\n"
                            "/updatecgp — Live status of cross-game parlay legs\n"
                            "/updateedge — Live status of EdgeFade7 picks\n"
                        ))
                    else:
                        reply(chat_id, (
                            "🤖 *Elite Betting Bot*\n\n"
                            "/picks — Today's picks\n"
                            "/record — All-time win/loss record\n"
                            "/schedule — Tonight's NBA games\n"
                            "/subscribe — Join VIP ($29/mo, 7-day trial)\n"
                        ))
        except Exception as e:
            import traceback
            print(f"[commands] Error: {e}\n{traceback.format_exc()}")
            try:
                reply(ADMIN_ID, f"⚠️ Bot command error: {e}")
            except Exception:
                pass
            time.sleep(5)


# ==========================
# 👤 PLAYER STATS
# ==========================
BDL_API_KEY = os.environ.get("BDL_API_KEY", "")
BDL_BASE = "https://api.balldontlie.io/v1"


def predict_player(stat_list):
    return advanced_predict(stat_list)


def advanced_predict(stats, stat_type=None, **kwargs):
    """
    Predict a player's stat total from their real BDL game log.
    No artificial boosts — pure weighted average of actual numbers with
    a light trend signal derived from real recent performance.

    If stat_type is provided (e.g. "points", "rebounds"), the bot applies the
    bias correction it has learned from past prediction errors for that stat type.
    For example: if the bot has historically over-predicted points by 2.1, the
    correction subtracts 2.1 from every future points prediction automatically.
    """
    if not stats or len(stats) < 5:
        return None

    last5 = [float(x) for x in stats[:5]]
    avg   = sum(last5) / len(last5)

    # Weighted average — recent games count more (1/1, 1/2, 1/3 …)
    weights = [1 / (i + 1) for i in range(len(last5))]
    wt_avg  = sum(v * w for v, w in zip(last5, weights)) / sum(weights)

    # Trend: positive means player is improving recently
    trend = last5[0] - last5[-1]

    # Blend simple avg (stability) + weighted avg (recency) + trend signal
    prediction = (avg * 0.4) + (wt_avg * 0.6) + (trend * 0.10)

    # Apply learned bias correction for this stat type
    # error = prediction - actual; so corrected = prediction - error
    if stat_type:
        ld = load_learning_data()
        bias_map = ld.get("prediction_bias", {})
        bias = bias_map.get(stat_type, 0.0)
        if bias != 0.0:
            prediction = prediction - bias

    return round(float(prediction), 1)


def ml_predict(stats, context=None):
    """
    Predict a player's stat output from their real BDL game log.
    No artificial boosts or nudges — just the player's actual recent numbers.
    """
    if len(stats.get("pts", [])) < 5:
        return None
    return advanced_predict(stats["pts"])


def _bdl_get(url):
    import urllib.request as _ur
    import json as _json
    hdrs = {"Authorization": BDL_API_KEY} if BDL_API_KEY else {}
    try:
        req = _ur.Request(url, headers=hdrs)
        with _ur.urlopen(req, timeout=10) as resp:
            return _json.loads(resp.read())
    except Exception as e:
        code = getattr(e, "code", None)
        if code == 404:
            return {"data": []}
        raise

# ==========================
# 📡 ESPN LIVE DATA
# ==========================
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

_injury_cache     = {}
_injury_cache_ts  = 0
_INJURY_TTL       = 3600   # re-fetch at most once per hour


def _espn_get(url):
    import urllib.request as _ur
    import json as _json
    try:
        with _ur.urlopen(url, timeout=10) as r:
            return _json.loads(r.read())
    except Exception as e:
        print(f"_espn_get error: {e}")
        return {}


def get_espn_injuries():
    """
    Return {lower-case player name: {status, comment, team}} from ESPN.
    Falls back to BDL inactive-player list if ESPN errors out.
    All existing callers stay unchanged — fallback is transparent.
    """
    global _injury_cache, _injury_cache_ts
    now = time.time()
    if now - _injury_cache_ts < _INJURY_TTL and _injury_cache:
        return _injury_cache
    result = {}
    espn_ok = False
    try:
        data = _espn_get(f"{ESPN_BASE}/injuries")
        for team_block in data.get("injuries", []):
            team_name = team_block.get("team", {}).get("displayName", "").lower()
            for inj in team_block.get("injuries", []):
                ath  = inj.get("athlete", {})
                name = f"{ath.get('firstName','')} {ath.get('lastName','')}".strip()
                if name:
                    result[name.lower()] = {
                        "status":  inj.get("status", "Unknown"),
                        "comment": inj.get("shortComment", ""),
                        "team":    team_name,
                    }
        espn_ok = True
        print(f"[ESPN] Loaded {len(result)} injuries")
    except Exception as e:
        print(f"get_espn_injuries error: {e} — will try BDL fallback")

    # ── BDL fallback: supplement or replace ESPN when ESPN fails ────────────
    if not espn_ok or not result:
        try:
            if BDL_API_KEY:
                bdl_inactive = _bdl_get(
                    f"{BDL_BASE}/players?per_page=100&active=false"
                ).get("data", [])
                for p in bdl_inactive:
                    name = f"{p.get('first_name','')} {p.get('last_name','')}".strip().lower()
                    if name and name not in result:
                        result[name] = {
                            "status":  "Out",
                            "comment": "Inactive (BDL)",
                            "team":    (p.get("team") or {}).get("full_name", "").lower(),
                        }
                print(f"[BDL] Injury fallback: +{len(bdl_inactive)} inactive players")
        except Exception as _be:
            print(f"[BDL] injury fallback failed: {_be}")

    if result:
        _injury_cache    = result
        _injury_cache_ts = now
    return result


def get_espn_team_stats(team_full_name, days=7):
    """
    Pull player boxscore stats from ESPN for the past `days` days.
    Returns {player_display_name: {pts:[], reb:[], ast:[], fg3:[], mins:[]}}
    with most-recent game first in each list.
    """
    keyword   = team_full_name.split()[-1].lower()   # e.g. "Celtics"
    end_date  = datetime.now().date()
    by_player = {}

    for offset in range(1, days + 1):
        date     = end_date - timedelta(days=offset)
        date_str = date.strftime("%Y%m%d")
        board    = _espn_get(f"{ESPN_BASE}/scoreboard?dates={date_str}")

        for event in board.get("events", []):
            comp  = event.get("competitions", [{}])[0]
            teams = comp.get("competitors", [])
            if not any(keyword in t.get("team", {}).get("displayName", "").lower()
                       for t in teams):
                continue

            event_id = event.get("id")
            box      = _espn_get(f"{ESPN_BASE}/summary?event={event_id}").get("boxscore", {})

            for team_data in box.get("players", []):
                tname = team_data.get("team", {}).get("displayName", "")
                if keyword not in tname.lower():
                    continue

                for section in team_data.get("statistics", []):
                    keys = section.get("keys", [])
                    try:
                        idx_min = keys.index("minutes")
                        idx_pts = keys.index("points")
                        idx_reb = keys.index("rebounds")
                        idx_ast = keys.index("assists")
                        idx_3pt = next(
                            i for i, k in enumerate(keys)
                            if "threePoint" in k and "Made-" in k
                        )
                    except (ValueError, StopIteration):
                        try:
                            idx_min = keys.index("minutes")
                            idx_pts = keys.index("points")
                            idx_reb = keys.index("rebounds")
                            idx_ast = keys.index("assists")
                            idx_3pt = next(
                                i for i, k in enumerate(keys)
                                if "three" in k.lower() and i < len(keys)
                            )
                        except Exception:
                            continue

                    for athlete in section.get("athletes", []):
                        ath_name  = athlete.get("athlete", {}).get("displayName", "")
                        stats_raw = athlete.get("stats", [])
                        if not stats_raw or len(stats_raw) <= max(idx_pts, idx_reb, idx_ast, idx_3pt):
                            continue
                        try:
                            ms  = stats_raw[idx_min]
                            mins = (float(ms.split(":")[0]) + float(ms.split(":")[1]) / 60
                                    if ":" in ms else float(ms))
                            if mins < 10:
                                continue
                            pts  = float(stats_raw[idx_pts])
                            reb  = float(stats_raw[idx_reb])
                            ast  = float(stats_raw[idx_ast])
                            fg3r = str(stats_raw[idx_3pt])
                            fg3  = float(fg3r.split("-")[0]) if "-" in fg3r else float(fg3r)
                        except (ValueError, IndexError):
                            continue

                        if ath_name not in by_player:
                            by_player[ath_name] = {"pts": [], "reb": [], "ast": [], "fg3": [], "mins": []}
                        by_player[ath_name]["pts"].insert(0, pts)
                        by_player[ath_name]["reb"].insert(0, reb)
                        by_player[ath_name]["ast"].insert(0, ast)
                        by_player[ath_name]["fg3"].insert(0, fg3)
                        by_player[ath_name]["mins"].insert(0, mins)

    return by_player


def get_team_starters_espn(team_full_name):
    """
    Build Starting Five from ESPN boxscores (fresh data).
    Falls back to empty list so caller can still use BDL.
    """
    try:
        by_player = get_espn_team_stats(team_full_name, days=7)
        if not by_player:
            return []

        def wt_avg(arr):
            if not arr:
                return 0.0
            arr = arr[-10:]
            w   = [1 / (i + 1) for i in range(len(arr))]
            return round(sum(v * wi for v, wi in zip(arr, w)) / sum(w), 1)

        starters = []
        for name, d in by_player.items():
            avg_mins = wt_avg(d["mins"])
            if avg_mins < 15:
                continue
            starters.append({
                "name":     name,
                "avg_mins": avg_mins,
                "pred_pts": predict_player(list(reversed(d["pts"]))) or wt_avg(d["pts"]),
                "pred_reb": predict_player(list(reversed(d["reb"]))) or wt_avg(d["reb"]),
                "pred_ast": predict_player(list(reversed(d["ast"]))) or wt_avg(d["ast"]),
                "pred_fg3": predict_player(list(reversed(d["fg3"]))) or wt_avg(d["fg3"]),
            })

        starters.sort(key=lambda x: x["avg_mins"], reverse=True)
        return starters[:8]
    except Exception as e:
        print(f"get_team_starters_espn error: {e}")
        return []


_espn_scoreboard_cache = {}   # date → list of events

def _get_espn_today_events():
    """Return today's ESPN NBA scoreboard events (cached per hour)."""
    try:
        hour_key = datetime.now().strftime("%Y-%m-%d-%H")
        if _espn_scoreboard_cache.get("key") == hour_key:
            return _espn_scoreboard_cache.get("events", [])
        resp = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            timeout=10
        ).json()
        events = resp.get("events", [])
        _espn_scoreboard_cache["key"]    = hour_key
        _espn_scoreboard_cache["events"] = events
        return events
    except Exception as e:
        print(f"[ESPN scoreboard] error: {e}")
        return []


def get_confirmed_lineup_espn(team_full_name, stats_pool):
    """
    Fetch the official confirmed starting lineup from ESPN game summary.
    Merges confirmed starter names with prediction stats from stats_pool.
    Returns (list_of_starters, confirmed: bool).
    confirmed=True  → official lineup released, use ✅ label
    confirmed=False → lineups not out yet, caller should fall back
    """
    try:
        events = _get_espn_today_events()
        game_id = None
        for event in events:
            for comp in event.get("competitions", []):
                for competitor in comp.get("competitors", []):
                    tn = competitor.get("team", {}).get("displayName", "")
                    if (team_full_name.lower() in tn.lower() or
                            tn.lower() in team_full_name.lower()):
                        game_id = event.get("id")
                        break
                if game_id:
                    break
            if game_id:
                break

        if not game_id:
            return [], False

        summary = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}",
            timeout=10
        ).json()

        # Build a stats lookup from the pool keyed by lowercased name
        stats_by_name = {p["name"].lower(): p for p in stats_pool}

        confirmed = []
        for team_data in summary.get("boxscore", {}).get("players", []):
            tn = team_data.get("team", {}).get("displayName", "")
            if (team_full_name.lower() not in tn.lower() and
                    tn.lower() not in team_full_name.lower()):
                continue
            for stat_group in team_data.get("statistics", []):
                for athlete in stat_group.get("athletes", []):
                    if not athlete.get("starter", False):
                        continue
                    name = athlete.get("athlete", {}).get("displayName", "")
                    if not name:
                        continue
                    base = stats_by_name.get(name.lower(), {})
                    confirmed.append({
                        "name":     name,
                        "avg_mins": base.get("avg_mins", 28.0),
                        "pred_pts": base.get("pred_pts", 0),
                        "pred_reb": base.get("pred_reb", 0),
                        "pred_ast": base.get("pred_ast", 0),
                        "pred_fg3": base.get("pred_fg3", 0),
                        "sub_for":  None,
                    })

        if len(confirmed) >= 5:
            print(f"  [Lineup] ✅ Official confirmed — {team_full_name}")
            return confirmed[:5], True

        return [], False
    except Exception as e:
        print(f"get_confirmed_lineup_espn error: {e}")
        return [], False


def _get_player_stats_espn(player_name):
    """
    ESPN fallback for player stats when BDL returns nothing.
    Mirrors the same dict shape and filter-safety as get_player_stats().
    Uses multiple key-name aliases to handle ESPN API inconsistencies.
    Safe defaults ensure players are never silently dropped by avg_mins/avg_usage filters.
    """
    import urllib.parse as _up

    # ESPN uses inconsistent key names across endpoints — check all known aliases
    _PTS_KEYS  = {"avgPoints",  "pointsPerGame",  "avgPTS"}
    _REB_KEYS  = {"avgRebounds","reboundsPerGame","avgREB","avgTotalRebounds"}
    _AST_KEYS  = {"avgAssists", "assistsPerGame", "avgAST"}
    _FG3_KEYS  = {"avg3PointFieldGoalsMade","avg3PointFGMade","avgThreePointersMade","avg3PM"}
    _MIN_KEYS  = {"avgMinutes", "minutesPerGame", "avgMin", "avgMPG", "mpg"}
    _GP_KEYS   = {"gamesPlayed","GP","games"}

    def _parse_stat(name, value, pts, reb, ast, fg3, mins, games):
        v = float(value or 0)
        if name in _PTS_KEYS:  pts   = v
        elif name in _REB_KEYS: reb  = v
        elif name in _AST_KEYS: ast  = v
        elif name in _FG3_KEYS: fg3  = v
        elif name in _MIN_KEYS: mins = v
        elif name in _GP_KEYS:  games = int(v)
        return pts, reb, ast, fg3, mins, games

    try:
        search_url = (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/athletes"
            f"?limit=5&search={_up.quote(player_name)}"
        )
        resp     = requests.get(search_url, timeout=7, headers={"User-Agent": "Mozilla/5.0"})
        athletes = resp.json().get("athletes", [])
        if not athletes:
            return None

        target   = player_name.lower()
        athlete  = next(
            (a for a in athletes if a.get("fullName", "").lower() == target),
            athletes[0]
        )
        athlete_id = athlete.get("id")
        full_name  = athlete.get("fullName", player_name)
        team_obj   = athlete.get("team") or {}
        team_name  = team_obj.get("displayName", "") if isinstance(team_obj, dict) else ""
        pos_obj    = athlete.get("position") or {}
        position   = pos_obj.get("abbreviation", "") if isinstance(pos_obj, dict) else ""

        avg_pts = avg_reb = avg_ast = avg_fg3 = avg_mins = 0.0
        games = 0

        # ── Try overview endpoint first ───────────────────────────────────
        try:
            ov   = requests.get(
                f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba"
                f"/athletes/{athlete_id}/overview",
                timeout=7, headers={"User-Agent": "Mozilla/5.0"}
            ).json()
            cats = (ov.get("statistics") or {}).get("splits", {}).get("categories", [])
            for cat in cats:
                for stat in cat.get("stats", []):
                    avg_pts, avg_reb, avg_ast, avg_fg3, avg_mins, games = _parse_stat(
                        stat.get("name",""), stat.get("value", 0),
                        avg_pts, avg_reb, avg_ast, avg_fg3, avg_mins, games
                    )
        except Exception:
            pass

        # ── Try /stats endpoint as second source if overview missed values ─
        if avg_pts == 0 or avg_mins == 0:
            try:
                st   = requests.get(
                    f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
                    f"/athletes/{athlete_id}/stats",
                    timeout=7, headers={"User-Agent": "Mozilla/5.0"}
                ).json()
                for split in (st.get("splits") or {}).get("categories", []):
                    for stat in split.get("stats", []):
                        avg_pts, avg_reb, avg_ast, avg_fg3, avg_mins, games = _parse_stat(
                            stat.get("name",""), stat.get("value", 0),
                            avg_pts, avg_reb, avg_ast, avg_fg3, avg_mins, games
                        )
            except Exception:
                pass

        if avg_pts == 0 and avg_reb == 0 and avg_ast == 0:
            return None  # genuinely no data

        # ── Safe defaults so engine filters never silently drop this player ──
        # Anyone in the prop pool is getting meaningful minutes
        if avg_mins < 15:
            avg_mins = 25.0
        # Usage proxy — minimum 10 for any prop-pool player
        avg_usage = max(10.0, round(avg_pts * 0.5 + 6.0, 1))

        print(f"[ESPN fallback] {player_name} → {avg_pts}pts {avg_reb}reb "
              f"{avg_ast}ast {avg_mins}min ({games}g)")
        return {
            "name":       full_name,
            "player_id":  None,
            "position":   position,
            "team":       team_name,
            "pts":  [], "reb": [], "ast": [], "fg3": [],
            "avg_pts":    avg_pts,
            "avg_reb":    avg_reb,
            "avg_ast":    avg_ast,
            "avg_fg3":    avg_fg3,
            "avg_mins":   avg_mins,
            "avg_usage":  avg_usage,
            "pred_pts":   avg_pts,
            "pred_reb":   avg_reb,
            "pred_ast":   avg_ast,
            "pred_fg3":   avg_fg3,
            "games":      games,
            "pts_form":   0.0,
            "reb_form":   0.0,
            "ast_form":   0.0,
            "fg3_form":   0.0,
            "is_hot":     False,
            "is_cold":    False,
            "source":     "espn",
            "confidence_adj": get_player_confidence_adjustment(full_name),
        }
    except Exception as e:
        print(f"[ESPN fallback] {player_name}: {e}")
        return None


def get_player_stats(player_name):
    import time as _time_mod
    _cache_key = player_name.strip().lower()
    _cached = _player_stats_cache.get(_cache_key)
    if _cached:
        _stats, _fetched = _cached
        if _time_mod.time() - _fetched < _PLAYER_STATS_TTL:
            return _stats

    if not BDL_API_KEY:
        print("BDL_API_KEY not set — trying ESPN fallback")
        result = _get_player_stats_espn(player_name)
        if result:
            _player_stats_cache[_cache_key] = (result, _time_mod.time())
        return result

    # BDL search only works with a single word — use first name, then verify full match
    parts = player_name.strip().split()
    search_term = parts[0] if parts else player_name
    import urllib.parse as _up
    search_url = f"{BDL_BASE}/players?search={_up.quote(search_term)}&per_page=10"
    try:
        res = _bdl_get(search_url)
    except Exception as e:
        print(f"[BDL] player search error: {e} — trying ESPN fallback")
        return _get_player_stats_espn(player_name)

    if not res.get("data"):
        print(f"[BDL] no data for {player_name} — trying ESPN fallback")
        return _get_player_stats_espn(player_name)

    # Try to find exact full-name match first, fall back to first result
    target = player_name.lower()
    player = next(
        (p for p in res["data"]
         if f"{p['first_name']} {p['last_name']}".lower() == target),
        res["data"][0]
    )
    player_id = player["id"]
    full_name = f"{player['first_name']} {player['last_name']}"

    try:
        stats_url = f"{BDL_BASE}/stats?player_ids[]={player_id}&seasons[]=2024&per_page=25"
        stats = _bdl_get(stats_url).get("data", [])
        # If thin sample this season, supplement with previous season
        if len(stats) < 8:
            try:
                prev_url  = f"{BDL_BASE}/stats?player_ids[]={player_id}&seasons[]=2023&per_page=20"
                prev_data = _bdl_get(prev_url).get("data", [])
                stats = (stats + prev_data)[:25]
            except Exception:
                pass
    except Exception as e:
        print(f"[BDL] player stats error: {e} — trying ESPN fallback")
        return _get_player_stats_espn(player_name)

    pts  = [g.get("pts", 0)     for g in stats]
    reb  = [g.get("reb", 0)     for g in stats]
    ast  = [g.get("ast", 0)     for g in stats]
    fg3  = [g.get("fg3m", 0)    for g in stats]   # 3-pointers made
    fga  = [g.get("fga",  0)    for g in stats]   # field goal attempts
    fta  = [g.get("fta",  0)    for g in stats]   # free throw attempts
    tov  = [g.get("turnover",0) for g in stats]   # turnovers

    # Parse minutes from each game ("36:25" → 36.4) and average them
    def _parse_min(m):
        try:
            if not m:
                return 0.0
            parts = str(m).split(":")
            return float(parts[0]) + float(parts[1]) / 60 if len(parts) == 2 else float(parts[0])
        except Exception:
            return 0.0

    mins_raw = [g.get("min", "0") for g in stats]
    avg_mins = round(sum(_parse_min(m) for m in mins_raw) / len(mins_raw), 1) if mins_raw else 0.0

    def avg(arr):
        return round(sum(arr) / len(arr), 1) if arr else 0.0

    position  = player.get("position", "") or ""
    team_obj  = player.get("team") or {}
    team_name = team_obj.get("full_name", "") if isinstance(team_obj, dict) else ""

    # Usage proxy per game: FGA + 0.44*FTA + TOV (simplified usage formula)
    # High number = primary ball handler; low = spot-up / role player
    usage_per_game = [
        (fga[i] or 0) + 0.44 * (fta[i] or 0) + (tov[i] or 0)
        for i in range(len(stats))
    ]
    avg_usage = round(avg(usage_per_game), 1) if usage_per_game else 0.0

    # Form score: compare last-5 avg to full-sample avg
    # Positive = hot streak, Negative = cold streak
    def _form_score(arr):
        if len(arr) < 6:
            return 0.0
        full_avg  = sum(arr) / len(arr)
        last5_avg = sum(arr[:5]) / 5
        if full_avg == 0:
            return 0.0
        return round((last5_avg - full_avg) / full_avg, 3)

    pts_form = _form_score(pts)
    reb_form = _form_score(reb)
    ast_form = _form_score(ast)
    fg3_form = _form_score(fg3)

    _result = {
        "name":      full_name,
        "player_id": player_id,
        "position":  position,
        "team":      team_name,
        "pts":  pts,  "reb": reb,  "ast": ast,  "fg3": fg3,
        "avg_pts":  avg(pts),
        "avg_reb":  avg(reb),
        "avg_ast":  avg(ast),
        "avg_fg3":  avg(fg3),
        "avg_mins": avg_mins,
        "avg_usage": avg_usage,
        "pred_pts": predict_player(pts),
        "pred_reb": predict_player(reb),
        "pred_ast": predict_player(ast),
        "pred_fg3": predict_player(fg3),
        "games":    len(stats),
        # Form scores — used to boost/penalise confidence in engine
        "pts_form": pts_form,
        "reb_form": reb_form,
        "ast_form": ast_form,
        "fg3_form": fg3_form,
        "is_hot":   any(f > 0.12 for f in [pts_form, reb_form, ast_form, fg3_form]),
        "is_cold":  all(f < -0.08 for f in [pts_form, reb_form, ast_form, fg3_form]),
        # Historical confidence adjustment from settled picks — 0.0 until ≥5 picks tracked
        "confidence_adj": get_player_confidence_adjustment(full_name),
    }
    import time as _time_mod2
    _player_stats_cache[_cache_key] = (_result, _time_mod2.time())
    return _result


# ==========================
# 🔍 LINEUP INTELLIGENCE
# ==========================

def assess_injury_boost(injuries, props_data):
    """
    When a star player is OUT/Doubtful, find their teammates in the prop pool
    and return a confidence boost map: {player_name_lower: boost_pct}

    Logic: OUT player's usage gets redistributed — teammates inherit production.
    Higher usage star out = bigger boost to teammates.
    """
    boost_map = {}
    if not injuries or not props_data:
        return boost_map

    # Build set of players in tonight's prop pool
    prop_players = set()
    team_players = {}   # team_lower -> [player_names]
    for game in props_data:
        _bks = game.get("bookmakers", [])
        _fd_bk = next((b for b in _bks if b.get("key") == "fanduel"), None)
        for bk in ([_fd_bk] if _fd_bk else []):
            for mkt in bk.get("markets", []):
                for o in mkt.get("outcomes", []):
                    p = o.get("description", "")
                    ht = game.get("home_team", "")
                    at = game.get("away_team", "")
                    if p:
                        prop_players.add(p.lower())
                        for team in [ht, at]:
                            tkey = team.split()[-1].lower()
                            team_players.setdefault(tkey, set()).add(p.lower())

    for player_lower, inj_info in injuries.items():
        status = inj_info.get("status", "")
        if status not in ("Out", "Doubtful"):
            continue
        if player_lower in prop_players:
            continue  # star is in prop pool = not boosting (they might still play)

        # Find which team this OUT player belongs to
        team_key = inj_info.get("team", "").split()[-1] if inj_info.get("team") else ""
        if not team_key:
            continue

        # Find their teammates in the prop pool
        teammates = team_players.get(team_key, set())
        if not teammates:
            continue

        # Boost scales with how significant the OUT player likely was
        # We don't have their usage here so use a flat +6% per OUT star
        boost = 0.06
        for tm in teammates:
            boost_map[tm] = boost_map.get(tm, 0) + boost
            print(f"[InjBoost] {tm} +{int(boost*100)}% — {player_lower} OUT")

    return boost_map


_b2b_cache     = {}
_b2b_cache_ts  = 0
_B2B_TTL       = 3600  # 1 hour


def detect_back_to_back_teams():
    """
    Check ESPN scoreboard for yesterday's games.
    Returns a set of team name keywords (lowercase) that played yesterday.
    Players on these teams get a confidence penalty.
    """
    global _b2b_cache, _b2b_cache_ts
    now = time.time()
    if now - _b2b_cache_ts < _B2B_TTL and _b2b_cache:
        return _b2b_cache

    yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
    try:
        board = _espn_get(f"{ESPN_BASE}/scoreboard?dates={yesterday}")
        b2b_teams = set()
        for event in board.get("events", []):
            for comp in event.get("competitions", []):
                for competitor in comp.get("competitors", []):
                    name = competitor.get("team", {}).get("displayName", "")
                    if name:
                        b2b_teams.add(name.split()[-1].lower())
        _b2b_cache    = b2b_teams
        _b2b_cache_ts = now
        if b2b_teams:
            print(f"[B2B] Teams on back-to-back: {b2b_teams}")
        return b2b_teams
    except Exception as e:
        print(f"[B2B] Error: {e}")
        return set()


# ==========================
# 📡 BALL DON'T LIE — GAME DATA
# ==========================
def get_games_bdl(date=None):
    if not date:
        date = datetime.now().date()
    try:
        return _bdl_get(f"{BDL_BASE}/games?dates[]={date}").get("data", [])
    except Exception as e:
        print(f"get_games_bdl error: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED DATA LAYER  — BDL → ESPN → NBA CDN  (automatic fallback chain)
# All three sources return a consistent dict format so callers are source-agnostic.
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_bdl_game(g):
    """Normalize a BDL game dict to the shared format."""
    home = g.get("home_team", {})
    away = g.get("visitor_team", {})
    status_raw = (g.get("status") or "").strip()
    if status_raw in ("", "TBD"):
        state = "pre"
    elif status_raw == "Final":
        state = "post"
    else:
        state = "in"
    return {
        "id":          str(g.get("id", "")),
        "home_team":   home.get("full_name", ""),
        "away_team":   away.get("full_name", ""),
        "home_score":  int(g.get("home_team_score") or 0),
        "away_score":  int(g.get("visitor_team_score") or 0),
        "status":      state,
        "period":      int(g.get("period") or 0),
        "clock":       g.get("time", ""),
        "tip_time":    status_raw if state == "pre" else "",
        "source":      "bdl",
    }


def _normalize_espn_game(g):
    """Normalize an ESPN _fetch_bdl_live_games() dict to the shared format."""
    return {
        "id":          str(g.get("game_id", "")),
        "home_team":   g.get("home", ""),
        "away_team":   g.get("away", ""),
        "home_score":  int(g.get("home_score") or 0),
        "away_score":  int(g.get("away_score") or 0),
        "status":      g.get("status", "pre"),
        "period":      int(g.get("period") or 0),
        "clock":       g.get("time", ""),
        "tip_time":    "",
        "source":      "espn",
    }


def _normalize_cdn_game(g):
    """Normalize an NBA CDN scoreboard game to the shared format."""
    home = g.get("homeTeam", {})
    away = g.get("awayTeam", {})
    cdn_status = int(g.get("gameStatus") or 1)   # 1=pre, 2=in, 3=post
    state = {1: "pre", 2: "in", 3: "post"}.get(cdn_status, "pre")
    home_name = f"{home.get('teamCity','')} {home.get('teamName','')}".strip()
    away_name = f"{away.get('teamCity','')} {away.get('teamName','')}".strip()
    clock_raw = g.get("gameClock", "")
    # CDN clock format: "PT04M32.00S" → "4:32"
    clock_str = ""
    try:
        import re as _re
        m = _re.match(r"PT(\d+)M([\d.]+)S", clock_raw)
        if m:
            clock_str = f"{int(m.group(1))}:{int(float(m.group(2))):02d}"
    except Exception:
        clock_str = clock_raw
    return {
        "id":          str(g.get("gameId", "")),
        "home_team":   home_name,
        "away_team":   away_name,
        "home_score":  int(home.get("score") or 0),
        "away_score":  int(away.get("score") or 0),
        "status":      state,
        "period":      int(g.get("period") or 0),
        "clock":       clock_str,
        "tip_time":    g.get("gameStatusText", ""),
        "source":      "cdn",
    }


def get_todays_games(date=None):
    """
    Unified game fetcher — BDL → ESPN → NBA CDN.
    Returns list of normalized game dicts (see _normalize_*_game above).
    Automatically falls back to the next source if the primary returns empty.
    Always returns the richest available data, never an empty list if any
    source has games.
    """
    if not date:
        date = datetime.now().date()

    # ── Source 1: BDL ────────────────────────────────────────────────
    try:
        bdl_raw = _bdl_get(f"{BDL_BASE}/games?dates[]={date}").get("data", [])
        if bdl_raw:
            games = [_normalize_bdl_game(g) for g in bdl_raw]
            print(f"[DataLayer] get_todays_games: {len(games)} games from BDL")
            return games
    except Exception as _e:
        print(f"[DataLayer] BDL games failed: {_e}")

    # ── Source 2: ESPN ───────────────────────────────────────────────
    try:
        espn_raw = _fetch_bdl_live_games()   # already uses ESPN scoreboard
        if espn_raw:
            games = [_normalize_espn_game(g) for g in espn_raw]
            print(f"[DataLayer] get_todays_games: {len(games)} games from ESPN")
            return games
    except Exception as _e:
        print(f"[DataLayer] ESPN games failed: {_e}")

    # ── Source 3: NBA CDN ────────────────────────────────────────────
    try:
        cdn_raw = _cdn_scoreboard()
        if cdn_raw:
            games = [_normalize_cdn_game(g) for g in cdn_raw]
            print(f"[DataLayer] get_todays_games: {len(games)} games from NBA CDN")
            return games
    except Exception as _e:
        print(f"[DataLayer] CDN games failed: {_e}")

    print("[DataLayer] get_todays_games: ALL sources failed — returning []")
    return []


def get_injuries():
    """
    Unified injury fetcher — ESPN primary, BDL active-status supplement.
    Returns {lower-case player name: {status, comment, team}} merged from both.
    ESPN is authoritative; BDL fills gaps for players ESPN doesn't list.
    """
    merged = {}

    # ── Source 1: ESPN ───────────────────────────────────────────────
    try:
        espn_inj = get_espn_injuries()
        merged.update(espn_inj)
        print(f"[DataLayer] get_injuries: {len(espn_inj)} from ESPN")
    except Exception as _e:
        print(f"[DataLayer] ESPN injuries failed: {_e}")

    # ── Source 2: BDL active=false players ──────────────────────────
    try:
        if BDL_API_KEY:
            bdl_players = _bdl_get(
                f"{BDL_BASE}/players?per_page=100&active=true"
            ).get("data", [])
            # BDL doesn't have an injury field — but inactive players are a signal
            bdl_inactive = _bdl_get(
                f"{BDL_BASE}/players?per_page=100&active=false"
            ).get("data", [])
            for p in bdl_inactive:
                name = f"{p.get('first_name','')} {p.get('last_name','')}".strip().lower()
                if name and name not in merged:
                    merged[name] = {
                        "status":  "Out",
                        "comment": "Inactive (BDL)",
                        "team":    (p.get("team") or {}).get("full_name", "").lower(),
                    }
            print(f"[DataLayer] get_injuries: +{len(bdl_inactive)} inactive from BDL")
    except Exception as _e:
        print(f"[DataLayer] BDL injuries skipped: {_e}")

    return merged


def get_live_scores():
    """
    Unified live score fetcher — NBA CDN primary (real-time), ESPN fallback.
    Returns list of normalized game dicts, same format as get_todays_games().
    CDN is preferred for live data because it updates every ~10s; ESPN lags ~30s.
    """
    # ── Source 1: NBA CDN ────────────────────────────────────────────
    try:
        cdn_raw = _cdn_scoreboard()
        if cdn_raw:
            games = [_normalize_cdn_game(g) for g in cdn_raw]
            in_progress = [g for g in games if g["status"] == "in"]
            print(f"[DataLayer] get_live_scores: {len(games)} games from CDN "
                  f"({len(in_progress)} live)")
            return games
    except Exception as _e:
        print(f"[DataLayer] CDN live scores failed: {_e}")

    # ── Source 2: ESPN ───────────────────────────────────────────────
    try:
        espn_raw = _fetch_bdl_live_games()
        if espn_raw:
            games = [_normalize_espn_game(g) for g in espn_raw]
            in_progress = [g for g in games if g["status"] == "in"]
            print(f"[DataLayer] get_live_scores: {len(games)} games from ESPN "
                  f"({len(in_progress)} live)")
            return games
    except Exception as _e:
        print(f"[DataLayer] ESPN live scores failed: {_e}")

    # ── Source 3: BDL ────────────────────────────────────────────────
    try:
        bdl_raw = get_games_bdl()
        if bdl_raw:
            games = [_normalize_bdl_game(g) for g in bdl_raw]
            print(f"[DataLayer] get_live_scores: {len(games)} games from BDL (delayed)")
            return games
    except Exception as _e:
        print(f"[DataLayer] BDL live scores failed: {_e}")

    print("[DataLayer] get_live_scores: ALL sources failed — returning []")
    return []


def get_recent(team_id):
    try:
        end   = datetime.now().date()
        start = end - timedelta(days=60)
        url   = (
            f"{BDL_BASE}/games"
            f"?team_ids[]={team_id}&per_page=15"
            f"&start_date={start}&end_date={end}"
        )
        return _bdl_get(url).get("data", [])
    except Exception as e:
        print(f"get_recent error: {e}")
        return []


# ==========================
# 🧠 ELO SYSTEM
# ==========================
# Pre-seeded with 2024-25 NBA standings (updated each season)
ELO = {
    # Elite tier
    "Oklahoma City Thunder": 1645,
    "Cleveland Cavaliers":   1635,
    "Boston Celtics":        1625,
    "Denver Nuggets":        1585,
    # Good tier
    "Houston Rockets":       1575,
    "New York Knicks":       1565,
    "Memphis Grizzlies":     1555,
    "Golden State Warriors": 1545,
    "Los Angeles Clippers":  1540,
    "Minnesota Timberwolves":1535,
    # Above average
    "Dallas Mavericks":      1525,
    "Milwaukee Bucks":       1518,
    "Atlanta Hawks":         1510,
    "San Antonio Spurs":     1505,
    "Sacramento Kings":      1500,
    "Indiana Pacers":        1495,
    "Miami Heat":            1490,
    "Phoenix Suns":          1485,
    "Orlando Magic":         1478,
    "Brooklyn Nets":         1472,
    # Below average
    "Los Angeles Lakers":    1462,
    "Utah Jazz":             1452,
    "Philadelphia 76ers":    1448,
    "Toronto Raptors":       1442,
    "New Orleans Pelicans":  1438,
    "Portland Trail Blazers":1430,
    # Lottery tier
    "Chicago Bulls":         1415,
    "Detroit Pistons":       1402,
    "Charlotte Hornets":     1392,
    "Washington Wizards":    1375,
}


def get_elo(team):
    # Fuzzy match — handles slight name differences from API
    if team in ELO:
        return ELO[team]
    for k in ELO:
        if k.split()[-1] == team.split()[-1]:  # match by city name
            return ELO[k]
    return 1500


def update_elo(team_a, team_b, winner):
    K = 20
    Ra, Rb = get_elo(team_a), get_elo(team_b)
    Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))
    Sa = 1 if winner == team_a else 0
    ELO[team_a] = Ra + K * (Sa - Ea)
    ELO[team_b] = Rb + K * ((1 - Sa) - (1 - Ea))
    # Persist updated ratings immediately so they survive restarts
    try:
        import json as _json
        conn = _db_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO learning_data (key, value, updated_at)
                VALUES ('elo_ratings', %s::jsonb, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (_json.dumps(ELO),))
            conn.commit()
            cur.close()
            conn.close()
    except Exception as _elo_err:
        print(f"[ELO] DB save error: {_elo_err}")


def get_h2h(home_id, away_id):
    """
    Return the average point differential (home pts - away pts) across
    the last 10 head-to-head meetings between these two teams (2 seasons).
    Returns 0.0 when no history is available.
    """
    try:
        end   = datetime.now().date()
        start = end - timedelta(days=730)
        url   = (
            f"{BDL_BASE}/games"
            f"?team_ids[]={home_id}&team_ids[]={away_id}"
            f"&per_page=10&start_date={start}&end_date={end}"
        )
        games = _bdl_get(url).get("data", [])
        diffs = []
        for g in games:
            hs = g.get("home_team_score", 0) or 0
            vs = g.get("visitor_team_score", 0) or 0
            if not hs and not vs:
                continue
            if g["home_team"]["id"] == home_id:
                diffs.append(hs - vs)
            else:
                diffs.append(vs - hs)
        return round(sum(diffs) / len(diffs), 1) if diffs else 0.0
    except Exception as e:
        print(f"get_h2h error: {e}")
        return 0.0


# ==========================
# 📊 TEAM STATS
# ==========================
def team_stats(team_id, team_name=None):
    # ELO-based smart fallback: better teams get better pts/def estimates
    _elo = ELO.get(team_name, 1450) if team_name else 1450
    _elo_pts = round(100 + (_elo - 1450) * 0.055, 1)
    _elo_def = round(103 + (1450 - _elo) * 0.035, 1)
    _blank = {
        "pts": _elo_pts, "def": _elo_def, "l5": _elo_pts, "rest": 1,
        "b2b": False, "b2b_road": False, "sos": 113.0, "team_id": team_id,
    }
    games = get_recent(team_id)
    if not games:
        print(f"[Stats] No BDL data for {team_name or team_id} — using ELO fallback (pts={_elo_pts}, def={_elo_def})")
        return _blank

    pts, allowed, opp_scores = [], [], []
    last_date     = None
    last_was_road = False

    for g in games:
        if not g.get("home_team_score") and not g.get("visitor_team_score"):
            continue
        try:
            game_date = datetime.fromisoformat(g["date"].replace("Z", "").split(".")[0])
        except Exception:
            continue
        if last_date is None or game_date > last_date:
            last_date     = game_date
            last_was_road = (g.get("visitor_team", {}).get("id") == team_id)
        if g["home_team"]["id"] == team_id:
            pts.append(g["home_team_score"])
            allowed.append(g["visitor_team_score"])
            opp_scores.append(g["visitor_team_score"])
        else:
            pts.append(g["visitor_team_score"])
            allowed.append(g["home_team_score"])
            opp_scores.append(g["home_team_score"])

    if not pts:
        return _blank

    rest_days = max(1, (datetime.now() - last_date).days) if last_date else 1
    b2b       = (rest_days <= 1)
    sos       = round(sum(opp_scores) / len(opp_scores), 1) if opp_scores else 113.0

    return {
        "pts":      sum(pts) / len(pts),
        "def":      sum(allowed) / len(allowed),
        "l5":       sum(pts[:5]) / min(5, len(pts)),
        "rest":     rest_days,
        "b2b":      b2b,
        "b2b_road": b2b and last_was_road,
        "sos":      sos,
        "team_id":  team_id,
    }


# ==========================
# 📈 ODDS API — h2h + spreads + totals
# ==========================
_odds_quota_alerted   = set()   # "THRESHOLD:DATE" keys already fired e.g. "200:2026-03-23"

def _check_odds_quota(resp):
    """Alert admin DM when quota is low — max once per threshold per calendar day."""
    global _odds_quota_alerted
    try:
        remaining = int(resp.headers.get("x-requests-remaining", -1))
        used      = int(resp.headers.get("x-requests-used", -1))
        if remaining < 0:
            return
        print(f"[Odds API] Quota — {remaining} remaining ({used} used)")
        today = datetime.now().strftime("%Y-%m-%d")
        for threshold in [200, 100, 50, 10]:
            key = f"{threshold}:{today}"
            if remaining <= threshold and key not in _odds_quota_alerted:
                _odds_quota_alerted.add(key)
                send(
                    f"⚠️ *Odds API Quota Warning*\n\n"
                    f"Only *{remaining}* requests remaining this month.\n"
                    f"Renew at the-odds-api.com before picks stop.",
                    ADMIN_ID
                )
                break
    except Exception:
        pass

_schedule_cache: dict = {
    "date":         None,   # ET date string (YYYY-MM-DD) when cache was built
    "has_games":    False,
    "window_start": None,   # datetime (ET-aware) — when Odds API opens
    "window_end":   None,   # datetime (ET-aware) — when Odds API closes
}


def _refresh_schedule_cache() -> None:
    """
    Fetch today's schedule ONCE per day (free BDL/ESPN/CDN).
    Parse actual tip-off times → compute exact Odds API window:
      window_start = earliest tip  − 3 h
      window_end   = latest tip    + 3 h  (covers full game duration)
    Falls back to 11 AM–1 AM ET broad window if tip times can't be parsed.
    """
    import re as _re
    import zoneinfo as _zi

    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow().replace(tzinfo=timezone.utc)

    today_str = et_now.strftime("%Y-%m-%d")
    _schedule_cache["date"] = today_str

    games = get_todays_games()
    if not games:
        _schedule_cache["has_games"]    = False
        _schedule_cache["window_start"] = None
        _schedule_cache["window_end"]   = None
        print("[Schedule] No NBA games today — Odds API blocked all day")
        return

    _schedule_cache["has_games"] = True

    # ── Parse tip-off times from tip_time strings ──────────────────
    # Formats seen: "7:30 pm et", "7:30 PM ET", "7:30 pm", "19:30"
    tip_dts = []
    for g in games:
        tip_str = (g.get("tip_time") or "").strip().lower()
        if not tip_str:
            continue
        try:
            # 12-hour with am/pm
            m = _re.search(r"(\d{1,2}):(\d{2})\s*(am|pm)", tip_str)
            if m:
                h, mn, ampm = int(m.group(1)), int(m.group(2)), m.group(3)
                if ampm == "pm" and h != 12:
                    h += 12
                elif ampm == "am" and h == 12:
                    h = 0
                tip_dts.append(et_now.replace(hour=h, minute=mn, second=0, microsecond=0))
                continue
            # 24-hour fallback
            m2 = _re.search(r"(\d{1,2}):(\d{2})", tip_str)
            if m2:
                h, mn = int(m2.group(1)), int(m2.group(2))
                tip_dts.append(et_now.replace(hour=h, minute=mn, second=0, microsecond=0))
        except Exception:
            continue

    if tip_dts:
        first_tip = min(tip_dts)
        last_tip  = max(tip_dts)
        _schedule_cache["window_start"] = first_tip - timedelta(hours=3)
        _schedule_cache["window_end"]   = last_tip  + timedelta(hours=3)
        print(
            f"[Schedule] {len(games)} game(s) · window "
            f"{_schedule_cache['window_start'].strftime('%-I:%M %p')} – "
            f"{_schedule_cache['window_end'].strftime('%-I:%M %p')} ET"
        )
    else:
        # Tip times not available yet — broad fallback (11 AM – 1 AM next day)
        _schedule_cache["window_start"] = et_now.replace(hour=11, minute=0, second=0, microsecond=0)
        _schedule_cache["window_end"]   = et_now.replace(hour=23, minute=59, second=0, microsecond=0)
        print(f"[Schedule] {len(games)} game(s) today but tip times not parsed — broad window 11 AM–midnight ET")


def _in_game_window() -> bool:
    """
    Returns True if the Odds API should be called right now.
    Uses the exact tip-time schedule fetched once per day (free APIs only).
    Re-fetches only when the ET date rolls over to a new day.
    """
    import zoneinfo as _zi

    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    today_str = et_now.strftime("%Y-%m-%d")

    # Rebuild cache if it's a new day (or first run)
    if _schedule_cache["date"] != today_str:
        _refresh_schedule_cache()

    if not _schedule_cache["has_games"]:
        return False

    start = _schedule_cache.get("window_start")
    end   = _schedule_cache.get("window_end")
    if start and end:
        in_window = start <= et_now <= end
        print(f"[GameWindow] ET {et_now.strftime('%-I:%M %p')} → {'OPEN' if in_window else 'CLOSED'} (window {start.strftime('%-I:%M %p')}–{end.strftime('%-I:%M %p')})")
        return in_window

    return False


def get_odds_full():
    """Returns dict of team -> moneyline price, plus raw game objects for spreads/totals."""
    if not ODDS_API_KEY:
        return {}, []
    if not _in_game_window():
        print("[OddsAPI] Outside game window — skipping get_odds_full")
        return {}, []
    try:
        resp = requests.get(
            f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
            f"?apiKey={ODDS_API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american&bookmakers=fanduel",
            timeout=10
        )
        _check_odds_quota(resp)
        data = resp.json()
        if not isinstance(data, list):
            print(f"get_odds_full unexpected response: {data}")
            return {}, []
        moneyline = {}
        for g in data:
            try:
                books = g.get("bookmakers", [])
                bk = next((b for b in books if b.get("key") == "fanduel"), None)
                if not bk:
                    continue
                for market in bk["markets"]:
                    if market["key"] == "h2h":
                        o = market["outcomes"]
                        moneyline[g["home_team"]] = o[0]["price"]
                        moneyline[g["away_team"]] = o[1]["price"]
            except Exception:
                continue
        return moneyline, data
    except Exception as e:
        print(f"get_odds_full error: {e}")
        return {}, []


_props_cache        = []   # cached props data
_props_cache_hour   = -1   # -1 = never seeded; 1 = seeded (matches _odds_cache_hour convention)
_props_cache_ts     = 0.0  # epoch time of last real API fetch (throttles retries to 1 per 10 min)
_player_stats_cache: dict = {}   # player_name -> (stats_dict, fetched_at_epoch) — 90 min TTL
_PLAYER_STATS_TTL   = 5400       # 90 minutes

_PROPS_CACHE_TTL = 1800  # seconds — re-fetch at most once per 30 minutes

def get_player_props(force=False):
    """
    Fetch player props from Odds API via per-event endpoint.

    Player prop markets (player_points, etc.) are ONLY available on the
    /events/{id}/odds endpoint — NOT on the batch /odds/ endpoint.

    Quota optimisations vs original:
      - Events list filtered to TODAY's games starting within 12 h (5-8 games
        on a typical night vs up to 15 in the old code).
      - Cache TTL is 30 minutes instead of 10 — 3× fewer fetches.
      - Typical cost: 1 (events) + 6 (games) = 7 calls per fetch vs 16 before.

    Cache policy:
      - Returns cached data if last fetch was < 30 min ago, unless force=True.
      - force=True bypasses cache (used only on confirmed game-time triggers).
    """
    import time as _time_mod
    global _props_cache, _props_cache_hour, _props_cache_ts

    elapsed = _time_mod.time() - _props_cache_ts
    if not force and elapsed < _PROPS_CACHE_TTL:
        print(f"[Props] Cache hit — {int(_PROPS_CACHE_TTL - elapsed)}s until next fetch")
        return _props_cache

    if not _in_game_window():
        print("[OddsAPI] Outside game window — skipping get_player_props")
        return _props_cache  # return last known cache, don't stamp timestamp

    try:
        events_url = (
            f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/"
            f"?apiKey={ODDS_API_KEY}"
        )
        resp = requests.get(events_url, timeout=10)
        _check_odds_quota(resp)
        events = resp.json()
        if not isinstance(events, list):
            print(f"[Props] Events fetch error: {events}")
            _props_cache_ts = _time_mod.time()
            return _props_cache

        # Only today's games starting within the next 12 hours (saves per-event calls)
        now = datetime.utcnow()
        window_end = now + timedelta(hours=12)
        todays_events = sorted(
            [e for e in events
             if now <= datetime.strptime(e["commence_time"], "%Y-%m-%dT%H:%M:%SZ") <= window_end],
            key=lambda e: e["commence_time"]
        )
        print(f"[Props] {len(todays_events)} games in 12-h window (of {len(events)} total)")

        all_games = []
        markets = (
            "player_points,player_rebounds,player_assists,player_threes,"
            "player_points_rebounds_assists,player_points_rebounds,"
            "player_points_assists,player_first_basket"
        )
        for event in todays_events:
            try:
                url = (
                    f"https://api.the-odds-api.com/v4/sports/basketball_nba"
                    f"/events/{event['id']}/odds"
                    f"?apiKey={ODDS_API_KEY}&regions=us&markets={markets}&bookmakers=fanduel&oddsFormat=american"
                )
                resp2 = requests.get(url, timeout=10)
                _check_odds_quota(resp2)
                game_data = resp2.json()
                if game_data.get("bookmakers"):
                    all_games.append(game_data)
            except Exception as e:
                print(f"[Props] Event {event.get('id','?')} error: {e}")

        # Stamp timestamp regardless — throttle even "no props yet" retries
        _props_cache_ts = _time_mod.time()
        print(f"[Props] Fetched — {len(todays_events)} games checked, "
              f"{len(all_games)} with FanDuel lines "
              f"({1 + len(todays_events)} API calls)")
        _props_cache = all_games
        if all_games:
            _props_cache_hour = 1
        else:
            print(f"[Props] No FanDuel props posted yet — retry in {_PROPS_CACHE_TTL//60} min")
        return all_games

    except Exception as e:
        print(f"get_player_props error: {e}")
        _props_cache_ts = _time_mod.time()  # stamp on error too
        return _props_cache


def extract_props(data):
    PROP_MARKETS = {
        "player_points", "player_rebounds", "player_assists", "player_threes",
        "player_points_rebounds_assists", "player_points_rebounds",
        "player_points_assists", "player_first_basket",
    }
    props = []
    seen = set()

    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        matchup = f"{away} @ {home}" if away and home else ""
        _books = game.get("bookmakers", [])
        _fd = next((b for b in _books if b.get("key") == "fanduel"), None)
        for book in ([_fd] if _fd else []):
            for market in book.get("markets", []):
                if market["key"] not in PROP_MARKETS:
                    continue
                prop_type = market["key"].replace("player_", "")

                if prop_type == "first_basket":
                    # First basket: outcome name IS the player, no line
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("name", "")
                        odds   = outcome.get("price", 0)
                        if not player or odds == 0:
                            continue
                        key = (player, "first_basket")
                        if key in seen:
                            continue
                        seen.add(key)
                        props.append({
                            "player":    player,
                            "line":      None,
                            "odds":      odds,
                            "prop_type": "first_basket",
                            "game":      matchup,
                        })
                else:
                    # Standard and combo over/under markets
                    for outcome in market.get("outcomes", []):
                        if outcome.get("name") != "Over":
                            continue
                        player = outcome.get("description", "")
                        line   = outcome.get("point", 0)
                        key    = (player, prop_type)
                        if key in seen:
                            continue
                        seen.add(key)
                        props.append({
                            "player":    player,
                            "line":      line,
                            "odds":      outcome.get("price", -110),
                            "prop_type": prop_type,
                            "game":      matchup,
                        })
    return props


def implied_prob(o):
    if o == 0:
        return 0
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100)


# ==========================
# 🤖 PREDICTION MODEL
# ==========================
def predict(home_stats, away_stats, home_name, away_name):
    elo_edge = (get_elo(home_name) - get_elo(away_name)) / 40

    # ── Home court advantage (NBA historical ~3.5 pts) ───────────────
    home_court = 3.5

    # ── Back-to-back fatigue penalties ──────────────────────────────
    home_b2b = 0.0
    if home_stats.get("b2b"):
        home_b2b = -2.5
        if home_stats.get("b2b_road"):
            home_b2b = -4.0        # road b2b is harsher

    away_b2b = 0.0
    if away_stats.get("b2b"):
        away_b2b = -2.5
        if away_stats.get("b2b_road"):
            away_b2b = -4.0

    # ── Head-to-head history (weighted at 15%) ───────────────────────
    home_id = home_stats.get("team_id")
    away_id = away_stats.get("team_id")
    h2h_edge = 0.0
    if home_id and away_id:
        h2h_raw  = get_h2h(home_id, away_id)
        h2h_edge = h2h_raw * 0.15

    # ── Strength of schedule (league avg ~113 pts) ────────────────────
    league_avg = 113.0
    home_sos   = (home_stats.get("sos", league_avg) - league_avg) / 10
    away_sos   = (away_stats.get("sos", league_avg) - league_avg) / 10
    sos_edge   = (home_sos - away_sos) * 0.5

    base = (
        (home_stats["pts"] - away_stats["def"]) +
        (home_stats["l5"] - away_stats["l5"]) +
        (home_stats["rest"] - away_stats["rest"]) * 1.5 +
        elo_edge +
        home_court +
        (home_b2b - away_b2b) +
        h2h_edge +
        sos_edge
    )
    base = max(-100, min(100, base))
    prob = 1 / (1 + math.exp(-base / 10))

    pred_spread = (
        home_stats["pts"] - away_stats["pts"]
        + home_court
        + (home_b2b - away_b2b)
    )
    pred_total = home_stats["pts"] + away_stats["pts"]

    # Apply self-learned bias corrections
    try:
        ld = load_learning_data()
        pred_spread -= ld.get("spread_bias", 0.0)
        pred_total  -= ld.get("total_bias", 0.0)
    except Exception:
        pass

    return prob, pred_spread, pred_total


# ==========================
# 💰 KELLY / SIZING
# ==========================
def edge_moneyline(prob, odds_val):
    return prob - implied_prob(odds_val)


def kelly(prob, odds_val):
    if odds_val == 0:
        return 0
    b = odds_val / 100 if odds_val > 0 else 100 / abs(odds_val)
    return max(0, (b * prob - (1 - prob)) / b)


def bet_size(prob, odds_val):
    return round(BANKROLL * kelly(prob, odds_val) * KELLY_FRACTION, 2)


def grade(e_val, p):
    if e_val > 0.12 and p > 0.65:
        return "💎 MAX"
    if e_val > 0.08:
        return "🔥 STRONG"
    return "⚡ VALUE"


def assign_tier(confidence):
    """Return (tier_name, units) based on model confidence."""
    conf = confidence or 0
    if conf >= 80:
        return "SAFE", 3
    if conf >= 65:
        return "BALANCED", 2
    return "AGGRESSIVE", 1


TIER_BADGE = {
    "SAFE":       "🔒 SAFE · 3 units",
    "BALANCED":   "🎯 BALANCED · 2 units",
    "AGGRESSIVE": "🧨 AGGRESSIVE · 1 unit",
}


# ==========================
# 💾 TRACKING
# ==========================
def load_bets():
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT game, player, pick, bet_type, line, prediction, odds, prob,
                       edge, confidence, result, bet_time, tier,
                       COALESCE(script, 'NORMAL') AS script,
                       game_total, game_spread, player_avg_mins, player_avg_usage,
                       COALESCE(script_combo, 'NORMAL') AS script_combo,
                       pick_category, ev, role, true_edge, is_fade
                FROM bets ORDER BY created_at ASC
            """)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            keys = ["game","player","pick","betType","line","prediction","odds","prob",
                    "edge","confidence","result","time","tier","script","game_total","game_spread",
                    "player_avg_mins","player_avg_usage","script_combo",
                    "pick_category","ev","role","true_edge","is_fade"]
            return [dict(zip(keys, r)) for r in rows]
        except Exception as e:
            print(f"[DB] load_bets error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        if os.path.exists(BETS_FILE):
            return json.load(open(BETS_FILE))
    except Exception:
        pass
    return []


def _norm_sf(x, loc, scale):
    """Normal survival function P(X > x) where X ~ N(loc, scale). Pure math, no scipy."""
    import math
    if scale <= 0:
        return 1.0 if loc > x else 0.0
    z = (x - loc) / scale
    return (1.0 - math.erf(z / math.sqrt(2))) / 2.0


# Per-sport standard deviations for real probability calculation
_NBA_TOTAL_STD  = 12.5   # NBA game total points std dev
_NBA_SPREAD_STD = 10.5   # NBA margin-of-victory std dev
_PROP_STD = {
    "points":                    7.0,
    "rebounds":                  3.0,
    "assists":                   2.5,
    "threes":                    1.5,
    "steals":                    1.2,
    "blocks":                    1.0,
    "points_rebounds_assists":   9.0,
    "points_rebounds":           8.0,
    "points_assists":            8.0,
    "first_basket":              3.0,
}


def save_bet(bet):
    # ── Pattern gate — holds picks when context pattern is weak ───────────────
    try:
        from bot.decision_engine import gate_pick as _gate_pick
        if not _gate_pick(bet):
            return False   # PASS decision — don't save or send
    except Exception as _ge:
        print(f"[PatternGate] gate check error (fail-open): {_ge}")

    # Reject picks with no valid player name — only for bet types that require one.
    # System-level bets (VIP_LOCK, EDGE_FADE, MONEYLINE, TOTAL, SPREAD) have no player.
    _PLAYER_REQUIRED = {"ELITE_PROP", "SGP", "CGP", "INDIVIDUAL", "PROP"}
    _raw_player = bet.get("player")
    if bet.get("betType") in _PLAYER_REQUIRED:
        if not _raw_player or str(_raw_player).strip() in ("None", "none", ""):
            print(f"[save_bet] Rejected pick with invalid player: {_raw_player!r}")
            return False

    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            # Include player in uniqueness check so two players with the same
            # pick direction/line/stat in the same game both get saved.
            _dup_player = bet.get("player") or ""
            cur.execute(
                "SELECT id FROM bets WHERE game=%s AND pick=%s AND bet_type=%s AND COALESCE(player,'')=%s "
                "AND DATE(COALESCE(bet_time, created_at)) = CURRENT_DATE",
                (bet.get("game",""), bet.get("pick",""), bet.get("betType",""), _dup_player)
            )
            if cur.fetchone():
                cur.close(); conn.close()
                return False
            tier_name, _ = assign_tier(bet.get("confidence", 0))
            script_name   = bet.get("script", "AVERAGE_PACE_NORMAL_SCORING")
            _cat = bet.get("pick_category") or (
                "VIP_LOCK" if bet.get("betType") == "VIP_LOCK" else
                "SGP"      if bet.get("betType") == "SGP"      else
                "INDIVIDUAL"
            )
            # Determine game_pace from bet dict (set by pick generators)
            _pace  = bet.get("game_pace", "AVERAGE_PACE")
            _phase = bet.get("game_phase", "pregame")
            cur.execute(
                """INSERT INTO bets
                   (game,player,pick,bet_type,line,prediction,odds,prob,
                    edge,confidence,result,bet_time,tier,script,game_total,game_spread,
                    player_avg_mins,player_avg_usage,script_combo,pick_category,
                    slip_grade,role,is_fade,is_benefactor,fade_target,ev,
                    line_rating,line_decision,true_edge,parlay_hit_prob,parlay_ev,
                    game_pace,game_phase)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (bet.get("game"), bet.get("player"), bet.get("pick"), bet.get("betType"),
                 bet.get("line"), bet.get("prediction"), bet.get("odds"), bet.get("prob"),
                 bet.get("edge"), bet.get("confidence"), bet.get("result"),
                 bet.get("time"), tier_name, script_name,
                 bet.get("game_total"), bet.get("game_spread"),
                 bet.get("player_avg_mins"), bet.get("player_avg_usage"),
                 bet.get("script_combo", script_name), _cat,
                 bet.get("slip_grade"), bet.get("role"),
                 bet.get("is_fade", False), bet.get("is_benefactor", False),
                 bet.get("fade_target"), bet.get("ev"),
                 bet.get("line_rating", "GOOD"), bet.get("line_decision", "RISK"),
                 bet.get("true_edge"), bet.get("parlay_hit_prob"), bet.get("parlay_ev"),
                 _pace, _phase)
            )
            conn.commit()
            cur.close(); conn.close()
            return True
        except Exception as e:
            print(f"[DB] save_bet error: {e}")
            try: conn.close()
            except Exception: pass
    data = load_bets()
    for b in data:
        if b["game"] == bet["game"] and b["pick"] == bet["pick"] and b.get("betType") == bet.get("betType"):
            return False
    data.append(bet)
    json.dump(data, open(BETS_FILE, "w"), indent=2, cls=_DatetimeEncoder)
    return True


def _update_bet_result_db(game, pick, bet_type, result, actual_value=None, player=None):
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            # Include player in WHERE so two players with identical pick text in the
            # same game (e.g. both OVER 22.5 points) settle independently.
            _player_clause = "AND COALESCE(player,'')=%s"
            _player_val    = player or ""
            if actual_value is not None:
                # Also compute and store prediction error so the model can learn
                cur.execute(
                    f"""UPDATE bets
                       SET result=%s, actual_value=%s,
                           prediction_error = CASE
                               WHEN prediction IS NOT NULL THEN prediction - %s
                               ELSE NULL
                           END
                       WHERE game=%s AND pick=%s AND bet_type=%s {_player_clause} AND result IS NULL""",
                    (result, actual_value, actual_value, game, pick, bet_type, _player_val)
                )
            else:
                cur.execute(
                    f"UPDATE bets SET result=%s WHERE game=%s AND pick=%s AND bet_type=%s {_player_clause} AND result IS NULL",
                    (result, game, pick, bet_type, _player_val)
                )
            conn.commit()
            cur.close()
            # ── After settling a result, recompute adaptive thresholds ────
            try:
                from bot.adaptive_thresholds import run_adaptive_update
                run_adaptive_update(conn)
            except Exception as _ae:
                print(f"[DB] adaptive update after settle skipped: {_ae}")
            conn.close()
        except Exception as e:
            print(f"[DB] update_bet_result error: {e}")
            try: conn.close()
            except Exception: pass


def _tag_parlay_legs_db(legs, category):
    """After parlays are built, tag each leg's DB record with its tier category."""
    conn = _db_conn()
    if not conn or not legs:
        return
    try:
        cur = conn.cursor()
        for leg in legs:
            cur.execute(
                "UPDATE bets SET pick_category=%s WHERE game=%s AND bet_type=%s AND result IS NULL",
                (category, leg.get("game", ""), leg.get("bet_type", ""))
            )
        conn.commit()
        cur.close(); conn.close()
        print(f"[DB] Tagged {len(legs)} legs as {category}")
    except Exception as e:
        print(f"[DB] _tag_parlay_legs_db error: {e}")
        try: conn.close()
        except Exception: pass



def load_status():
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM bot_status")
            rows = cur.fetchall()
            cur.close(); conn.close()
            return {k: _try_parse(v) for k, v in rows}
        except Exception as e:
            print(f"[DB] load_status error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        if os.path.exists(STATUS_FILE):
            return json.load(open(STATUS_FILE))
    except Exception:
        pass
    return {}


def _try_parse(v):
    try: return json.loads(v)
    except Exception: return v


def save_status(picks_today, extra=None):
    updates = {"lastRun": str(datetime.now()), "picksToday": str(picks_today)}
    if extra:
        updates.update({k: _safe_json_dumps(v) if not isinstance(v, str) else v for k, v in extra.items()})
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            for k, v in updates.items():
                val = v if isinstance(v, str) else _safe_json_dumps(v)
                cur.execute(
                    "INSERT INTO bot_status (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                    (k, val)
                )
            conn.commit()
            cur.close(); conn.close()
            return
        except Exception as e:
            print(f"[DB] save_status error: {e}")
            try: conn.close()
            except Exception: pass
    status = {}
    try:
        if os.path.exists(STATUS_FILE):
            status = json.load(open(STATUS_FILE))
    except Exception:
        pass
    status.update(updates)
    json.dump(status, open(STATUS_FILE, "w"), indent=2, cls=_DatetimeEncoder)


# ==========================
# 💾 MEMORY PERSISTENCE
# ==========================
def save_memory_state():
    """Persist all critical in-memory state to DB so a restart picks up where it left off."""
    global _system_sent_date, _vip_lock_desc, _sgp_sent_games, line_history
    global _elite_props_sent_games, _edge_fade_sent_date, _cgp_sent_date
    try:
        save_status(0, {
            "_mem_system_sent_date":     _system_sent_date or "",
            "_mem_vip_lock_desc":        _vip_lock_desc or "",
            "_mem_sgp_sent_games":       list(_sgp_sent_games),
            "_mem_line_history":         line_history,
            "_mem_elite_props_sent":     list(_elite_props_sent_games),
            "_mem_edge_fade_sent_date":  _edge_fade_sent_date or "",
            "_mem_cgp_sent_date":        _cgp_sent_date or "",
        })
    except Exception as _sme:
        print(f"[Memory] save_memory_state error: {_sme}")


def restore_memory_state():
    """
    Load all persisted state back into memory on startup or after restart.
    This covers both operational flags AND all learning data so the bot
    picks up exactly where it left off — no cold starts.
    """
    global _system_sent_date, _vip_lock_desc, _sgp_sent_games, line_history
    global _elite_props_sent_games, _edge_fade_sent_date, _cgp_sent_date
    import json as _json

    # ── 1. Operational flags (send-date guards, VIP state) ────────────────
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        st = load_status()

        saved_date = st.get("_mem_system_sent_date", "")
        if saved_date == today_str:
            _system_sent_date = saved_date
            print(f"[Memory] Restored _system_sent_date: {_system_sent_date}")

        saved_vip = st.get("_mem_vip_lock_desc", "")
        if saved_vip:
            _vip_lock_desc = saved_vip
            print(f"[Memory] Restored _vip_lock_desc: {_vip_lock_desc}")

        saved_sgp = st.get("_mem_sgp_sent_games", [])
        if isinstance(saved_sgp, list):
            _sgp_sent_games.update(saved_sgp)
            print(f"[Memory] Restored _sgp_sent_games: {len(_sgp_sent_games)} games")

        saved_lines = st.get("_mem_line_history", {})
        if isinstance(saved_lines, dict):
            line_history.update(saved_lines)
            print(f"[Memory] Restored line_history: {len(line_history)} entries")

        # ── Elite props sent set (prevents re-sends on same-day restart) ──
        saved_elite = st.get("_mem_elite_props_sent", [])
        if isinstance(saved_elite, list) and saved_elite:
            _elite_props_sent_games.update(saved_elite)
            print(f"[Memory] Restored _elite_props_sent_games: {len(_elite_props_sent_games)} games")

        # ── Edge-Fade 7 sent guard (prevents double-slip on restart) ──────
        # Uses both the memory key and the legacy _ef7_sent_date key
        saved_ef7 = (st.get("_mem_edge_fade_sent_date") or
                     st.get("_ef7_sent_date") or "")
        if saved_ef7 == today_str:
            _edge_fade_sent_date = saved_ef7
            print(f"[Memory] Restored _edge_fade_sent_date: {_edge_fade_sent_date}")

        saved_cgp = st.get("_mem_cgp_sent_date", "")
        if saved_cgp == today_str:
            _cgp_sent_date = saved_cgp
            print(f"[Memory] Restored _cgp_sent_date: {_cgp_sent_date}")

    except Exception as _rme:
        print(f"[Memory] restore_memory_state error: {_rme}")

    # ── 2. ELO ratings — load DB values over hardcoded starting ratings ───
    try:
        conn = _db_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM learning_data WHERE key = 'elo_ratings'")
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                saved_elo = row[0] if isinstance(row[0], dict) else _json.loads(row[0])
                ELO.update(saved_elo)
                print(f"[Memory] Restored ELO ratings for {len(saved_elo)} teams")
    except Exception as _elo_err:
        print(f"[Memory] ELO restore error: {_elo_err}")

    # ── 3. Calibrated team styles — override hardcoded TEAM_STYLES ────────
    try:
        _load_and_apply_team_styles()
    except Exception as _ts_err:
        print(f"[Memory] Team styles restore error: {_ts_err}")

    print("[Memory] restore_memory_state complete — all learning data loaded")


# ==========================
# 🔄 RESULT UPDATES
# ==========================
_CATEGORY_LABEL = {
    "VIP_LOCK":          "🔒 VIP Lock",
    "EDGE_FADE":         "⚡ Edge-Fade 7",
    "CROSS_GAME_PARLAY": "🌐 Cross Game Parlay",
    "SGP":               "🎲 SGP",
    "INDIVIDUAL":        "📋 Top Pick",
}

def _notify_pick_result(bet, actual_value=None):
    """
    Send ✅/❌ settlement card to VIP channel when a pick settles.
    - Game must be confirmed Final before any alert fires.
    - Single picks: immediate card on settlement.
    - Parlays (SGP / CROSS_GAME_PARLAY): shows all legs + which busted.
      Any losing leg immediately busts and closes the whole slip.
    - All alerts go to VIP channel only (admin gets mid-game tracker alerts separately).
    """
    global _parlay_notified
    try:
        result   = bet.get("result", "")
        category = bet.get("pick_category", bet.get("betType", ""))

        if result not in ("win", "loss"):
            return

        is_parlay = category in ("SGP", "CROSS_GAME_PARLAY")

        if not is_parlay:
            # ── Single pick ───────────────────────────────────────────────
            player    = bet.get("player") or bet.get("pick", "")
            pick_text = bet.get("pick", "")
            cat_label = _CATEGORY_LABEL.get(category, category)
            icon         = "✅" if result == "win" else "❌"
            result_label = "WON" if result == "win" else "LOST"
            actual_str   = f"\n📊 Final: {actual_value}" if actual_value is not None else ""
            msg = (
                f"{icon} *{cat_label} — {result_label}*\n"
                f"👤 {player or pick_text}"
                f"{actual_str}"
            )
            send(msg, VIP_CHANNEL)
            print(f"[Notify] {category} {result_label}: {player or pick_text}")

        else:
            # ── Parlay: group all legs, apply bust logic ───────────────────
            game     = bet.get("game", "")
            today_et = datetime.now(ET).strftime("%Y-%m-%d")

            # Unique key to prevent duplicate alerts for the same slip.
            # SGP: keyed by game (multiple SGPs possible across different games in one day).
            # CGP: keyed by date only — one CGP slip per day spanning multiple games,
            #      so using game would create a different key per leg and fire multiple alerts.
            if category == "CROSS_GAME_PARLAY":
                parlay_key = f"{category}:{today_et}"
            else:
                parlay_key = f"{category}:{game}:{today_et}"
            if parlay_key in _parlay_notified:
                return

            conn = _db_conn()
            if not conn:
                return
            cur = conn.cursor()

            if category == "SGP":
                cur.execute("""
                    SELECT player, pick, line, result, actual_value
                    FROM bets
                    WHERE pick_category = 'SGP'
                      AND game = %s
                      AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                    ORDER BY id ASC
                """, (game, today_et))
            else:  # CROSS_GAME_PARLAY
                cur.execute("""
                    SELECT player, pick, line, result, actual_value
                    FROM bets
                    WHERE pick_category = 'CROSS_GAME_PARLAY'
                      AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                    ORDER BY id ASC
                """, (today_et,))

            legs = cur.fetchall()

            # If any leg lost → bust all remaining pending legs in the DB
            any_loss    = any(row[3] == "loss" for row in legs)
            all_settled = all(row[3] in ("win", "loss") for row in legs)

            if any_loss:
                # Mark all still-pending legs as loss (parlay busted)
                if category == "SGP":
                    cur.execute("""
                        UPDATE bets SET result = 'loss'
                        WHERE pick_category = 'SGP'
                          AND game = %s
                          AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                          AND result IS NULL
                    """, (game, today_et))
                else:
                    cur.execute("""
                        UPDATE bets SET result = 'loss'
                        WHERE pick_category = 'CROSS_GAME_PARLAY'
                          AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                          AND result IS NULL
                    """, (today_et,))
                conn.commit()
                # Reload legs with updated results
                if category == "SGP":
                    cur.execute("""
                        SELECT player, pick, line, result, actual_value
                        FROM bets
                        WHERE pick_category = 'SGP'
                          AND game = %s
                          AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                        ORDER BY id ASC
                    """, (game, today_et))
                else:
                    cur.execute("""
                        SELECT player, pick, line, result, actual_value
                        FROM bets
                        WHERE pick_category = 'CROSS_GAME_PARLAY'
                          AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s
                        ORDER BY id ASC
                    """, (today_et,))
                legs = cur.fetchall()

            cur.close()
            conn.close()

            if not legs:
                return

            # Only send if busted (any_loss) or all legs settled and all won
            all_settled_now = all(row[3] in ("win", "loss") for row in legs)
            if not any_loss and not all_settled_now:
                return  # still waiting on more legs

            overall_result = "loss" if any_loss else "win"
            icon         = "✅" if overall_result == "win" else "❌"
            result_label = "WON 🔥" if overall_result == "win" else "LOST"
            cat_label    = _CATEGORY_LABEL.get(category, category)

            n_busted = sum(1 for row in legs if row[3] == "loss")
            bust_note = f" ({n_busted} leg{'s' if n_busted > 1 else ''} busted)" if any_loss else ""

            lines = [f"{icon} *{cat_label} — {result_label}*{bust_note}"]

            n = len(legs)
            for i, (player, pick_text, line, leg_result, leg_actual) in enumerate(legs):
                prefix   = "└" if i == n - 1 else "├"
                leg_icon = "✅" if leg_result == "win" else "❌"
                actual_note = f" ({leg_actual})" if leg_actual is not None else ""
                bust_tag    = " ← busted it" if leg_result == "loss" else ""
                label_text  = player or pick_text or f"Leg {i+1}"
                lines.append(f"{prefix} {leg_icon} {label_text}{actual_note}{bust_tag}")

            send("\n".join(lines), VIP_CHANNEL)
            _parlay_notified.add(parlay_key)
            print(f"[Notify] {category} slip {overall_result} — {parlay_key}")

    except Exception as _ne:
        print(f"[Notify] result notification error: {_ne}")


def update_results():
    """Settle pending bets. Returns the number of newly settled picks."""
    bets = load_bets()

    today     = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    et_hour   = datetime.now(ET).hour
    dates_to_check = [str(today)]
    if et_hour < 8:
        dates_to_check.insert(0, str(yesterday))

    # ── Game data: unified layer (ESPN/CDN/BDL) for today ─────────────────────
    # Normalized format: {home_team(str), away_team(str), home_score, away_score,
    #                     status("pre"/"in"/"post")}
    games = list(get_todays_games())  # already normalized

    # For overnight settlement (before 8 AM ET) also pull yesterday from BDL
    # and normalize it to match the shared format
    if et_hour < 8:
        try:
            url = f"{BDL_BASE}/games?dates[]={str(yesterday)}&per_page=20"
            for g in _bdl_get(url).get("data", []):
                ht = g.get("home_team", {})
                vt = g.get("visitor_team", {})
                games.append({
                    "home_team":  ht.get("full_name", "") if isinstance(ht, dict) else str(ht),
                    "away_team":  vt.get("full_name", "") if isinstance(vt, dict) else str(vt),
                    "home_score": g.get("home_team_score", 0) or 0,
                    "away_score": g.get("visitor_team_score", 0) or 0,
                    "status":     "post" if "final" in (g.get("status", "")).lower() else "pre",
                })
        except Exception:
            pass

    changed = False
    newly_settled = 0

    # ── Load today's causality events once — shared across all settlements ────
    _today_causal_events = []
    try:
        _tc = _db_conn()
        if _tc:
            _tcu = _tc.cursor()
            _tcu.execute(
                "SELECT DISTINCT game_id FROM game_observations WHERE game_date = %s",
                (str(today),)
            )
            for (_tgid,) in _tcu.fetchall():
                _today_causal_events.extend(_get_game_causality_events(_tgid))
            _tcu.close()
            _tc.close()
    except Exception:
        pass

    for b in bets:
        if b.get("result"):
            continue
        bet_type  = b.get("betType", "MONEYLINE")
        pick_cat  = b.get("pick_category", "")

        for g in games:
            # Unified field names from the normalized dict
            home_name  = g.get("home_team", "")
            away_name  = g.get("away_team", "")
            name       = f"{home_name} vs {away_name}"
            if name != b.get("game", "") or g.get("status") != "post":
                continue

            home_score    = g.get("home_score", 0) or 0
            away_score    = g.get("away_score", 0) or 0
            actual_total  = home_score + away_score
            actual_spread = home_score - away_score
            winner        = home_name if home_score > away_score else away_name

            if bet_type == "MONEYLINE":
                b["result"] = "win" if winner == b["pick"] else "loss"
                update_elo(home_name, away_name, winner)
            elif bet_type == "TOTAL":
                line      = b.get("line", 0)
                direction = "OVER" if "OVER" in str(b.get("pick", "")).upper() else "UNDER"
                b["result"] = "win" if (actual_total > line if direction == "OVER" else actual_total < line) else "loss"
            elif bet_type == "OVER":   # legacy label
                b["result"] = "win" if actual_total > (b.get("line", 0) or 0) else "loss"
            elif bet_type == "UNDER":  # legacy label
                b["result"] = "win" if actual_total < (b.get("line", 0) or 0) else "loss"
            elif bet_type == "SPREAD":
                b["result"] = "win" if actual_spread > (b.get("line", 0) or 0) else "loss"
            elif bet_type in ("VIP_LOCK", "EDGE_FADE", "FADE") or pick_cat == "EDGE_FADE":
                # Grade by pick content — OVER/UNDER = total, else moneyline
                pick_str = str(b.get("pick", "")).upper()
                if "OVER" in pick_str:
                    b["result"] = "win" if actual_total > (b.get("line", 0) or 0) else "loss"
                elif "UNDER" in pick_str:
                    b["result"] = "win" if actual_total < (b.get("line", 0) or 0) else "loss"
                else:
                    b["result"] = "win" if winner in b.get("pick", "") else "loss"
                    # Moneyline VIP_LOCK/EDGE_FADE also feed ELO — same as MONEYLINE picks
                    update_elo(home_name, away_name, winner)
            else:
                continue  # player-prop betTypes settled in the prop block below

            _update_bet_result_db(b.get("game"), b.get("pick"), b.get("betType"), b["result"],
                                  player=b.get("player"))
            changed = True
            newly_settled += 1
            _notify_pick_result(b)

            try:
                from decision_engine import get_upset_flag, record_signal as _rs
                if get_upset_flag(float(b.get("odds", 0) or 0),
                                  float(b.get("confidence", 0) or 0) / 100):
                    _rs("upset_signal", b["result"] == "win")
            except Exception:
                pass

            # ── Causality closed loop for game-level picks ─────────────────
            try:
                from decision_engine import record_causality_outcome as _rco
                _gs = _resolve_stat(b) or "game"
                _rco(b["result"], _gs, "UNKNOWN", _today_causal_events)
            except Exception:
                pass

    # ── PLAYER PROP SETTLEMENT (ELITE_PROP, SGP legs, CGP legs) ───────────────
    # Covers any pick with a known stat betType OR with pick_category SGP/CGP
    PROP_TYPES = {"points", "rebounds", "assists", "threes"}

    def _resolve_stat(b):
        """Return the stat key for a bet, or None if it can't be determined."""
        bt = b.get("betType", "").lower()
        if bt in PROP_TYPES:
            return bt
        # SGP / CGP legs: stat is embedded in the pick text
        pick = str(b.get("pick", "")).lower()
        for s in ("points", "rebounds", "assists", "threes"):
            if s in pick:
                return s
        if "3pt" in pick or "three" in pick:
            return "threes"
        return None

    # ELITE_PROP and INDIVIDUAL bets store betType as the category name, not the
    # stat ("points"/"rebounds"/etc.) — _resolve_stat() parses the stat from pick text.
    _PLAYER_PROP_TYPES = {"elite_prop", "individual", "prop", "neutral_prop",
                          "fade_prop", "benefactor_prop"}
    unsettled_props = [
        b for b in bets
        if not b.get("result")
        and b.get("player")   # must have a player name to settle
        and (
            b.get("betType", "").lower() in PROP_TYPES
            or b.get("betType", "").lower() in _PLAYER_PROP_TYPES
            or b.get("pick_category", "").lower() in _PLAYER_PROP_TYPES
            or b.get("pick_category") in ("SGP", "CROSS_GAME_PARLAY")
        )
    ]
    if unsettled_props:
        for d in dates_to_check:
            try:
                url = f"{BDL_BASE}/stats?dates[]={d}&per_page=100"
                stats_data = _bdl_get(url).get("data", [])
                for stat in stats_data:
                    p = stat.get("player", {})
                    full_name = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
                    for b in unsettled_props:
                        if b.get("result"):
                            continue
                        if b.get("player", "").lower() != full_name.lower():
                            continue
                        # Use _resolve_stat so SGP/CGP legs (betType="SGP") get
                        # their stat type from the pick text instead of betType
                        resolved_stat = _resolve_stat(b)
                        line      = b.get("line", 0)
                        pick      = b.get("pick", "").upper()
                        direction = "OVER" if "OVER" in pick else "UNDER"
                        if resolved_stat == "points":
                            actual = stat.get("pts", None)
                        elif resolved_stat == "rebounds":
                            actual = stat.get("reb", None)
                        elif resolved_stat == "assists":
                            actual = stat.get("ast", None)
                        elif resolved_stat == "threes":
                            actual = stat.get("fg3m", None)
                        else:
                            actual = None
                        if actual is None:
                            continue
                        # Only settle if the game is actually Final — prevents
                        # yesterday's or in-progress stats from settling tonight's picks
                        game_status = stat.get("game", {}).get("status", "")
                        if "final" not in game_status.lower():
                            continue
                        if direction == "OVER":
                            b["result"] = "win" if actual > line else "loss"
                        else:
                            b["result"] = "win" if actual < line else "loss"
                        b["actual_value"] = actual
                        # Pass actual stat so DB stores prediction_error = prediction - actual
                        _update_bet_result_db(
                            b.get("game"), b.get("pick"), b.get("betType"),
                            b["result"], actual_value=actual,
                            player=b.get("player")
                        )
                        changed = True
                        newly_settled += 1
                        _notify_pick_result(b, actual_value=actual)

                        # Upset signal self-learning
                        try:
                            from decision_engine import get_upset_flag, record_signal as _rs
                            if get_upset_flag(float(b.get("odds", 0) or 0),
                                              float(b.get("confidence", 0) or 0) / 100):
                                _rs("upset_signal", b["result"] == "win")
                        except Exception:
                            pass

                        # ── Causality closed loop for prop picks ──────────────
                        try:
                            from decision_engine import record_causality_outcome as _rco
                            _rco(b["result"], resolved_stat or "points",
                                 "UNKNOWN", _today_causal_events)
                        except Exception:
                            pass
            except Exception as pe:
                print(f"Prop settlement error: {pe}")

    # ── GAME TOTAL SETTLEMENT (CGP game total legs — no player, bet_type TOTAL) ─
    unsettled_totals = [
        b for b in bets
        if not b.get("result")
        and b.get("pick_category") == "CROSS_GAME_PARLAY"
        and b.get("betType", "").upper() in ("TOTAL", "OVER", "UNDER")
        and not b.get("player")
    ]
    for b in unsettled_totals:
        try:
            game_name = b.get("game", "")
            pick_text = b.get("pick", "").upper()
            line      = float(b.get("line") or 0)
            direction = "OVER" if "OVER" in pick_text else "UNDER"

            matched = None
            gname_lower = game_name.lower()
            for g in games:
                if g.get("status") != "post":
                    continue
                home = g.get("home_team", "").lower()
                away = g.get("away_team", "").lower()
                if home in gname_lower or away in gname_lower:
                    matched = g
                    break

            if not matched:
                continue

            home_score = int(matched.get("home_score", 0) or 0)
            away_score = int(matched.get("away_score", 0) or 0)
            combined   = home_score + away_score
            if combined == 0:
                continue  # no score yet, skip

            result = ("win" if combined > line else "loss") if direction == "OVER" \
                     else ("win" if combined < line else "loss")

            b["result"]       = result
            b["actual_value"] = combined
            _update_bet_result_db(
                b.get("game"), b.get("pick"), b.get("betType"),
                result, actual_value=combined,
                player=b.get("player")
            )
            changed        = True
            newly_settled += 1
            _notify_pick_result(b, actual_value=combined)
            print(f"[Settlement] Game total: {game_name} combined={combined} vs {direction} {line} → {result}")
        except Exception as gte:
            print(f"Game total settlement error: {gte}")

    # ── DB-SIDE PROP SETTLEMENT ────────────────────────────────────────────────
    # neutral_prop / fade_prop / benefactor_prop legs are saved directly to the
    # DB bets table by _save_pick_legs_to_bets() and never appear in bets.json.
    # No date restriction — handles both today's picks and the historical backlog.
    _db_prop_conn = _db_conn()
    if _db_prop_conn:
        try:
            _dp_cur = _db_prop_conn.cursor()
            _dp_cur.execute("""
                SELECT id, player, pick, line, bet_type, pick_category, game,
                       DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York')
                FROM bets
                WHERE (result IS NULL OR result = 'void')
                  AND pick_category IN ('neutral_prop','fade_prop','benefactor_prop')
                  AND player IS NOT NULL AND player != ''
                ORDER BY id
            """)
            _db_prop_rows = _dp_cur.fetchall()
            _dp_cur.close()

            print(f"[DB-PropSettle] query returned {len(_db_prop_rows)} unsettled prop rows")
            if _db_prop_rows:
                # Collect unique dates across all unsettled bets so we can batch
                # BDL API calls — one request per date, not one per bet row.
                # Cap at 40 most-recent dates to avoid excess API calls.
                _all_dates = sorted({str(r[7]) for r in _db_prop_rows if r[7]}, reverse=True)
                _bet_dates = list(reversed(_all_dates[:40]))
                print(f"[DB-PropSettle] {len(_db_prop_rows)} unsettled bets across {len(_bet_dates)} date(s)")

                # Build date → {player_lower → stat_row} lookup for final games only
                _bdl_by_date: dict = {}
                for _d in _bet_dates:
                    _bdl_by_date[_d] = {}
                    try:
                        # Paginate through all BDL pages for this date so late-game
                        # teams (pages 2-3) are included — not just the first 100.
                        _page, _max_pages = 1, 1
                        while _page <= _max_pages and _page <= 5:
                            _url = f"{BDL_BASE}/stats?dates[]={_d}&per_page=100&page={_page}"
                            _resp = _bdl_get(_url)
                            _meta = _resp.get("meta", {})
                            _max_pages = int(_meta.get("total_pages") or 1)
                            for _st in _resp.get("data", []):
                                _p = _st.get("player", {})
                                _nm = f"{_p.get('first_name','')} {_p.get('last_name','')}".strip().lower()
                                if "final" in (_st.get("game", {}).get("status", "")).lower():
                                    _bdl_by_date[_d][_nm] = _st
                            _page += 1
                        print(f"[DB-PropSettle] {_d}: {len(_bdl_by_date[_d])} final player rows from BDL ({_max_pages} page(s))")
                    except Exception as _dbe:
                        print(f"[DB-PropSettle] BDL fetch error {_d}: {_dbe}")

                # ── ESPN fallback: covers fringe/two-way players BDL doesn't track ──
                # Fetches full box scores (all players, all games) for each date.
                # Returns same {pts, reb, ast, fg3m} format as BDL lookup.
                _espn_by_date: dict = {}
                for _d in _bet_dates:
                    _espn_by_date[_d] = {}
                    try:
                        _esp_date = _d.replace("-", "")  # "2026-04-02" → "20260402"
                        _sb = _espn_get(f"{ESPN_BASE}/scoreboard?dates={_esp_date}")
                        for _ev in _sb.get("events", []):
                            _gid   = _ev.get("id", "")
                            _gstat = _ev.get("status", {}).get("type", {}).get("description", "")
                            if "final" not in _gstat.lower():
                                continue
                            _summ = _espn_get(f"{ESPN_BASE}/summary?event={_gid}")
                            for _tm in _summ.get("boxscore", {}).get("players", []):
                                for _sb_stat in _tm.get("statistics", []):
                                    _keys = _sb_stat.get("keys", [])
                                    # ESPN uses full words: "points", "rebounds", "assists"
                                    # and "threePointFieldGoalsMade-threePointFieldGoalsAttempted"
                                    _i_pts = next((i for i, k in enumerate(_keys) if k == "points"), None)
                                    _i_reb = next((i for i, k in enumerate(_keys) if k == "rebounds"), None)
                                    _i_ast = next((i for i, k in enumerate(_keys) if k == "assists"), None)
                                    _i_3pt = next((i for i, k in enumerate(_keys) if "threePoint" in k and "Made" in k), None)
                                    if _i_pts is None or _i_reb is None or _i_ast is None:
                                        continue
                                    for _ath in _sb_stat.get("athletes", []):
                                        # ESPN uses displayName, not fullName
                                        _aname = (_ath.get("athlete", {}).get("displayName") or "").lower()
                                        _astats = _ath.get("stats", [])
                                        if not _aname or not _astats:
                                            continue
                                        try:
                                            _fg3m = 0.0
                                            if _i_3pt is not None and _i_3pt < len(_astats):
                                                _v3 = str(_astats[_i_3pt])
                                                _fg3m = float(_v3.split("-")[0]) if "-" in _v3 else float(_v3 or 0)
                                            _espn_by_date[_d][_aname] = {
                                                "pts":  float(_astats[_i_pts] if _i_pts < len(_astats) else 0),
                                                "reb":  float(_astats[_i_reb] if _i_reb < len(_astats) else 0),
                                                "ast":  float(_astats[_i_ast] if _i_ast < len(_astats) else 0),
                                                "fg3m": _fg3m,
                                            }
                                        except Exception:
                                            continue
                        print(f"[DB-PropSettle] {_d}: {len(_espn_by_date[_d])} ESPN player rows")
                    except Exception as _ee:
                        print(f"[DB-PropSettle] ESPN fallback error {_d}: {_ee}")

                # ── NBA official API fallback (stats.nba.com) ──────────────────────
                # Third tier: catches anything ESPN also misses (rare).
                _nba_hdrs = {
                    "Host": "stats.nba.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.nba.com/",
                    "Origin": "https://www.nba.com",
                    "x-nba-stats-origin": "stats",
                    "x-nba-stats-token": "true",
                    "Connection": "keep-alive",
                }
                _nba_by_date: dict = {}
                for _d in _bet_dates:
                    _nba_by_date[_d] = {}
                    try:
                        _mo, _dy, _yr = _d[5:7], _d[8:10], _d[:4]
                        _gdate_nba = f"{_mo}/{_dy}/{_yr}"  # "04/02/2026"
                        import urllib.request as _nba_ur
                        _sb_req = _nba_ur.Request(
                            f"https://stats.nba.com/stats/scoreboardv2?GameDate={_gdate_nba}&LeagueID=00&DayOffset=0",
                            headers=_nba_hdrs
                        )
                        with _nba_ur.urlopen(_sb_req, timeout=15) as _r:
                            _sb_nba = json.loads(_r.read())
                        _nba_game_ids = []
                        for _rs in _sb_nba.get("resultSets", []):
                            if _rs.get("name") == "GameHeader":
                                _nh = _rs.get("headers", [])
                                _nr = _rs.get("rowSet", [])
                                _si = _nh.index("GAME_STATUS_TEXT") if "GAME_STATUS_TEXT" in _nh else -1
                                _gi = _nh.index("GAME_ID") if "GAME_ID" in _nh else 0
                                for _nr_row in _nr:
                                    if _si >= 0 and "Final" in str(_nr_row[_si]):
                                        _nba_game_ids.append(str(_nr_row[_gi]))
                        for _gid in _nba_game_ids:
                            try:
                                _bs_req = _nba_ur.Request(
                                    f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={_gid}"
                                    "&StartPeriod=0&EndPeriod=10&StartRange=0&EndRange=2800&RangeType=0",
                                    headers=_nba_hdrs
                                )
                                with _nba_ur.urlopen(_bs_req, timeout=15) as _r:
                                    _bs_nba = json.loads(_r.read())
                                for _rs in _bs_nba.get("resultSets", []):
                                    if _rs.get("name") == "PlayerStats":
                                        _nh = _rs.get("headers", [])
                                        _idx = lambda k: _nh.index(k) if k in _nh else -1
                                        _i_nm  = _idx("PLAYER_NAME")
                                        _i_pts = _idx("PTS")
                                        _i_reb = _idx("REB")
                                        _i_ast = _idx("AST")
                                        _i_fg3 = _idx("FG3M")
                                        for _nr_row in _rs.get("rowSet", []):
                                            _nm = str(_nr_row[_i_nm]).lower() if _i_nm >= 0 else ""
                                            if not _nm:
                                                continue
                                            _nba_by_date[_d][_nm] = {
                                                "pts":  float(_nr_row[_i_pts] or 0) if _i_pts >= 0 else 0,
                                                "reb":  float(_nr_row[_i_reb] or 0) if _i_reb >= 0 else 0,
                                                "ast":  float(_nr_row[_i_ast] or 0) if _i_ast >= 0 else 0,
                                                "fg3m": float(_nr_row[_i_fg3] or 0) if _i_fg3 >= 0 else 0,
                                            }
                            except Exception:
                                continue
                        print(f"[DB-PropSettle] {_d}: {len(_nba_by_date[_d])} NBA player rows")
                    except Exception as _ne:
                        print(f"[DB-PropSettle] NBA fallback error {_d}: {_ne}")

                _dp_upd = _db_prop_conn.cursor()
                _STAT_BDL = {"points": "pts", "rebounds": "reb", "assists": "ast", "threes": "fg3m"}

                def _resolve_stat(pick_txt, stored_line, stat_row):
                    """Return (stat_key, actual_value) or (None, None).
                    New rows: pick is 'OVER|points' — parse directly.
                    Legacy rows: pick is bare 'OVER'/'UNDER' — infer from
                    which BDL stat value is numerically closest to stored_line.
                    Accepts both full words and FanDuel/BDL abbreviations:
                      pts/points, reb/rebounds, ast/assists, 3pt/fg3/threes."""
                    _pl = (pick_txt or "").lower()
                    # New embedded format
                    if "|" in _pl:
                        _part = _pl.split("|", 1)[1].strip()
                        if "point" in _part or _part == "pts":
                            return "points",   float(stat_row.get("pts")  or 0)
                        if "rebound" in _part or _part == "reb":
                            return "rebounds", float(stat_row.get("reb")  or 0)
                        if "assist" in _part or _part == "ast":
                            return "assists",  float(stat_row.get("ast")  or 0)
                        if "three" in _part or "fg3" in _part or "3pt" in _part:
                            return "threes",   float(stat_row.get("fg3m") or 0)
                    # Legacy bare OVER/UNDER — infer from closest BDL stat.
                    # The bot only makes single-stat props (no PRA combos), so
                    # the nearest stat is always the correct one.
                    try:
                        _ln = float(stored_line or 0)
                        _candidates_stat = {
                            "points":   abs(float(stat_row.get("pts")  or 0) - _ln),
                            "rebounds": abs(float(stat_row.get("reb")  or 0) - _ln),
                            "assists":  abs(float(stat_row.get("ast")  or 0) - _ln),
                            "threes":   abs(float(stat_row.get("fg3m") or 0) - _ln),
                        }
                        _best    = min(_candidates_stat, key=_candidates_stat.get)
                        _bdl_key = _STAT_BDL[_best]
                        return _best, float(stat_row.get(_bdl_key) or 0)
                    except Exception:
                        return None, None

                for _row in _db_prop_rows:
                    _rid, _player, _pick_txt, _line, _btype, _pcat, _game, _bet_date = _row
                    _pkey = (_player or "").strip().lower()
                    # Three-tier lookup: BDL → ESPN → NBA official API
                    _stat_row = _bdl_by_date.get(str(_bet_date), {}).get(_pkey)
                    if not _stat_row:
                        _stat_row = _espn_by_date.get(str(_bet_date), {}).get(_pkey)
                    if not _stat_row:
                        _stat_row = _nba_by_date.get(str(_bet_date), {}).get(_pkey)
                    if not _stat_row:
                        continue
                    _rstat, _actual = _resolve_stat(_pick_txt, _line, _stat_row)
                    if _rstat is None or _actual is None:
                        continue
                    _line_f = float(_line or 0)
                    _dir    = "OVER" if "OVER" in (_pick_txt or "").upper() else "UNDER"
                    _res    = ("win" if _actual > _line_f else "loss") if _dir == "OVER" \
                              else ("win" if _actual < _line_f else "loss")
                    try:
                        _dp_upd.execute("""
                            UPDATE bets
                               SET result=%s, actual_value=%s,
                                   prediction_error = CASE
                                       WHEN prediction IS NOT NULL THEN prediction - %s
                                       ELSE NULL
                                   END
                             WHERE id=%s AND (result IS NULL OR result = 'void')
                        """, (_res, _actual, _actual, _rid))
                        _db_prop_conn.commit()
                        newly_settled += 1
                        print(f"[DB-PropSettle] #{_rid} {_player} | {_pick_txt} | "
                              f"actual={_actual} {_dir} {_line_f} → {_res}")
                    except Exception as _upe:
                        print(f"[DB-PropSettle] update error #{_rid}: {_upe}")
                _dp_upd.close()
        except Exception as _dpe:
            print(f"[DB-PropSettle] error: {_dpe}")
        finally:
            try:
                _db_prop_conn.close()
            except Exception:
                pass

    if changed:
        json.dump(bets, open(BETS_FILE, "w"), indent=2, cls=_DatetimeEncoder)

    return newly_settled


def send_results_recap():
    """Send a morning results recap to VIP — once per day, after 8 AM ET."""
    global _results_recap_sent
    _results_recap_sent = _results_recap_sent or load_status().get("_recap_sent_date", "")

    import zoneinfo as _zi
    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    # Fire between 12 AM and 9 AM ET — after all games are done
    if not (0 <= et_now.hour < 9):
        return

    # The "game night" is yesterday (recap fires after midnight)
    game_date = (et_now.date() - timedelta(days=1)).isoformat()
    if _results_recap_sent == game_date:
        return

    today_str = game_date  # re-use variable name for guard below

    bets = load_bets()
    cutoff = game_date
    def _bet_date_str(b):
        t = b.get("time", "")
        if hasattr(t, "strftime"):
            return t.strftime("%Y-%m-%d")
        return str(t)[:10]
    settled_recent = [
        b for b in bets
        if b.get("result") and _bet_date_str(b) >= cutoff
    ]
    if not settled_recent:
        return

    wins   = [b for b in settled_recent if b["result"] == "win"]
    losses = [b for b in settled_recent if b["result"] == "loss"]
    total  = len(settled_recent)
    win_pct = round(len(wins) / total * 100) if total else 0

    # ── Pick-by-pick lines (grouped: wins then losses) ──────────────
    PICK_EMOJI = {
        "SPREAD": "📉", "MONEYLINE": "🔥", "OVER": "📈",
        "UNDER": "📉", "points": "🏀", "rebounds": "💪",
        "assists": "🔥", "threes": "🎯", "PARLAY": "🎯",
    }

    def _fmt_pick(b):
        bet_type = b.get("betType", "ML")
        em       = PICK_EMOJI.get(bet_type, "🎯")
        player   = b.get("player", "") or ""
        pick     = b.get("pick", "")
        game     = b.get("game", "") or ""
        conf     = b.get("confidence", 0)
        conf_str = f" — {conf}%" if conf else ""
        # Player prop — show player name
        if player and player != game:
            return f"{em} {player} {pick}{conf_str}"
        # Game total (OVER/UNDER) — show game so you know which matchup
        if bet_type in ("OVER", "UNDER") and game:
            return f"{em} {pick} ({game}){conf_str}"
        # Spread / Moneyline — pick already contains team name
        return f"{em} {pick} ({bet_type}){conf_str}"

    win_lines  = [_fmt_pick(b) for b in settled_recent if b["result"] == "win"]
    loss_lines = [_fmt_pick(b) for b in settled_recent if b["result"] == "loss"]

    picks_block = (
        f"✅ *WINS ({len(win_lines)})*\n" + "\n".join(win_lines) +
        f"\n\n❌ *LOSSES ({len(loss_lines)})*\n" + ("\n".join(loss_lines) if loss_lines else "_None_")
    )
    pick_lines = win_lines + loss_lines  # keep for compat

    # ── Bet-type breakdown ──────────────────────────────────────────
    type_record = {}
    for b in settled_recent:
        t = b.get("betType", "ML")
        type_record.setdefault(t, {"w": 0, "l": 0})
        if b["result"] == "win":
            type_record[t]["w"] += 1
        else:
            type_record[t]["l"] += 1
    type_lines = []
    for t, rec in sorted(type_record.items()):
        em = "✅" if rec["w"] > rec["l"] else ("❌" if rec["l"] > rec["w"] else "➖")
        type_lines.append(f"  {em} {t}: {rec['w']}-{rec['l']}")

    # ── Current streak (across all-time settled bets) ───────────────
    all_settled = sorted(
        [b for b in bets if b.get("result")],
        key=lambda b: b.get("time", "")
    )
    streak_count, streak_type = 0, None
    for b in reversed(all_settled):
        r = b["result"]
        if streak_type is None:
            streak_type, streak_count = r, 1
        elif r == streak_type:
            streak_count += 1
        else:
            break
    if streak_type == "win":
        streak_line = f"🔥 *{streak_count} WIN STREAK*" if streak_count > 1 else "🔥 Won last pick"
    else:
        streak_line = f"❄️ *{streak_count} game skid*" if streak_count > 1 else "❌ Lost last pick"

    # ── Best pick of the night ──────────────────────────────────────
    best = max(wins, key=lambda b: b.get("confidence", 0), default=None)
    best_line = ""
    if best:
        best_line = (
            f"⭐ *Best Pick:* {best['pick']} ({best.get('betType','ML')}) "
            f"— {best.get('confidence', 0)}% conf"
        )

    # ── ROI tracker (1 unit = $100, default -110 juice) ─────────────
    roi = 0.0
    for b in settled_recent:
        odds = b.get("odds", 0) or 0
        if b["result"] == "win":
            if odds > 0:
                roi += (odds / 100.0)
            elif odds < 0:
                roi += (100.0 / abs(odds))
            else:
                roi += 0.909      # -110 default
        else:
            roi -= 1.0
    roi_sign = "+" if roi >= 0 else ""
    roi_line = f"💰 *ROI tonight:* {roi_sign}{roi:.2f}u  _(1u = $100)_"

    # ── Confidence accuracy ─────────────────────────────────────────
    high_conf = [b for b in settled_recent if b.get("confidence", 0) >= 75]
    low_conf  = [b for b in settled_recent if b.get("confidence", 0) <  75]
    hc_hits   = sum(1 for b in high_conf if b["result"] == "win")
    lc_hits   = sum(1 for b in low_conf  if b["result"] == "win")
    conf_parts = []
    if high_conf:
        conf_parts.append(f"75%+ conf: *{round(hc_hits/len(high_conf)*100)}%* hit")
    if low_conf:
        conf_parts.append(f"<75% conf: *{round(lc_hits/len(low_conf)*100)}%* hit")
    conf_line = "🎯 " + "  ·  ".join(conf_parts) if conf_parts else ""

    # ── All-time record ─────────────────────────────────────────────
    all_wins  = sum(1 for b in all_settled if b["result"] == "win")
    all_total = len(all_settled)
    all_pct   = round(all_wins / all_total * 100) if all_total else 0
    all_time_line = f"📈 All-time: {all_wins}-{all_total - all_wins} ({all_pct}%)"

    # ── Tonight's preview teaser ────────────────────────────────────
    preview_line = ""
    try:
        tonight = get_todays_games()
        gc = len(tonight)
        if gc:
            preview_line = (
                f"\n🏀 *{gc} game{'s' if gc != 1 else ''} on the slate tonight* "
                f"— picks drop 1 hour before each tip-off"
            )
    except Exception:
        pass

    # ── Assemble full message (Performance Report format) ───────────
    D   = "━━━━━━━━━━━━━━━━━━━"
    SEP = "\n---\n"
    roi_sign = "+" if roi >= 0 else ""

    # ── Highlights ──────────────────────────────────────────────────
    top_play   = max(wins,   key=lambda b: b.get("confidence", 0), default=None)
    worst_beat = max(losses, key=lambda b: b.get("confidence", 0), default=None)
    # Best value = win with lowest confidence (beat the odds)
    value_hit  = min(wins,   key=lambda b: b.get("confidence", 0), default=None)

    def _pick_label(b):
        """Short readable label for a single bet."""
        player   = b.get("player", "") or ""
        pick     = b.get("pick", "")
        bet_type = b.get("betType", "")
        game     = b.get("game", "") or ""
        conf     = b.get("confidence", 0)
        conf_str = f" ({conf}%)" if conf else ""
        if player and player != game:
            return f"{player} — {pick}{conf_str}"
        if bet_type in ("OVER", "UNDER") and game:
            return f"{pick} ({game}){conf_str}"
        return f"{pick}{conf_str}"

    highlights = ""
    if top_play:
        tp_rec = f"1–0"
        highlights += (
            f"🔥 *TOP PLAY OF THE DAY*\n"
            f"Record: {tp_rec}\n"
            f"{_pick_label(top_play)}\n"
        )
    if value_hit and value_hit != top_play:
        highlights += f"\n💎 *BEST VALUE HIT*\n{_pick_label(value_hit)}\n"
    if worst_beat:
        highlights += f"\n📉 *WORST BEAT OF THE DAY*\n{_pick_label(worst_beat)}"

    # ── Category grouping helpers ────────────────────────────────────
    PROP_TYPES_RC = {"points", "rebounds", "assists", "threes", "steals", "blocks"}

    _PROP_BET_CATS = {
        "elite_prop", "individual", "prop", "neutral_prop",
        "fade_prop", "benefactor_prop",
    }

    def _cat(b):
        bt = b.get("betType", "")
        if bt in PROP_TYPES_RC:               return "props"
        if bt.lower() in _PROP_BET_CATS:      return "props"
        if bt == "SPREAD":                     return "spreads"
        if bt in ("OVER", "UNDER"):            return "totals"
        if b.get("betType") == "MONEYLINE":   return "moneylines"
        return "moneylines"

    def _cat_lines(bet_list):
        groups = {"props": [], "spreads": [], "totals": [], "moneylines": []}
        for b in bet_list:
            groups[_cat(b)].append(b)
        out = []
        if groups["props"]:
            out.append("🏀 *Player Props*")
            for b in groups["props"]:
                out.append(f"  {_pick_label(b)}")
        if groups["spreads"]:
            out.append("📉 *Spreads*")
            for b in groups["spreads"]:
                out.append(f"  {_pick_label(b)}")
        if groups["totals"]:
            out.append("📊 *Totals*")
            for b in groups["totals"]:
                out.append(f"  {_pick_label(b)}")
        if groups["moneylines"]:
            out.append("🔥 *Moneylines*")
            for b in groups["moneylines"]:
                out.append(f"  {_pick_label(b)}")
        return "\n".join(out)

    wins_section   = f"✅ *SUCCESSFUL PLAYS ({len(wins)})*\n\n{_cat_lines(wins)}"
    losses_section = f"❌ *UNSUCCESSFUL PLAYS ({len(losses)})*\n\n" + (_cat_lines(losses) if losses else "_None_")

    # ── Performance breakdown table ──────────────────────────────────
    # Group by category (Props / Spreads / Totals / Moneylines)
    cat_record = {"props": {"w":0,"l":0}, "spreads": {"w":0,"l":0},
                  "totals": {"w":0,"l":0}, "moneylines": {"w":0,"l":0}}
    for b in settled_recent:
        c = _cat(b)
        cat_record[c]["w" if b["result"]=="win" else "l"] += 1

    CAT_DISPLAY = {"props": "Player Props", "spreads": "Spreads",
                   "totals": "Totals", "moneylines": "Moneylines"}
    perf_rows = []
    for c, label in CAT_DISPLAY.items():
        w, l = cat_record[c]["w"], cat_record[c]["l"]
        if w + l == 0:
            continue
        pct = round(w / (w + l) * 100)
        perf_rows.append(f"  {label:<14} {w}–{l}    {pct}%")
    perf_table = (
        "*📈 PERFORMANCE BREAKDOWN*\n"
        "`  Category       Record  Win Rate`\n"
        "`" + "`\n`".join(perf_rows) + "`"
    ) if perf_rows else ""

    # ── Strategy breakdown (SAFE / BALANCED / AGGRESSIVE) ───────────
    tier_record = {}
    for b in settled_recent:
        tier = (b.get("tier") or "").upper()
        if tier not in ("SAFE", "BALANCED", "AGGRESSIVE"):
            continue
        tier_record.setdefault(tier, {"w": 0, "l": 0})
        tier_record[tier]["w" if b["result"] == "win" else "l"] += 1

    strat_table = ""
    if tier_record:
        strat_rows = []
        for tier in ("SAFE", "BALANCED", "AGGRESSIVE"):
            if tier not in tier_record:
                continue
            w, l = tier_record[tier]["w"], tier_record[tier]["l"]
            pct = round(w / (w + l) * 100)
            strat_rows.append(f"  {tier:<12} {w}–{l}    {pct}%")
        strat_table = (
            "*🎯 STRATEGY BREAKDOWN*\n"
            "`  Tier          Record  Win Rate`\n"
            "`" + "`\n`".join(strat_rows) + "`"
        )

    # ── Pick category breakdown ──────────────────────────────────────
    _cat_record = {}
    for b in settled_recent:
        _bc = b.get("pick_category") or (
            "VIP_LOCK" if b.get("betType") == "VIP_LOCK" else
            "SGP"      if b.get("betType") == "SGP"      else
            "INDIVIDUAL"
        )
        _cat_record.setdefault(_bc, {"w": 0, "l": 0})
        _cat_record[_bc]["w" if b["result"] == "win" else "l"] += 1

    _cat_label_map = {
        "VIP_LOCK":         "VIP Lock",
        "CROSS_GAME_PARLAY": "Cross Game Parlay",
        "SGP":              "SGP",
        "INDIVIDUAL":       "Individual",
    }
    cat_table = ""
    if _cat_record:
        _cat_rows = []
        for _ck in ("VIP_LOCK", "INDIVIDUAL", "CROSS_GAME_PARLAY", "SGP"):
            if _ck not in _cat_record:
                continue
            _cw, _cl = _cat_record[_ck]["w"], _cat_record[_ck]["l"]
            _cpct = round(_cw / (_cw + _cl) * 100) if (_cw + _cl) else 0
            _clabel = _cat_label_map.get(_ck, _ck)
            _cat_rows.append(f"  {_clabel:<16} {_cw}–{_cl}    {_cpct}%")
        if _cat_rows:
            cat_table = (
                "*🗂 PICK CATEGORY BREAKDOWN*\n"
                "`  Category         Record  Win Rate`\n"
                "`" + "`\n`".join(_cat_rows) + "`"
            )

    # ── Final takeaway (auto-generated) ─────────────────────────────
    prop_w = cat_record["props"]["w"]
    prop_l = cat_record["props"]["l"]
    tot_w  = cat_record["totals"]["w"]
    tot_l  = cat_record["totals"]["l"]
    spr_w  = cat_record["spreads"]["w"]

    if win_pct >= 70:
        if prop_w > 0 and prop_l == 0:
            takeaway = "Dominant night — player props went perfect. Model is dialled in."
        elif spr_w > 0:
            takeaway = "Strong performance across the board. Spreads and props both delivered."
        else:
            takeaway = f"Profitable night at {win_pct}%. Model continues to identify high-value spots."
    elif win_pct >= 50:
        if tot_l > 0:
            takeaway = "Solid night on props and spreads. Game totals were the weak spot — model is adjusting."
        else:
            takeaway = "Steady night. Above breakeven and the model is building confidence."
    else:
        takeaway = "Tough night. Model is reviewing the losses and self-correcting for tomorrow."

    # ── Build full message ───────────────────────────────────────────
    parts = [
        f"🏆 *ELITE PICKS — PERFORMANCE REPORT*\n_{et_now.strftime('%B %d, %Y')}_",
        D,
        highlights,
        D,
        wins_section,
        D,
        losses_section,
        D,
        perf_table,
    ]
    if strat_table:
        parts += [SEP.strip(), strat_table]
    if cat_table:
        parts += [SEP.strip(), cat_table]

    parts += [
        D,
        f"📊 *Record: {len(wins)}-{len(losses)} ({win_pct}% hit rate)*\n"
        f"📈 All-time: *{all_wins}-{all_total - all_wins}* ({all_pct}%)\n"
        f"🔒 Model is learning from every result.",
        D,
        f"🔒 *FINAL TAKEAWAY*\n{takeaway}",
    ]
    if preview_line:
        parts += [D, preview_line.strip()]

    msg = "\n".join(parts)
    send(msg, VIP_CHANNEL)

    # ── Free channel — trimmed record + one named win + streak callout ──
    if FREE_CHANNEL:
        streak_emoji = "🔥" if streak_type == "win" else "❄️"
        roi_sign_f   = "+" if roi >= 0 else ""
        result_emoji = "✅" if win_pct >= 55 else ("➖" if win_pct >= 45 else "❌")

        # #3 — Name the best winning pick so free users see what they missed
        best_win_line = ""
        if top_play:
            best_win_line = f"⭐ *VIP hit:* {_pick_label(top_play)} ✅"

        # #4 — Streak callout with urgency when on a run
        streak_callout = ""
        if streak_type == "win" and streak_count >= 2:
            streak_callout = f"🔥 *VIP is on a {streak_count}-pick win streak — next picks drop tonight*"
        elif streak_type == "win":
            streak_callout = f"🔥 Won last pick — next picks drop tonight"

        free_recap = "\n".join(filter(None, [
            f"📊 *LAST NIGHT'S RESULTS*\n_{et_now.strftime('%B %d, %Y')}_",
            f"━━━━━━━━━━━━━━━━━━━",
            f"{result_emoji} *{len(wins)}-{len(losses)}*  ({win_pct}% hit rate)",
            f"💰 ROI: *{roi_sign_f}{roi:.2f}u*  _(1u = $100)_",
            f"📈 All-time: *{all_wins}-{all_total - all_wins}* ({all_pct}%)",
            best_win_line,
            f"━━━━━━━━━━━━━━━━━━━",
            streak_callout,
            f"🔒 Full breakdown + tonight's picks → VIP members only",
            f"👉 {CHECKOUT_URL}",
            preview_line.strip() if preview_line else "",
        ]))
        send(free_recap, FREE_CHANNEL)

        # Send free sample only for parlays that actually fully hit
        _status_data    = load_status()
        _saved_parlays  = _status_data.get("_last_parlay_legs", {})
        _tier_labels    = {
            "safe":       ("💰 SAFE PARLAY",       "💰"),
            "balanced":   ("🎯 BALANCED PARLAY",   "🎯"),
            "aggressive": ("🧨 AGGRESSIVE PARLAY", "🧨"),
        }
        _wins_index = {}
        for _w in wins:
            _key = (_w.get("game", ""), _w.get("betType", ""))
            _wins_index[_key] = _w

        for _tier in ("safe", "balanced", "aggressive"):
            _tier_legs = _saved_parlays.get(_tier, [])
            if not _tier_legs:
                continue
            # Check every leg has a matching win
            _leg_hits = []
            _all_hit  = True
            for _lg in _tier_legs:
                _match_key = (_lg.get("game",""), _lg.get("bet_type",""))
                _matched   = _wins_index.get(_match_key)
                if not _matched:
                    _all_hit = False
                    break
                _lg_player  = _matched.get("player", "") or ""
                _lg_game    = _lg.get("game", "")
                _lg_desc    = _lg.get("desc", "")
                _lg_conf    = _matched.get("confidence", 0)
                _lg_display = _lg_player if _lg_player and _lg_player != _lg_game else _lg_game
                _leg_hits.append(f"✅ {_lg_display} — {_lg_desc} ({_lg_conf}%)")
            if not _all_hit:
                continue
            _label, _icon = _tier_labels[_tier]
            _n = len(_leg_hits)
            _tier_odds = _parlay_odds(_n)
            sample_msg = (
                f"{_icon} *FREE SAMPLE — VIP {_label} HIT LAST NIGHT*\n\n"
                + "\n".join(_leg_hits)
                + f"\n\n{_n}/{_n} legs hit 🔥  (approx +{_tier_odds:,})\n"
                f"_VIP members had this parlay before the games started_\n\n"
                f"Want picks like this every night?\n"
                f"👉 {CHECKOUT_URL}"
            )
            send(sample_msg, FREE_CHANNEL)

    _results_recap_sent = today_str
    save_status(0, {"_recap_sent_date": today_str})
    print(f"  [recap] Results sent — {len(wins)}/{total} picks hit")


def send_monthly_report():
    """Send a full monthly recap on the 1st of each month."""
    global _monthly_report_sent
    _monthly_report_sent = _monthly_report_sent or load_status().get("_monthly_sent_key", "")
    now = datetime.now()
    if now.day != 1:
        return
    month_key = now.strftime("%Y-%m")
    if _monthly_report_sent == month_key:
        return

    bets = load_bets()
    last_month = (now.replace(day=1) - timedelta(days=1))
    lm_str = last_month.strftime("%Y-%m")
    lm_bets = [b for b in bets if b.get("time", "").startswith(lm_str)]

    if not lm_bets:
        _monthly_report_sent = month_key
        return

    settled = [b for b in lm_bets if b.get("result")]
    wins   = sum(1 for b in settled if b["result"] == "win")
    losses = len(settled) - wins
    total  = len(settled)
    pct    = round(wins / total * 100, 1) if total else 0

    # Best bet type
    type_wins = {}
    for b in settled:
        t = b.get("betType", "ML")
        type_wins.setdefault(t, {"w": 0, "l": 0})
        if b["result"] == "win": type_wins[t]["w"] += 1
        else: type_wins[t]["l"] += 1
    best_type = max(type_wins, key=lambda t: type_wins[t]["w"] / max(type_wins[t]["w"] + type_wins[t]["l"], 1)) if type_wins else "—"

    # All-time
    all_settled = [b for b in bets if b.get("result")]
    all_wins = sum(1 for b in all_settled if b["result"] == "win")
    all_pct  = round(all_wins / len(all_settled) * 100, 1) if all_settled else 0

    msg = (
        f"📅 *{last_month.strftime('%B %Y')} — MONTHLY REPORT*\n\n"
        f"📊 Record: *{wins}-{losses}* ({pct}% hit rate)\n"
        f"🏆 Best category: *{best_type}*\n"
        f"📈 All-time: {all_wins}-{len(all_settled)-all_wins} ({all_pct}%)\n\n"
        f"_A new month. New edges. Let's get it._"
    )
    send(msg, VIP_CHANNEL)
    _monthly_report_sent = month_key
    save_status(0, {"_monthly_sent_key": month_key})
    print(f"  [monthly] Report sent for {lm_str}")


def send_free_preview():
    """Send a daily game-day preview to the free channel at noon ET."""
    global _free_preview_sent
    _free_preview_sent = _free_preview_sent or load_status().get("_free_preview_date", "")
    try:
        import zoneinfo as _zi
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    today_str = et_now.strftime("%Y-%m-%d")
    if _free_preview_sent == today_str:
        return
    if et_now.hour != 8:   # only fire during the 8 AM ET hour
        return

    # Use already-cached odds data for tip-off times — no extra API call
    try:
        import zoneinfo as _zi2
        _, odds_events = get_odds_cached()
        today_date = et_now.date()
        games_raw = []
        for e in odds_events:
            try:
                ct = datetime.strptime(e["commence_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                et = ct.astimezone(_zi2.ZoneInfo("America/New_York"))
                if et.date() == today_date:
                    games_raw.append((e["away_team"], e["home_team"], et))
            except Exception:
                continue
        games_raw.sort(key=lambda x: x[2])
    except Exception:
        games_raw = []

    if not games_raw:
        # Fallback to unified data layer if odds API fails
        games_raw_unified = get_todays_games()
        if not games_raw_unified:
            return
        games_raw = [(g.get("away_team", ""), g.get("home_team", ""), None)
                     for g in games_raw_unified[:8]]

    lines = []
    for item in games_raw[:8]:
        a, h, et_time = item
        tip_str = et_time.strftime("%-I:%M %p ET") if et_time else "TBD"
        lines.append(f"🏀 {a} @ {h} — {tip_str}")

    game_count = len(games_raw)
    msg = (
        f"🔥 *TONIGHT'S NBA SLATE* — {et_now.strftime('%b %d')}\n\n"
        + "\n".join(lines)
        + f"\n\n🔒 *Elite picks drop 1 hour before each tip-off — VIP only*\n"
        f"📊 Moneyline · Spread · O/U · Player Props · Starting Five\n\n"
        f"👉 {CHECKOUT_URL}"
    )
    if FREE_CHANNEL:
        send(msg, FREE_CHANNEL)
    _free_preview_sent = today_str
    save_status(0, {"_free_preview_date": today_str})
    print(f"  [free] Daily preview sent — {game_count} games")


# ==========================
# 🧠 SELF-LEARNING ENGINE
# ==========================
# ── Script trigger thresholds (defaults; self-calibrated from real data) ─────
_SCRIPT_THRESHOLD_DEFAULTS = {
    "halfcourt_total_max":  208,   # Vegas total < this  → HALFCOURT (defensive grind)
    "slow_paced_total_max": 218,   # 208 ≤ total < this  → SLOW_PACED (sluggish pace)
    "uptempo_total_min":    224,   # this ≤ total < transition_total_min → UPTEMPO (fast, not shootout)
    "transition_total_min": 232,   # Vegas total ≥ this  → TRANSITION_HEAVY (shootout)
    "blowout_spread_min":   12,    # |spread| >= this    → BLOWOUT (heavy favourite)
    "tight_spread_max":     3.5,   # |spread| <= this    → TIGHT_GAME
    "upset_spread_min":     6,     # upset range start
    "upset_spread_max":     14,    # upset range end
    "prop_minutes_gate":    25,    # min avg minutes for a prop to fire — learned from data
    "prop_starter_mins":    28,    # avg minutes to be classified a starter — learned from data
    "prop_usage_gate":      10,    # min avg possession usage (FGA+0.44*FTA+TOV) — learned from data
}

_LEARNING_DEFAULTS = {
    "spread_bias": 0.0,
    "total_bias": 0.0,
    "spread_edge_threshold": SPREAD_EDGE_THRESHOLD,
    "total_edge_threshold": TOTALS_EDGE_THRESHOLD,
    "spread_errors": [],
    "total_errors": [],
    "win_rate": None,
    "total_settled": 0,
    "version": 0,
    # Per-type historical win rates — populated by retrain_from_results()
    # Each key: {"win_rate": float (0-100), "count": int}
    "win_rate_by_type": {},
    # Script trigger thresholds — start at defaults; self-calibrate from graded picks
    "script_thresholds": dict(_SCRIPT_THRESHOLD_DEFAULTS),
}

def load_learning_data():
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM learning_data")
            rows = cur.fetchall()
            cur.close(); conn.close()
            if rows:
                result = dict(_LEARNING_DEFAULTS)
                for k, v in rows:
                    result[k] = v if not isinstance(v, str) else _try_parse(v)
                return result
        except Exception as e:
            print(f"[DB] load_learning_data error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        if os.path.exists(LEARNING_FILE):
            return json.load(open(LEARNING_FILE))
    except Exception:
        pass
    return dict(_LEARNING_DEFAULTS)


def save_learning_data(data):
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            for k, v in data.items():
                val = _safe_json_dumps(v)
                cur.execute(
                    "INSERT INTO learning_data (key, value) VALUES (%s, %s::jsonb) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                    (k, val)
                )
            conn.commit()
            cur.close(); conn.close()
            return
        except Exception as e:
            print(f"[DB] save_learning_data error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        json.dump(data, open(LEARNING_FILE, "w"), indent=2, cls=_DatetimeEncoder)
    except Exception as e:
        print(f"save_learning_data error: {e}")


def update_prediction_bias():
    """
    Lightweight bias update — runs every cycle (every 10 minutes).
    No sklearn, no model refit. Just queries the DB for the average prediction
    error per stat type and updates the stored bias correction so the next
    prediction immediately benefits from it.
    """
    STAT_TYPES = ("points", "rebounds", "assists", "threes")
    conn = _db_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT bet_type, AVG(prediction_error), COUNT(*)
               FROM bets
               WHERE bet_type = ANY(%s)
                 AND actual_value IS NOT NULL
                 AND prediction IS NOT NULL
                 AND result IS NOT NULL
               GROUP BY bet_type""",
            (list(STAT_TYPES),)
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        print(f"  [bias] update error: {e}")
        try: conn.close()
        except Exception: pass
        return

    if not rows:
        return

    ld   = load_learning_data()
    bias = ld.get("prediction_bias", {})
    changed = False
    for bet_type, mean_err, n in rows:
        if bet_type not in STAT_TYPES or n is None or n < 5 or mean_err is None:
            continue
        mean_err  = float(mean_err)
        old_bias  = bias.get(bet_type, 0.0)
        # 20% blend per cycle so it converges gradually
        new_bias  = round(old_bias * 0.80 + mean_err * 0.20, 2)
        new_bias  = max(-8.0, min(8.0, new_bias))
        if abs(new_bias - old_bias) >= 0.05:
            print(f"  [bias] {bet_type}: {old_bias:+.2f} → {new_bias:+.2f}"
                  f"  (mean err {mean_err:+.2f} over {n} picks)")
            bias[bet_type] = new_bias
            changed = True

    if changed:
        ld["prediction_bias"] = bias
        save_learning_data(ld)


def retrain_from_results():
    from sklearn.ensemble import GradientBoostingClassifier

    bets = load_bets()
    settled = [b for b in bets if b.get("result") in ("win", "loss")]

    if len(settled) < 5:
        print(f"  [learn] {len(settled)} settled bets — need 5+ to retrain")
        return None

    ld = load_learning_data()

    # ── 1. Bias correction ────────────────────────────────────────
    spread_errors, total_errors = [], []
    for b in settled:
        pred = b.get("prediction")
        line = b.get("line")
        if pred is None or line is None:
            continue
        bt = b.get("betType", "")
        if bt == "SPREAD":
            spread_errors.append(float(pred) - float(line))
        elif bt in ("TOTAL", "OVER", "UNDER"):   # handle both old and new labels
            total_errors.append(float(pred) - float(line))

    if spread_errors:
        ld["spread_errors"] = (ld.get("spread_errors", []) + spread_errors)[-100:]
        ld["spread_bias"] = round(sum(ld["spread_errors"]) / len(ld["spread_errors"]), 2)
    else:
        ld.setdefault("spread_bias", 0.0)

    if total_errors:
        ld["total_errors"] = (ld.get("total_errors", []) + total_errors)[-100:]
        ld["total_bias"] = round(sum(ld["total_errors"]) / len(ld["total_errors"]), 2)
    else:
        ld.setdefault("total_bias", 0.0)

    # ── 2. Win rate by bet type ───────────────────────────────────
    _PROP_CAT_TYPES = {"elite_prop", "individual", "prop", "neutral_prop",
                       "fade_prop", "benefactor_prop"}
    by_type = {}
    for b in settled:
        bt = b.get("betType", "OTHER")
        # Normalize legacy labels
        if bt in ("OVER", "UNDER"):
            bt = "TOTAL"
        # ELITE_PROP / INDIVIDUAL bets: extract the actual stat from pick text
        # so calibrated_confidence("points", ...) can find the real win rate.
        elif bt.lower() in _PROP_CAT_TYPES:
            _ptxt = b.get("pick", "").lower()
            for _stat in ("points", "rebounds", "assists", "threes"):
                if _stat in _ptxt:
                    bt = _stat
                    break
            # If stat can't be parsed from pick text, keep the original betType
        by_type.setdefault(bt, {"w": 0, "n": 0})
        by_type[bt]["n"] += 1
        if b.get("result") == "win":
            by_type[bt]["w"] += 1

    # ── 3. Win rate by script (new 5-bucket pace × flow × scoring labels) ──
    by_script = {}
    for b in settled:
        sc = b.get("script", "AVERAGE_PACE_NORMAL_SCORING")
        by_script.setdefault(sc, {"w": 0, "n": 0})
        by_script[sc]["n"] += 1
        if b.get("result") == "win":
            by_script[sc]["w"] += 1

    # Persist win rate per bet type — used by calibrated_confidence()
    ld["win_rate_by_type"] = {
        bt: {"win_rate": round(v["w"] / v["n"] * 100, 1), "count": v["n"]}
        for bt, v in by_type.items() if v["n"] > 0
    }

    ld["win_rate_by_script"] = {
        sc: {"win_rate": round(v["w"] / v["n"] * 100, 1), "count": v["n"]}
        for sc, v in by_script.items() if v["n"] > 0
    }

    # ── 3b. Win rate by script COMBINATION (multi-signal learning) ─
    by_combo = {}
    for b in settled:
        ck = b.get("script_combo") or b.get("script", "AVERAGE_PACE_NORMAL_SCORING")
        by_combo.setdefault(ck, {"w": 0, "n": 0})
        by_combo[ck]["n"] += 1
        if b.get("result") == "win":
            by_combo[ck]["w"] += 1

    # Store raw w/n so fits_multi_script() can compute live win rates
    ld["combo_win_rates"] = {
        ck: {"w": v["w"], "n": v["n"]}
        for ck, v in by_combo.items() if v["n"] > 0
    }

    # Log any combo with enough data
    for ck, v in sorted(by_combo.items(), key=lambda x: -x[1]["n"]):
        if v["n"] >= 5:
            wr = round(v["w"] / v["n"] * 100, 1)
            print(f"  [combo] {ck}: {wr}% ({v['w']}/{v['n']})")

    # ── 4. Win rate by tier (SAFE / BALANCED / AGGRESSIVE) ────────
    by_tier = {}
    for b in settled:
        t = b.get("tier", "BALANCED")
        by_tier.setdefault(t, {"w": 0, "n": 0})
        by_tier[t]["n"] += 1
        if b.get("result") == "win":
            by_tier[t]["w"] += 1

    ld["win_rate_by_tier"] = {
        t: {"win_rate": round(v["w"] / v["n"] * 100, 1), "count": v["n"]}
        for t, v in by_tier.items() if v["n"] > 0
    }

    # ── 4b. Win rate by pick category ──────────────────────────────
    by_category = {}
    for b in settled:
        cat = b.get("pick_category") or (
            "VIP_LOCK" if b.get("betType") == "VIP_LOCK" else
            "SGP"      if b.get("betType") == "SGP"      else
            "INDIVIDUAL"
        )
        by_category.setdefault(cat, {"w": 0, "n": 0})
        by_category[cat]["n"] += 1
        if b.get("result") == "win":
            by_category[cat]["w"] += 1

    ld["win_rate_by_category"] = {
        cat: {"win_rate": round(v["w"] / v["n"] * 100, 1), "count": v["n"]}
        for cat, v in by_category.items() if v["n"] > 0
    }

    # Log category performance
    for cat, v in sorted(by_category.items(), key=lambda x: -x[1]["n"]):
        if v["n"] >= 3:
            wr = round(v["w"] / v["n"] * 100, 1)
            print(f"  [category] {cat}: {wr}% ({v['w']}/{v['n']})")

    # ── 4c. EDGE_FADE deep learning — per role + adaptive threshold ──────────
    #
    # For every settled EDGE_FADE pick we track win rate by role
    # (fade / beneficiary / hedge) independently.  The fade edge threshold
    # self-calibrates: if fades are winning we can accept a lower edge gap;
    # if they're losing we require a larger gap to fire.
    ef_settled = [b for b in settled if (b.get("pick_category") or "") == "EDGE_FADE"]
    if ef_settled:
        by_role = {"fade": {"w": 0, "n": 0},
                   "beneficiary": {"w": 0, "n": 0},
                   "hedge": {"w": 0, "n": 0}}

        for b in ef_settled:
            desc = (b.get("pick") or "").lower()
            # infer role from note stored in desc
            role = ("fade"        if "public-fade"  in desc else
                    "beneficiary" if "benefit"       in desc else
                    "hedge"       if "hedge"         in desc else
                    "beneficiary")
            by_role[role]["n"] += 1
            if b.get("result") == "win":
                by_role[role]["w"] += 1

        ld["edge_fade_role_win_rates"] = {
            role: {"win_rate": round(v["w"] / v["n"] * 100, 1), "count": v["n"]}
            for role, v in by_role.items() if v["n"] > 0
        }

        for role, v in by_role.items():
            if v["n"] >= 3:
                wr = round(v["w"] / v["n"] * 100, 1)
                print(f"  [edge_fade] {role}: {wr}% ({v['w']}/{v['n']})")

        # Adaptive fade edge threshold — uses fade-specific win rate
        MIN_CAL_EF = 5
        fade_v = by_role["fade"]
        if fade_v["n"] >= MIN_CAL_EF:
            fade_wr = fade_v["w"] / fade_v["n"]
            current = ld.get("edge_fade_threshold", 2.0)
            if fade_wr < 0.45:
                # Fades underperforming → require bigger edge gap
                new_thr = round(min(current + 0.3, 6.0), 1)
            elif fade_wr > 0.60:
                # Fades winning → relax threshold to capture more games
                new_thr = round(max(current - 0.2, 0.5), 1)
            else:
                new_thr = current
            if new_thr != current:
                print(f"  [learn] edge_fade_threshold {current} → {new_thr} "
                      f"(fade win rate {round(fade_wr*100,1)}%)")
            ld["edge_fade_threshold"] = new_thr

        # Adaptive beneficiary confidence boost — if beneficiaries overperform
        bene_v = by_role["beneficiary"]
        if bene_v["n"] >= MIN_CAL_EF:
            bene_wr = bene_v["w"] / bene_v["n"]
            current_boost = ld.get("edge_fade_bene_boost", 0.0)
            if bene_wr > 0.65:
                new_boost = round(min(current_boost + 2.0, 10.0), 1)
            elif bene_wr < 0.50:
                new_boost = round(max(current_boost - 2.0, -5.0), 1)
            else:
                new_boost = current_boost
            if new_boost != current_boost:
                print(f"  [learn] edge_fade_bene_boost {current_boost} → {new_boost} "
                      f"(beneficiary win rate {round(bene_wr*100,1)}%)")
            ld["edge_fade_bene_boost"] = new_boost

    # ── 5. Strengths & weaknesses summary ─────────────────────────
    strengths, weaknesses = [], []
    for bt, counts in {**by_type, **by_script}.items():
        if counts["n"] < 5:
            continue
        wr = counts["w"] / counts["n"]
        if wr >= 0.60:
            strengths.append(f"{bt} ({round(wr*100,1)}%)")
        elif wr <= 0.44:
            weaknesses.append(f"{bt} ({round(wr*100,1)}%)")

    ld["strengths"]  = strengths
    ld["weaknesses"] = weaknesses

    # ── 6. Adaptive edge thresholds ──────────────────────────────
    for bt, counts in by_type.items():
        if counts["n"] >= 10:
            wr = counts["w"] / counts["n"]
            if bt == "SPREAD":
                if wr < 0.45:
                    ld["spread_edge_threshold"] = round(
                        min(ld.get("spread_edge_threshold", SPREAD_EDGE_THRESHOLD) + 0.5, 8.0), 1)
                elif wr > 0.60:
                    ld["spread_edge_threshold"] = round(
                        max(ld.get("spread_edge_threshold", SPREAD_EDGE_THRESHOLD) - 0.5, 2.0), 1)
            elif bt == "TOTAL":
                if wr < 0.45:
                    ld["total_edge_threshold"] = round(
                        min(ld.get("total_edge_threshold", TOTALS_EDGE_THRESHOLD) + 0.5, 10.0), 1)
                elif wr > 0.60:
                    ld["total_edge_threshold"] = round(
                        max(ld.get("total_edge_threshold", TOTALS_EDGE_THRESHOLD) - 0.5, 3.0), 1)
            elif bt.lower() in {"points", "rebounds", "assists", "threes"}:
                key = f"prop_{bt.lower()}_edge"
                current = ld.get(key, 3.0)
                if wr < 0.45:
                    ld[key] = round(min(current + 0.5, 7.0), 1)
                elif wr > 0.60:
                    ld[key] = round(max(current - 0.5, 1.5), 1)
                print(f"  [learn] {bt} prop edge → {ld[key]} (win rate {round(wr*100,1)}%)")

    # ── 7. Auto-calibrate script thresholds from real win/loss data ──────────
    #
    # For each settled pick that has game_total / game_spread context, we collect
    # the actual Vegas line values where each script's picks WON, then set the
    # threshold at the value that historically performs best.
    #   HALFCOURT wins: UNDERs hit → raise halfcourt_total_max to capture more grind games
    #   TRANSITION_HEAVY wins: OVERs hit → lower transition_total_min to capture more fast games
    #   BLOWOUT wins: covers  → lower blowout_spread_min (tighter favourites still cover)
    #   TIGHT_GAME wins: covers → raise tight_spread_max (spreads up to X are still "tight")
    #   UPSET   wins: covers  → adjust upset range from the distribution of hitting spreads
    #
    MIN_CAL = 8   # minimum samples before we move a threshold
    thr = ld.get("script_thresholds") or dict(_SCRIPT_THRESHOLD_DEFAULTS)
    for k, v in _SCRIPT_THRESHOLD_DEFAULTS.items():
        thr.setdefault(k, v)

    halfcourt_win_totals, transition_win_totals = [], []
    slow_paced_win_totals, uptempo_win_totals   = [], []
    blowout_win_spreads, tight_win_spreads      = [], []
    upset_win_spreads                           = []

    # Old label → new pace bucket for reading legacy DB records
    _PACE_REMAP = {"GRIND": "HALFCOURT", "HIGH": "TRANSITION_HEAVY", "MID": "AVERAGE_PACE", "NORMAL": "AVERAGE_PACE"}
    _FLOW_REMAP = {"CLOSE": "TIGHT_GAME", "MODERATE": "COMPETITIVE"}

    for b in settled:
        raw_sc = b.get("script", "AVERAGE_PACE_NORMAL_SCORING")
        sc     = _PACE_REMAP.get(raw_sc.upper(), raw_sc) if "_" not in raw_sc else raw_sc
        result = b.get("result")
        gt     = b.get("game_total")
        gs     = b.get("game_spread")

        if result != "win":
            continue
        if gt is not None:
            pace_part = sc.split("_")[0] if "_" in sc else sc
            if pace_part == "HALFCOURT":
                halfcourt_win_totals.append(float(gt))
            elif sc.startswith("SLOW_PACED"):
                slow_paced_win_totals.append(float(gt))
            elif pace_part == "UPTEMPO":
                uptempo_win_totals.append(float(gt))
            elif pace_part in ("TRANSITION", "TRANSITION_HEAVY"):
                transition_win_totals.append(float(gt))
        if gs is not None:
            gs_abs = abs(float(gs))
            flow_part = sc.split("_")[0] if "_" in sc else sc
            if flow_part == "BLOWOUT":
                blowout_win_spreads.append(gs_abs)
            elif flow_part in ("TIGHT", "TIGHT_GAME"):
                tight_win_spreads.append(gs_abs)
            elif sc == "UPSET":
                upset_win_spreads.append(gs_abs)

    def _pct(data, p):
        """Return p-th percentile (0-100) of a sorted list."""
        if not data:
            return None
        s = sorted(data)
        idx = max(0, min(len(s) - 1, int(len(s) * p / 100)))
        return s[idx]

    # ── Sanity bounds — only reject physically impossible NBA values ────────────
    # The bot is free to set any value within these; no artificial constraints.
    # NBA game totals: 190–270 pts  |  NBA spreads: 1–30 pts
    _T_MIN, _T_MAX = 190.0, 270.0   # total sanity
    _S_MIN, _S_MAX = 1.0,  30.0     # spread sanity

    def _sanity_t(v): return round(max(_T_MIN, min(_T_MAX, v)), 1)
    def _sanity_s(v): return round(max(_S_MIN, min(_S_MAX, v)), 1)

    # HALFCOURT: 75th-pct of winning totals — UNDERs that actually hit
    if len(halfcourt_win_totals) >= MIN_CAL:
        new_val = _sanity_t(_pct(halfcourt_win_totals, 75))
        if new_val != thr["halfcourt_total_max"]:
            print(f"  [learn] halfcourt_total_max  {thr['halfcourt_total_max']} → {new_val}  ({len(halfcourt_win_totals)} samples)")
            thr["halfcourt_total_max"] = new_val

    # TRANSITION_HEAVY: 25th-pct of winning totals — OVERs hit even at the lower end
    if len(transition_win_totals) >= MIN_CAL:
        new_val = _sanity_t(_pct(transition_win_totals, 25))
        if new_val != thr["transition_total_min"]:
            print(f"  [learn] transition_total_min {thr['transition_total_min']} → {new_val}  ({len(transition_win_totals)} samples)")
            thr["transition_total_min"] = new_val

    # BLOWOUT: 25th-pct of winning spreads — tighter favourites that still covered
    if len(blowout_win_spreads) >= MIN_CAL:
        new_val = _sanity_s(_pct(blowout_win_spreads, 25))
        if new_val != thr["blowout_spread_min"]:
            print(f"  [learn] blowout_spread_min  {thr['blowout_spread_min']} → {new_val}  ({len(blowout_win_spreads)} samples)")
            thr["blowout_spread_min"] = new_val

    # TIGHT_GAME: 75th-pct of winning spreads — widest spread still producing tight-game hits
    if len(tight_win_spreads) >= MIN_CAL:
        new_val = _sanity_s(_pct(tight_win_spreads, 75))
        if new_val != thr["tight_spread_max"]:
            print(f"  [learn] tight_spread_max    {thr['tight_spread_max']} → {new_val}  ({len(tight_win_spreads)} samples)")
            thr["tight_spread_max"] = new_val

    # UPSET: 10th–90th pct of winning upset spreads — the real upset range from data
    if len(upset_win_spreads) >= MIN_CAL:
        new_min = _sanity_s(_pct(upset_win_spreads, 10))
        new_max = _sanity_s(_pct(upset_win_spreads, 90))
        if new_min != thr["upset_spread_min"] or new_max != thr["upset_spread_max"]:
            print(f"  [learn] upset_spread range {thr['upset_spread_min']}–{thr['upset_spread_max']} → {new_min}–{new_max}  ({len(upset_win_spreads)} samples)")
            thr["upset_spread_min"] = new_min
            thr["upset_spread_max"] = new_max

    # SLOW_PACED: 75th-pct of winning totals — upper ceiling of sluggish pace that still hits
    if len(slow_paced_win_totals) >= MIN_CAL:
        new_val = _sanity_t(_pct(slow_paced_win_totals, 75))
        cur     = thr.get("slow_paced_total_max", _SCRIPT_THRESHOLD_DEFAULTS["slow_paced_total_max"])
        if new_val != cur:
            print(f"  [learn] slow_paced_total_max {cur} → {new_val}  ({len(slow_paced_win_totals)} samples)")
            thr["slow_paced_total_max"] = new_val

    # UPTEMPO: 25th-pct of winning totals — lower floor of fast games that still hit
    if len(uptempo_win_totals) >= MIN_CAL:
        new_val = _sanity_t(_pct(uptempo_win_totals, 25))
        cur     = thr.get("uptempo_total_min", _SCRIPT_THRESHOLD_DEFAULTS["uptempo_total_min"])
        if new_val != cur:
            print(f"  [learn] uptempo_total_min    {cur} → {new_val}  ({len(uptempo_win_totals)} samples)")
            thr["uptempo_total_min"] = new_val

    # USAGE GATE — learned from settled prop pick win rates by possession usage
    _PROP_BET_TYPES = {"points", "rebounds", "assists", "threes",
                       "elite_prop", "individual", "prop", "neutral_prop",
                       "fade_prop", "benefactor_prop"}
    prop_picks_with_usage = [
        b for b in settled
        if b.get("betType", "").lower() in _PROP_BET_TYPES
        and b.get("player_avg_usage") is not None
    ]
    if len(prop_picks_with_usage) >= MIN_CAL:
        best_usg_gate = thr["prop_usage_gate"]
        best_usg_wr   = -1.0
        # Test every gate from 5–22 possessions per game
        for candidate in [x * 0.5 for x in range(10, 45)]:   # 5.0, 5.5 … 22.0
            above = [b for b in prop_picks_with_usage if b["player_avg_usage"] >= candidate]
            if len(above) < MIN_CAL:
                continue
            wr = sum(1 for b in above if b.get("result") == "win") / len(above)
            if wr > best_usg_wr:
                best_usg_wr   = wr
                best_usg_gate = candidate
        # Sanity: usage gate must stay between 5–22 possessions per game
        best_usg_gate = max(5.0, min(22.0, best_usg_gate))
        if best_usg_gate != thr["prop_usage_gate"]:
            print(f"  [learn] prop_usage_gate  {thr['prop_usage_gate']} → {best_usg_gate}"
                  f"  (best win-rate {best_usg_wr:.1%} across {len(prop_picks_with_usage)} props)")
            thr["prop_usage_gate"] = best_usg_gate

    # MINUTES GATE + STARTER THRESHOLD — learned from settled prop pick win rates
    # Collect all settled prop picks that have avg_mins recorded
    prop_picks_with_mins = [
        b for b in settled
        if b.get("betType", "").lower() in _PROP_BET_TYPES
        and b.get("player_avg_mins") is not None
    ]
    if len(prop_picks_with_mins) >= MIN_CAL:
        # Test every possible gate from 20–38 in 1-min steps
        # Choose the gate where picks ABOVE it have the best win rate
        best_gate    = thr["prop_minutes_gate"]
        best_wr      = -1.0
        for candidate in range(20, 39):
            above = [b for b in prop_picks_with_mins if b["player_avg_mins"] >= candidate]
            if len(above) < MIN_CAL:
                continue
            wr = sum(1 for b in above if b.get("result") == "win") / len(above)
            if wr > best_wr:
                best_wr   = wr
                best_gate = candidate
        # Sanity: gate must stay between 20–40 minutes (physical NBA range)
        best_gate = max(20, min(40, best_gate))
        if best_gate != thr["prop_minutes_gate"]:
            print(f"  [learn] prop_minutes_gate {thr['prop_minutes_gate']} → {best_gate}"
                  f"  (best win-rate {best_wr:.1%} across {len(prop_picks_with_mins)} props)")
            thr["prop_minutes_gate"] = best_gate

        # STARTER THRESHOLD — find the minutes value that best separates
        # higher win-rate picks (treat as starters) from lower ones (bench)
        best_starter = thr["prop_starter_mins"]
        best_gap     = -999.0
        for candidate in range(20, 39):
            starters = [b for b in prop_picks_with_mins if b["player_avg_mins"] >= candidate]
            bench    = [b for b in prop_picks_with_mins if b["player_avg_mins"] <  candidate]
            if len(starters) < MIN_CAL or len(bench) < MIN_CAL:
                continue
            starter_wr = sum(1 for b in starters if b.get("result") == "win") / len(starters)
            bench_wr   = sum(1 for b in bench    if b.get("result") == "win") / len(bench)
            gap = starter_wr - bench_wr     # widest gap → clearest starter/bench split
            if gap > best_gap:
                best_gap     = gap
                best_starter = candidate
        best_starter = max(20, min(40, best_starter))
        if best_starter != thr["prop_starter_mins"]:
            print(f"  [learn] prop_starter_mins {thr['prop_starter_mins']} → {best_starter}"
                  f"  (starter/bench wr gap {best_gap:+.1%})")
            thr["prop_starter_mins"] = best_starter

    ld["script_thresholds"] = thr

    # ── 7b. 7-Dimension Intelligence Scoring ─────────────────────
    #
    # Scores every new signal dimension added by the Edge-Fade 7 engine:
    # 1. Role accuracy (go_to_scorer / floor_general / glass_cleaner / rim_anchor / spot_up_shooter / combo_creator / sixth_man / utility_player)
    # 2. Fade accuracy (is_fade picks win rate)
    # 3. Benefactor accuracy (is_benefactor picks win rate)
    # 4. EV calibration — does positive EV actually predict wins?
    # 5. Juice tier accuracy (GREEN / YELLOW / RED)
    # 6. Slip grade accuracy (A / B / C)
    # 7. Full 4D script label win rates (HIGH_CLOSE, GRIND_BLOWOUT …)
    _conn7 = _db_conn()
    if _conn7:
        try:
            _cur7 = _conn7.cursor()

            # Dim 1 — Role accuracy
            _cur7.execute("""
                SELECT role, COUNT(*), SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE role IS NOT NULL AND result IN ('win','loss')
                GROUP BY role
            """)
            _role_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_role", {})
            for _rl, _n, _w in _role_rows:
                if _n and int(_n) >= 3:
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["win_rate_by_role"][_rl] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:role]  {_rl}: {_wr}%  ({_w}/{_n})")

            # Dim 2 — Fade accuracy
            _cur7.execute("""
                SELECT is_fade, COUNT(*), SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE is_fade IS NOT NULL AND result IN ('win','loss')
                GROUP BY is_fade
            """)
            _fade_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_fade", {})
            for _isf, _n, _w in _fade_rows:
                if _n and int(_n) >= 3:
                    _k  = "fade" if _isf else "non_fade"
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["win_rate_by_fade"][_k] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:fade]  {_k}: {_wr}%  ({_w}/{_n})")

            # Dim 3 — Benefactor accuracy
            _cur7.execute("""
                SELECT is_benefactor, COUNT(*), SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE is_benefactor IS NOT NULL AND result IN ('win','loss')
                GROUP BY is_benefactor
            """)
            _bene_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_benefactor", {})
            for _isb, _n, _w in _bene_rows:
                if _n and int(_n) >= 3:
                    _k  = "benefactor" if _isb else "non_benefactor"
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["win_rate_by_benefactor"][_k] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:bene]  {_k}: {_wr}%  ({_w}/{_n})")

            # Dim 4 — EV calibration + adaptive minimum EV threshold
            _cur7.execute("""
                SELECT
                    CASE WHEN ev > 0 THEN 'positive'
                         WHEN ev < 0 THEN 'negative'
                         ELSE 'neutral' END AS ev_tier,
                    COUNT(*),
                    SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE ev IS NOT NULL AND result IN ('win','loss')
                GROUP BY 1
            """)
            _ev_rows = _cur7.fetchall() or []
            ld.setdefault("ev_calibration", {})
            for _evt, _n, _w in _ev_rows:
                if _n and int(_n) >= 5:
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["ev_calibration"][_evt] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:ev]    {_evt}: {_wr}%  ({_w}/{_n})")
            # Adaptive: if positive-EV picks consistently outperform, raise the bar
            _pos_cal = ld["ev_calibration"].get("positive", {})
            _neg_cal = ld["ev_calibration"].get("negative", {})
            if _pos_cal.get("count", 0) >= 5 and _neg_cal.get("count", 0) >= 5:
                _pos_wr = _pos_cal["win_rate"]
                _neg_wr = _neg_cal["win_rate"]
                _cur_min_ev = ld.get("min_ev_threshold", 0.0)
                if _pos_wr > _neg_wr + 5:
                    _new_min_ev = round(min(_cur_min_ev + 0.005, 0.05), 3)
                elif _pos_wr < _neg_wr - 5:
                    _new_min_ev = round(max(_cur_min_ev - 0.005, -0.02), 3)
                else:
                    _new_min_ev = _cur_min_ev
                if _new_min_ev != _cur_min_ev:
                    print(f"  [learn:ev]    min_ev_threshold {_cur_min_ev} → {_new_min_ev}")
                ld["min_ev_threshold"] = _new_min_ev

            # Dim 5 — Juice tier accuracy
            _cur7.execute("""
                SELECT
                    CASE WHEN odds >= -150 THEN 'GREEN'
                         WHEN odds >= -200 THEN 'YELLOW'
                         ELSE 'RED' END AS juice_tier,
                    COUNT(*),
                    SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE odds IS NOT NULL AND odds < 0 AND result IN ('win','loss')
                GROUP BY 1
            """)
            _juice_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_juice", {})
            for _jt, _n, _w in _juice_rows:
                if _n and int(_n) >= 5:
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["win_rate_by_juice"][_jt] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:juice] {_jt}: {_wr}%  ({_w}/{_n})")

            # Dim 6 — Slip grade accuracy (A / B / C)
            _cur7.execute("""
                SELECT slip_grade, COUNT(*), SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE slip_grade IS NOT NULL AND result IN ('win','loss')
                GROUP BY slip_grade
            """)
            _grade_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_grade", {})
            for _gr, _n, _w in _grade_rows:
                if _n and int(_n) >= 3:
                    _wr = round(float(_w or 0) / int(_n) * 100, 1)
                    ld["win_rate_by_grade"][_gr] = {"win_rate": _wr, "count": int(_n)}
                    print(f"  [learn:grade] {_gr}: {_wr}%  ({_w}/{_n})")

            # Dim 7 — Full 4D script label win rates (e.g. HIGH_CLOSE, GRIND_BLOWOUT)
            _cur7.execute("""
                SELECT script, COUNT(*), SUM(CASE WHEN result='win' THEN 1 ELSE 0 END)
                FROM bets
                WHERE script IS NOT NULL AND result IN ('win','loss')
                  AND script LIKE '%\_%' ESCAPE '\'
                GROUP BY script
                HAVING COUNT(*) >= 5
                ORDER BY COUNT(*) DESC
            """)
            _s4d_rows = _cur7.fetchall() or []
            ld.setdefault("win_rate_by_4d_script", {})
            for _sc4, _n, _w in _s4d_rows:
                _wr = round(float(_w or 0) / int(_n) * 100, 1)
                ld["win_rate_by_4d_script"][_sc4] = {"win_rate": _wr, "count": int(_n)}
                print(f"  [learn:4d]    {_sc4}: {_wr}%  ({_w}/{_n})")

            _cur7.close(); _conn7.close()
        except Exception as _e7:
            print(f"  [learn:7d] scoring error: {_e7}")
            try: _conn7.close()
            except Exception: pass

    # ── 8. Retrain sklearn player model ──────────────────────────
    X, y = [], []
    for b in settled:
        edge = b.get("edge") or 0
        prob = b.get("prob") or 0.5
        pred = b.get("prediction") or 0
        line = b.get("line") or 0
        X.append([float(edge), float(prob), float(pred), float(line)])
        y.append(1 if b.get("result") == "win" else 0)

    model_retrained = False
    if len(X) >= 10 and sum(y) > 0 and sum(y) < len(y):
        try:
            clf = GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=42)
            clf.fit(X, y)
            import pickle as _pk, base64 as _b64
            model_bytes = _pk.dumps(clf)
            # Save to file (local cache)
            with open(_model_path, "wb") as _f:
                _f.write(model_bytes)
            # Save to DB as base64 so it survives Railway redeploys
            ld["model_b64"] = _b64.b64encode(model_bytes).decode("utf-8")
            ld["version"] = ld.get("version", 0) + 1
            model_retrained = True
            print(f"  [learn] Model retrained — v{ld['version']} ({len(X)} samples) — saved to DB")
        except Exception as me:
            print(f"  [learn] Model retrain error: {me}")

    # ── 9. Prediction accuracy — learn bias correction per stat type ──────────
    #
    # For every settled prop pick where we stored both the prediction and the
    # real stat (actual_value), compute:
    #   error = prediction - actual
    # A positive mean error means we over-predicted → subtract from future picks.
    # A negative mean error means we under-predicted → add to future picks.
    # We apply an exponential-weighted update so recent errors matter more.
    #
    STAT_TYPES = ("points", "rebounds", "assists", "threes")
    bias = ld.get("prediction_bias", {})
    for stat in STAT_TYPES:
        bias.setdefault(stat, 0.0)

    # Pull from DB: all picks with actual_value recorded
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT bet_type, AVG(prediction_error), COUNT(*), STDDEV(prediction_error)
                   FROM bets
                   WHERE bet_type = ANY(%s)
                     AND actual_value IS NOT NULL
                     AND prediction IS NOT NULL
                     AND result IS NOT NULL
                   GROUP BY bet_type""",
                (list(STAT_TYPES),)
            )
            rows = cur.fetchall()
            cur.close(); conn.close()
            for bet_type, mean_err, n, stddev in rows:
                if bet_type not in STAT_TYPES or n is None or n < 5:
                    continue
                mean_err = float(mean_err)
                # Blend: 30% new evidence, 70% existing correction (stable learning)
                old_bias = bias.get(bet_type, 0.0)
                new_bias = round(old_bias * 0.70 + mean_err * 0.30, 2)
                # Sanity: never correct more than ±8 points
                new_bias = max(-8.0, min(8.0, new_bias))
                if abs(new_bias - old_bias) >= 0.05:
                    print(f"  [learn] {bet_type} bias: {old_bias:+.2f} → {new_bias:+.2f}"
                          f"  (mean error {mean_err:+.2f} over {n} picks, σ={stddev:.2f})")
                bias[bet_type] = new_bias
        except Exception as e:
            print(f"  [learn] bias update error: {e}")

    ld["prediction_bias"] = bias

    # ── 5. Per-player prop learning ───────────────────────────────
    # Track each player's hit rate per prop type so confidence can
    # be adjusted up/down in future picks based on real results.
    PROP_BET_TYPES = {"points", "rebounds", "assists", "threes",
                      "pts", "reb", "ast", "fg3", "fg3m"}
    prop_settled = [
        b for b in settled
        if b.get("betType", "").lower() in PROP_BET_TYPES
        and b.get("player")
    ]

    player_history = ld.get("player_prop_history", {})
    for b in prop_settled:
        pkey = b["player"].lower().strip()
        bt   = b.get("betType", "pts").lower()
        # Normalise bet type label
        bt = "pts" if bt == "points" else bt
        bt = "reb" if bt == "rebounds" else bt
        bt = "ast" if bt == "assists" else bt
        bt = "fg3" if bt in ("threes", "fg3m") else bt

        if pkey not in player_history:
            player_history[pkey] = {}
        if bt not in player_history[pkey]:
            player_history[pkey][bt] = {"w": 0, "n": 0}

        player_history[pkey][bt]["n"] += 1
        if b.get("result") == "win":
            player_history[pkey][bt]["w"] += 1

        # Keep overall across all prop types too
        if "overall" not in player_history[pkey]:
            player_history[pkey]["overall"] = {"w": 0, "n": 0}
        player_history[pkey]["overall"]["n"] += 1
        if b.get("result") == "win":
            player_history[pkey]["overall"]["w"] += 1

    ld["player_prop_history"] = player_history

    # Log top 5 best and worst tracked players
    _ranked = []
    for pkey, stats_dict in player_history.items():
        ov = stats_dict.get("overall", {})
        n  = ov.get("n", 0)
        if n >= 5:
            _ranked.append((pkey, round(ov["w"] / n * 100, 1), n))
    _ranked.sort(key=lambda x: -x[1])
    for name, wr, cnt in _ranked[:3]:
        print(f"  [player learn] {name}: {wr}% ({cnt} picks) ✅")
    for name, wr, cnt in _ranked[-3:]:
        if wr < 45:
            print(f"  [player learn] {name}: {wr}% ({cnt} picks) ⚠️")

    # ── 5b. Summary stats ────────────────────────────────────────
    wins = sum(1 for b in settled if b.get("result") == "win")
    ld["win_rate"] = round(wins / len(settled) * 100, 1) if settled else ld.get("win_rate", 0.0)
    ld["total_settled"] = len(settled)
    save_learning_data(ld)

    # Only broadcast model update once per day max
    today_str = datetime.now().strftime("%Y-%m-%d")
    last_model_msg = ld.get("_last_model_msg_date", "")
    if model_retrained and last_model_msg != today_str:
        # Build win-rate breakdown by bet type
        type_lines = []
        for bt, counts in sorted(by_type.items()):
            if counts["n"] >= 3:
                wr_pct = round(counts["w"] / counts["n"] * 100, 1)
                type_lines.append(f"  {bt}: {wr_pct}% ({counts['w']}/{counts['n']})")

        # Build script breakdown — all 6 scripts
        script_lines = []
        for sc in ["INJURY", "HALFCOURT", "SLOW_PACED", "AVERAGE_PACE", "UPTEMPO", "TRANSITION_HEAVY",
                   "BLOWOUT", "DOUBLE_DIGIT_LEAD", "COMFORTABLE_LEAD", "COMPETITIVE", "TIGHT_GAME",
                   "SHOOTOUT", "HIGH_SCORING", "NORMAL_SCORING", "DEFENSIVE_BATTLE", "UPSET"]:
            v = by_script.get(sc, {})
            if v.get("n", 0) >= 3:
                wr_pct = round(v["w"] / v["n"] * 100, 1)
                script_lines.append(f"  {sc}: {wr_pct}% ({v['w']}/{v['n']})")

        # Build calibrated threshold lines
        thr_now = ld.get("script_thresholds", _SCRIPT_THRESHOLD_DEFAULTS)
        thr_lines = [
            f"  HALFCOURT total < {thr_now.get('halfcourt_total_max', 208)}",
            f"  TRANSITION total > {thr_now.get('transition_total_min', 232)}",
            f"  BLOWOUT spread ≥ {thr_now.get('blowout_spread_min', 12)}",
            f"  TIGHT   spread ≤ {thr_now.get('tight_spread_max', 3.5)}",
            f"  UPSET   spread {thr_now.get('upset_spread_min', 6)}–{thr_now.get('upset_spread_max', 14)}",
        ]

        # Strengths / weaknesses labels
        str_txt = ", ".join(ld.get("strengths", [])) or "Still building data"
        wk_txt  = ", ".join(ld.get("weaknesses", [])) or "None yet"

        summary = (
            f"🧠 *MODEL UPDATED — v{ld.get('version', 0)}*\n\n"
            f"📊 Overall Win Rate: *{ld['win_rate']}%* ({wins}/{len(settled)} bets)\n\n"
            f"*By Bet Type:*\n" + "\n".join(type_lines or ["  Not enough data yet"]) + "\n\n"
            + (f"*By Game Script:*\n" + "\n".join(script_lines) + "\n\n" if script_lines else "")
            + f"💪 *Strengths:* {str_txt}\n"
            f"⚠️ *Weaknesses:* {wk_txt}\n\n"
            f"📉 Spread bias: {ld['spread_bias']:+.1f} pts  |  🎯 Total bias: {ld['total_bias']:+.1f} pts\n\n"
            f"*📐 Self-Calibrated Script Triggers:*\n"
            + "\n".join(thr_lines) + "\n"
            f"_All triggers learned from {ld['total_settled']} graded picks. Adjusts daily._"
        )
        send(summary, VIP_CHANNEL)
        ld["_last_model_msg_date"] = today_str
        save_learning_data(ld)
    print(
        f"  [learn] Win rate: {ld['win_rate']}% | "
        f"Strengths: {ld.get('strengths')} | Weaknesses: {ld.get('weaknesses')} | "
        f"Spread bias: {ld['spread_bias']} | Total bias: {ld['total_bias']}"
    )
    return ld


def get_player_confidence_adjustment(player_name, prop_type=None):
    """
    Reads the player's historical prop win rate from learning_data and
    returns a confidence adjustment (positive = boost, negative = penalty).

    Requires at least 5 settled picks before applying any adjustment.
    Scale: ±2 per 5% deviation from 50% baseline, capped at ±10.
    """
    try:
        ld      = load_learning_data()
        history = ld.get("player_prop_history", {})
        pkey    = player_name.lower().strip()
        pdata   = history.get(pkey, {})
        if not pdata:
            return 0.0

        # Use prop-type specific data if available, else overall
        bt = None
        if prop_type:
            bt = prop_type.lower()
            bt = "pts" if bt in ("points", "player_points") else bt
            bt = "reb" if bt in ("rebounds", "player_rebounds", "total_rebounds") else bt
            bt = "ast" if bt in ("assists", "player_assists") else bt
            bt = "fg3" if bt in ("threes", "fg3m", "fg3_made") else bt

        record = pdata.get(bt) if bt and bt in pdata else pdata.get("overall", {})
        n = record.get("n", 0)
        w = record.get("w", 0)
        if n < 5:
            return 0.0  # not enough data yet

        hit_rate  = w / n
        deviation = hit_rate - 0.50          # how far above/below 50%
        adj       = round(deviation * 40, 1) # ±10 at 75%/25% extremes
        adj       = max(-10.0, min(10.0, adj))
        return adj
    except Exception:
        return 0.0


# ==========================
# 🚀 MAIN RUN
# ==========================
def is_elite_pick(edge, confidence, prop_type=None):
    ld = load_learning_data()
    if prop_type:
        threshold = ld.get(f"prop_{prop_type.lower()}_edge", 3.0)
    else:
        threshold = 3.0
    return abs(edge) > threshold and confidence >= 65


def calculate_confidence(edge, variance, history=None, line=None, direction="OVER"):
    """
    Real confidence using normal distribution probability.

    If we have the player's actual game log (history) and the betting line,
    we compute P(stat exceeds line) directly from their real mean and std.
    Falls back to edge/variance heuristic when history is unavailable.
    """
    import math as _math

    # ── Real probability path ─────────────────────────────────────────
    if history and line is not None and len(history) >= 5:
        mean = sum(history) / len(history)
        variance_real = sum((x - mean) ** 2 for x in history) / len(history)
        std = _math.sqrt(variance_real) if variance_real > 0 else 1.0

        # Z-score of the line relative to player's distribution
        z = (line - mean) / std

        # Normal CDF approximation (Abramowitz & Stegun)
        def _norm_cdf(z_val):
            t = 1.0 / (1.0 + 0.2316419 * abs(z_val))
            d = 0.3989422820 * _math.exp(-0.5 * z_val * z_val)
            p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 +
                t * (-1.8212560 + t * 1.3302744))))
            return 1.0 - p if z_val >= 0 else p

        # For OVER bet: probability player exceeds line = 1 - CDF(line)
        # For UNDER bet: probability player stays under line = CDF(line)
        raw_prob = (1.0 - _norm_cdf(z)) if direction == "OVER" else _norm_cdf(z)

        # Apply sample-size penalty — less certain with fewer games
        n = len(history)
        sample_weight = min(n / 20.0, 1.0)   # full weight at 20+ games
        regressed = 0.5 * (1.0 - sample_weight) + raw_prob * sample_weight

        return round(max(45.0, min(regressed * 100, 95.0)), 1)

    # ── Fallback: edge + variance heuristic ──────────────────────────
    abs_edge = abs(edge)
    if abs_edge > 7:
        base = 72
    elif abs_edge > 5:
        base = 65
    elif abs_edge > 3:
        base = 58
    elif abs_edge > 1.5:
        base = 52
    else:
        base = 47

    # Tighter variance = more reliable player = higher confidence
    var_bonus = max(0, round(10 - variance * 1.5, 1))
    score = min(base + var_bonus, 85)
    return round(max(45.0, float(score)), 1)


def calibrated_confidence(bet_type: str, base_conf: float,
                          pick_category: str = None, role: str = None) -> float:
    """
    Blend the model's raw confidence with real historical win rates.

    - bet_type  : "TOTAL", "SPREAD", "points", "rebounds" etc.
    - pick_category : optional "EDGE_FADE", "VIP_LOCK", "SGP" etc. — layers
                      category-level real-world performance on top of bet_type blend
    - role      : for EDGE_FADE — "fade" / "beneficiary" / "hedge" — applies
                  per-role win rate and learned beneficiary boost

    Once we have ≥10 graded results for a type the blended value is
    50% model + 50% real-world.  Below that threshold we weight toward the
    model and cap conservatively.
    """
    ld = load_learning_data()
    norm = "TOTAL" if bet_type in ("OVER", "UNDER") else bet_type

    # ── Bet-type blend ────────────────────────────────────────────────
    # Requires 25+ graded results before historical win rate influences confidence.
    # Weight starts at 0.20 (20% historical) and grows to max 0.40 — not 0.50.
    # The old formula (0.5 weight at 10 samples) was dragging a 72% model pick
    # down to 56% when win rate was 40%, silently blocking all individual picks.
    stats = ld.get("win_rate_by_type", {}).get(norm)
    if stats and stats.get("count", 0) >= 25:
        real_wr      = float(stats["win_rate"])
        count        = stats["count"]
        real_weight  = min(0.20 + (count - 25) * 0.004, 0.40)
        blended      = (1.0 - real_weight) * float(base_conf) + real_weight * real_wr
        # Safety floor: never let calibration drag more than 12 pts below model
        blended      = max(blended, float(base_conf) - 12.0)
    else:
        blended = min(float(base_conf), 82.0)

    # ── Category-level overlay (EDGE_FADE, VIP_LOCK, SGP …) ──────────
    if pick_category:
        cat_stats = ld.get("win_rate_by_category", {}).get(pick_category)
        if cat_stats and cat_stats.get("count", 0) >= 10:
            cat_wr     = float(cat_stats["win_rate"])
            cat_count  = cat_stats["count"]
            # Category weight: 0.2 → 0.35 as sample grows
            cat_weight = min(0.20 + (cat_count - 10) * 0.003, 0.35)
            blended    = (1.0 - cat_weight) * blended + cat_weight * cat_wr

    # ── EDGE_FADE per-role refinement ─────────────────────────────────
    if pick_category == "EDGE_FADE" and role:
        role_rates = ld.get("edge_fade_role_win_rates", {})
        role_data  = role_rates.get(role)
        if role_data and role_data.get("count", 0) >= 5:
            role_wr     = float(role_data["win_rate"])
            role_weight = min(0.15 + (role_data["count"] - 5) * 0.002, 0.25)
            blended     = (1.0 - role_weight) * blended + role_weight * role_wr

        # Apply beneficiary production boost if learned
        if role == "beneficiary":
            boost = float(ld.get("edge_fade_bene_boost", 0.0))
            blended = blended + boost

    return round(max(45.0, min(blended, 95.0)), 1)


def get_live_injuries():
    url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/injuries"
    res = requests.get(url).json()

    injuries = {}

    for team in res.get("teams", []):
        for athlete in team.get("athletes", []):
            name = athlete["fullName"]
            status = athlete["status"]

            injuries[name] = status != "ACTIVE"

    return injuries


def adjust_usage_dynamic(player, base_usage, injuries, player_team=None):
    """
    Boost a player's usage only when a teammate is out — not for every
    injury across the whole league.

    injuries format (from get_espn_injuries):
      {player_name_lower: {"status": str, "team": str, "comment": str}}

    player_team: the player's team name (from get_player_stats → stats["team"])
    """
    if not injuries:
        return base_usage

    boost = base_usage
    teammate_boosts = 0

    for inj_name, inj_info in injuries.items():
        # Skip the player themselves
        if inj_name == player.lower():
            continue

        # Only count injuries on the same team when we have team info
        if player_team:
            inj_team = (inj_info.get("team") or "") if isinstance(inj_info, dict) else ""
            if inj_team and player_team.lower() not in inj_team.lower() \
                        and inj_team.lower() not in player_team.lower():
                continue

        is_out = (inj_info.get("status") in ("Out", "Doubtful")) \
                 if isinstance(inj_info, dict) else bool(inj_info)
        if is_out:
            teammate_boosts += 1

    # Each absent teammate adds a small usage bump — capped at 2 teammates (max 1.12x)
    multiplier = 1.0 + (min(teammate_boosts, 2) * 0.06)
    return round(base_usage * multiplier, 3)


def get_context(player, stats=None):
    """
    Returns real per-player context from BDL stats.
    Only avg_mins is kept — all artificial boost fields removed.
    """
    s = stats or get_player_stats(player) or {}
    return {"avg_mins": s.get("avg_mins", 0.0)}


def find_player_edges():
    """
    Scan ALL players with lines from the Odds API.
    Routes through the full 7-step Edge-Fade engine (same as the nightly slip)
    so every candidate has passed: juice test → game script → EV check →
    public pressure → injury/B2B adjustments → confidence calibration.
    Returns engine-cleared legs sorted by confidence descending.
    """
    from bot.slip_builder import build_slip_from_props, get_top_candidates

    try:
        odds_data = get_player_props()
        if not odds_data:
            return []

        injuries   = get_espn_injuries()
        inj_boost  = assess_injury_boost(injuries, odds_data)
        b2b_teams  = detect_back_to_back_teams()
        _load_and_apply_team_styles()
        _fe_shadow = _load_shadow_hit_rates()
        _fe_wr_ctx = _load_win_rate_context()
        _fe_mults  = _load_conf_multipliers()

        slip, _, _ = build_slip_from_props(
            props_data          = odds_data,
            get_player_stats_fn = get_player_stats,
            games_data          = _games_data,
            checkout_url        = CHECKOUT_URL,
            injuries            = injuries,
            injury_boost        = inj_boost,
            back_to_back_teams  = b2b_teams,
            shadow_hit_rates    = _fe_shadow,
            win_rate_context    = _fe_wr_ctx,
            conf_multipliers    = _fe_mults,
        )

        if slip:
            return sorted(slip.legs, key=lambda l: l.get("confidence", 0), reverse=True)

        candidates = get_top_candidates(
            props_data          = odds_data,
            get_player_stats_fn = get_player_stats,
            games_data          = _games_data,
            injuries            = injuries,
            injury_boost        = inj_boost,
            back_to_back_teams  = b2b_teams,
            shadow_hit_rates    = _fe_shadow,
            win_rate_context    = _fe_wr_ctx,
        )
        return sorted(candidates, key=lambda l: l.get("confidence", 0), reverse=True)

    except Exception as e:
        print(f"[find_player_edges] engine error: {e}")
        return []


def generate_signals(picks):
    """Label engine-cleared picks with bet-size signals based on confidence."""
    signals = []
    for p in picks:
        conf = p.get("confidence", 0)
        if conf >= 80:
            p["signal"] = "💰 MAX BET"
        elif conf >= 70:
            p["signal"] = "🔥 STRONG BET"
        else:
            p["signal"] = "⚠️ LEAN"
        signals.append(p)
    return signals


def kelly_bet_size(bankroll, edge, odds=1.91):
    prob = 0.5 + (edge / 20)  # convert edge to probability
    b = odds - 1

    kelly = (b * prob - (1 - prob)) / b

    kelly = max(0, min(kelly, 0.25))  # cap at 25%

    bet_size = bankroll * kelly

    return round(bet_size, 2)


def detect_sharp_action(edge, movement):
    if movement > 1 and edge > 2:
        return "📈 Sharp Money (Over)"

    if movement < -1 and edge < -2:
        return "📉 Sharp Money (Under)"

    return "No Sharp Signal"


def track_line_movement(player, current_line):
    movement = 0

    if player in line_history:
        movement = current_line - line_history[player]

    line_history[player] = current_line

    return movement


def run_prop_model():
    """Alias — routes through the full 7-step engine."""
    return generate_signals(find_player_edges())


def run_elite_model(bankroll=1000):
    """Alias — routes through the full 7-step engine."""
    return find_player_edges()


def parse_minutes(min_str):
    if not min_str:
        return 0.0
    try:
        s = str(min_str)
        if ":" in s:
            parts = s.split(":")
            return float(parts[0]) + float(parts[1]) / 60
        return float(s)
    except Exception:
        return 0.0


def get_team_starters(team_id):
    try:
        end_date   = datetime.now().date()
        start_date = end_date - timedelta(days=14)
        url = (
            f"{BDL_BASE}/stats"
            f"?team_ids[]={team_id}&per_page=100"
            f"&start_date={start_date}&end_date={end_date}"
        )
        stats = _bdl_get(url).get("data", [])
        if not stats:
            return []

        def wt_avg(arr):
            if not arr:
                return 0.0
            arr = arr[:10]
            w = [1 / (i + 1) for i in range(len(arr))]
            return round(sum(v * wi for v, wi in zip(arr, w)) / sum(w), 1)

        by_player = {}
        for s in stats:
            if s.get("team", {}).get("id") != team_id:
                continue          # skip the opponent's players
            p   = s.get("player", {})
            pid = p.get("id")
            nm  = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
            if not pid or not nm:
                continue
            mins = parse_minutes(s.get("min"))
            if pid not in by_player:
                by_player[pid] = {"name": nm, "pts":[], "reb":[], "ast":[], "fg3":[], "mins":[]}
            by_player[pid]["pts"].append(s.get("pts",  0) or 0)
            by_player[pid]["reb"].append(s.get("reb",  0) or 0)
            by_player[pid]["ast"].append(s.get("ast",  0) or 0)
            by_player[pid]["fg3"].append(s.get("fg3m", 0) or 0)
            by_player[pid]["mins"].append(mins)

        starters = []
        for d in by_player.values():
            avg_mins = wt_avg(d["mins"])
            if avg_mins < 15:
                continue
            starters.append({
                "name":     d["name"],
                "avg_mins": avg_mins,
                "pred_pts": predict_player(d["pts"]) or wt_avg(d["pts"]),
                "pred_reb": predict_player(d["reb"]) or wt_avg(d["reb"]),
                "pred_ast": predict_player(d["ast"]) or wt_avg(d["ast"]),
                "pred_fg3": predict_player(d["fg3"]) or wt_avg(d["fg3"]),
            })

        starters.sort(key=lambda x: x["avg_mins"], reverse=True)
        return starters[:8]

    except Exception as e:
        print(f"get_team_starters error: {e}")
        return []


def get_team_player_positions(team_id):
    """Return {player_name_lower: raw_position_str} from BDL players endpoint."""
    try:
        url = f"{BDL_BASE}/players?team_ids[]={team_id}&per_page=100"
        data = _bdl_get(url).get("data", [])
        pos_map = {}
        for p in data:
            name = f"{p.get('first_name','')} {p.get('last_name','')}".strip().lower()
            pos_map[name] = (p.get("position") or "").upper()
        return pos_map
    except Exception as e:
        print(f"get_team_player_positions error: {e}")
        return {}


def positions_compatible(starter_pos, sub_pos):
    """True if sub_pos can cover starter_pos.
    G-F and F-C cover both their letter roles.
    Empty/unknown position is treated as compatible with anything.
    """
    if not starter_pos or not sub_pos:
        return True
    def roles(p):
        r = set()
        if "G" in p: r.add("G")
        if "F" in p: r.add("F")
        if "C" in p: r.add("C")
        return r or {"F"}
    return bool(roles(starter_pos) & roles(sub_pos))


def run_starters_report(games, tip_utc=None):
    SLOTS = ["🔵", "🟢", "🟡", "🟠", "🔴"]
    injuries = get_espn_injuries()

    INJURY_BADGE = {
        "Out":          "⛔ OUT",
        "Doubtful":     "❌ DBT",
        "Questionable": "⚠️ GTD",
        "Day-To-Day":   "⚠️ GTD",
    }

    def inj_tag(name):
        info = injuries.get(name.lower())
        if not info:
            return ""
        badge = INJURY_BADGE.get(info["status"], f"🩹 {info['status']}")
        return f" {badge}"

    DIV = "─────────────────"

    # Reset the sent-today set if the calendar day has rolled over — persist across restarts
    global _starters_sent_today, _starters_sent_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _starters_sent_date != today:
        _s = load_status()
        if _s.get("_starters_date") == today:
            _starters_sent_today = set(_s.get("_starters_sent", []))
        else:
            _starters_sent_today = set()
        # Also check learning_data — written immediately after each send,
        # more reliable than save_status on restarts
        try:
            _ld_conn = _db_conn()
            if _ld_conn:
                _ld_cur = _ld_conn.cursor()
                _ld_cur.execute(
                    "SELECT value FROM learning_data WHERE key = 'starters_sent_today'"
                )
                _ld_row = _ld_cur.fetchone()
                _ld_cur.close()
                _ld_conn.close()
                if _ld_row:
                    _ld_val = _ld_row[0]
                    if isinstance(_ld_val, list):
                        _starters_sent_today |= set(_ld_val)
                    elif isinstance(_ld_val, str):
                        import json as _j
                        _starters_sent_today |= set(_j.loads(_ld_val))
        except Exception:
            pass
        _starters_sent_date = today

    def build_lineup(pool, pos_map):
        """
        Given a pool of up to 8 players (sorted by avg_mins desc) and a
        position map, return a final 5-man lineup with injured starters
        (OUT / Doubtful) swapped for the highest-minute bench player at
        the same position group.  Returns list of dicts with extra keys:
          sub_for  – name of the player being replaced (or None)
        """
        MUST_SUB = {"Out", "Doubtful"}
        starters = [dict(p, sub_for=None) for p in pool[:5]]
        bench    = list(pool[5:])          # remaining bench candidates
        used_bench = set()

        for idx, s in enumerate(starters):
            inj = injuries.get(s["name"].lower())
            if not inj or inj["status"] not in MUST_SUB:
                continue
            s_pos = pos_map.get(s["name"].lower(), "")
            # Find first available bench player with a compatible position
            for b in bench:
                if b["name"] in used_bench:
                    continue
                b_inj = injuries.get(b["name"].lower())
                if b_inj and b_inj["status"] in MUST_SUB:
                    continue          # bench player is also out
                b_pos = pos_map.get(b["name"].lower(), "")
                if positions_compatible(s_pos, b_pos):
                    starters[idx] = dict(b, sub_for=s["name"])
                    used_bench.add(b["name"])
                    break

        return starters

    for game in games[:4]:
        try:
            h = game.get("home_team", "")
            a = game.get("away_team", "")
            if not h or not a:
                continue
            game_key = f"{a}@{h}"

            if game_key in _starters_sent_today:
                print(f"  Starters: {a} @ {h} (already sent today, skipping)")
                continue

            print(f"  Starters: {a} @ {h}")

            # Fetch position maps from BDL (have team IDs from game object)
            away_pos = get_team_player_positions(None)
            home_pos = get_team_player_positions(None)

            # Build stats pools first (needed for confirmed lineup stat merging)
            away_pool = get_team_starters_espn(a)
            if not away_pool:
                time.sleep(2)
                away_pool = get_team_starters(None)

            home_pool = get_team_starters_espn(h)
            if not home_pool:
                time.sleep(2)
                home_pool = get_team_starters(None)

            # Try official confirmed lineup first, fall back to minutes-based pool
            away_confirmed, away_official = get_confirmed_lineup_espn(a, away_pool)
            home_confirmed, home_official = get_confirmed_lineup_espn(h, home_pool)

            # Build BDL fallback pools keyed by lowercased name
            away_bdl = {p["name"].lower(): p for p in away_pool}
            home_bdl = {p["name"].lower(): p for p in home_pool}

            def _fill_projections(lineup, bdl_pool):
                """For any starter with zero projections, fill from BDL pool."""
                for p in lineup:
                    if p.get("pred_pts", 0) == 0:
                        bdl = bdl_pool.get(p["name"].lower(), {})
                        p["pred_pts"] = bdl.get("pred_pts", 0)
                        p["pred_reb"] = bdl.get("pred_reb", 0)
                        p["pred_ast"] = bdl.get("pred_ast", 0)
                        p["pred_fg3"] = bdl.get("pred_fg3", 0)
                return lineup

            if away_official:
                away_lineup = _fill_projections(away_confirmed, away_bdl)
            else:
                away_lineup = build_lineup(away_pool, away_pos) if away_pool else []
                print(f"  [Lineup] Projected (not confirmed) — {a}")

            if home_official:
                home_lineup = _fill_projections(home_confirmed, home_bdl)
            else:
                home_lineup = build_lineup(home_pool, home_pos) if home_pool else []
                print(f"  [Lineup] Projected (not confirmed) — {h}")

            if not away_lineup and not home_lineup:
                print(f"  [Lineup] No data at all — skipping {a} @ {h}")
                continue

            lineup_status = "✅ CONFIRMED" if (away_official and home_official) else "📊 PROJECTED"

            def fmt_sub(s, i):
                tag = inj_tag(s["name"])
                pts = round(s["pred_pts"])
                reb = round(s["pred_reb"])
                ast = round(s["pred_ast"])
                fg3 = round(s["pred_fg3"], 1)
                sub_note = f"\n   ↩️ _SUB for {s['sub_for']}_" if s.get("sub_for") else ""
                return (
                    f"{SLOTS[i]} *{s['name']}*{tag}{sub_note}\n"
                    f"   🏀 {pts} pts · 💪 {reb} reb · 🔥 {ast} ast · 🎯 {fg3} 3s"
                )

            away_lines = "\n".join(fmt_sub(s, i) for i, s in enumerate(away_lineup)) if away_lineup else "_Lineup data unavailable_"
            home_lines = "\n".join(fmt_sub(s, i) for i, s in enumerate(home_lineup)) if home_lineup else "_Lineup data unavailable_"

            try:
                import zoneinfo as _zi3
                if tip_utc:
                    _tip_et = tip_utc.astimezone(_zi3.ZoneInfo("America/New_York"))
                else:
                    raw = game.get("tip_time", "") or game.get("status", "") or game.get("date", "")
                    if "T" in str(raw):
                        _tip_et = datetime.fromisoformat(
                            str(raw).replace("Z", "").split(".")[0]
                        ).replace(tzinfo=timezone.utc).astimezone(_zi3.ZoneInfo("America/New_York"))
                    else:
                        _tip_et = None
                tip_label = _tip_et.strftime("%b %d, %-I:%M %p ET") if _tip_et else "Time TBD"
            except Exception:
                tip_label = "Time TBD"

            msg = (
                f"{lineup_status} *STARTING FIVE*\n"
                f"_{a} @ {h}_\n"
                f"_Tip-off: {tip_label}_\n\n"
                f"✈️ *{a}* (Away)\n"
                f"{DIV}\n"
                f"{away_lines}\n\n"
                f"🏠 *{h}* (Home)\n"
                f"{DIV}\n"
                f"{home_lines}"
            )

            send("🔒 *VIP LOCK*\n\n" + msg, VIP_CHANNEL)
            _starters_sent_today.add(game_key)
            # Write to DB immediately — prevents duplicate if bot restarts
            # between this send and the next save_status call
            try:
                _st_conn = _db_conn()
                if _st_conn:
                    _st_cur = _st_conn.cursor()
                    _st_cur.execute("""
                        INSERT INTO learning_data (key, value, updated_at)
                        VALUES ('starters_sent_today', %s::jsonb, NOW())
                        ON CONFLICT (key) DO UPDATE SET
                            value = EXCLUDED.value, updated_at = NOW()
                    """, (json.dumps(list(_starters_sent_today)),))
                    _st_conn.commit()
                    _st_cur.close()
                    _st_conn.close()
            except Exception:
                pass
            save_status(0, {"_starters_date": today, "_starters_sent": list(_starters_sent_today)})
            print(f"  Starting Five sent ✓")

        except Exception as e:
            print(f"  run_starters_report error: {e}")
            continue


def run_full_system():
    global _props_sent_today, _props_sent_date
    _props_today = datetime.now().strftime("%Y-%m-%d")
    if _props_sent_date != _props_today:
        _ps = load_status()
        if _ps.get("_props_date") == _props_today:
            _props_sent_today = set(_ps.get("_props_sent", []))
        else:
            _props_sent_today = set()
        _props_sent_date = _props_today

    try:
        # Skip API call entirely if all cached games are already sent today
        if _props_cache and _props_sent_today:
            cached_games = {
                f"{g.get('away_team', '')} @ {g.get('home_team', '')}"
                for g in _props_cache if g.get("home_team")
            }
            cached_keys = {f"{g}:GAME_PROPS" for g in cached_games}
            if cached_keys and cached_keys.issubset(_props_sent_today):
                print("[Props] All games already sent today — skipping API call")
                return []

        odds_data = get_player_props()
        if not odds_data:
            print("No odds data")
            return []

        props = extract_props(odds_data)
        if not props:
            print("No props found")
            return []

        # Group all lines by player so we only look each player up once
        by_player = {}
        for p in props:
            player = p.get("player")
            if player:
                by_player.setdefault(player, []).append(p)

        PRED_KEY = {
            "points":   "pred_pts",
            "rebounds": "pred_reb",
            "assists":  "pred_ast",
            "threes":   "pred_fg3",
        }
        COMBO_PRED_KEYS = {
            "points_rebounds_assists": ("pred_pts", "pred_reb", "pred_ast"),
            "points_rebounds":         ("pred_pts", "pred_reb"),
            "points_assists":          ("pred_pts", "pred_ast"),
        }
        EMOJI = {
            "points":                    "🏀",
            "rebounds":                  "💪",
            "assists":                   "🔥",
            "threes":                    "🎯",
            "points_rebounds_assists":   "🎯",
            "points_rebounds":           "🎯",
            "points_assists":            "🎯",
            "first_basket":              "🥇",
        }

        UNIT = {
            "points": "pts", "rebounds": "reb", "assists": "ast", "threes": "3s",
            "points_rebounds_assists": "PRA", "points_rebounds": "PR",
            "points_assists": "PA", "first_basket": "1st basket",
        }

        # Get injuries once for health dots
        try:
            injuries = get_espn_injuries()
        except Exception:
            injuries = {}

        # Regroup props by game → player
        by_game = {}
        for p in props:
            game = p.get("game", "Unknown")
            player = p.get("player")
            if player:
                by_game.setdefault(game, {}).setdefault(player, []).append(p)

        all_picks = []

        for game, players_dict in list(by_game.items()):
            game_prop_key = f"{game}:GAME_PROPS"
            if game_prop_key in _props_sent_today:
                print(f"  {game}: props already sent today, skipping")
                continue

            away_team, home_team = (game.split(" @ ", 1) + ["Home"])[:2] if " @ " in game else ("Away", game)

            # Build per-player data
            _game_picks_start = len(all_picks)   # track new picks for this game
            player_data = {}
            for player, pprops in list(players_dict.items())[:8]:
                try:
                    time.sleep(2)
                    stats = get_player_stats(player)
                    if not stats:
                        continue

                    # ── Load learned thresholds once per player ──────────
                    _thr       = load_learning_data().get("script_thresholds") or _SCRIPT_THRESHOLD_DEFAULTS
                    _min_gate  = min(_thr.get("prop_minutes_gate", _SCRIPT_THRESHOLD_DEFAULTS["prop_minutes_gate"]), 25)
                    _start_thr = _thr.get("prop_starter_mins", _SCRIPT_THRESHOLD_DEFAULTS["prop_starter_mins"])
                    _usg_gate  = _thr.get("prop_usage_gate",   _SCRIPT_THRESHOLD_DEFAULTS["prop_usage_gate"])

                    # ── Minutes gate: skip low-minute players ────────────
                    avg_mins = stats.get("avg_mins", 30)
                    if avg_mins < _min_gate:
                        print(f"  [Gate] {player} — {avg_mins:.0f} min/game avg (need {_min_gate:.0f}+), skipping")
                        continue

                    # ── Usage gate: skip low-usage / role players ─────────
                    avg_usage = stats.get("avg_usage", 10)
                    if avg_usage < _usg_gate:
                        print(f"  [Gate] {player} — usage {avg_usage:.1f}/game (need {_usg_gate:.1f}+), skipping")
                        continue

                    # Whether this player qualifies as a rotation starter
                    is_starter = avg_mins >= _start_thr

                    # Health dot
                    inj_info  = injuries.get(player.lower(), {})
                    inj_status = inj_info.get("status", "")

                    # Skip props for players confirmed out or doubtful
                    if inj_status in ("Out", "Doubtful"):
                        print(f"  [Props] Skipping {player} — {inj_status}")
                        continue

                    dot = "🟡" if inj_status in ("Questionable", "Day-To-Day") else "🟢"

                    # Predictions (guard against None from predict_player)
                    def _safe_round(v):
                        try:
                            return round(float(v or 0), 1)
                        except Exception:
                            return 0.0
                    pred = {
                        "points":   _safe_round(stats.get("pred_pts")),
                        "rebounds": _safe_round(stats.get("pred_reb")),
                        "assists":  _safe_round(stats.get("pred_ast")),
                        "threes":   _safe_round(stats.get("pred_fg3")),
                    }

                    # Extend pred dict with combo values for quick lookup
                    pred["points_rebounds_assists"] = round(
                        pred["points"] + pred["rebounds"] + pred["assists"], 1)
                    pred["points_rebounds"] = round(pred["points"] + pred["rebounds"], 1)
                    pred["points_assists"]  = round(pred["points"] + pred["assists"], 1)

                    # Elite pick detection
                    elite_picks = []
                    for prop in pprops:
                        prop_type = prop.get("prop_type", "points")
                        line = prop.get("line")

                        # ── First basket: no line, handled separately ─────────
                        if prop_type == "first_basket":
                            fb_odds = float(prop.get("odds", 0))
                            fb_impl = implied_prob(fb_odds) if fb_odds > 0 else 1.0
                            if fb_odds >= 400 and fb_impl < 0.20:
                                fb_conf = round(min(72, max(55, (0.20 - fb_impl) * 400 + 55)), 1)
                                elite_picks.append({
                                    "emoji":      "🥇",
                                    "pick_side":  "SCORER",
                                    "line":       fb_odds,
                                    "prediction": 0,
                                    "prop_type":  "first_basket",
                                    "confidence": fb_conf,
                                    "edge":       round(0.20 - fb_impl, 3),
                                    "odds":       fb_odds,
                                    "unit":       "1st basket",
                                    "dot":        dot,
                                    "is_starter": is_starter,
                                    "mismatch":   "",
                                    "desc": f"🥇 {player} FIRST BASKET SCORER +{int(fb_odds)} — {fb_conf:.0f}%",
                                })
                            continue

                        if not line:
                            continue
                        # ── Hard block: -400 wall ─────────────────────────────
                        _raw_odds = float(prop.get("odds", -110))
                        if _raw_odds <= -400:
                            print(f"  [HardBlock] {player} {prop_type} {_raw_odds} — beyond -400 wall, skipped")
                            continue

                        # ── Prediction: sum for combo props, single for standard ──
                        if prop_type in COMBO_PRED_KEYS:
                            prediction = round(sum(
                                float(stats.get(pk) or 0)
                                for pk in COMBO_PRED_KEYS[prop_type]
                            ), 1)
                        else:
                            prediction = float(stats.get(PRED_KEY.get(prop_type, "pred_pts")) or 0)

                        edge       = prediction - float(line)
                        pick_side  = "OVER" if edge > 0 else "UNDER"
                        _stat_hist_map = {
                            "points":   stats.get("pts", []),
                            "rebounds": stats.get("reb", []),
                            "assists":  stats.get("ast", []),
                            "threes":   stats.get("fg3", []),
                        }
                        stat_vals  = [x for x in (_stat_hist_map.get(prop_type) or []) if x is not None]
                        variance   = float(np.std(stat_vals[:5])) if len(stat_vals) >= 2 else 3.0
                        confidence = calibrated_confidence(
                            prop_type,
                            calculate_confidence(
                                edge, variance,
                                history=stat_vals,
                                line=float(line),
                                direction=pick_side,
                            )
                        )
                        if is_elite_pick(edge, confidence, prop_type=prop_type):
                            # ── Gate 1: Role alignment check (advisory — adds tag, never kills pick) ──
                            _prop_role_tag = ""
                            _role_mismatch_warn = ""
                            try:
                                from game_script import analyze_game_script as _ags_prop, assign_role as _ar_prop
                                _prop_gs = _ags_prop(home_team, away_team, 220, 5)
                                _p_team  = stats.get("team", "")
                                _is_home = any(w in _p_team.lower() for w in home_team.lower().split())
                                _prop_role = _ar_prop(
                                    player       = player,
                                    team         = _p_team,
                                    avg_pts      = float(stats.get("pred_pts") or 0),
                                    avg_reb      = float(stats.get("pred_reb") or 0),
                                    avg_ast      = float(stats.get("pred_ast") or 0),
                                    avg_mins     = float(avg_mins or 28),
                                    avg_usage    = float(avg_usage or 15),
                                    game_script  = _prop_gs,
                                    is_home      = _is_home,
                                )
                                _prop_role_tag = _prop_role.role
                                # Role-prop mismatches: advisory warning only, does NOT block pick.
                                # Big men (glass_cleaner/rim_anchor) CAN score — removed "points" block.
                                _mismatches = {
                                    "go_to_scorer":    ["rebounds"],
                                    "floor_general":   ["rebounds"],
                                    "glass_cleaner":   ["assists", "threes", "3pm"],
                                    "rim_anchor":      ["assists", "threes", "3pm"],
                                    "spot_up_shooter": ["rebounds", "assists"],
                                    "combo_creator":   ["rebounds"],
                                    "sixth_man":       ["rebounds"],
                                    "utility_player":  [],
                                }
                                if prop_type in _mismatches.get(_prop_role_tag, []):
                                    _role_mismatch_warn = ""  # advisory only — not shown to users
                                    print(f"  [Role:{_prop_role_tag}] {player} {prop_type} advisory — mismatch noted, pick proceeds")
                            except Exception:
                                pass  # fail-open: role check is advisory

                            # ── Gate 2: Juice trap check (quantitative — can block pick) ─────────
                            _prop_juice_ok   = True
                            _prop_juice_warn = ""
                            try:
                                _prop_odds = float(prop.get("odds", -110))
                                from decision_engine import juice_test as _jt_prop
                                _jt_res = _jt_prop(_prop_odds)
                                if _jt_res.flag == "RED":
                                    # Heavy juice: require model to beat implied prob by ≥5%
                                    from decision_engine import implied_probability as _ip_prop
                                    _imp = _ip_prop(_prop_odds)
                                    _stat_history = stat_vals[:10] if stat_vals else []
                                    _prop_true_prob = float(calculate_confidence(edge, variance,
                                                        history=_stat_history, line=line,
                                                        direction=pick_side)) / 100
                                    if _prop_true_prob < _imp + 0.05:
                                        _prop_juice_ok = False
                                        print(f"  [Juice:RED] {player} {prop_type} {_prop_odds} — model {round(_prop_true_prob*100,1)}% ≤ implied {round(_imp*100,1)}%+5%")
                                    else:
                                        _prop_juice_warn = " ⚠️"
                                elif _jt_res.flag == "YELLOW":
                                    _prop_juice_warn = " ⚡"
                            except Exception:
                                pass  # fail-open

                            if _prop_juice_ok:
                                unit = UNIT.get(prop_type, prop_type)
                                _role_suffix = f" [{_prop_role_tag}]" if _prop_role_tag else ""

                                # ── Real prob + edge (same formula as all live picks) ──
                                from decision_engine import implied_probability as _ip_ep
                                _ep_odds    = float(prop.get("odds", -110))
                                # Use real per-player std from game log; only fall back
                                # to the league-wide constant when < 5 games exist.
                                _ep_real_std = float(np.std(stat_vals)) if len(stat_vals) >= 5 else _PROP_STD.get(prop_type, 5.0)
                                _ep_sf      = _norm_sf(line, prediction, _ep_real_std)
                                _ep_prob    = round(_ep_sf if pick_side == "OVER" else 1.0 - _ep_sf, 4)
                                _ep_implied = round(_ip_ep(_ep_odds), 4)
                                _ep_edge    = round(_ep_prob - _ep_implied, 4)
                                # Real EV: (true_prob × win_payout) − (1 − true_prob)
                                _ep_win_pay = (_ep_odds / 100) if _ep_odds > 0 else (100 / abs(_ep_odds)) if _ep_odds != 0 else 0.909
                                _ep_ev      = round((_ep_prob * _ep_win_pay) - (1 - _ep_prob), 4)

                                # FanDuel-style odds display
                                _ep_odds_disp = int(_ep_odds)
                                _ep_odds_str  = f"+{_ep_odds_disp}" if _ep_odds_disp > 0 else str(_ep_odds_disp)
                                elite_picks.append(
                                    f"✅ {player} — *{_fd_label(prop_type, line, pick_side)}*  ({_ep_odds_str})"
                                )
                                all_picks.append({
                                    "game":       game,
                                    "player":     player,
                                    "pick":       pick_side,
                                    "line":       line,
                                    "odds":       _ep_odds,
                                    "prob":       _ep_prob,
                                    "edge_real":  _ep_edge,
                                    "implied":    _ep_implied,
                                    "ev":         _ep_ev,
                                    "confidence": confidence,
                                    "bet_size":   50,
                                    "prop_type":  prop_type,
                                    "prediction": round(prediction, 1),
                                    "is_starter": is_starter,
                                    "avg_mins":   avg_mins,
                                    "avg_usage":  avg_usage,
                                    "role":       _prop_role_tag,
                                })

                    player_data[player] = {
                        "dot":         dot,
                        "position":    stats.get("position", ""),
                        "team":        stats.get("team", ""),
                        "pred":        pred,
                        "elite_picks": elite_picks,
                    }
                except Exception as inner:
                    print(f"Player error ({player}):", inner)

            if not player_data:
                continue

            # Split by team using keyword matching
            away_words = set(away_team.lower().split())
            home_words  = set(home_team.lower().split())
            buckets = {"away": {}, "home": {}, "other": {}}
            for p, d in player_data.items():
                tw = set(d["team"].lower().split())
                if tw & away_words:
                    buckets["away"][p] = d
                elif tw & home_words:
                    buckets["home"][p] = d
                else:
                    buckets["other"][p] = d

            def player_line(p, d):
                pos = f" · {d['position']}" if d["position"] else ""
                pr  = d["pred"]
                return (
                    f"{d['dot']} *{p}*{pos}\n"
                    f"🏀 {pr['points']} pts · 💪 {pr['rebounds']} reb · "
                    f"🔥 {pr['assists']} ast · 🎯 {pr['threes']} 3s"
                )

            sections = []
            if buckets["away"]:
                label = away_team.split()[-1].upper()
                sections.append(
                    f"━━━ *{label}* ━━━\n" +
                    "\n\n".join(player_line(p, d) for p, d in buckets["away"].items())
                )
            if buckets["home"]:
                label = home_team.split()[-1].upper()
                sections.append(
                    f"━━━ *{label}* ━━━\n" +
                    "\n\n".join(player_line(p, d) for p, d in buckets["home"].items())
                )
            if buckets["other"]:
                sections.append(
                    "\n\n".join(player_line(p, d) for p, d in buckets["other"].items())
                )

            # Elite picks section
            all_elite_lines = []
            for d in player_data.values():
                all_elite_lines.extend(d["elite_picks"])
            if all_elite_lines:
                sections.append("━━━ *🎯 ELITE PICKS* ━━━\n" + "\n".join(all_elite_lines))

            msg = (
                f"🏀 *ELITE PLAYER PROPS*\n"
                f"_{game}_\n\n"
                + "\n\n".join(sections)
            )
            send("🔒 *VIP LOCK*\n\n" + msg, VIP_CHANNEL)
            _props_sent_today.add(game_prop_key)
            save_status(0, {"_props_date": _props_today, "_props_sent": list(_props_sent_today)})
            # Save each elite prop pick to DB for live tracking
            _ep_gdata = _games_data.get(game, {})
            for _ep in all_picks[_game_picks_start:]:
                try:
                    _ep_stat = _ep.get("prop_type", "points")
                    _ep_dir  = _ep.get("pick", "OVER")
                    _ep_line = _ep.get("line", 0)
                    save_bet({
                        "game":             game,
                        "player":           _ep.get("player", ""),
                        "pick":             f"{_ep_dir} {_ep_line} {_ep_stat}",
                        "betType":          "ELITE_PROP",
                        "line":             _ep_line,
                        "confidence":       _ep.get("confidence", 0),
                        "odds":             _ep.get("odds", -115),
                        "prob":             _ep.get("prob", 0.5),
                        "edge":             _ep.get("edge_real", 0.0),
                        "ev":               _ep.get("ev", 0.0),
                        "prediction":       _ep.get("prediction"),
                        "script":           detect_game_script(_ep_gdata),
                        "game_total":       _ep_gdata.get("total"),
                        "game_spread":      _ep_gdata.get("spread"),
                        "player_avg_mins":  _ep.get("avg_mins"),
                        "player_avg_usage": _ep.get("avg_usage"),
                        "role":             _ep.get("role"),
                        "pick_category":    "ELITE_PROP",
                        "time":             datetime.now().isoformat(),
                    })
                except Exception:
                    pass
            print(f"  {game}: props sent ({len(player_data)} players, {len(all_elite_lines)} elite picks)")

        return all_picks

    except Exception as e:
        print("SYSTEM ERROR:", e)
        return []


def auto_run():
    while True:
        picks = run_full_system()

        for p in picks:
            send_telegram(
                f"{p['player']} {p['pick']} {p['line']} | "
                f"{p['confidence']}% | Bet ${p['bet_size']}"
            )

        time.sleep(180)


# ── EDGE-FADE 7: Full 7-step decision engine slip ─────────────────────────────
_edge_fade_sent_date   = None   # date string when today's slip was sent
_edge_fade_alerted_date = None  # date string when "no picks" alert was last sent (max once/day)
_line_monitor_active   = False  # live line-refresh loop running flag
_line_monitor_thread   = None   # daemon thread reference
_bet_history: list     = []     # [{time, legs, stake, ev, prob, bankroll}] — session log
_session_bankroll_start: float = 0.0  # starting bankroll of the current monitor session
_fd_retry_ts:          float = 0.0   # last time we tried when FanDuel wasn't posted
_fd_not_posted_alerted: bool  = False # True once we've sent the "not posted yet" alert


def run_edge_fade_7():
    """
    Build and send the Edge-Fade 7 parlay slip using the complete
    7-step decision engine: game script → role assignment → fade detection
    → benefactor mapping → EV check → slip validation → grading.

    Sends grade A/B slips to VIP. Grade A also previews to free channel.
    Sends once per day (guards with _edge_fade_sent_date).
    """
    global _edge_fade_sent_date

    import zoneinfo as _zi
    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    today_str = et_now.strftime("%Y-%m-%d")

    # Fire between 2 PM and 9 PM ET — props are live before tip-offs
    if not (14 <= et_now.hour < 21):
        return

    if _edge_fade_sent_date == today_str:
        return

    print("[EdgeFade7] Building slip...")

    try:
        from bot.slip_builder import build_slip_from_props, slip_to_bet_records
        from bot.adaptive_thresholds import run_adaptive_update

        # ── Adaptive threshold update ──────────────────────────────────────
        try:
            _ada_conn = _db_conn()
            if _ada_conn:
                ada = run_adaptive_update(_ada_conn)
                _ada_conn.close()
                print(
                    f"[EdgeFade7] Adaptive: {ada['tier']} "
                    f"({ada['win_rate']:.1%}) — {ada['label']}"
                )
        except Exception as _ae:
            print(f"[EdgeFade7] Adaptive threshold update skipped: {_ae}")

        # Fetch fresh props
        import time as _time_fd
        global _fd_retry_ts, _fd_not_posted_alerted
        odds_data = get_player_props()
        if not odds_data:
            _now_ts = _time_fd.time()
            # Enforce 15-minute gap between retries — bot cycles every 3 min so
            # without this gate it would spam every cycle until FanDuel posts.
            if _fd_retry_ts > 0 and _now_ts - _fd_retry_ts < 900:
                print(f"[EdgeFade7] FanDuel not posted — next retry in "
                      f"{int(900 - (_now_ts - _fd_retry_ts))}s")
                return
            _fd_retry_ts = _now_ts
            # Send ONE alert the first time we notice FanDuel hasn't posted.
            # After that, stay quiet and keep retrying silently every 15 min.
            if not _fd_not_posted_alerted:
                _fd_not_posted_alerted = True
                try:
                    send(
                        f"⏳ *FanDuel Props Not Posted Yet*\n\n"
                        f"Checked at {et_now.strftime('%-I:%M %p ET')} — "
                        f"no lines from FanDuel yet.\n"
                        f"Retrying every 15 min until 9 PM ET. "
                        f"Will alert you when they're live.",
                        str(ADMIN_ID)
                    )
                except Exception:
                    pass
            print("[EdgeFade7] FanDuel not posted — retrying in 15 min")
            return

        # FanDuel just posted — notify admin if we'd been waiting
        if _fd_not_posted_alerted:
            _fd_not_posted_alerted = False
            _fd_retry_ts = 0.0
            try:
                send(
                    f"✅ *FanDuel Props Are Live*\n\n"
                    f"Lines just posted at {et_now.strftime('%-I:%M %p ET')} — "
                    f"building slip now.",
                    str(ADMIN_ID)
                )
            except Exception:
                pass

        # Get injuries once
        try:
            injuries = get_espn_injuries()
        except Exception:
            injuries = {}

        # Compute injury boost (star out → teammate usage spikes)
        try:
            _inj_boost = assess_injury_boost(injuries, odds_data)
        except Exception:
            _inj_boost = {}

        # Detect back-to-back teams (real ESPN schedule data)
        try:
            _b2b = detect_back_to_back_teams()
        except Exception:
            _b2b = set()

        def _admin_alert(msg):
            try:
                send(f"🤖 *EdgeFade7 Alert*\n{msg}", str(ADMIN_ID))
            except Exception:
                pass

        # ── Load all learning data before building picks ─────────────────────
        _conf_mults = _load_conf_multipliers()
        print(f"[EdgeFade7] Conf multipliers loaded: {_conf_mults}")

        # Apply learned team styles (survive restarts via DB)
        _load_and_apply_team_styles()

        # Per-player-stat shadow hit rates → edge_bonus in engine
        _shadow_rates = _load_shadow_hit_rates()

        # All historical win-rate learning: by prop type, by script, by role
        _wr_ctx = _load_win_rate_context()

        # ── Query players already bet today — prevent same player re-picks ──────
        _bet_players_today: set = set()
        try:
            _dup_conn = _db_conn()
            _dup_cur  = _dup_conn.cursor()
            _dup_cur.execute(
                "SELECT DISTINCT player FROM bets "
                "WHERE DATE(bet_time) = CURRENT_DATE "
                "  AND player IS NOT NULL AND player != ''"
            )
            _bet_players_today = {r[0].strip().lower() for r in _dup_cur.fetchall()}
            _dup_cur.close(); _dup_conn.close()
            if _bet_players_today:
                print(f"[EdgeFade7] Skipping {len(_bet_players_today)} already-bet players today")
        except Exception as _de:
            print(f"[EdgeFade7] Daily dedup query failed (fail-open): {_de}")

        slip, vip_msg, free_msg = build_slip_from_props(
            props_data          = odds_data,
            get_player_stats_fn = get_player_stats,
            games_data          = _games_data,
            checkout_url        = CHECKOUT_URL,
            admin_alert_fn      = _admin_alert,
            injuries            = injuries,
            injury_boost        = _inj_boost,
            back_to_back_teams  = _b2b,
            shadow_hit_rates    = _shadow_rates,
            win_rate_context    = _wr_ctx,
            conf_multipliers    = _conf_mults,
            players_bet_today   = _bet_players_today,
        )

        if slip is None:
            print("[EdgeFade7] No valid slip built — engine blocked it")
            # ── Fallback: send best individual candidates that passed ──────────
            # Even if a full 7-leg slip couldn't be graded A/B, surface the top
            # engine-cleared picks so the channel gets something tonight.
            try:
                from bot.slip_builder import get_top_candidates
                _inj_boost_vip = {}
                _b2b_vip       = set()
                try:
                    _inj_boost_vip = assess_injury_boost(injuries, odds_data)
                except Exception:
                    pass
                try:
                    _b2b_vip = detect_back_to_back_teams()
                except Exception:
                    pass
                # Three-pass fallback: 5 → 10 → 15
                candidates = []
                for _pass_n in (5, 10, 15):
                    if candidates:
                        break
                    try:
                        candidates = get_top_candidates(
                            props_data          = odds_data,
                            get_player_stats_fn = get_player_stats,
                            games_data          = _games_data,
                            injuries            = injuries,
                            top_n               = _pass_n,
                            injury_boost        = _inj_boost_vip,
                            back_to_back_teams  = _b2b_vip,
                            shadow_hit_rates    = _shadow_rates,
                            win_rate_context    = _wr_ctx,
                        )
                        if candidates:
                            print(f"[EdgeFade7] Fallback pass top_n={_pass_n} → {len(candidates)} candidates")
                    except Exception:
                        pass

                # Apply pick-record confidence multipliers to fallback candidates
                if candidates:
                    try:
                        _apply_conf_multipliers(candidates, _conf_mults)
                    except Exception:
                        pass

                if candidates:
                    # Individual fallback removed — CGP fires instead on slip-fail nights
                    # Stamp the guard so Edge-Fade 7 doesn't retry this cycle
                    _edge_fade_sent_date = today_str
                    print(f"[EdgeFade7] Slip graded out — {len(candidates)} candidates available, CGP will cover tonight")
                else:
                    # Props not posted yet — do NOT stamp guard, retry next cycle
                    print("[EdgeFade7] No candidates — props likely not posted yet, will retry")
                    global _edge_fade_alerted_date
                    # Restore from DB if memory was wiped by a restart
                    if _edge_fade_alerted_date is None:
                        _ef_st = load_status()
                        if _ef_st.get("_ef_alerted_date") == today_str:
                            _edge_fade_alerted_date = today_str
                    if _edge_fade_alerted_date != today_str:
                        _edge_fade_alerted_date = today_str
                        save_status(0, {"_ef_alerted_date": today_str})
                        _admin_alert("⚠️ Engine ran — no picks cleared. Props may not be posted yet — retrying next cycle.")
            except Exception as _fb_err:
                _edge_fade_sent_date = today_str  # guard on crash only — prevents spam on code errors
                print(f"[EdgeFade7] Fallback error: {_fb_err}")
                _admin_alert(f"⚠️ Engine error tonight: {_fb_err}")
            return

        print(f"[EdgeFade7] Slip grade {slip.grade} | {len(slip.legs)} legs | "
              f"payout +{slip.estimated_payout:.0f}")

        # Save each leg to DB
        timestamp = str(datetime.now())
        records = slip_to_bet_records(slip, timestamp)
        saved_count = 0
        for rec in records:
            if save_bet(rec):
                saved_count += 1

        # ── Auto-swap: re-check injuries between build and send ──────────
        try:
            _fresh_inj = get_espn_injuries()
            _out_names = {
                name.lower()
                for name, info in _fresh_inj.items()
                if isinstance(info, dict) and info.get("status") in ("Out", "Doubtful")
            }
            for _leg in slip.legs:
                _pname = (_leg.player or "").lower()
                _parts = _pname.split()
                if len(_parts) >= 2:
                    if any(_parts[0] in n and _parts[-1] in n for n in _out_names):
                        _admin_alert(
                            f"🔄 *AUTO-SWAP ALERT*\n"
                            f"_{_leg.player} now listed Out/Doubtful — "
                            f"was included in tonight's slip. Manual review needed._"
                        )
                        send(
                            f"🔄 *EDGE-FADE AUTO-SWAP*\n"
                            f"_{_leg.player} ruled out after slip was built — "
                            f"leg removed. Engine is sourcing best replacement._",
                            VIP_CHANNEL
                        )
        except Exception as _sw_err:
            print(f"[EdgeFade7] Auto-swap check error: {_sw_err}")

        # Send to channels
        if slip.send_to_vip and vip_msg:
            send("🔒 *VIP EDGE-FADE 7*\n\n" + vip_msg, VIP_CHANNEL)
            print(f"[EdgeFade7] Sent to VIP ({saved_count} legs saved)")

        if slip.send_to_free and free_msg:
            send(free_msg, FREE_CHANNEL)
            print("[EdgeFade7] Free preview sent")

        # Mark sent for today
        _edge_fade_sent_date = today_str
        save_status(saved_count, {"_ef7_sent_date": today_str,
                                  "_ef7_grade": slip.grade})

    except Exception as e:
        import traceback
        print(f"[EdgeFade7] ERROR: {e}")
        print(traceback.format_exc())
        try:
            send(f"⚠️ EdgeFade7 error: {e}", str(ADMIN_ID))
        except Exception:
            pass


def get_odds_cached(force=False):
    """
    Return cached odds data.  Only hits the Odds API when:
      - force=True  (game-time trigger in run())
      - cache has never been seeded (_odds_cache_hour == -1)
    All other callers (admin commands, engine) get the cache for free.
    """
    global _odds_cache, _odds_cache_hour

    if force or _odds_cache_hour == -1:
        reason = "forced game-time refresh" if force else "initial seed"
        print(f"[Odds] Fetching fresh ({reason})...")
        _odds_cache      = get_odds_full()
        _odds_cache_hour = 1   # any non-(-1) value means seeded
    else:
        print(f"[Odds] Using cached odds")

    return _odds_cache


def run():
    global _odds_game_fetch_date
    games_bdl = get_todays_games()

    # ── Early exit: all games Final — skip pick engine and odds fetch ─────────
    if games_bdl and all(g.get("status") == "post" for g in games_bdl):
        print("[run] All games Final — skipping pick engine and odds fetch")
        return 0

    # ── Game-time-aware Odds API trigger ─────────────────────────────────────
    # Uses free BDL tip-off times to decide exactly when to call the Odds API.
    #
    # Seed fetch  : once per day, 3 hours before the first game (props go live).
    # Cluster fetch: 30 min before each distinct start-time cluster.
    #   If two games tip within 30 min of each other they share one fetch.
    #   Each cluster triggers exactly one refresh regardless of how many games.
    # ─────────────────────────────────────────────────────────────────────────
    global _game_cluster_fetched
    try:
        import zoneinfo as _zi_run
        _et_run   = datetime.now(_zi_run.ZoneInfo("America/New_York"))
        _now_utc  = datetime.now(timezone.utc)
        _today_et = _et_run.strftime("%Y-%m-%d")

        # ── Collect all tip times from unified game data ─────────────────
        _tip_times = []
        for _g in games_bdl:
            # Normalized format: tip_time holds ISO timestamp for pre-game
            _raw = _g.get("tip_time", "") or _g.get("status", "")
            if "T" in str(_raw):
                try:
                    _t = datetime.fromisoformat(
                        str(_raw).replace("Z", "").split(".")[0]
                    ).replace(tzinfo=timezone.utc)
                    if _t.astimezone(_zi_run.ZoneInfo("America/New_York")).date() == _et_run.date():
                        _tip_times.append(_t)
                except Exception:
                    pass
        _tip_times.sort()

        if _tip_times:
            _first_tip = _tip_times[0]
            _mins_to_first = (_first_tip - _now_utc).total_seconds() / 60

            # ── Seed fetch: 3 hours before first tip of the day ──────────
            if _mins_to_first <= 180 and _odds_game_fetch_date.get("early") != _today_et:
                print(f"[Odds] Seed fetch — {_mins_to_first:.0f}min to first tip")
                get_odds_cached(force=True)
                get_player_props(force=True)
                _odds_game_fetch_date["early"] = _today_et

            # ── Cluster fetches: 30 min before each distinct tip cluster ─
            # Group games tipping within 30 min of each other into one cluster.
            _clusters = []
            for _t in _tip_times:
                if not _clusters or (_t - _clusters[-1]).total_seconds() > 1800:
                    _clusters.append(_t)  # new cluster anchor

            for _anchor in _clusters:
                _mins = (_anchor - _now_utc).total_seconds() / 60
                _ckey = f"{_today_et} {_anchor.strftime('%H:%M')}"
                if 0 <= _mins <= 30 and _ckey not in _game_cluster_fetched:
                    _et_label = _anchor.astimezone(
                        _zi_run.ZoneInfo("America/New_York")
                    ).strftime("%-I:%M %p ET")
                    print(f"[Odds] Cluster fetch — {_mins:.0f}min to {_et_label} tip cluster")
                    get_odds_cached(force=True)
                    get_player_props(force=True)
                    _game_cluster_fetched.add(_ckey)

            # Clear yesterday's cluster keys to avoid unbounded growth
            _game_cluster_fetched = {
                k for k in _game_cluster_fetched if k.startswith(_today_et)
            }

    except Exception as _gte:
        print(f"[Odds] Game-time trigger error: {_gte}")

    moneyline_odds, odds_games = get_odds_cached()
    picks_count = 0
    _games_in_window = False  # track if any game is within 180 min of tip

    # Build a lookup from team name to odds game object
    # Also build a normalized (lowercase last-word) index for fuzzy fallback
    odds_by_teams = {}
    _odds_fuzzy = {}   # "nuggets|jazz" → og
    for og in odds_games:
        key = f"{og['home_team']}|{og['away_team']}"
        odds_by_teams[key] = og
        hk = og['home_team'].split()[-1].lower()
        ak = og['away_team'].split()[-1].lower()
        _odds_fuzzy[f"{hk}|{ak}"] = og

    print(f"[{datetime.now()}] Analyzing {len(games_bdl)} games with {len(odds_games)} odds entries...")

    # ── DAILY INJURY BULLETIN (VIP, once per day at 8 AM ET) ────────────
    global _last_injury_bulletin
    try:
        import zoneinfo as _zi_inj
        _now_et = datetime.now(_zi_inj.ZoneInfo("America/New_York"))
    except Exception:
        _now_et = datetime.utcnow() - timedelta(hours=5)
    _injury_date_key = _now_et.strftime("%Y-%m-%d")
    if _now_et.hour >= 8 and _last_injury_bulletin != _injury_date_key:
        try:
            inj = get_espn_injuries()
            # Build set of tonight's team name keywords (e.g. "lakers", "celtics")
            today_teams = set()
            for g in games_bdl:
                for word in g.get("home_team", "").lower().split():
                    today_teams.add(word)
                for word in g.get("away_team", "").lower().split():
                    today_teams.add(word)

            lines = []
            for player_lower, info in inj.items():
                if info["status"] not in ("Out", "Doubtful", "Questionable", "Day-To-Day"):
                    continue
                player_team = info.get("team", "")
                # Only include players whose team is playing tonight
                if not any(word in player_team for word in today_teams):
                    continue
                lines.append(
                    f"• *{player_lower.title()}* — {info['status']}\n"
                    f"  _{info['comment']}_"
                )
            if lines:
                bulletin = (
                    f"🏥 *INJURY REPORT — TONIGHT'S GAMES*\n"
                    f"_Source: ESPN · {datetime.now().strftime('%b %d %H:%M')} ET_\n\n"
                    + "\n".join(lines[:30])
                )
                send(bulletin, VIP_CHANNEL)
                print(f"[ESPN] Injury bulletin sent — {len(lines)} players listed")
            else:
                print(f"[ESPN] No injuries for tonight's teams — bulletin skipped")
            _last_injury_bulletin = _injury_date_key
        except Exception as e:
            print(f"Injury bulletin error: {e}")

    # Reset pre-game pick tracker at midnight — persist so restarts don't re-send
    global _pregame_picks_sent, _pregame_picks_date
    _today_str = datetime.now().strftime("%Y-%m-%d")
    if _pregame_picks_date != _today_str:
        _status = load_status()
        if _status.get("_picks_date") == _today_str:
            _pregame_picks_sent = set(_status.get("_picks_sent", []))
        else:
            _pregame_picks_sent = set()
        _pregame_picks_date = _today_str

    _full_card = {}  # game_name -> {ml, total, spread} — consolidated at end

    for g in games_bdl:
        try:
            h = g.get("home_team", "")
            a = g.get("away_team", "")
            if not h or not a:
                continue
            game_name = f"{h} vs {a}"

            # ── TIMING: BDL first (free, always has schedule), Odds API refines ──
            tip_utc = None
            # 1) BDL status field carries ISO datetime for upcoming games
            try:
                raw_time = g.get("tip_time", "") or g.get("status", "") or ""
                if "T" in str(raw_time):
                    tip_utc = datetime.fromisoformat(
                        str(raw_time).replace("Z", "").split(".")[0]
                    ).replace(tzinfo=timezone.utc)
            except Exception:
                pass
            # 2) Odds API commence_time (more precise, use if available)
            try:
                odds_key_timing = f"{h}|{a}"
                og_timing = odds_by_teams.get(odds_key_timing)
                if not og_timing:
                    fk = f"{h.split()[-1].lower()}|{a.split()[-1].lower()}"
                    og_timing = _odds_fuzzy.get(fk)
                if og_timing and og_timing.get("commence_time"):
                    tip_utc = datetime.strptime(
                        og_timing["commence_time"], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
            except Exception:
                pass
            # 3) Compute mins to tip
            if tip_utc:
                now_utc = datetime.now(timezone.utc)
                mins_to_tip = (tip_utc - now_utc).total_seconds() / 60
            else:
                mins_to_tip = None  # no timing data — don't block

            # Skip if game already started 30+ min ago
            if mins_to_tip is not None and mins_to_tip <= -30:
                continue
            # Skip if timing is known and tip is more than 3 hours away
            if mins_to_tip is not None and mins_to_tip > 180:
                continue
            _games_in_window = True

            def _pick_key(gname, ptype):
                return f"{gname}:{ptype}"

            def _already_sent(gname, ptype):
                return _pick_key(gname, ptype) in _pregame_picks_sent

            def _mark_sent(gname, ptype):
                _pregame_picks_sent.add(_pick_key(gname, ptype))
                save_status(0, {"_picks_date": _today_str, "_picks_sent": list(_pregame_picks_sent)})

            # ── STARTING FIVE (same timing as picks) ────────────────
            run_starters_report([g], tip_utc=tip_utc)

            home_stats = team_stats(None, h)
            away_stats = team_stats(None, a)

            # ── Populate _games_data early for script detection ──────
            try:
                _espn_inj = get_espn_injuries()
                home_lower = h.lower()
                away_lower = a.lower()
                has_key_injury = any(
                    isinstance(v, dict) and
                    v.get("status", "").upper() in ("OUT", "DOUBTFUL") and
                    (home_lower in v.get("team", "").lower() or
                     away_lower in v.get("team", "").lower())
                    for v in _espn_inj.values()
                )
            except Exception:
                has_key_injury = False

            _games_data.setdefault(game_name, {}).update({
                "home_pts":        home_stats.get("pts", 105),
                "away_pts":        away_stats.get("pts", 105),
                "has_key_injury":  has_key_injury,
            })

            prob, pred_spread, pred_total = predict(
                home_stats, away_stats, h, a
            )

            # ── UPSET detection: model disagrees with Vegas favorite ──
            # Use moneyline odds — negative ML = favorite
            # model picks home if prob > 0.5, picks away if prob < 0.5
            try:
                h_ml = moneyline_odds.get(h, 0) or 0
                a_ml = moneyline_odds.get(a, 0) or 0
                if h_ml != 0 or a_ml != 0:
                    # Determine Vegas favorite: lower (more negative) ML = favorite
                    vegas_picks_home = (h_ml < a_ml) if (h_ml < 0 or a_ml < 0) else (h_ml <= a_ml)
                    model_picks_home = prob > 0.5
                    _model_disagrees = vegas_picks_home != model_picks_home
                else:
                    _model_disagrees = False
            except Exception:
                _model_disagrees = False

            _games_data.setdefault(game_name, {})["model_disagrees_with_vegas"] = _model_disagrees

            # ── Store game metadata for full card display ────────────
            # Use fuzzy lookup for moneyline odds (match on team abbr)
            def _get_ml_fuzzy(team_full_name):
                o = moneyline_odds.get(team_full_name, 0)
                if o != 0:
                    return o
                team_abbr = team_full_name.split()[-1]
                for tn, odds in moneyline_odds.items():
                    if tn and tn.split()[-1] == team_abbr:
                        return odds
                return 0
            _full_card.setdefault(game_name, {}).update({
                "home":    h,
                "away":    a,
                "h_ml":    _get_ml_fuzzy(h),
                "a_ml":    _get_ml_fuzzy(a),
                "tip_utc": tip_utc,
            })

            # ── MONEYLINE PICK ──────────────────────────────────────
            if not _already_sent(game_name, "ML"):
                winner = h if prob > 0.5 else a
                win_prob = max(prob, 1 - prob)
                # Try exact lookup first, then fallback to fuzzy last-word match
                o_ml = moneyline_odds.get(winner, 0)
                if o_ml == 0:
                    # Fallback: match on team abbr (last word of full name)
                    winner_abbr = winner.split()[-1]
                    for team_name, odds in moneyline_odds.items():
                        if team_name and team_name.split()[-1] == winner_abbr:
                            o_ml = odds
                            break
                e_ml = edge_moneyline(win_prob, o_ml)

                if o_ml != 0 and e_ml >= EDGE_THRESHOLD:
                    # ── Gate 1: 4D Game Script alignment ────────────────────
                    try:
                        from game_script import analyze_game_script as _ags_ml
                        _ml_gs  = _ags_ml(h, a,
                                          pred_total, abs(pred_spread))
                        _ml_flow = _ml_gs.flow
                        _ml_pace = _ml_gs.pace
                        _ml_script_label = _ml_gs.label
                    except Exception:
                        _ml_flow = "COMPETITIVE"; _ml_pace = "AVERAGE_PACE"
                        _ml_script_label = detect_game_script(_games_data.get(game_name, {}))

                    _ml_ok = True

                    # ── Flow checks ──────────────────────────────────────────
                    # BLOWOUT expected but model confidence too low → mismatch
                    if _ml_flow in ("BLOWOUT", "DOUBLE_DIGIT_LEAD") and win_prob < 0.62:
                        _ml_ok = False
                        print(f"  [Script:{_ml_flow}] ML skip — win_prob {round(win_prob*100,1)}% too low for blowout")
                    # TIGHT_GAME but model has a big spread → mismatch
                    if _ml_flow == "TIGHT_GAME" and abs(pred_spread) > 8.0:
                        _ml_ok = False
                        print(f"  [Script:TIGHT_GAME] ML skip — spread {round(abs(pred_spread),1)} too wide for tight game")

                    # ── Pace checks ──────────────────────────────────────────
                    # TRANSITION_HEAVY: run-and-gun = more variance, chalk is riskier
                    if _ml_pace in ("TRANSITION_HEAVY", "UPTEMPO") and win_prob < 0.60:
                        _ml_ok = False
                        print(f"  [Script:{_ml_pace}] ML skip — {round(win_prob*100,1)}% not strong enough in fast game")
                    # HALFCOURT: defensive battle = tighter results, need higher certainty
                    if _ml_pace in ("HALFCOURT", "SLOW_PACED") and win_prob < 0.58:
                        _ml_ok = False
                        print(f"  [Script:{_ml_pace}] ML skip — {round(win_prob*100,1)}% not strong enough in halfcourt game")

                    # ── Gate 2: EV gate ─────────────────────────────────────
                    if _ml_ok and o_ml != 0:
                        try:
                            from decision_engine import implied_probability as _ip_ml
                            _ml_implied  = _ip_ml(o_ml)
                            _min_ev_gate = load_learning_data().get("min_ev_threshold", 0.0)
                            if win_prob < _ml_implied + _min_ev_gate:
                                _ml_ok = False
                                print(f"  [EV-Gate] ML skip — model {round(win_prob*100,1)}% ≤ implied {round(_ml_implied*100,1)}%")
                        except Exception:
                            pass  # fail-open: never block a pick due to import error

                    if _ml_ok:
                        bet = bet_size(win_prob, o_ml)
                        ml_conf = round(win_prob * 100, 1)
                        ml_tier, ml_units = assign_tier(ml_conf)
                        try:
                            from decision_engine import kelly_units as _kelly_units
                            _k_units = _kelly_units(win_prob, o_ml)
                        except Exception:
                            _k_units = "1u"
                        saved = save_bet({
                            "game":       game_name,
                            "player":     "",
                            "pick":       winner,
                            "betType":    "MONEYLINE",
                            "line":       None,
                            "prediction": round(pred_spread, 1),
                            "odds":       o_ml,
                            "prob":       round(win_prob, 4),
                            "edge":       round(e_ml, 4),
                            "confidence": ml_conf,
                            "tier":       ml_tier,
                            "units":      _k_units,
                            "time":       str(datetime.now()),
                            "result":     None,
                            "script":     _ml_script_label,
                            "game_total": _games_data.get(game_name, {}).get("total"),
                            "game_spread": _games_data.get(game_name, {}).get("spread"),
                            "game_pace":  _ml_pace,
                            "game_phase": "pregame",
                        })
                        if saved:
                            edge_pct = round(e_ml * 100, 1) if o_ml != 0 else None
                            teaser = (
                                f"🏀 *TONIGHT'S PICK*\n"
                                f"_{a} @ {h}_\n\n"
                                f"🔥 *{winner}* ML\n\n"
                                f"🔒 _Full breakdown, edge %, O/U + Starting Five in VIP_\n"
                                f"👉 {CHECKOUT_URL}"
                            )
                            send(teaser, FREE_CHANNEL)
                            _full_card.setdefault(game_name, {})["ml"] = {
                                "winner":   winner,
                                "odds":     o_ml,
                                "win_prob": round(win_prob * 100, 1),
                                "edge_pct": edge_pct,
                                "tier":     ml_tier,
                                "bet":      bet,
                            }
                            _mark_sent(game_name, "ML")
                            picks_count += 1
                            print(f"  ML Pick: {winner} (edge {round(e_ml*100,1)}%)")

            # ── TOTALS + SPREADS from Odds API ───────────────────────
            odds_key = f"{h}|{a}"
            odds_game = odds_by_teams.get(odds_key)

            if odds_game:
                _og_bks = odds_game.get("bookmakers", [])
                _og_bk = next((b for b in _og_bks if b.get("key") == "fanduel"), None)
                for bk in ([_og_bk] if _og_bk else []):
                    for market in bk.get("markets", []):

                        # TOTALS
                        if market["key"] == "totals" and not _already_sent(game_name, "TOTAL"):
                            try:
                                vegas_total = float(market["outcomes"][0]["point"])
                                diff = pred_total - vegas_total
                                _total_thresh = load_learning_data().get("total_edge_threshold", TOTALS_EDGE_THRESHOLD)
                                if abs(diff) >= _total_thresh:
                                    pick_side = "OVER" if diff > 0 else "UNDER"

                                    # ── Gate 1: 4D Game Script (upgraded from 1D) ─────────
                                    try:
                                        from game_script import analyze_game_script as _ags_tot
                                        _tot_gs   = _ags_tot(h, a,
                                                             vegas_total, abs(pred_spread))
                                        _tot_pace = _tot_gs.pace
                                        _tot_flow = _tot_gs.flow
                                        _tot_label = _tot_gs.label
                                    except Exception:
                                        _tot_pace  = "AVERAGE_PACE"; _tot_flow = "COMPETITIVE"
                                        _tot_label = detect_game_script(
                                            _games_data.get(game_name, {"total": vegas_total}))
                                    _tot_block = False
                                    # HALFCOURT pace (low-scoring expected) → OVERs don't fit
                                    if _tot_pace in ("HALFCOURT", "SLOW_PACED") and pick_side == "OVER":
                                        _tot_block = True
                                        print(f"  [4D:{_tot_label}] Total OVER skip — halfcourt game")
                                    # TRANSITION_HEAVY pace (high-scoring expected) → UNDERs don't fit
                                    if _tot_pace in ("TRANSITION_HEAVY", "UPTEMPO") and pick_side == "UNDER":
                                        _tot_block = True
                                        print(f"  [4D:{_tot_label}] Total UNDER skip — fast pace game")
                                    # BLOWOUT flow + OVER is risky (garbage time = fewer meaningful buckets)
                                    if _tot_flow == "BLOWOUT" and pick_side == "OVER":
                                        _tot_block = True
                                        print(f"  [4D:{_tot_label}] Total OVER skip — BLOWOUT game (garbage time risk)")
                                    if _tot_block:
                                        _games_data.setdefault(game_name, {})["total"] = vegas_total
                                        continue  # type: ignore

                                    # ── Gate 1b: Line Movement check ───────────────────
                                    _tot_move = track_line_movement(
                                        f"{game_name}:TOTAL", vegas_total
                                    )
                                    _tot_sharp = ""
                                    # Line steamed UP but we're picking UNDER → sharp
                                    # money went the other way, exit
                                    if _tot_move >= 1.5 and pick_side == "UNDER":
                                        print(f"  [LineMove] Total skip — line rose {_tot_move:+.1f} pts, sharp money on OVER")
                                        continue  # type: ignore
                                    # Line steamed DOWN but we're picking OVER → exit
                                    if _tot_move <= -1.5 and pick_side == "OVER":
                                        print(f"  [LineMove] Total skip — line dropped {_tot_move:+.1f} pts, sharp money on UNDER")
                                        continue  # type: ignore
                                    # Movement confirms our pick → note it
                                    if _tot_move >= 1.5 and pick_side == "OVER":
                                        _tot_sharp = "📈 Sharp money confirming OVER"
                                    elif _tot_move <= -1.5 and pick_side == "UNDER":
                                        _tot_sharp = "📉 Sharp money confirming UNDER"

                                    # ── Gate 2: EV gate on actual totals odds ──────────
                                    _tot_implied = 0.5
                                    _tot_odds    = -110
                                    try:
                                        _tot_odds_out = next(
                                            (o for o in market["outcomes"]
                                             if o.get("name", "").lower() == pick_side.lower()), None)
                                        _tot_odds = float(_tot_odds_out["price"]) if _tot_odds_out else -110
                                        from decision_engine import implied_probability as _ip_tot
                                        _tot_implied  = _ip_tot(_tot_odds)
                                        _tot_model_prob = _norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD) if pick_side == "OVER" else 1.0 - _norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD)
                                        _min_ev_g = load_learning_data().get("min_ev_threshold", 0.0)
                                        if _tot_model_prob < _tot_implied + _min_ev_g:
                                            print(f"  [EV-Gate] Total {pick_side} skip — model {round(_tot_model_prob*100,1)}% ≤ implied {round(_tot_implied*100,1)}%")
                                            _games_data.setdefault(game_name, {})["total"] = vegas_total
                                            continue  # type: ignore
                                    except Exception:
                                        _tot_label = _tot_label if "_" in _tot_label else detect_game_script(
                                            _games_data.get(game_name, {"total": vegas_total}))

                                    tot_conf  = calibrated_confidence("TOTAL", round(min(55 + abs(diff) * 5, 90), 1))
                                    tot_tier, tot_units = assign_tier(tot_conf)
                                    saved = save_bet({
                                        "game":       game_name,
                                        "player":     "",
                                        "pick":       f"{pick_side} {vegas_total}",
                                        "betType":    "TOTAL",
                                        "line":       vegas_total,
                                        "prediction": round(pred_total, 1),
                                        "odds":       _tot_odds,
                                        "prob":       round(_norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD) if pick_side == "OVER" else 1.0 - _norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD), 4),
                                        "edge":       round(((_norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD) if pick_side == "OVER" else 1.0 - _norm_sf(vegas_total, pred_total, _NBA_TOTAL_STD)) - _tot_implied), 4),
                                        "confidence": tot_conf,
                                        "tier":       tot_tier,
                                        "time":       str(datetime.now()),
                                        "result":     None,
                                        "script":     _tot_label,
                                        "game_total": vegas_total,
                                        "game_spread": _games_data.get(game_name, {}).get("spread"),
                                        "game_pace":  _tot_pace,
                                        "game_phase": "pregame",
                                    })
                                    if saved:
                                        _full_card.setdefault(game_name, {})["total"] = {
                                            "side":       pick_side,
                                            "line":       vegas_total,
                                            "model":      round(pred_total, 1),
                                            "edge":       round(abs(diff), 1),
                                            "sharp":      _tot_sharp or "",
                                        }
                                        _mark_sent(game_name, "TOTAL")
                                        picks_count += 1
                                        _games_data.setdefault(game_name, {}).update({
                                            "total":  vegas_total,
                                            "spread": abs(pred_spread - (vegas_total / 2)),
                                        })
                                        _todays_parlay_legs.append({
                                            "desc":        f"GAME TOTAL {pick_side} {vegas_total}",
                                            "game":        game_name,
                                            "bet_type":    "TOTAL",
                                            "edge":        round(abs(diff), 2),
                                            "confidence":  round(min(55 + abs(diff) * 5, 90), 1),
                                            "correlation": pick_side,   # "OVER" or "UNDER"
                                            "team":        None,        # applies to both teams
                                        })
                                        print(f"  {pick_side} {vegas_total} (model {round(pred_total,1)})")
                            except Exception as te:
                                print(f"  Totals error: {te}")

                        # SPREADS
                        elif market["key"] == "spreads" and not _already_sent(game_name, "SPREAD"):
                            try:
                                home_outcome = next(
                                    (o for o in market["outcomes"] if o["name"] == h), None
                                )
                                if home_outcome:
                                    vegas_spread = float(home_outcome["point"])
                                    diff = pred_spread - vegas_spread
                                    _spread_thresh = load_learning_data().get("spread_edge_threshold", SPREAD_EDGE_THRESHOLD)
                                    if abs(diff) >= _spread_thresh:
                                        cover_team = h if diff > 0 else a
                                        spread_str = f"{vegas_spread:+.1f}" if vegas_spread >= 0 else f"{vegas_spread:.1f}"

                                        # ── Script-aware detection ───
                                        _games_data.setdefault(game_name, {}).update({
                                            "spread":         abs(vegas_spread),
                                            "raw_spread":     vegas_spread,
                                        })
                                        _spr_script = detect_game_script(_games_data[game_name])

                                        # UPSET: model picks favourite → swap to underdog
                                        if _spr_script == "UPSET":
                                            is_home_fav = vegas_spread < 0
                                            model_picks_home = diff > 0
                                            model_picks_fav  = (is_home_fav and model_picks_home) or \
                                                               (not is_home_fav and not model_picks_home)
                                            if model_picks_fav:
                                                # Flip to underdog
                                                cover_team = a if is_home_fav else h
                                                underdog_spread = -vegas_spread
                                                spread_str = f"{underdog_spread:+.1f}"
                                                print(f"  [Script:UPSET] Flipped to underdog {cover_team} {spread_str}")

                                        # ── Gate 1: 4D Game Script alignment ──────────────
                                        try:
                                            from game_script import analyze_game_script as _ags_spr
                                            _spr_gs   = _ags_spr(h, a,
                                                                  _games_data.get(game_name, {}).get("total", 220),
                                                                  abs(vegas_spread))
                                            _spr_flow  = _spr_gs.flow
                                            _spr_label = _spr_gs.label
                                        except Exception:
                                            _spr_flow  = "COMPETITIVE"
                                            _spr_label = _spr_script

                                        _spr_ok = True
                                        _spr_pace = _spr_gs.pace if hasattr(_spr_gs, "pace") else "AVERAGE_PACE"

                                        # ── Flow checks ────────────────────────────────────
                                        # Big spread in a TIGHT_GAME → games expected close
                                        if _spr_flow in ("TIGHT_GAME", "COMPETITIVE") and abs(vegas_spread) > 6.5:
                                            _spr_ok = False
                                            print(f"  [4D:{_spr_label}] Spread skip — {abs(vegas_spread)} pts too wide for tight game")
                                        # Tiny spread in a BLOWOUT → wrong direction
                                        if _spr_flow in ("BLOWOUT", "DOUBLE_DIGIT_LEAD") and abs(vegas_spread) < 4.0:
                                            _spr_ok = False
                                            print(f"  [4D:{_spr_label}] Spread skip — {abs(vegas_spread)} pts too tight for blowout game")

                                        # ── Pace checks ────────────────────────────────────
                                        # HALFCOURT: defensive battle → tiny spreads are coin flips, skip
                                        if _spr_pace in ("HALFCOURT", "SLOW_PACED") and abs(vegas_spread) < 2.5:
                                            _spr_ok = False
                                            print(f"  [4D:{_spr_label}] Spread skip — halfcourt game, {abs(vegas_spread)} pt spread is a coin flip")
                                        # TRANSITION_HEAVY: run-and-gun → large spreads shrink, underdogs hang around
                                        if _spr_pace in ("TRANSITION_HEAVY", "UPTEMPO") and abs(vegas_spread) > 9.0:
                                            _spr_ok = False
                                            print(f"  [4D:{_spr_label}] Spread skip — fast pace, {abs(vegas_spread)} pt spread too wide (underdogs hang in open games)")

                                        if not _spr_ok:
                                            continue  # type: ignore

                                        # ── Gate 1c: Line Movement check ─────────────────
                                        _spr_move = track_line_movement(
                                            f"{game_name}:SPREAD", vegas_spread
                                        )
                                        _spr_sharp = ""
                                        # Spread moved AGAINST cover team → sharp money
                                        # fading them, skip
                                        is_home_cover = (cover_team == h)
                                        # Home spread moving more negative = books making home bigger fav
                                        # If we're on home and spread moved away (less negative/more positive) → fade
                                        if is_home_cover and _spr_move >= 1.0:
                                            print(f"  [LineMove] Spread skip — home line moved {_spr_move:+.1f}, sharp money fading {cover_team}")
                                            continue  # type: ignore
                                        if not is_home_cover and _spr_move <= -1.0:
                                            print(f"  [LineMove] Spread skip — away line moved {_spr_move:+.1f}, sharp money fading {cover_team}")
                                            continue  # type: ignore
                                        # Movement confirms cover team → note it
                                        if is_home_cover and _spr_move <= -1.0:
                                            _spr_sharp = f"📈 Sharp money confirming {cover_team}"
                                        elif not is_home_cover and _spr_move >= 1.0:
                                            _spr_sharp = f"📈 Sharp money confirming {cover_team}"

                                        # ── Gate 2: EV gate on spread odds ───────────────
                                        _spr_implied = 0.5
                                        _spr_odds    = -110
                                        try:
                                            _spr_odds_out = next(
                                                (o for o in market["outcomes"]
                                                 if o.get("name") == cover_team), None)
                                            _spr_odds = float(_spr_odds_out["price"]) if _spr_odds_out else -110
                                            from decision_engine import implied_probability as _ip_spr
                                            _spr_implied   = _ip_spr(_spr_odds)
                                            _spr_model_prob = _norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD) if is_home_cover else 1.0 - _norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD)
                                            _min_ev_s = load_learning_data().get("min_ev_threshold", 0.0)
                                            if _spr_model_prob < _spr_implied + _min_ev_s:
                                                print(f"  [EV-Gate] Spread skip — model {round(_spr_model_prob*100,1)}% ≤ implied {round(_spr_implied*100,1)}%")
                                                continue  # type: ignore
                                        except Exception:
                                            pass  # fail-open

                                        spr_conf   = calibrated_confidence("SPREAD", round(min(55 + abs(diff) * 5, 90), 1))
                                        spr_tier, spr_units = assign_tier(spr_conf)
                                        saved = save_bet({
                                            "game":       game_name,
                                            "player":     "",
                                            "pick":       f"{cover_team} SPREAD {spread_str}",
                                            "betType":    "SPREAD",
                                            "line":       vegas_spread,
                                            "prediction": round(pred_spread, 1),
                                            "odds":       _spr_odds,
                                            "prob":       round(_norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD) if is_home_cover else 1.0 - _norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD), 4),
                                            "edge":       round((_norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD) if is_home_cover else 1.0 - _norm_sf(-vegas_spread, pred_spread, _NBA_SPREAD_STD)) - _spr_implied, 4),
                                            "confidence": spr_conf,
                                            "tier":       spr_tier,
                                            "time":       str(datetime.now()),
                                            "result":     None,
                                            "script":     _spr_label,
                                            "game_total": _games_data.get(game_name, {}).get("total"),
                                            "game_spread": abs(vegas_spread),
                                            "game_pace":  _spr_pace,
                                            "game_phase": "pregame",
                                        })
                                        if saved:
                                            other_team = a if cover_team == h else h
                                            opp_spread = f"+{abs(vegas_spread)}" if vegas_spread < 0 else f"-{abs(vegas_spread)}"
                                            _full_card.setdefault(game_name, {})["spread"] = {
                                                "cover_team":  cover_team,
                                                "spread_str":  spread_str,
                                                "other_team":  other_team,
                                                "opp_spread":  opp_spread,
                                                "model":       round(pred_spread, 1),
                                                "edge":        round(abs(diff), 1),
                                                "sharp":       _spr_sharp or "",
                                            }
                                            _mark_sent(game_name, "SPREAD")
                                            picks_count += 1
                                            _games_data.setdefault(game_name, {})["spread"] = abs(vegas_spread)
                                            is_fav = (diff > 0 and vegas_spread < 0) or (diff < 0 and vegas_spread > 0)
                                            _todays_parlay_legs.append({
                                                "desc":        f"SPREAD {cover_team} {spread_str}",
                                                "game":        game_name,
                                                "bet_type":    "SPREAD",
                                                "edge":        round(abs(diff), 2),
                                                "confidence":  round(min(55 + abs(diff) * 5, 90), 1),
                                                "correlation": "NEUTRAL",
                                                "team":        cover_team,
                                                "team_role":   "favorite" if is_fav else "underdog",
                                            })
                                            print(f"  SPREAD: {cover_team} vs {vegas_spread} (model {round(pred_spread,1)})")
                            except Exception as se:
                                print(f"  Spreads error: {se}")
            time.sleep(120)  # 2-min gap between game bursts

        except Exception as err:
            print(f"Error processing game: {err}")

    # ── CONSOLIDATED FULL CARD — sportsbook style ─────────────────────────
    global _full_card_sent_today
    _card_today = datetime.now(ET).strftime("%Y-%m-%d")
    if _full_card and _full_card_sent_today == _card_today:
        print(f"[FullCard] Already sent today ({_card_today}) — skipping repeat")
        _full_card = {}   # clear so the block below is skipped cleanly

    # ── Backfill picks from today's saved bets so the card is never blank ──
    # When picks were already sent (marked via _mark_sent), they won't re-add
    # their data to _full_card.  Pull them from the bets table and fill in
    # the ml/tot/spr subdicts for any game entry that is still missing them.
    if _full_card:
        try:
            _bf_conn = _db_conn()
            if _bf_conn:
                _bf_cur = _bf_conn.cursor()
                _bf_cur.execute("""
                    SELECT game, pick, bet_type, odds, prob, confidence, script,
                           game_total, game_spread
                    FROM bets
                    WHERE DATE(bet_time AT TIME ZONE 'America/New_York') = %s
                      AND bet_type IN ('ML', 'MONEYLINE', 'TOTAL', 'SPREAD', 'OVER', 'UNDER')
                """, (_card_today,))
                _bf_rows = _bf_cur.fetchall()
                _bf_cur.close(); _bf_conn.close()

                for (_bg, _bpick, _btype, _bodds, _bprob,
                     _bconf, _bscript, _btotal, _bspread) in _bf_rows:
                    # Match bet game name to _full_card key
                    _fc_key = next(
                        (k for k in _full_card if k == _bg or
                         _bg.split(" vs ")[0] in k or _bg.split(" vs ")[-1] in k),
                        None
                    )
                    if not _fc_key:
                        continue
                    entry = _full_card[_fc_key]

                    if _btype in ("ML", "MONEYLINE") and not entry.get("ml"):
                        entry["ml"] = {
                            "winner":   _bpick,
                            "odds":     _bodds or 0,
                            "win_prob": round(float(_bprob or 0) * 100, 1),
                            "edge_pct": None,
                            "tier":     "STRONG" if (_bconf or 0) >= 75 else "LEAN",
                            "bet":      None,
                        }
                    elif _btype in ("TOTAL", "OVER", "UNDER") and not entry.get("total"):
                        _side = "OVER" if _btype in ("TOTAL", "OVER") else "UNDER"
                        entry["total"] = {
                            "side":   _side,
                            "line":   _btotal or "",
                            "model":  f"{'+' if _side == 'OVER' else ''}{_bspread or ''}",
                            "sharp":  "",
                        }
                    elif _btype == "SPREAD" and not entry.get("spread"):
                        entry["spread"] = {
                            "cover_team":  _bpick,
                            "spread_str":  f"{_bspread:+.1f}" if _bspread else "",
                            "opp_spread":  "",
                            "edge":        "",
                        }
        except Exception as _bf_e:
            print(f"[FullCard] backfill error: {_bf_e}")

    if _full_card:
        _full_card_sent_today = _card_today
        D = "━━━━━━━━━━━━━━━━━━━━━━━"

        # Only include games where at least one pick was generated
        _picks_games = {g: i for g, i in _full_card.items()
                        if i.get("ml") or i.get("total") or i.get("spread")}

        if _picks_games:
            card_lines = ["🔒 *VIP LOCK — TONIGHT'S CARD*", ""]
            for gname, info in _picks_games.items():
                home   = info.get("home", "Home")
                away   = info.get("away", "Away")
                h_ml_v = info.get("h_ml", 0)
                a_ml_v = info.get("a_ml", 0)
                tip    = info.get("tip_utc")
                ml     = info.get("ml")
                tot    = info.get("total")
                spr    = info.get("spread")

                h_short = home.split()[-1]
                a_short = away.split()[-1]

                # Game time in ET
                time_str = ""
                if tip:
                    try:
                        import pytz as _ptz
                        _et = tip.astimezone(_ptz.timezone("America/New_York"))
                        time_str = _et.strftime("%-I:%M%p ET")
                    except Exception:
                        pass

                # ML odds strings for display
                h_ml_s = f"{h_ml_v:+.0f}" if h_ml_v else ""
                a_ml_s = f"{a_ml_v:+.0f}" if a_ml_v else ""

                # ── Section header ───────────────────────────────────
                time_part = f"  ·  {time_str}" if time_str else ""
                card_lines.append(D)
                card_lines.append(f"🏀 *{a_short} @ {h_short}*{time_part}")
                card_lines.append("")

                # ── Picks only ───────────────────────────────────────
                if ml:
                    w = ml["winner"].split()[-1]
                    w_odds = f"{ml['odds']:+.0f}" if ml.get("odds") else ""
                    opp_odds = (a_ml_s if w == h_short else h_ml_s)
                    opp_str = f"  ·  opp {opp_odds}" if opp_odds else ""
                    prob_str = f"  ·  {ml['win_prob']}%" if ml.get("win_prob") else ""
                    edge_str = f"  ·  edge {ml['edge_pct']}%" if ml.get("edge_pct") else ""
                    card_lines.append(
                        f"💰 *{w}* ML {w_odds}{opp_str}{prob_str}{edge_str}"
                    )
                if spr:
                    card_lines.append(
                        f"📉 *{spr['cover_team'].split()[-1]} {spr['spread_str']}* to cover"
                        f"  ·  Edge {spr['edge']} pts"
                    )
                if tot:
                    sharp_str = f"  ·  {tot['sharp']}" if tot.get("sharp") else ""
                    card_lines.append(
                        f"🎯 *{tot['side']} {tot['line']}*"
                        f"  ·  Model {tot['model']}{sharp_str}"
                    )
                card_lines.append("")

            card_lines += [D, f"⚡ _{len(_picks_games)} play{'s' if len(_picks_games) != 1 else ''} cleared edge threshold_"]
            send("\n".join(card_lines), VIP_CHANNEL)
            print(f"[FullCard] Sent consolidated card — {len(_picks_games)} picks from {len(_full_card)} games")
        else:
            print(f"[FullCard] No picks cleared threshold tonight — card suppressed")

    # ── PLAYER PROP EDGES — fired by _fire_prop_wave() at tip-2h, not here ──────
    # Props are precision-timed: the main loop calls _fire_prop_wave() once per day
    # at exactly 2 hours before the earliest tip-off, pulling a fresh FanDuel batch.
    # Skipping the old _games_in_window gate to avoid early/empty fetches.
    save_status(picks_count)
    return picks_count
    player_picks = run_full_system()  # kept for direct calls only (unreachable via run())
    from decision_engine import implied_probability as _ip_prop
    for pk in player_picks:
        confidence  = pk.get("confidence", 0)
        prop_type   = pk.get("prop_type", "props")
        _pk_game    = pk.get("game", pk.get("player", ""))  # "game" always present since fix
        _pk_gdata   = _games_data.get(_pk_game, {})
        _prop_tier, _ = assign_tier(confidence)
        # ── Real probability + edge for props ─────────────────────────────────
        _pk_odds    = pk.get("odds", -115)
        _pk_line    = pk.get("line") or 0
        _pk_pred    = pk.get("prediction") or _pk_line
        _pk_std     = _PROP_STD.get(prop_type, 5.0)
        _pk_sf      = _norm_sf(_pk_line, _pk_pred, _pk_std)
        _pk_prob    = round(_pk_sf if pk.get("pick", "OVER").upper() == "OVER" else 1.0 - _pk_sf, 4)
        _pk_implied = _ip_prop(_pk_odds)
        _pk_edge    = round(_pk_prob - _pk_implied, 4)
        # ── Already saved to DB by run_full_system() — only track for parlay builder ──
        picks_count += 1
        _todays_parlay_legs.append({
            "desc":        f"{pk.get('player')} {pk.get('pick','OVER')} {_pk_line} {pk.get('prop_type','pts')}",
            "game":        _pk_game,
            "bet_type":    pk.get("prop_type", "PROP"),
            "edge":        _pk_edge,
            "confidence":  confidence,
            "correlation": pk.get("pick", "OVER").upper(),
            "team":        pk.get("team"),
            "team_role":   pk.get("role", "unknown"),
            "is_starter":  pk.get("is_starter", True),
            "avg_mins":    pk.get("avg_mins", 30),
        })

    save_status(picks_count)
    return picks_count


# ==========================
# 🚫 AVOID LIST
# ==========================
def send_admin_dm(force=False):
    """Removed — briefing replaced by /admins system health panel."""
    pass




def send_avoid_list():
    """Send a nightly list of overpriced players + injury risks to VIP. Once per day."""
    global _avoid_sent_date
    import zoneinfo as _zi
    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    today_str = et_now.strftime("%Y-%m-%d")
    if _avoid_sent_date == today_str:
        return

    # Only fire between 2 PM and 8 PM ET (before evening games)
    if not (14 <= et_now.hour < 20):
        return

    # Need at least one game within 3 hours
    try:
        games = get_todays_games()
        if not games:
            return
        upcoming = [g for g in games if g.get("status") == "pre"]
        if not upcoming:
            return
    except Exception:
        return

    PRED_KEY = {
        "points":   "pred_pts",
        "rebounds": "pred_reb",
        "assists":  "pred_ast",
        "threes":   "pred_fg3",
    }
    UNIT = {"points": "pts", "rebounds": "reb", "assists": "ast", "threes": "3s"}
    AVOID_THRESHOLD = 4.0   # model must disagree by this many units to flag

    avoid_players  = []   # (player, team, prop_type, line, model_pred, gap)
    injury_flags   = []   # (player, status)

    # ── Pull injury list ───────────────────────────────────────────
    try:
        injuries = get_espn_injuries()
    except Exception:
        injuries = {}

    # Flag questionable / doubtful players from tonight's games
    game_teams = set()
    for g in upcoming:
        game_teams.add((g.get("home_team") or "").lower())
        game_teams.add((g.get("away_team") or "").lower())

    for player_name, info in injuries.items():
        status = info.get("status", "")
        team   = info.get("team", "")
        if status in ("Out", "Doubtful", "Questionable", "Day-To-Day"):
            injury_flags.append((player_name.title(), team, status))

    # ── Pull prop lines and find overpriced players ────────────────
    try:
        odds_data = get_player_props()
        if odds_data:
            props = extract_props(odds_data)
            # Group by player
            by_player = {}
            for p in props:
                player = p.get("player")
                if player:
                    by_player.setdefault(player, []).append(p)

            checked = 0
            for player, pprops in list(by_player.items())[:30]:
                try:
                    time.sleep(1)
                    stats = get_player_stats(player)
                    if not stats:
                        continue
                    # Skip injured/out players
                    inj = injuries.get(player.lower(), {})
                    if inj.get("status") in ("Out", "Doubtful"):
                        continue
                    team = stats.get("team", "")
                    for prop in pprops:
                        prop_type = prop.get("prop_type", "points")
                        line = prop.get("line")
                        if not line:
                            continue
                        pred_key = PRED_KEY.get(prop_type, "pred_pts")
                        prediction = float(stats.get(pred_key) or 0)
                        gap = float(line) - prediction   # positive = Vegas has player OVER model
                        # Flag when Vegas line is significantly higher than model
                        if gap >= AVOID_THRESHOLD:
                            unit = UNIT.get(prop_type, prop_type)
                            avoid_players.append({
                                "player":     player,
                                "team":       team,
                                "prop_type":  prop_type,
                                "line":       line,
                                "prediction": round(prediction, 1),
                                "gap":        round(gap, 1),
                                "unit":       unit,
                            })
                    checked += 1
                except Exception:
                    continue
            print(f"[Avoid] Checked {checked} players, {len(avoid_players)} flagged")
    except Exception as e:
        print(f"[Avoid] Props error: {e}")

    # Nothing to send
    if not avoid_players and not injury_flags:
        return

    # ── Build message ──────────────────────────────────────────────
    D = "━━━━━━━━━━━━━━━━━━━"
    date_str = et_now.strftime("%B %d, %Y")

    lines = [
        f"🚫 *PLAYERS TO AVOID TONIGHT*\n_{date_str}_",
        D,
    ]

    if avoid_players:
        # Sort by gap descending — biggest mismatch first
        avoid_players.sort(key=lambda x: x["gap"], reverse=True)
        lines.append("*❌ OVERPRICED BY VEGAS — Fade these in your SGPs:*\n")
        for p in avoid_players[:6]:
            lines.append(
                f"❌ *{p['player']}*  _({p['team']})_\n"
                f"   Line: OVER {p['line']} {p['unit']}  —  Model says *{p['prediction']}*\n"
                f"   Vegas overestimates by *{p['gap']} {p['unit']}* — fade him"
            )

    if injury_flags:
        lines.append(f"\n{D}")
        lines.append("*⚠️ INJURY RISKS TONIGHT:*\n")
        STATUS_ICON = {
            "Out": "🔴", "Doubtful": "🔴",
            "Questionable": "🟡", "Day-To-Day": "🟡",
        }
        for name, team, status in injury_flags[:8]:
            icon = STATUS_ICON.get(status, "🟡")
            lines.append(f"{icon} *{name}*  _({team})_ — {status}")

    lines += [
        D,
        "_Build your SGPs around tonight's VIP picks, not these players._",
    ]

    msg = "\n".join(lines)
    send(msg, VIP_CHANNEL)
    _avoid_sent_date = today_str
    print(f"[Avoid] Sent — {len(avoid_players)} avoid, {len(injury_flags)} injuries")


# ==========================
# 📊 DAILY SYSTEM (VIP LOCK + Auto Parlay Builder)
# ==========================
_system_sent_date  = None
_prop_wave_fired   = None   # ET date string — prop wave fires once at tip-2h


def _parlay_odds(n_legs, decimal_per_leg=1.909):
    """Approximate American odds for an n-leg parlay at -110 per leg."""
    combined = decimal_per_leg ** n_legs
    american = int((combined - 1) * 100)
    return american


def _should_fire_prop_wave() -> bool:
    """
    Returns True exactly once per day when ET time >= earliest tip-off minus 2 hours.
    Uses the schedule cache (already populated by send_daily_system) — no extra API call.
    """
    global _prop_wave_fired
    import zoneinfo as _zi
    try:
        et_now = datetime.now(_zi.ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.utcnow()

    today_str = et_now.strftime("%Y-%m-%d")
    if _prop_wave_fired == today_str:
        return False

    # Ensure schedule cache is fresh
    if _schedule_cache.get("date") != today_str:
        _refresh_schedule_cache()

    start = _schedule_cache.get("window_start")  # already = earliest_tip - 3h
    if not start:
        return False

    # Fire at earliest tip-off minus 2 hours (1 hour into the existing 3h window)
    fire_at = start + timedelta(hours=1)
    if et_now >= fire_at:
        print(f"[PropWave] Triggered at {et_now.strftime('%-I:%M %p ET')} (fire_at={fire_at.strftime('%-I:%M %p ET')})")
        return True
    return False


def _fire_prop_wave():
    """
    Precision-timed prop wave — fires once per day at tip-2h.
    1. Force-refreshes player prop lines from FanDuel (all games in one batch).
    2. Scores props via run_full_system() → picks saved to DB + _todays_parlay_legs.
    3. Sends per-game Elite Game Lines and SGPs.
    4. Sends CGP from the full combined pool.
    """
    global _prop_wave_fired
    import zoneinfo as _zi
    try:
        today_str = datetime.now(_zi.ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        today_str = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"[PropWave] Firing prop wave for {today_str} — force-fetching all game props...")

    # Force refresh the prop cache for all tonight's games at once
    get_player_props(force=True)

    # Score and send props (saves to DB + populates _todays_parlay_legs)
    try:
        player_picks = run_full_system()
        from decision_engine import implied_probability as _ip_pw
        for pk in player_picks:
            confidence = pk.get("confidence", 0)
            prop_type  = pk.get("prop_type", "props")
            _pk_game   = pk.get("game", pk.get("player", ""))
            _pk_odds   = pk.get("odds", -115)
            _pk_line   = pk.get("line") or 0
            _pk_pred   = pk.get("prediction") or _pk_line
            _pk_std    = _PROP_STD.get(prop_type, 5.0)
            _pk_sf     = _norm_sf(_pk_line, _pk_pred, _pk_std)
            _pk_prob   = round(_pk_sf if pk.get("pick", "OVER").upper() == "OVER" else 1.0 - _pk_sf, 4)
            _todays_parlay_legs.append({
                "desc":        f"{pk.get('player')} {pk.get('pick','OVER')} {_pk_line} {prop_type}",
                "player":      pk.get("player", ""),
                "game":        _pk_game,
                "bet_type":    prop_type,
                "line":        _pk_line,
                "odds":        _pk_odds,
                "pick":        pk.get("pick", "OVER").upper(),
                "edge":        round(_pk_prob - _ip_pw(_pk_odds), 4),
                "confidence":  confidence,
                "correlation": pk.get("pick", "OVER").upper(),
                "team":        pk.get("team"),
                "team_role":   pk.get("role", "unknown"),
                "position":    pk.get("position", ""),
                "is_starter":  pk.get("is_starter", True),
                "avg_mins":    pk.get("avg_mins", 30),
            })
        print(f"[PropWave] run_full_system returned {len(player_picks)} picks")
    except Exception as _pw_err:
        print(f"[PropWave] run_full_system error: {_pw_err}")

    # Send CGP from full pool
    try:
        send_cgp()
    except Exception as _cgp_err:
        print(f"[PropWave] CGP error: {_cgp_err}")

    # Send per-game Elite Game Lines + SGP
    try:
        by_game: dict = {}
        for leg in _todays_parlay_legs:
            g = leg.get("game", "")
            if g:
                by_game.setdefault(g, []).append(leg)
        for game_name, game_legs in by_game.items():
            if len(game_legs) >= 2:
                try:
                    send_elite_player_props(game_name, game_legs)
                except Exception as _ep_err:
                    print(f"[PropWave] EliteProps error {game_name}: {_ep_err}")
                try:
                    send_sgp_for_game(game_name, game_legs)
                except Exception as _sgp_err:
                    print(f"[PropWave] SGP error {game_name}: {_sgp_err}")
    except Exception as _by_err:
        print(f"[PropWave] per-game send error: {_by_err}")

    _prop_wave_fired = today_str
    print(f"[PropWave] Complete — {len(_todays_parlay_legs)} total legs in pool")


def _gs_supported_stats(gs) -> set:
    """
    Return which stat types are supported/amplified by this game script.
    Used to tier parlay legs: supported → SAFE, neutral → BALANCED, all → AGGRESSIVE.
    """
    supported = set()
    if gs is None:
        return {"points", "rebounds", "assists", "threes"}

    # Pace
    if gs.pace in ("TRANSITION_HEAVY", "UPTEMPO"):
        supported.update({"points", "assists", "threes"})
    elif gs.pace in ("HALFCOURT", "SLOW_PACED"):
        supported.add("rebounds")
    else:  # AVERAGE_PACE
        supported.update({"points", "rebounds"})

    # Flow
    if gs.flow in ("TIGHT_GAME", "COMPETITIVE"):
        supported.update({"points", "assists"})
    elif gs.flow in ("BLOWOUT", "DOUBLE_DIGIT_LEAD"):
        supported.update({"rebounds", "threes"})

    # Offense style (home — used as primary indicator for cross-game context)
    if gs.offense_home == "STAR_HEAVY" or gs.offense_away == "STAR_HEAVY":
        supported.add("points")
    if gs.offense_home == "FACILITATOR" or gs.offense_away == "FACILITATOR":
        supported.add("assists")

    return supported if supported else {"points", "rebounds", "assists", "threes"}


def _build_cross_game_parlay(pool):
    """
    Cross Game Parlay — PlayerRole + greedy selection across multiple games.

    Core rules:
    - Props only — no TOTAL / SPREAD / MONEYLINE legs
    - Each leg filtered against its own game's dominant script
    - PlayerRole scoring: edge × role_fit + dep_bonus (same as SGP)
    - Cross-game conflict gate: volatile scripts (INJURY / UPSET / TRANSITION_HEAVY)
      are capped at 1 game in SAFE, 2 in BALANCED/AGGRESSIVE — prevents stacking
      multiple legs that all require extreme conditions simultaneously
    - Intra-game correlation: BALANCED 2nd leg per game must have dep_bonus > 0
      with the first (i.e. the stats must correlate inside that game)
    - SAFE:       primary stats only · 1 leg per game  · 2-4 legs
    - BALANCED:   primary+secondary  · 2 legs per game · 4-6 legs
    - AGGRESSIVE: full script pool   · 3 legs per game · 6-8 legs
    Returns {"safe": [...], "balanced": [...], "aggressive": [...]}.
    """
    import random

    # ── Props only ────────────────────────────────────────────────────
    GAME_LEVEL = {"total", "spread", "moneyline", "over", "under"}
    prop_pool = [
        l for l in pool
        if l.get("bet_type", "").lower() not in GAME_LEVEL
        and not l.get("desc", "").upper().startswith("GAME TOTAL")
        and not l.get("desc", "").upper().startswith("SPREAD ")
        and not l.get("desc", "").upper().startswith("MONEYLINE")
    ]
    empty = {"safe": [], "balanced": [], "aggressive": []}
    if len(prop_pool) < 2:
        return empty

    # ── Sort by edge descending, dedup by player ──────────────────────
    pool_sorted = sorted(prop_pool, key=lambda x: x.get("edge", 0), reverse=True)
    _seen_p: set = set()
    _deduped: list = []
    for _l in pool_sorted:
        _p = (_l.get("player") or "").strip()
        if not _p:
            _d = _l.get("desc", "")
            for _kw in (" OVER ", " UNDER ", " over ", " under "):
                if _kw in _d:
                    _p = _d[:_d.index(_kw)].strip()
                    break
        _pk = _p.lower() if _p else ""
        if _pk and _pk in _seen_p:
            continue
        if _pk:
            _seen_p.add(_pk)
        _deduped.append(_l)
    pool_sorted = _deduped

    # ── Per-game metadata: dominant script + position lookup ──────────
    _SS_SCORE = lambda total, spread, sc: {
        "INJURY":            100,
        "TRANSITION_HEAVY":  total * 0.45,
        "UPTEMPO":           total * 0.40,
        "BLOWOUT":           spread * 9,
        "DOUBLE_DIGIT_LEAD": spread * 7,
        "TIGHT_GAME":        max(0, (6 - spread)) * 12,
        "HALFCOURT":         max(0, (215 - total)) * 0.5,
        "SLOW_PACED":        max(0, (218 - total)) * 0.4,
        "UPSET":             30,
        "COMPETITIVE":       10,
    }.get(sc, 0)

    game_meta: dict = {}  # gname -> {dominant, pos_lookup, total, spread}
    for leg in pool_sorted:
        gname = leg.get("game", "")
        if not gname or gname in game_meta:
            continue
        gd = _games_data.get(gname, {})
        if not gd:
            parts = gname.split(" @ ")
            if len(parts) == 2:
                gd = _games_data.get(f"{parts[1]} @ {parts[0]}", {})
        total  = float(gd.get("total",  220) or 220)
        spread = abs(float(gd.get("spread", 5.0) or 5.0))
        scripts   = detect_all_game_scripts(gd) if gd else ["COMPETITIVE"]
        dominant  = max(scripts, key=lambda sc: _SS_SCORE(total, spread, sc))
        pos_lookup: dict = {}
        if " @ " in gname:
            _aw, _hm = gname.split(" @ ", 1)
            for _tm in (_aw.strip(), _hm.strip()):
                try:
                    for _s in get_team_starters_espn(_tm):
                        _n = (_s.get("name") or "").lower()
                        if _n:
                            pos_lookup[_n] = _normalize_pos(_s.get("position", ""))
                except Exception:
                    pass
        game_meta[gname] = {
            "dominant":   dominant,
            "pos_lookup": pos_lookup,
            "total":      total,
            "spread":     spread,
        }
        print(f"  [CGP] {gname} → dominant:{dominant}")

    # Volatile scripts — cap at 1 game per type in SAFE
    _VOLATILE = {"INJURY", "UPSET", "TRANSITION_HEAVY"}

    # ── Position helper (per-game ESPN + BDL fallback) ────────────────
    def _get_pos_cgp(leg):
        pos = _normalize_pos(leg.get("position", ""))
        if pos:
            return pos
        player = (leg.get("player") or "").strip()
        if not player:
            desc = leg.get("desc", "")
            for kw in (" OVER ", " UNDER ", " over ", " under "):
                if kw in desc:
                    player = desc[:desc.index(kw)].strip()
                    break
        gname = leg.get("game", "")
        pos = game_meta.get(gname, {}).get("pos_lookup", {}).get(player.lower(), "")
        if pos:
            return pos
        if player:
            pos = _resolve_position_bdl(player)
        return pos

    # ── Script filter — each leg must fit its own game's dominant script ─
    script_pool = [
        l for l in pool_sorted
        if fits_script(l, game_meta.get(l.get("game", ""), {}).get("dominant", "COMPETITIVE"))
    ]
    if len(script_pool) < 2:
        script_pool = pool_sorted  # graceful fallback

    # ── Greedy cross-game selection ───────────────────────────────────
    # score = edge × role_fit + dep_bonus (cross-pool dependency)
    def _leg_score_cgp(leg, selected, role_filter, volatile_count, game_counts, max_per_game):
        gname    = leg.get("game", "")
        dominant = game_meta.get(gname, {}).get("dominant", "COMPETITIVE")

        # Per-game cap
        if game_counts.get(gname, 0) >= max_per_game:
            return -1.0

        # Volatile script cap (SAFE: 1 per type; BALANCED/AGGRESSIVE: 2 per type)
        vol_cap = 1 if role_filter == "primary" else 2
        if dominant in _VOLATILE and volatile_count.get(dominant, 0) >= vol_cap:
            return -1.0

        # Intra-game correlation gate: 2nd+ leg per game must correlate with 1st
        if game_counts.get(gname, 0) >= 1 and role_filter != "all":
            game_sel_types = [(s.get("bet_type") or "").lower()
                              for s in selected if s.get("game") == gname]
            bt = (leg.get("bet_type") or "").lower()
            if _dep_bonus(bt, game_sel_types) == 0.0:
                return -1.0  # no correlation — reject

        bt  = (leg.get("bet_type") or "").lower()
        pos = _get_pos_cgp(leg)
        rf  = _role_fit_score(bt, pos)

        if role_filter == "primary" and rf < 1.0:
            return -1.0
        if role_filter == "primary+secondary" and rf == 0.0:
            return -1.0

        sel_types = [(s.get("bet_type") or "").lower() for s in selected]
        dep       = _dep_bonus(bt, sel_types)
        edge      = max(leg.get("edge", 0), 0)
        return edge * max(rf, 0.1) + dep

    def _greedy_cgp(candidates, size, role_filter, max_per_game):
        selected      = []
        remaining     = list(candidates)
        game_counts   = {}
        volatile_count = {}
        for _ in range(size):
            if not remaining:
                break
            scores = [
                (l, _leg_score_cgp(l, selected, role_filter, volatile_count, game_counts, max_per_game))
                for l in remaining
            ]
            scores = [(l, s) for l, s in scores if s >= 0]
            if not scores:
                break
            best = max(scores, key=lambda x: x[1])[0]
            selected.append(best)
            remaining.remove(best)
            gname    = best.get("game", "")
            dominant = game_meta.get(gname, {}).get("dominant", "COMPETITIVE")
            game_counts[gname]      = game_counts.get(gname, 0) + 1
            if dominant in _VOLATILE:
                volatile_count[dominant] = volatile_count.get(dominant, 0) + 1
        return selected

    safe_size = random.randint(2, 4)
    bal_size  = random.randint(4, 6)
    agg_size  = random.randint(6, 8)

    safe       = _greedy_cgp(script_pool, safe_size, "primary",          max_per_game=1)
    balanced   = _greedy_cgp(script_pool, bal_size,  "primary+secondary", max_per_game=2)
    aggressive = _greedy_cgp(script_pool, agg_size,  "all",              max_per_game=3)

    # ── Fallbacks — ensure minimum viable tiers ───────────────────────
    if len(safe) < 2:
        safe = pool_sorted[:safe_size]
    if len(balanced) < 4:
        balanced = pool_sorted[:min(bal_size, len(pool_sorted))]
    if len(aggressive) < 6:
        aggressive = pool_sorted[:min(agg_size, len(pool_sorted))]

    # ── Ensure at least 2 different games per tier ────────────────────
    def _ensure_cross_game(tier_legs):
        games_in = {l.get("game", "") for l in tier_legs}
        if len(games_in) < 2:
            used = {l["desc"] for l in tier_legs}
            for fb in pool_sorted:
                if fb.get("game", "") not in games_in and fb["desc"] not in used:
                    tier_legs.append(fb)
                    break
        return tier_legs

    safe       = _ensure_cross_game(safe)
    balanced   = _ensure_cross_game(balanced)
    aggressive = _ensure_cross_game(aggressive)

    # ── Collapse identical tiers ──────────────────────────────────────
    safe_set = {l["desc"] for l in safe}
    bal_set  = {l["desc"] for l in balanced}
    agg_set  = {l["desc"] for l in aggressive}
    if bal_set == safe_set:
        balanced = []
    if agg_set == safe_set or agg_set == bal_set:
        aggressive = []

    for leg in safe + balanced + aggressive:
        print(f"  [CGP] {leg.get('game','')} · {leg.get('player', leg.get('desc',''))} · "
              f"script={game_meta.get(leg.get('game',''), {}).get('dominant','?')}")

    return {"safe": safe, "balanced": balanced, "aggressive": aggressive}


# ==========================
# 🌐 CROSS GAME PARLAY SENDER
# ==========================

def send_cgp(parlay_pool=None):
    """
    Cross Game Parlay — fires once per day.
    Called from send_daily_system() on good nights (pool pre-built and passed in),
    and from the main loop when 8+ prop legs are available on slip-fail nights.
    Has its own daily guard so it never fires twice.
    """
    global _cgp_sent_date, _todays_parlay_legs, _vip_lock_desc

    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── Restore guard from DB if memory was cleared by restart ────────────────
    if _cgp_sent_date is None:
        _cst = load_status()
        if _cst.get("_cgp_sent_date") == today_str:
            _cgp_sent_date = today_str

    if _cgp_sent_date == today_str:
        return

    # ── Build pool if not passed in (slip-fail path) ──────────────────────────
    if parlay_pool is None:
        if not _todays_parlay_legs:
            return
        seen = set()
        unique_legs = []
        for leg in _todays_parlay_legs:
            if leg["desc"] not in seen:
                seen.add(leg["desc"])
                unique_legs.append(leg)
        # Remove VIP Lock leg so it stays standalone
        if _vip_lock_desc:
            unique_legs = [l for l in unique_legs if l["desc"] != _vip_lock_desc]
        parlay_pool = unique_legs

    if len(parlay_pool) < 3:
        print("[CGP] Not enough legs to build a cross-game parlay")
        return

    D = "━━━━━━━━━━━━━━━━━━━"

    _CGP_ICON = {
        "points": "🏀", "rebounds": "💪", "assists": "🔥",
        "threes": "🎯", "blocks": "🛡️", "steals": "⚡",
    }

    def _fmt_odds_cgp(o):
        try:
            o = float(o)
            if 1.01 <= o <= 30:
                o = int(round((o - 1) * 100)) if o >= 2.0 else int(round(-100 / (o - 1)))
            else:
                o = int(o)
            return f"+{o}" if o > 0 else str(o)
        except Exception:
            return "-110"

    def _ls(leg):
        player = (leg.get("player") or "").strip()
        if not player:
            desc = leg.get("desc", "")
            for kw in (" OVER ", " UNDER ", " over ", " under "):
                if kw in desc:
                    player = desc[:desc.index(kw)].strip()
                    break
        if not player:
            player = leg.get("desc", "Unknown")
        bt       = (leg.get("bet_type") or "").lower()
        label    = _fd_label(bt, leg.get("line", 0), leg.get("pick", "OVER"))
        odds_str = _fmt_odds_cgp(leg.get("odds", -110))
        icon     = _CGP_ICON.get(bt, "🎯")
        game     = leg.get("game", "")
        game_tag = f" _{game}_" if game else ""
        return f"  {icon} {player} — {label}  ({odds_str}){game_tag}"

    # ── Build CGP tiers ────────────────────────────────────────────────────────
    cgp_tiers      = _build_cross_game_parlay(parlay_pool)
    cgp_safe       = cgp_tiers["safe"]
    cgp_balanced   = cgp_tiers["balanced"]
    cgp_aggressive = cgp_tiers["aggressive"]
    cgp_all        = list({l["desc"]: l for l in cgp_safe + cgp_balanced + cgp_aggressive}.values())

    if not cgp_all:
        print("[CGP] No CGP legs built — pool too small or no cross-game coverage")
        return

    safe_odds = _parlay_odds(len(cgp_safe))
    bal_odds  = _parlay_odds(len(cgp_balanced))
    agg_odds  = _parlay_odds(len(cgp_aggressive))

    # ── VIP message ────────────────────────────────────────────────────────────
    cgp_lines = [
        f"🌐 *CROSS GAME PARLAY — PLAYER PROPS*",
        f"_Script-filtered · Props only · Each leg fits its own game_",
        f"",
    ]
    if cgp_safe:
        cgp_lines += [f"🟢 *SAFE ({len(cgp_safe)} legs)*"] + [_ls(l) for l in cgp_safe] + [f"  📊 Approx: *+{safe_odds:,}*", f""]
    if cgp_balanced:
        cgp_lines += [f"🟡 *BALANCED ({len(cgp_balanced)} legs)*"] + [_ls(l) for l in cgp_balanced] + [f"  📊 Approx: *+{bal_odds:,}*", f""]
    if cgp_aggressive:
        cgp_lines += [f"🔴 *AGGRESSIVE ({len(cgp_aggressive)} legs)*"] + [_ls(l) for l in cgp_aggressive] + [f"  📊 Approx: *+{agg_odds:,}*", f""]
    cgp_lines += [D, f"⚡ Each leg fits its own game script · Multi-game · Props only"]

    send("\n".join(cgp_lines), VIP_CHANNEL)

    # ── Free channel teaser — first 2 SAFE legs only ──────────────────────────
    if cgp_safe:
        teaser_legs  = cgp_safe[:2]
        teaser_lines = "\n".join([f"  {l['desc']}" for l in teaser_legs])
        n_more       = max((len(cgp_safe) - 2) + len(cgp_balanced) + len(cgp_aggressive), 0)
        free_msg = (
            f"🌐 *FREE PLAY — CROSS GAME PARLAY*\n\n"
            f"{teaser_lines}\n"
            f"_...+{n_more} more legs in VIP (BALANCED + AGGRESSIVE tiers)_\n\n"
            f"📊 Approx: *+{_parlay_odds(2):,}* (full parlay higher)\n\n"
            f"🔒 Full CGP system → VIP only\n"
            f"👉 {CHECKOUT_URL}"
        )
        send(free_msg, FREE_CHANNEL)

    # ── Tag DB records ─────────────────────────────────────────────────────────
    _tag_parlay_legs_db(cgp_all, "CROSS_GAME_PARLAY")

    # ── Save parlay legs for morning recap ─────────────────────────────────────
    _parlay_legs_saved = {
        "cross_game": [{"desc": l["desc"], "game": l.get("game", ""), "bet_type": l.get("bet_type", "")} for l in cgp_all],
    }
    save_status(0, {"_last_parlay_legs": _parlay_legs_saved, "_last_parlay_date": today_str})

    # ── Save CGP legs to DB for training ──────────────────────────────────────
    all_cgp_unique = {l["desc"]: l for l in cgp_safe + cgp_balanced + cgp_aggressive}
    for leg in all_cgp_unique.values():
        save_bet({
            "game":          leg.get("game", ""),
            "player":        leg.get("player") or leg.get("desc", ""),
            "pick":          leg.get("desc", ""),
            "betType":       leg.get("bet_type", "PROP"),
            "pick_category": "CROSS_GAME_PARLAY",
            "line":          leg.get("line"),
            "prediction":    None,
            "odds":          leg.get("odds", 0),
            "prob":          round(leg.get("confidence", 70) / 100, 4),
            "edge":          round(leg.get("edge", 0), 2),
            "confidence":    leg.get("confidence", 0),
            "time":          str(datetime.now()),
            "result":        None,
        })

    _cgp_sent_date = today_str
    save_status(0, {"_cgp_sent_date": today_str})
    save_memory_state()
    n_games = len(set(l.get("game", "") for l in cgp_all if l.get("game")))
    print(f"[CGP] Sent — {len(cgp_all)} legs across {n_games} games")


def send_daily_system():
    """VIP LOCK + tiered auto-parlay system. Once per day after picks are in."""
    global _system_sent_date, _todays_parlay_legs, _vip_lock_desc, _sgp_sent_games

    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── Restore _system_sent_date after crash/restart ─────────────────────────
    # Primary: in-memory flag. Secondary: status table. Tertiary: direct DB check
    # for a VIP_LOCK bet today (survives Railway restarts that clear status table).
    if _system_sent_date is None:
        _st = load_status()
        if _st.get("_system_sent_date") == today_str:
            _system_sent_date = today_str
        else:
            # Hard DB check — if a VIP_LOCK row exists for today, already sent
            try:
                _chk = _db_conn()
                if _chk:
                    _cc = _chk.cursor()
                    _cc.execute(
                        "SELECT 1 FROM bets WHERE bet_type='VIP_LOCK' "
                        "AND DATE(COALESCE(bet_time, created_at) AT TIME ZONE 'America/New_York') = %s LIMIT 1",
                        (today_str,)
                    )
                    if _cc.fetchone():
                        _system_sent_date = today_str
                        print(f"[System] Restored _system_sent_date={today_str} from DB (VIP_LOCK row found)")
                    _cc.close(); _chk.close()
            except Exception as _dce:
                print(f"[System] DB check for sent_date failed: {_dce}")

    # Reset legs and SGP tracking each new day
    if _system_sent_date and _system_sent_date != today_str:
        _todays_parlay_legs.clear()
        _sgp_sent_games.clear()
        _elite_props_sent_games.clear()
        _vip_lock_desc = None

    if _system_sent_date == today_str:
        return

    # ── Rebuild _todays_parlay_legs from DB if bot restarted mid-day ─────────
    if not _todays_parlay_legs:
        try:
            todays_bets = [
                b for b in load_bets()
                if str(b.get("time") or "").startswith(today_str)
                and b.get("betType", "") in (
                    "MONEYLINE", "SPREAD", "TOTAL", "OVER", "UNDER",
                    "PROP", "points", "rebounds", "assists", "threes",
                    "blocks", "steals", "pra", "pr", "pa"
                )
            ]
            for b in todays_bets:
                _desc = b.get("pick", "")
                _pick_dir = "OVER" if "OVER" in _desc.upper() else ("UNDER" if "UNDER" in _desc.upper() else "OVER")
                _todays_parlay_legs.append({
                    "desc":        _desc,
                    "player":      b.get("player", ""),
                    "game":        b.get("game", ""),
                    "bet_type":    b.get("betType", ""),
                    "line":        b.get("line", 0),
                    "odds":        b.get("odds", -110),
                    "pick":        _pick_dir,
                    "confidence":  b.get("confidence", 0),
                    "edge":        b.get("edge", 0),
                    "correlation": _pick_dir,
                    "team":        "",
                    "team_role":   "",
                    "position":    "",
                })
            if _todays_parlay_legs:
                print(f"[System] Restored {len(_todays_parlay_legs)} parlay legs from DB after restart")
        except Exception as _re:
            print(f"[System] Could not restore parlay legs: {_re}")

    # ── Restore _vip_lock_desc from DB if not set (bot restarted) ─────────────
    if not _vip_lock_desc:
        try:
            todays_all = [
                b for b in load_bets()
                if str(b.get("time") or "").startswith(today_str)
                and b.get("pick_category", "") == "VIP_LOCK"
            ]
            if todays_all:
                _vip_lock_desc = todays_all[0].get("pick", "")
                print(f"[System] Restored VIP LOCK from DB: {_vip_lock_desc}")
        except Exception as _ve:
            print(f"[System] Could not restore VIP LOCK: {_ve}")

    if len(_todays_parlay_legs) < 2:
        return

    # ── De-duplicate ────────────────────────────────────────────────
    seen = set()
    unique_legs = []
    for leg in _todays_parlay_legs:
        if leg["desc"] not in seen:
            seen.add(leg["desc"])
            unique_legs.append(leg)

    # ── Injury filter — remove picks for OUT/Doubtful players ────────
    try:
        _inj_data = get_espn_injuries()
        _out_names = {
            name.lower()
            for name, info in _inj_data.items()
            if isinstance(info, dict) and info.get("status", "") in ("Out", "Doubtful")
        }
        def _player_is_out(leg):
            desc_lower = leg.get("desc", "").lower()
            for out_name in _out_names:
                # match first + last name appearing anywhere in the leg description
                parts = out_name.split()
                if len(parts) >= 2 and parts[0] in desc_lower and parts[-1] in desc_lower:
                    print(f"  [InjFilter] Removed '{leg['desc']}' — player marked {_inj_data[out_name]['status']}")
                    return True
            return False
        unique_legs = [l for l in unique_legs if not _player_is_out(l)]
    except Exception as _ie:
        print(f"  [InjFilter] Injury filter error: {_ie}")

    if len(unique_legs) < 2:
        return

    # ── VIP LOCK — highest positive-edge pick, removed from parlay pool ──────
    sorted_by_edge = sorted(unique_legs, key=lambda x: x.get("edge", 0), reverse=True)
    # Prefer a pick with positive edge; fall back to highest confidence if all negative
    positive_edge_legs = [l for l in sorted_by_edge if l.get("edge", 0) >= 0]
    if positive_edge_legs:
        vip_lock = positive_edge_legs[0]
    else:
        # All picks have negative edge — use highest confidence instead
        vip_lock = sorted(unique_legs, key=lambda x: x.get("confidence", 0), reverse=True)[0]
        print("[VIP LOCK] No positive-edge pick found — using highest confidence")
    parlay_pool    = [l for l in sorted_by_edge if l["desc"] != vip_lock["desc"]]
    _vip_lock_desc = vip_lock["desc"]   # save globally so SGPs know to exclude it
    save_memory_state()

    lock_tier, lock_units = assign_tier(vip_lock.get("confidence", 70))
    lock_badge = TIER_BADGE.get(lock_tier, "🎯 BALANCED · 2 units")

    D = "━━━━━━━━━━━━━━━━━━━"

    _VL_ICON = {
        "SPREAD": "📉", "TOTAL": "🎯", "MONEYLINE": "🔥",
        "points": "🏀", "rebounds": "💪", "assists": "🔥",
        "threes": "🎯", "blocks": "🛡️", "steals": "⚡",
    }

    def _fmt_lock_odds(o):
        try:
            o = float(o)
            if 1.01 <= o <= 30:
                o = int(round((o - 1) * 100)) if o >= 2.0 else int(round(-100 / (o - 1)))
            else:
                o = int(o)
            return f"+{o}" if o > 0 else str(o)
        except Exception:
            return "-110"

    def _lock_line(leg):
        icon     = _VL_ICON.get((leg.get("bet_type") or "").upper(), "🎯")
        bt       = (leg.get("bet_type") or "").lower()
        line     = leg.get("line", 0)
        pick_dir = leg.get("pick", "OVER")
        # Props get FanDuel label; game-level bets use desc directly
        if bt in ("points", "rebounds", "assists", "threes", "blocks", "steals", "pra", "pr", "pa"):
            label = _fd_label(bt, line, pick_dir)
            player = (leg.get("player") or "").strip()
            if not player:
                desc = leg.get("desc", "")
                for kw in (" OVER ", " UNDER ", " over ", " under "):
                    if kw in desc:
                        player = desc[:desc.index(kw)].strip()
                        break
            display = f"{player} — {label}" if player else leg.get("desc", "")
        else:
            display = leg.get("desc", "")
        odds_str = _fmt_lock_odds(leg.get("odds", -110))
        return f"{icon} {display}  ({odds_str})"

    # ── Send VIP Lock as standalone message ───────────────────────────────────
    lock_msg = "\n".join([
        f"🔒 *VIP LOCK — BEST PLAY OF THE DAY*",
        f"",
        f"{_lock_line(vip_lock)}",
        f"{lock_badge}",
        f"⚡ Standalone only — do not parlay this",
    ])
    send(lock_msg, VIP_CHANNEL)

    # ── Cross Game Parlay — handled by send_cgp() ─────────────────────────────
    send_cgp(parlay_pool)

    # ── Save VIP lock to DB for learning ──────────────────────────────────────
    _vl_game  = vip_lock.get("game", "")
    _vl_gdata = _games_data.get(_vl_game, {})
    save_bet({
        "game":          _vl_game,
        "player":        vip_lock.get("player", ""),
        "pick":          vip_lock["desc"],
        "betType":       "VIP_LOCK",
        "pick_category": "VIP_LOCK",
        "line":          vip_lock.get("line"),
        "prediction":    None,
        "odds":          vip_lock.get("odds", 0),
        "prob":          round(vip_lock.get("confidence", 70) / 100, 4),
        "edge":          round(vip_lock.get("edge", 0), 2),
        "confidence":    vip_lock.get("confidence", 0),
        "time":          str(datetime.now()),
        "result":        None,
        "script":        detect_game_script(_vl_gdata),
        "game_total":    _vl_gdata.get("total"),
        "game_spread":   _vl_gdata.get("spread"),
    })

    _system_sent_date = today_str
    save_status(0, {"_system_sent_date": today_str})
    save_memory_state()
    print(f"[System] Sent — LOCK: {vip_lock['desc']}")


# ==========================
# 🎲 PER-GAME SGP SENDER
# ==========================

def detect_game_script(game_data):
    """
    Detect game script from Vegas total, spread, and injuries.
    All numeric thresholds are read from the self-learning DB (learning_data key
    'script_thresholds') so the bot auto-calibrates them from real win/loss data.

    Priority order (first match wins):
      INJURY           — key player listed OUT/DOUBTFUL for either team
      HALFCOURT        — Vegas total < halfcourt_total_max
      TRANSITION_HEAVY — Vegas total >= transition_total_min
      BLOWOUT          — |spread| >= blowout_spread_min
      TIGHT_GAME       — |spread| <= tight_spread_max
      UPSET            — upset_spread_min <= |spread| <= upset_spread_max
      COMPETITIVE      — everything else
    """
    total          = game_data.get("total", 0)
    spread         = abs(game_data.get("spread", 0))
    has_key_injury = game_data.get("has_key_injury", False)

    # Load self-calibrated thresholds (falls back to defaults if not enough data yet)
    try:
        ld  = load_learning_data()
        thr = ld.get("script_thresholds") or dict(_SCRIPT_THRESHOLD_DEFAULTS)
        for k, v in _SCRIPT_THRESHOLD_DEFAULTS.items():
            thr.setdefault(k, v)
    except Exception:
        thr = dict(_SCRIPT_THRESHOLD_DEFAULTS)

    if has_key_injury:
        return "INJURY"
    if total > 0 and total < thr["halfcourt_total_max"]:
        return "HALFCOURT"
    if total > 0 and total < thr.get("slow_paced_total_max", _SCRIPT_THRESHOLD_DEFAULTS["slow_paced_total_max"]):
        return "SLOW_PACED"
    if total >= thr["transition_total_min"]:
        return "TRANSITION_HEAVY"
    if total > 0 and total >= thr.get("uptempo_total_min", _SCRIPT_THRESHOLD_DEFAULTS["uptempo_total_min"]):
        return "UPTEMPO"
    if spread >= thr["blowout_spread_min"]:
        return "BLOWOUT"
    if spread <= thr["tight_spread_max"]:
        return "TIGHT_GAME"
    if game_data.get("model_disagrees_with_vegas", False):
        return "UPSET"
    if thr["upset_spread_min"] <= spread <= thr["upset_spread_max"]:
        return "UPSET"
    return "COMPETITIVE"


def detect_all_game_scripts(game_data):
    """
    Return ALL scripts that match this game — no priority, no single winner.
    A game can legitimately match multiple scripts (e.g. TRANSITION_HEAVY + BLOWOUT).
    Returns a list; falls back to ["COMPETITIVE"] if nothing matches.
    """
    total          = game_data.get("total", 0)
    spread         = abs(game_data.get("spread", 0))
    has_key_injury = game_data.get("has_key_injury", False)

    try:
        ld  = load_learning_data()
        thr = ld.get("script_thresholds") or dict(_SCRIPT_THRESHOLD_DEFAULTS)
        for k, v in _SCRIPT_THRESHOLD_DEFAULTS.items():
            thr.setdefault(k, v)
    except Exception:
        thr = dict(_SCRIPT_THRESHOLD_DEFAULTS)

    matched = []
    if has_key_injury:
        matched.append("INJURY")
    if total > 0 and total < thr["halfcourt_total_max"]:
        matched.append("HALFCOURT")
    if total > 0 and thr["halfcourt_total_max"] <= total < thr.get("slow_paced_total_max", _SCRIPT_THRESHOLD_DEFAULTS["slow_paced_total_max"]):
        matched.append("SLOW_PACED")
    if total > 0 and thr.get("uptempo_total_min", _SCRIPT_THRESHOLD_DEFAULTS["uptempo_total_min"]) <= total < thr["transition_total_min"]:
        matched.append("UPTEMPO")
    if total >= thr["transition_total_min"]:
        matched.append("TRANSITION_HEAVY")
    if spread >= thr["blowout_spread_min"]:
        matched.append("BLOWOUT")
    if spread <= thr["tight_spread_max"]:
        matched.append("TIGHT_GAME")
    if game_data.get("model_disagrees_with_vegas", False) or \
            thr["upset_spread_min"] <= spread <= thr["upset_spread_max"]:
        matched.append("UPSET")

    return matched if matched else ["COMPETITIVE"]


def combo_key(scripts):
    """Consistent, sorted join key for a list of scripts — used as a DB key."""
    if not scripts:
        return "NORMAL"
    clean = sorted(set(scripts))
    return "+".join(clean) if len(clean) > 1 else clean[0]


def fits_script(leg, script):
    """
    Return True if a parlay/SGP leg fits the game script.
    Filters are applied to BALANCED and AGGRESSIVE tiers only;
    SAFE always passes unfiltered.
    """
    corr      = leg.get("correlation", "NEUTRAL")   # "OVER"/"UNDER"/"NEUTRAL"
    bet_type  = leg.get("bet_type", "")
    team_role = leg.get("team_role", "")             # "favorite"/"underdog"/""

    # ── TRANSITION_HEAVY: Shootout — OVERs, points, 3PT; block UNDERs ─
    if script in ("TRANSITION_HEAVY", "UPTEMPO"):
        if corr == "UNDER":
            return False
        return True  # OVERs, spreads, 3PT props all welcome

    # ── BLOWOUT / DOUBLE_DIGIT_LEAD: Fav dominates ──────────────────
    if script in ("BLOWOUT", "DOUBLE_DIGIT_LEAD"):
        is_starter = leg.get("is_starter", True)
        if team_role == "favorite":
            return True
        if team_role == "underdog":
            if corr == "OVER" and is_starter:
                return False     # underdog star OVER blocked — sits/limited when down big
            if corr == "OVER" and not is_starter:
                return True      # underdog bench player gets extra minutes → OVER valid
            return False
        return False

    # ── TIGHT_GAME / COMPETITIVE: Back & forth — star OVERs, clutch props
    if script in ("TIGHT_GAME", "COMPETITIVE"):
        if corr == "UNDER" and bet_type not in ("rebounds",):
            return False
        return bet_type in ("SPREAD", "TOTAL", "points", "assists")

    # ── UPSET: Underdog alive — underdog legs + fav star forced to carry
    if script == "UPSET":
        if team_role == "underdog":
            return True
        if corr == "OVER" and bet_type in ("points", "assists") and team_role == "favorite":
            return True          # fav star forced to carry = points/assists over
        if bet_type in ("MONEYLINE",):
            return True
        return False

    # ── HALFCOURT / SLOW_PACED: Defensive battle — UNDERs, rebounds OVERs
    if script in ("HALFCOURT", "SLOW_PACED"):
        if bet_type == "rebounds" and corr == "OVER":
            return True          # physical game → rebounds up
        if corr == "UNDER":
            return True          # unders fit perfectly
        if corr == "OVER" and bet_type in ("points", "TOTAL"):
            return False         # block scoring overs
        return False

    # ── INJURY: Usage spike — backup OVERs, ball-handler assists, usage props
    if script == "INJURY":
        if bet_type in ("assists",):
            return True
        if corr == "OVER" and bet_type in ("points", "rebounds", "assists", "threes"):
            return True          # role player volume up
        return False

    return True  # COMPETITIVE / unknown — everything passes


def fits_multi_script(leg, scripts, combo_win_rates=None):
    """
    Evaluate a leg against ALL matching scripts for a game.

    Returns (allow: bool, confidence_multiplier: float)

    Rules:
      - SAFE tier always passes — multiplier = 1.0
      - COMPETITIVE only → passes with multiplier = 1.0
      - If ANY non-COMPETITIVE script hard-blocks → skip (False, 1.0)
      - If all non-COMPETITIVE scripts approve → fire; multiplier based on:
            * Number of agreeing signals (+5% per extra script)
            * Learned combo win rate once >= 5 samples exist
      - If no non-COMPETITIVE script has an opinion → pass at 1.0 (abstention)
    """
    if not scripts or scripts == ["COMPETITIVE"]:
        return True, 1.0

    non_normal = [sc for sc in scripts if sc != "COMPETITIVE"]
    if not non_normal:
        return True, 1.0

    decisions = {sc: fits_script(leg, sc) for sc in non_normal}
    blocks    = [sc for sc, ok in decisions.items() if not ok]
    approvals = [sc for sc, ok in decisions.items() if ok]

    key = combo_key(scripts)
    cwr = (combo_win_rates or {}).get(key)

    # ── Conflict: at least one script blocks ─────────────────────────
    if blocks:
        # Default: block the pick
        # Exception: enough data proves this conflict combination is actually profitable
        # Requires 10+ graded results AND win rate >= 55% to unlock
        if cwr and cwr.get("n", 0) >= 10:
            wr = cwr["w"] / cwr["n"]
            if wr >= 0.55:
                # Conflict unlocked by data — fire at reduced confidence (conflict penalty)
                conflict_penalty = round(1.0 - (len(blocks) * 0.10), 2)   # -10% per blocking script
                conflict_penalty = max(0.70, conflict_penalty)             # floor at -30%
                return True, conflict_penalty
        # Not enough data or win rate too low — keep blocking
        return False, 1.0

    # ── All scripts approve — build confidence multiplier ────────────
    if cwr and cwr.get("n", 0) >= 5:
        wr         = cwr["w"] / cwr["n"]
        # Win rate below 40% → dampen confidence; above 50% → boost
        multiplier = round(1.0 + (wr - 0.50) * 1.5, 2)
        multiplier = max(0.75, min(1.40, multiplier))   # cap between -25% and +40%
    else:
        # No history yet: small raw bonus per extra agreeing signal
        multiplier = round(1.0 + (len(approvals) - 1) * 0.05, 2)

    return True, multiplier


def send_edge_fade_parlay(parlay_pool, bankroll=1000, swap_threshold=0.05,
                          min_legs=3, max_legs=7, poll_interval=15,
                          enable_alerts=True):
    """
    Edge-Fade 7 Parlay — Full implementation.
    - Fades public-heavy stars (juiced lines, lowest edge)
    - Backs beneficiaries who inherit production
    - Safety hedge per game
    - Kelly staking, auto-swap inactive players, live alerts,
      dynamic stake recalculation, formatted betting slip
    """
    import math as _math
    import threading as _threading
    from copy import deepcopy as _deepcopy

    EMOJI_R = {"points": "🏀", "rebounds": "💪", "assists": "🎯", "threes": "💥"}
    D        = "━━━━━━━━━━━━━━━━━━━"
    date_str = datetime.now().strftime("%B %d, %Y")

    prop_legs = [l for l in parlay_pool if l.get("bet_type", "") in
                 {"points", "rebounds", "assists", "threes"}]
    if len(prop_legs) < min_legs:
        return

    # ── Step 1: Group by game, build fades / beneficiaries / hedges ──
    by_game = {}
    for l in prop_legs:
        by_game.setdefault(l.get("game", "Unknown"), []).append(l)

    fades, beneficiaries, hedges = [], [], []
    alternates_by_game = {}   # pool of unused legs per game for auto-swap

    for game, legs in by_game.items():
        if len(legs) < 2:
            continue
        sorted_legs = sorted(legs, key=lambda x: x.get("edge", 0))

        # Fade = lowest edge (most juiced by public)
        fade_raw = sorted_legs[0]
        fade_conf = calibrated_confidence(
            fade_raw.get("bet_type", "points"),
            fade_raw.get("confidence", 65),
            pick_category="EDGE_FADE", role="fade"
        )
        fade = {**fade_raw, "_game": game, "_role": "fade",
                "note": "public-fade", "live_status": None,
                "expected_hit": round(fade_conf / 100, 2),
                "edge_score": round(fade_raw.get("edge", 0), 2)}
        fades.append(fade)

        bene_sorted = sorted(sorted_legs[1:], key=lambda x: x.get("edge", 0), reverse=True)

        for b in bene_sorted[:2]:
            b_conf = calibrated_confidence(
                b.get("bet_type", "points"),
                b.get("confidence", 65),
                pick_category="EDGE_FADE", role="beneficiary"
            )
            beneficiaries.append({**b, "_game": game, "_role": "beneficiary",
                                  "note": f"benefits from {sorted_legs[0].get('player','star')} fade",
                                  "live_status": None,
                                  "expected_hit": round(b_conf / 100, 2),
                                  "edge_score": round(b.get("edge", 0), 2)})

        if len(bene_sorted) > 2:
            h = bene_sorted[-1]
            h_conf = calibrated_confidence(
                h.get("bet_type", "points"),
                h.get("confidence", 65),
                pick_category="EDGE_FADE", role="hedge"
            )
            hedges.append({**h, "_game": game, "_role": "hedge",
                           "note": "safety hedge", "live_status": None,
                           "expected_hit": round(h_conf / 100, 2),
                           "edge_score": round(h.get("edge", 0), 2)})

        # Store unused legs as alternates for auto-swap
        used = {sorted_legs[0].get("desc"), *(b.get("desc") for b in bene_sorted[:2])}
        if len(bene_sorted) > 2:
            used.add(bene_sorted[-1].get("desc"))
        alternates_by_game[game] = [l for l in legs if l.get("desc") not in used]

    # ── Step 2: Trim to max_legs, enforce min_legs ───────────────────
    all_legs = _deepcopy(fades + beneficiaries + hedges)
    all_legs = sorted(all_legs, key=lambda x: x.get("edge_score", 0), reverse=True)
    all_legs = all_legs[:max_legs]
    if len(all_legs) < min_legs:
        return

    # ── Step 3: Kelly staking ────────────────────────────────────────
    def _recalc_stakes(legs, broll):
        active = [l for l in legs if l.get("live_status") is None]
        n = max(len(active), 1)
        for leg in active:
            p = min(max(leg.get("expected_hit", 0.65), 0.01), 0.99)
            f = max((p - (1 - p)), 0)
            leg["suggested_stake"] = round(
                min(f * (broll / n), (broll * 0.03 / n) * 2), 2
            )

    _recalc_stakes(all_legs, bankroll)

    # ── Step 4: Pre-send active check + auto-swap ────────────────────
    auto_swaps = 0
    injuries   = _injury_cache.get("injuries", {}) if isinstance(_injury_cache, dict) else {}

    for leg in all_legs:
        player = leg.get("player", "")
        if not player:
            continue
        is_out = any(player.lower() in str(k).lower() for k in injuries.keys())
        if is_out:
            game = leg.get("_game", leg.get("game", ""))
            alts = alternates_by_game.get(game, [])
            alts_active = [a for a in alts
                           if not any(a.get("player","").lower() in str(k).lower()
                                      for k in injuries.keys())]
            if alts_active:
                alt = max(alts_active, key=lambda x: x.get("edge", 0))
                old_player = player
                leg["player"]       = alt.get("player", player)
                leg["desc"]         = alt.get("desc", leg["desc"])
                leg["edge_score"]   = round(alt.get("edge", 0), 2)
                leg["expected_hit"] = round(alt.get("confidence", 65) / 100, 2)
                leg["note"]        += f" / auto-swap → {alt.get('player','')}"
                leg["live_status"]  = None
                alts.remove(alt)
                auto_swaps += 1
                if enable_alerts:
                    send(f"🔄 *EDGE-FADE AUTO-SWAP*\n"
                         f"_{old_player} inactive — replaced with {alt.get('player','')}_ "
                         f"({leg.get('_game','')}) · Stake recalculated",
                         VIP_CHANNEL)

    _recalc_stakes(all_legs, bankroll)

    # ── Step 5: Build and send initial Telegram message ──────────────
    _fade_legs = [l for l in all_legs if l.get("_role") == "fade"]
    _bene_legs = [l for l in all_legs if l.get("_role") == "beneficiary"]
    _hedge_legs= [l for l in all_legs if l.get("_role") == "hedge"]

    overall_prob = _math.prod(
        [min(max(l.get("expected_hit", 0.65), 0.01), 0.99) for l in all_legs]
    )

    msg_lines = [
        f"🎯 *EDGE-FADE 7 PARLAY*",
        f"_{date_str} · {len(all_legs)}-Leg · Fade public stars · Back value_",
        D,
    ]

    if _fade_legs:
        msg_lines += [f"", f"❌ *FADES* _(bet UNDER — juiced lines)_"]
        for l in _fade_legs:
            icon = EMOJI_R.get(l.get("bet_type", ""), "📉")
            msg_lines.append(
                f"  {icon} {l['desc']} — {int(l.get('confidence',0))}% "
                f"· ExpHit: {l['expected_hit']:.2f} · Stake: ${l['suggested_stake']}\n"
                f"  _↳ {l.get('_game','')} · {l.get('note','public-fade')}_"
            )

    if _bene_legs:
        msg_lines += [f"", f"✅ *BENEFICIARIES* _(bet OVER — inherit production)_"]
        for l in _bene_legs:
            icon = EMOJI_R.get(l.get("bet_type", ""), "🏀")
            msg_lines.append(
                f"  {icon} {l['desc']} — {int(l.get('confidence',0))}% "
                f"· ExpHit: {l['expected_hit']:.2f} · Stake: ${l['suggested_stake']}\n"
                f"  _↳ {l.get('_game','')} · {l.get('note','secondary edge')}_"
            )

    if _hedge_legs:
        msg_lines += [f"", f"🛡️ *SAFETY HEDGES*"]
        for l in _hedge_legs:
            icon = EMOJI_R.get(l.get("bet_type", ""), "🏀")
            msg_lines.append(
                f"  {icon} {l['desc']} — {int(l.get('confidence',0))}% "
                f"· ExpHit: {l['expected_hit']:.2f} · Stake: ${l['suggested_stake']}\n"
                f"  _↳ {l.get('_game','')} · covers main leg miss_"
            )

    # Betting slip
    slip = [
        f"{l.get('player','')} ({l.get('_game', l.get('game',''))}) – "
        f"{l.get('bet_type','').capitalize()} {l.get('line','')} "
        f"[{l.get('note','')}] (ExpHit: {l.get('expected_hit',0):.2f}, "
        f"Stake: ${l.get('suggested_stake',0)})"
        for l in all_legs
    ]

    summary_line = (
        f"Fades: {len(_fade_legs)} · Beneficiaries: {len(_bene_legs)} · "
        f"Hedges: {len(_hedge_legs)} · Auto-swaps: {auto_swaps}"
    )

    msg_lines += [
        f"",
        D,
        f"📋 *BETTING SLIP*",
        *[f"  {i+1}. {s}" for i, s in enumerate(slip)],
        D,
        f"📈 *Combined Hit Prob:* {round(overall_prob * 100, 1)}%",
        f"💰 *Approx Payout:* +{_parlay_odds(len(all_legs)):,}",
        f"🔄 *{summary_line}*",
        f"🧠 _Kelly-sized · poll every {poll_interval}s · alerts {'on' if enable_alerts else 'off'}_",
    ]

    send("\n".join(msg_lines), VIP_CHANNEL)

    # ── Step 6: Live polling thread — monitors legs, alerts on miss ──
    def _live_monitor(legs, broll, interval, alerts):
        _settled = set()
        # Collect player stats from all today's ESPN games
        try:
            today_games = _fetch_bdl_live_games()  # already ESPN-backed
            all_espn_stats = []
            for _g in (today_games or []):
                all_espn_stats.extend(_espn_summary_player_stats(_g.get("game_id") or _g.get("id", "")))
        except Exception:
            all_espn_stats = []

        type_map = {"points": "pts", "rebounds": "reb", "assists": "ast", "threes": "fg3m"}

        for stat in all_espn_stats:
            p_name = stat.get("pname", "")
            for leg in legs:
                if leg.get("desc") in _settled:
                    continue
                if leg.get("player", "").lower() != p_name.lower():
                    continue
                bt     = leg.get("bet_type", "")
                actual = stat.get(type_map.get(bt, ""), None)
                if actual is None:
                    continue
                line      = leg.get("line", 0)
                direction = "OVER" if "OVER" in leg.get("desc", "").upper() else "UNDER"
                hit       = (actual > line) if direction == "OVER" else (actual < line)
                leg["live_status"] = hit
                _settled.add(leg.get("desc"))

                if alerts:
                    status_emoji = "✅" if hit else "❌"
                    send(
                        f"📡 *EDGE-FADE LEG UPDATE*\n"
                        f"{status_emoji} {leg.get('player','')} — {bt} {direction} {line}\n"
                        f"_Actual: {actual} · {'HIT' if hit else 'MISS'}_\n"
                        f"_{leg.get('_game','')} · {leg.get('note','')}_",
                        VIP_CHANNEL
                    )

                # Recalculate stakes for remaining unsettled legs
                remaining = [l for l in legs if l.get("live_status") is None]
                if remaining:
                    remaining_broll = sum(l.get("suggested_stake", 0) for l in remaining)
                    _recalc_stakes(remaining, remaining_broll)
                    if alerts and not hit:
                        send(
                            f"⚠️ *EDGE-FADE STAKE RECALC*\n"
                            f"_{len(remaining)} legs remaining · stakes adjusted_",
                            VIP_CHANNEL
                        )

    _threading.Thread(
        target=_live_monitor,
        args=(all_legs, bankroll, poll_interval, enable_alerts),
        daemon=True
    ).start()

    # ── Step 7: Save all legs for settling + model learning ──────────
    for leg in all_legs:
        try:
            save_bet({
                "game":          leg.get("game", ""),
                "player":        leg.get("player", ""),
                "pick":          leg.get("desc", ""),
                "betType":       leg.get("bet_type", leg.get("betType", "points")),
                "line":          leg.get("line", 0),
                "confidence":    leg.get("confidence", 0),
                "prob":          round(leg.get("win_prob", leg.get("prob", leg.get("confidence", 55))) / 100, 4),
                "edge":          leg.get("edge", 0),
                "pick_category": "EDGE_FADE",
                "time":          datetime.now().isoformat(),
            })
        except Exception as _se:
            print(f"[EDGE_FADE] save_bet error: {_se}")


def _get_player_baseline(player_name):
    """Pull season averages from the player_baseline learning_data cache."""
    try:
        conn = _db_conn()
        if not conn:
            return {}
        cur = conn.cursor()
        cur.execute("SELECT value FROM learning_data WHERE key=%s",
                    (f"player_baseline:{player_name}",))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            bl = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            return {
                "pts":  round(float(bl.get("avg_pts",  0) or 0), 1),
                "reb":  round(float(bl.get("avg_reb",  0) or 0), 1),
                "ast":  round(float(bl.get("avg_ast",  0) or 0), 1),
                "fg3m": round(float(bl.get("avg_fg3",  0) or 0), 1),
            }
    except Exception:
        pass
    return {}


def send_elite_player_props(game_name, game_legs):
    """
    Per-game Elite Player Props — sent to VIP once per game.
    Shows key player season averages for both teams + elite prop picks.
    """
    global _elite_props_sent_games

    if game_name in _elite_props_sent_games:
        return
    if not game_legs:
        return

    from collections import defaultdict

    # ── Parse teams from game_name "Away @ Home" ─────────────────────
    if " @ " in game_name:
        away_team, home_team = game_name.split(" @ ", 1)
        away_team = away_team.strip()
        home_team = home_team.strip()
    else:
        away_team, home_team = "Away", "Home"

    # ── Get key players per team via ESPN starters ────────────────────
    def _starters(team):
        try:
            return get_team_starters_espn(team)[:3]
        except Exception:
            return []

    away_starters = _starters(away_team)
    home_starters = _starters(home_team)

    # Fallback — pull from game_legs if ESPN data empty
    if not away_starters and not home_starters:
        leg_players = {}
        for leg in game_legs:
            p = leg.get("player", "")
            t = leg.get("team", "")
            if p and p not in leg_players:
                leg_players[p] = t
        away_starters = [{"name": p} for p, t in list(leg_players.items())[:3]
                         if away_team.lower() in t.lower()]
        home_starters  = [{"name": p} for p, t in list(leg_players.items())[:3]
                         if home_team.lower() in t.lower()]
        if not away_starters and not home_starters:
            mid = max(1, len(game_legs) // 2)
            away_starters = [{"name": l.get("player", "")} for l in game_legs[:mid] if l.get("player")]
            home_starters  = [{"name": l.get("player", "")} for l in game_legs[mid:] if l.get("player")]

    def _player_block(starters):
        lines = []
        for s in starters:
            name = s.get("name", "")
            if not name:
                continue
            bl   = _get_player_baseline(name)
            pts  = bl.get("pts",  s.get("pred_pts", 0))
            reb  = bl.get("reb",  s.get("pred_reb", 0))
            ast  = bl.get("ast",  s.get("pred_ast", 0))
            fg3m = bl.get("fg3m", s.get("pred_fg3", 0))
            pos  = s.get("position", "")
            pos_str = f" · {pos}" if pos else ""
            # Confidence dot based on avg pts vs league avg
            dot = "🟢" if pts >= 20 else ("🟡" if pts >= 12 else "🔴")
            lines.append(f"{dot} {name}{pos_str}")
            lines.append(f"🏀 {pts} pts · 💪 {reb} reb · 🔥 {ast} ast · 🎯 {fg3m} 3s")
        return lines

    # ── Elite Game Lines — game-level bets only (totals, spreads, moneylines) ──
    _GAME_LEVEL_TYPES = {"TOTAL", "SPREAD", "MONEYLINE", "ML", "OVER", "UNDER"}
    game_level_legs = [
        l for l in game_legs
        if (l.get("bet_type") or l.get("betType") or "").upper() in _GAME_LEVEL_TYPES
    ]

    seen_picks = set()
    top_picks  = []
    for leg in sorted(game_level_legs, key=lambda x: x.get("edge", 0), reverse=True):
        desc = leg.get("desc", "")
        if desc and desc not in seen_picks:
            top_picks.append(leg)
            seen_picks.add(desc)
        if len(top_picks) >= 5:
            break

    def _fmt_gl_odds(o):
        try:
            o = int(o)
            return f"+{o}" if o > 0 else str(o)
        except Exception:
            return "-110"

    pick_lines = []
    for leg in top_picks:
        odds_str = _fmt_gl_odds(leg.get("odds", -110))
        pick_lines.append(f"✅ {leg['desc']}  ({odds_str})")

    if not pick_lines:
        return  # nothing to send

    D = "━━━━━━━━━━━━━━━━━━━"

    away_block = _player_block(away_starters)
    home_block = _player_block(home_starters)

    msg_parts = [
        f"📊 *ELITE GAME LINES*",
        f"_{game_name}_",
        f"",
        f"━━━ {away_team.upper()} ━━━",
        *(away_block or ["  _No data yet_"]),
        f"",
        f"━━━ {home_team.upper()} ━━━",
        *(home_block or ["  _No data yet_"]),
        f"",
        f"━━━ 🎯 GAME LINES ━━━",
        *pick_lines,
        f"",
        D,
        f"⚡ Engine-cleared · Confidence ranked",
    ]

    send("\n".join(msg_parts), VIP_CHANNEL)
    _elite_props_sent_games.add(game_name)
    print(f"[EliteGameLines] Sent for {game_name} — {len(pick_lines)} game lines")


# ── PlayerRole system ─────────────────────────────────────────────────────
class PlayerRole:
    def __init__(self, name, primary_stats, secondary_stats):
        self.name            = name
        self.primary_stats   = primary_stats
        self.secondary_stats = secondary_stats
        self.legs            = {}
        self.confidence      = {}
        self.script_fit      = {}

    def assign_leg(self, stat, confidence, script_fit):
        if stat in self.primary_stats + self.secondary_stats:
            self.legs[stat] = {"confidence": confidence, "script_fit": script_fit}
        else:
            print(f"[PlayerRole] Warning: {stat} not tracked for {self.name}")

    def __repr__(self):
        return f"{self.name}: {self.legs}"


_STARTING_5 = [
    PlayerRole("PG", primary_stats=["assists", "points"],  secondary_stats=["rebounds", "threes"]),
    PlayerRole("SG", primary_stats=["points",  "threes"],  secondary_stats=["assists",  "rebounds"]),
    PlayerRole("SF", primary_stats=["points",  "rebounds"],secondary_stats=["assists",  "threes", "steals"]),
    PlayerRole("PF", primary_stats=["rebounds","points"],  secondary_stats=["assists",  "threes"]),
    PlayerRole("C",  primary_stats=["rebounds","blocks"],  secondary_stats=["points",   "assists"]),
]

# Prop-to-prop dependency bonuses — used in greedy sequential selection
_DEPENDENCY_PAIRS: dict = {
    ("points",   "assists"):  0.15,   # scorer → playmaker assists up
    ("assists",  "points"):   0.15,   # playmaker → scorer points up
    ("points",   "threes"):   0.10,   # high-vol scorer → threes follow
    ("threes",   "points"):   0.10,
    ("points",   "rebounds"): 0.08,   # star dominates → big cleans up
    ("rebounds", "points"):   0.08,
    ("assists",  "threes"):   0.12,   # distributor → shooter gets looks
    ("threes",   "assists"):  0.12,
    ("blocks",   "rebounds"): 0.10,   # rim presence → rebounds
    ("rebounds", "blocks"):   0.10,
}


def _normalize_pos(pos: str) -> str:
    """Normalize any position string to PG / SG / SF / PF / C."""
    p = (pos or "").upper().strip()
    if p in ("PG", "SG", "SF", "PF", "C"):
        return p
    if "PG" in p: return "PG"
    if "SG" in p: return "SG"
    if "SF" in p: return "SF"
    if "PF" in p: return "PF"
    if p in ("G",):               return "SG"
    if p in ("F",):               return "SF"
    if p in ("C", "CENTER"):      return "C"
    return ""


def _role_fit_score(bet_type: str, position: str) -> float:
    """1.0 = primary stat for position, 0.6 = secondary, 0.5 = unknown pos, 0.0 = doesn't fit."""
    role = next((r for r in _STARTING_5 if r.name == position), None)
    if not role:
        return 0.5
    bt = bet_type.lower()
    if bt in role.primary_stats:   return 1.0
    if bt in role.secondary_stats: return 0.6
    return 0.0


def _dep_bonus(candidate_type: str, selected_types: list) -> float:
    """Sum of dependency bonuses from already-selected legs to the candidate."""
    bonus = 0.0
    for sel in selected_types:
        bonus += _DEPENDENCY_PAIRS.get((sel, candidate_type), 0.0)
    return bonus


def _fd_label(prop_type: str, line, pick: str = "OVER") -> str:
    """Convert prop type + line to FanDuel-style description string."""
    import math as _m
    try:
        n = int(_m.ceil(float(line)))
    except Exception:
        n = int(line or 0)
    bt = (prop_type or "").lower()
    if bt in ("points", "pts"):        return f"TO SCORE {n}+ POINTS"
    if bt in ("rebounds", "reb"):      return f"TO RECORD {n}+ REBOUNDS"
    if bt in ("assists",  "ast"):      return f"TO RECORD {n}+ ASSISTS"
    if bt in ("threes", "fg3m", "3s"): return f"{n}+ MADE THREES"
    if bt in ("blocks",  "blk"):       return f"TO RECORD {n}+ BLOCKS"
    if bt in ("steals",  "stl"):       return f"TO RECORD {n}+ STEALS"
    if bt in ("pra",):                 return f"TO RECORD {n}+ PTS+REB+AST"
    if bt in ("pr",):                  return f"TO RECORD {n}+ PTS+REB"
    if bt in ("pa",):                  return f"TO RECORD {n}+ PTS+AST"
    return f"{pick.upper()} {n}+ {prop_type.upper()}"


# ── BDL position resolver (session-cached) ────────────────────────────────
_pos_bdl_cache: dict = {}  # {player_name_lower: normalized_pos}

def _resolve_position_bdl(player_name: str) -> str:
    """
    Look up a player's position from BDL using the existing _bdl_get helper.
    Results are cached per session so each player is only fetched once.
    Returns a normalized position string (PG/SG/SF/PF/C) or '' if unavailable.
    """
    if not BDL_API_KEY or not player_name:
        return ""
    key = player_name.strip().lower()
    if key in _pos_bdl_cache:
        return _pos_bdl_cache[key]
    try:
        import urllib.parse as _up
        parts = player_name.strip().split()
        search_term = parts[0] if parts else player_name
        url = f"{BDL_BASE}/players?search={_up.quote(search_term)}&per_page=10"
        res = _bdl_get(url)
        players = res.get("data", [])
        if not players:
            _pos_bdl_cache[key] = ""
            return ""
        # Prefer exact full-name match, fall back to first result
        match = next(
            (p for p in players if f"{p['first_name']} {p['last_name']}".lower() == key),
            players[0]
        )
        pos = _normalize_pos(match.get("position", ""))
        _pos_bdl_cache[key] = pos
        return pos
    except Exception as _e:
        print(f"[BDL pos] {player_name}: {_e}")
        _pos_bdl_cache[key] = ""
        return ""


def send_sgp_for_game(game_name, game_legs):
    """
    Hybrid per-game SGP — PlayerRole system + greedy sequential selection:
    - 🟢 SAFE: primary stats only, positive edge, dependency-chain ordered (2-4 legs)
    - 🟡 BALANCED: primary + secondary stats (4-6 legs)
    - 🔴 AGGRESSIVE: full script-passing pool (6-8 legs)
    FanDuel-style display: per-leg odds + combined odds. Edge stays internal.
    VIP LOCK is NEVER included. Each leg saved to DB for training.
    """
    global _sgp_sent_games, _vip_lock_desc

    if game_name in _sgp_sent_games:
        return

    # ── Player props only — strip VIP lock and game-level bets ───────
    _SGP_EXCLUDE_TYPES = {"TOTAL", "SPREAD", "MONEYLINE", "ML", "OVER", "UNDER"}
    pool = [
        l for l in game_legs
        if l.get("desc") != _vip_lock_desc
        and (l.get("bet_type") or l.get("betType") or "").upper() not in _SGP_EXCLUDE_TYPES
    ]
    if len(pool) < 2:
        return

    # ── Game data ─────────────────────────────────────────────────────
    game_data = _games_data.get(game_name, {})
    total     = float(game_data.get("total", 220) or 220)
    spread    = abs(float(game_data.get("spread", 5.0) or 5.0))

    # ── Dominant script selection ─────────────────────────────────────
    scripts = detect_all_game_scripts(game_data)
    _ck     = combo_key(scripts)
    _SCRIPT_SCORE = {
        "INJURY":            100,
        "TRANSITION_HEAVY":  total * 0.45,
        "UPTEMPO":           total * 0.40,
        "BLOWOUT":           spread * 9,
        "DOUBLE_DIGIT_LEAD": spread * 7,
        "TIGHT_GAME":        max(0, (6 - spread)) * 12,
        "HALFCOURT":         max(0, (215 - total)) * 0.5,
        "SLOW_PACED":        max(0, (218 - total)) * 0.4,
        "UPSET":             30,
        "COMPETITIVE":       10,
    }
    dominant_script = max(scripts, key=lambda sc: _SCRIPT_SCORE.get(sc, 0))

    SCRIPT_LABEL = {
        "TRANSITION_HEAVY":  "🟡 Transition/Shootout",
        "UPTEMPO":           "🟡 Uptempo",
        "BLOWOUT":           "🟢 Blowout",
        "DOUBLE_DIGIT_LEAD": "🟢 Double-Digit Lead",
        "COMFORTABLE_LEAD":  "🟩 Comfortable Lead",
        "TIGHT_GAME":        "🔵 Tight Game",
        "COMPETITIVE":       "🔵 Competitive",
        "HALFCOURT":         "⚫ Halfcourt/Grind",
        "SLOW_PACED":        "⚫ Slow-Paced",
        "AVERAGE_PACE":      "📊 Average Pace",
        "UPSET":             "🔴 Underdog Upset",
        "INJURY":            "🟣 Injury / Usage Spike",
    }
    _SCRIPT_REASON = {
        "TRANSITION_HEAVY":  f"Vegas total {total:.1f} — market pricing a shootout",
        "UPTEMPO":           f"Vegas total {total:.1f} — fast-paced scoring expected",
        "BLOWOUT":           f"Spread {spread:.1f} — market expects a comfortable win",
        "DOUBLE_DIGIT_LEAD": f"Spread {spread:.1f} — one team priced to pull away",
        "TIGHT_GAME":        f"Spread {spread:.1f} — wire-to-wire battle expected",
        "HALFCOURT":         f"Vegas total {total:.1f} — defensive grind tonight",
        "SLOW_PACED":        f"Vegas total {total:.1f} — slow pace, low scoring",
        "UPSET":             "Model disagrees with Vegas — underdog value detected",
        "INJURY":            "Key injury changes usage — targeting role player spikes",
        "COMPETITIVE":       f"Spread {spread:.1f}, total {total:.1f} — balanced matchup",
    }
    _script_display = SCRIPT_LABEL.get(dominant_script, dominant_script)
    _script_reason  = _SCRIPT_REASON.get(dominant_script, "")

    # ── Build position lookup from ESPN starters ──────────────────────
    if " @ " in game_name:
        _away, _home = game_name.split(" @ ", 1)
        _away = _away.strip(); _home = _home.strip()
    else:
        _away, _home = "Away", "Home"
    _pos_lookup: dict = {}
    for _team in (_away, _home):
        try:
            for _s in get_team_starters_espn(_team):
                _n = (_s.get("name") or "").lower()
                if _n:
                    _pos_lookup[_n] = _normalize_pos(_s.get("position", ""))
        except Exception:
            pass

    def _get_pos(leg):
        # 1. Position already stored on the leg (from run_full_system)
        pos = _normalize_pos(leg.get("position", ""))
        if pos:
            return pos
        # Resolve player name for lookups
        player = (leg.get("player") or "").strip()
        if not player:
            desc = leg.get("desc", "")
            for kw in (" OVER ", " UNDER ", " over ", " under "):
                if kw in desc:
                    player = desc[:desc.index(kw)].strip()
                    break
        # 2. ESPN starters lookup (already built from game data)
        pos = _pos_lookup.get(player.lower(), "")
        if pos:
            return pos
        # 3. BDL fallback (session-cached)
        if player:
            pos = _resolve_position_bdl(player)
        return pos

    # ── Sort by edge descending, dedup by player+stat (not player-only) ──
    # Allows multiple props per player (e.g. Carter rebounds + Carter points)
    import random as _random
    pool_sorted = sorted(pool, key=lambda x: x.get("edge", 0), reverse=True)
    _seen: set = set()
    _deduped: list = []
    for _leg in pool_sorted:
        _p = (_leg.get("player") or "").strip()
        if not _p:
            _d = _leg.get("desc", "")
            for _kw in (" OVER ", " UNDER ", " over ", " under "):
                if _kw in _d:
                    _p = _d[:_d.index(_kw)].strip()
                    break
        _bt = ((_leg.get("bet_type") or _leg.get("betType") or "")).lower()
        _pk = f"{_p.lower()}|{_bt}" if _p else ""
        if _pk and _pk in _seen:
            continue
        if _pk:
            _seen.add(_pk)
        _deduped.append(_leg)
    pool_sorted = _deduped

    # ── Script filter — all legs must fit the dominant script story ───
    script_pool = [l for l in pool_sorted if fits_script(l, dominant_script)]
    if len(script_pool) < 2:
        script_pool = pool_sorted  # graceful fallback

    # ── Greedy sequential selection ───────────────────────────────────
    # score = edge × role_fit + dependency_bonus
    # role_filter: "primary" = SAFE, "primary+secondary" = BALANCED, "all" = AGGRESSIVE
    def _leg_score(leg, selected, role_filter):
        bt  = (leg.get("bet_type") or "").lower()
        pos = _get_pos(leg)
        rf  = _role_fit_score(bt, pos)
        if role_filter == "primary" and rf < 1.0:
            return -1.0
        if role_filter == "primary+secondary" and rf == 0.0:
            return -1.0
        sel_types = [(s.get("bet_type") or "").lower() for s in selected]
        dep       = _dep_bonus(bt, sel_types)
        edge      = max(leg.get("edge", 0), 0)
        return edge * max(rf, 0.1) + dep

    def _greedy_pick(candidates, size, role_filter):
        selected  = []
        remaining = list(candidates)
        for _ in range(size):
            if not remaining:
                break
            scores = [(l, _leg_score(l, selected, role_filter)) for l in remaining]
            scores = [(l, s) for l, s in scores if s >= 0]
            if not scores:
                break
            best = max(scores, key=lambda x: x[1])[0]
            selected.append(best)
            remaining.remove(best)
        return selected

    safe_size = _random.randint(2, 4)
    bal_size  = _random.randint(4, 6)
    agg_size  = _random.randint(6, 8)

    safe       = _greedy_pick(script_pool, safe_size, "primary")
    balanced   = _greedy_pick(script_pool, bal_size,  "primary+secondary")
    aggressive = _greedy_pick(script_pool, agg_size,  "all")

    if len(safe) < 2:       safe       = script_pool[:safe_size]
    if len(balanced) < 4:   balanced   = script_pool[:min(bal_size,  len(script_pool))]
    if len(aggressive) < 4: aggressive = script_pool[:min(agg_size,  len(script_pool))]

    # ── FanDuel-style leg formatter ───────────────────────────────────
    _ICON = {
        "points": "🏀", "rebounds": "💪", "assists": "🔥",
        "threes": "🎯", "blocks": "🛡️", "steals": "⚡",
    }

    def _fmt_odds(o):
        try:
            o = float(o)
            # Decimal odds detection: valid American odds are >= 100 or <= -100
            # Decimal format (e.g. 1.909, 2.0, 2.5) falls in range 1.01–30
            if 1.01 <= o <= 30:
                if o >= 2.0:
                    o = int(round((o - 1) * 100))   # 2.0 → +100, 2.5 → +150
                else:
                    o = int(round(-100 / (o - 1)))  # 1.5 → -200, 1.91 → -110
            else:
                o = int(o)
            return f"+{o}" if o > 0 else str(o)
        except Exception:
            return "-110"

    def _ls(leg):
        player = (leg.get("player") or "").strip()
        if not player:
            desc = leg.get("desc", "")
            for kw in (" OVER ", " UNDER ", " over ", " under "):
                if kw in desc:
                    player = desc[:desc.index(kw)].strip()
                    break
        if not player:
            player = leg.get("desc", "Unknown")
        bt       = (leg.get("bet_type") or "").lower()
        label    = _fd_label(bt, leg.get("line", 0), leg.get("pick", "OVER"))
        odds_str = _fmt_odds(leg.get("odds", -110))
        icon     = _ICON.get(bt, "🎯")
        return f"  {icon} {player} — {label}  ({odds_str})"

    safe_odds = _parlay_odds(len(safe))
    bal_odds  = _parlay_odds(len(balanced))
    agg_odds  = _parlay_odds(len(aggressive))
    D = "━━━━━━━━━━━━━━━━━━━"

    msg = "\n".join([
        f"🎲 *ELITE SGP — {game_name}*",
        f"_Script: {_script_display}_",
        f"_{_script_reason}_",
        f"",
        f"🟢 *SAFE ({len(safe)} legs) → +{safe_odds:,}*",
        *[_ls(l) for l in safe],
        f"",
        f"🟡 *BALANCED ({len(balanced)} legs) → +{bal_odds:,}*",
        *[_ls(l) for l in balanced],
        f"",
        f"🔴 *AGGRESSIVE ({len(aggressive)} legs) → +{agg_odds:,}*",
        *[_ls(l) for l in aggressive],
        f"",
        D,
        f"⚡ All from same game · FanDuel · Props only",
    ])

    send(msg, VIP_CHANNEL)
    _sgp_sent_games.add(game_name)
    save_memory_state()

    # ── Save all SGP legs to DB for training ─────────────────────────
    all_sgp_legs = {l["desc"]: l for l in safe + balanced + aggressive}
    for leg in all_sgp_legs.values():
        save_bet({
            "game":          game_name,
            "player":        leg.get("player") or leg.get("desc", ""),
            "pick":          leg.get("desc", ""),
            "betType":       "SGP",
            "pick_category": "SGP",
            "line":          leg.get("line"),
            "prediction":    None,
            "odds":          leg.get("odds", 0),
            "prob":          round(leg.get("confidence", 70) / 100, 4),
            "edge":          round(leg.get("edge", 0), 2),
            "confidence":    leg.get("confidence", 0),
            "time":          str(datetime.now()),
            "result":        None,
            "script":        dominant_script,
            "script_combo":  _ck,
        })

    print(f"[SGP] {game_name} · Dominant:{dominant_script} · {_ck} · {len(safe)}S/{len(balanced)}B/{len(aggressive)}A · {len(all_sgp_legs)} legs saved")


# ==========================
# 🔴 LOOP / ENTRY
# ==========================
def _register_commands():
    """Register bot commands with Telegram so they appear in the / menu."""
    if not BOT_TOKEN:
        return

    public_commands = [
        {"command": "start",      "description": "Help & info"},
        {"command": "help",       "description": "Help & info"},
        {"command": "picks",      "description": "Today's picks"},
        {"command": "schedule",   "description": "Today's NBA schedule"},
        {"command": "record",     "description": "Bot win/loss record"},
        {"command": "subscribe",  "description": "Join VIP ($29/mo — 7-day free trial)"},
        {"command": "vip",        "description": "Join VIP ($29/mo — 7-day free trial)"},
        {"command": "join",       "description": "Join VIP ($29/mo — 7-day free trial)"},
        {"command": "thresholds", "description": "Confidence thresholds"},
    ]

    admin_commands = public_commands + [
        {"command": "admins",         "description": "System health panel"},
        {"command": "todaypicks",     "description": "Full detailed card of today's picks"},
        {"command": "forcesettle",    "description": "Run full settlement pass on all pending picks"},
        {"command": "debugsettle",    "description": "Debug prop settlement — dumps sample rows + live BDL test"},
        {"command": "voidpending",    "description": "Void pending picks — /voidpending or /voidpending YYYY-MM-DD"},
        {"command": "checkpending",   "description": "All unsettled picks grouped by date"},
        {"command": "props",          "description": "Prop breakdown per team (/props lakers celtics)"},
        {"command": "sgp",            "description": "Generate & post an SGP pick"},
        {"command": "parlay",         "description": "Generate & post a parlay pick"},
        {"command": "feedpick",       "description": "Post a manual feed pick"},
        {"command": "editfeedpick",   "description": "Edit a feed pick (/editfeedpick <id> <text>)"},
        {"command": "deletefeedpick", "description": "Delete a feed pick (/deletefeedpick <id>)"},
        {"command": "updatefeed",     "description": "Live feed pick status"},
        {"command": "updateml",       "description": "Live ML pick status"},
        {"command": "updateprops",    "description": "Live props status"},
        {"command": "updatesgp",      "description": "Live SGP status"},
        {"command": "updatecgp",      "description": "Live CGP status"},
        {"command": "updateedge",     "description": "Live Edge-Fade status"},
        {"command": "checkpick",      "description": "Check a pick result (/checkpick <id>)"},
        {"command": "settle",         "description": "Settle a pick manually (/settle <id> W/L)"},
        {"command": "bankroll",       "description": "Bankroll info"},
        {"command": "historyfeed",    "description": "Feed pick history"},
        {"command": "historybot",     "description": "Bot pick history"},
        {"command": "historylive",    "description": "Live pick history"},
        {"command": "calibrate",      "description": "Calibrate model"},
        {"command": "linemonitor",    "description": "Monitor line movement"},
        {"command": "dbstatus",       "description": "DB table counts, thresholds & learning state"},
        {"command": "analyzedrop",    "description": "Analyze settled bets (/analyzedrop or /analyzedrop 50 or /analyzedrop 100-200)"},
        {"command": "resendall",      "description": "Clear today's sent flags and refire VIP LOCK + props + SGP + CGP"},
    ]

    base = f"https://api.telegram.org/bot{BOT_TOKEN}"
    try:
        # Public commands — visible to everyone
        requests.post(f"{base}/setMyCommands", json={
            "commands": public_commands,
            "scope": {"type": "default"},
        }, timeout=10)
        # Admin commands — visible only in admin's private chat
        requests.post(f"{base}/setMyCommands", json={
            "commands": admin_commands,
            "scope": {"type": "chat", "chat_id": int(ADMIN_ID)},
        }, timeout=10)
        print("[Bot] Commands registered with Telegram")
    except Exception as e:
        print(f"[Bot] setMyCommands error: {e}")


def main():
    global _auto_adjust_done_date
    _db_init()
    restore_memory_state()   # reload all critical state + learning data from DB
    _register_commands()

    # ── Graceful shutdown: save all state on SIGTERM / SIGINT ─────────────
    # Railway sends SIGTERM before killing the process on deploy/restart.
    # This ensures no sends are duplicated and no learning data is lost.
    import signal as _signal
    def _graceful_shutdown(signum, frame):
        print(f"[Shutdown] Signal {signum} received — saving state before exit")
        try:
            save_memory_state()
        except Exception as _se:
            print(f"[Shutdown] save_memory_state error: {_se}")
        try:
            reply(ADMIN_ID, "🔄 Bot restarting (Railway deploy/restart) — state saved.")
        except Exception:
            pass
        raise SystemExit(0)

    import threading as _threading
    if _threading.current_thread() is _threading.main_thread():
        _signal.signal(_signal.SIGTERM, _graceful_shutdown)
        _signal.signal(_signal.SIGINT,  _graceful_shutdown)
    else:
        print("[Shutdown] Running in daemon thread — signal handlers skipped (Railway handles SIGTERM at process level)")

    # ── Startup env-var health check ──────────────────────────────────────
    _missing_vars = []
    if not BOT_TOKEN:
        _missing_vars.append("BOT_TOKEN")
    if not VIP_CHANNEL:
        _missing_vars.append("VIP_CHANNEL")
    if not FREE_CHANNEL:
        _missing_vars.append("FREE_CHANNEL")
    if not os.environ.get("DATABASE_URL"):
        _missing_vars.append("DATABASE_URL")

    if _missing_vars:
        _warn_msg = (
            f"🚨 *BOT STARTUP — MISSING ENV VARS*\n\n"
            f"The following critical variables are not set:\n"
            + "\n".join(f"  ❌ `{v}`" for v in _missing_vars)
            + "\n\nVIP/Free channel sends will be silently dropped "
            f"until these are configured in Railway."
        )
        print(f"[Startup] MISSING ENV VARS: {_missing_vars}")
        try:
            if BOT_TOKEN and ADMIN_ID:
                send(_warn_msg, str(ADMIN_ID))
        except Exception as _sve:
            print(f"[Startup] Could not send env-var warning: {_sve}")
    else:
        _ok_msg = (
            f"✅ *BOT ONLINE*\n\n"
            f"All env vars confirmed. Channels:\n"
            f"  • VIP: `{VIP_CHANNEL}`\n"
            f"  • Free: `{FREE_CHANNEL}`\n\n"
            f"🕐 Started: {datetime.now(ET).strftime('%-I:%M %p ET')}"
        )
        print("[Startup] All env vars OK — sending startup DM")
        try:
            send(_ok_msg, str(ADMIN_ID))
        except Exception as _sve:
            print(f"[Startup] Could not send startup DM: {_sve}")

    # ── Startup learning sync ──────────────────────────────────────────────
    # If the bot restarted during the day, any picks that settled overnight
    # need to be processed into the model before picks go out.
    # Check if _auto_adjust_model ran today; if not, run it now in background.
    try:
        import json as _json
        _startup_conn = _db_conn()
        if _startup_conn:
            _sc = _startup_conn.cursor()
            _sc.execute(
                "SELECT value FROM learning_data WHERE key = 'last_auto_adjust_date'"
            )
            _adj_row = _sc.fetchone()
            _sc.close()
            _startup_conn.close()
            _last_adj = (_adj_row[0] if isinstance(_adj_row[0], str) else
                         _json.dumps(_adj_row[0])) if _adj_row else ""
            _today_adj = datetime.now(ET).strftime("%Y-%m-%d")
            if _last_adj.strip('"') != _today_adj:
                print("[Startup] _auto_adjust_model hasn't run today — running now")
                def _startup_adjust():
                    try:
                        _auto_adjust_model()
                        # Record that it ran
                        _c = _db_conn()
                        if _c:
                            _cu = _c.cursor()
                            _cu.execute("""
                                INSERT INTO learning_data (key, value, updated_at)
                                VALUES ('last_auto_adjust_date', %s::jsonb, NOW())
                                ON CONFLICT (key) DO UPDATE SET
                                    value = EXCLUDED.value, updated_at = NOW()
                            """, (_json.dumps(_today_adj),))
                            _c.commit()
                            _cu.close()
                            _c.close()
                    except Exception as _sae:
                        print(f"[Startup] auto_adjust error: {_sae}")
                threading.Thread(target=_startup_adjust, daemon=True).start()
            else:
                print(f"[Startup] _auto_adjust_model already ran today ({_today_adj}) — skipping")
    except Exception as _sae:
        print(f"[Startup] auto_adjust check error: {_sae}")

    once = "--once" in sys.argv

    if once:
        print("Running bot once...")
        n = run()
        update_results()
        retrain_from_results()
        print(f"Done. {n} picks found.")
    else:
        print("🔥 BOT RUNNING — scanning every 10 minutes")
        try:
            reply(ADMIN_ID, "✅ Bot online and scanning.")
        except Exception:
            pass

        def _watch_commands():
            """Watchdog: restarts handle_commands if the thread ever dies."""
            while True:
                t = threading.Thread(target=handle_commands, daemon=True)
                t.start()
                t.join()  # blocks until thread exits
                print("[WATCHDOG] Command thread died — restarting in 5s")
                try:
                    reply(ADMIN_ID, "⚠️ Command listener restarted.")
                except Exception:
                    pass
                time.sleep(5)

        threading.Thread(target=_watch_commands, daemon=True).start()
        # Volume-triggered retrain: accumulate newly-settled bets across cycles,
        # retrain the full sklearn model once ≥ RETRAIN_THRESHOLD new bets settle.
        # Time-based fallback: also retrain every RETRAIN_MAX_CYCLES cycles in case
        # games are slow to settle (e.g. quiet night).
        RETRAIN_THRESHOLD  = 10   # settled bets since last retrain → trigger full retrain
        RETRAIN_MAX_CYCLES = 6    # fallback: retrain at least every 60 minutes
        settled_since_retrain = 0
        cycle = 0
        while True:
            try:
                n = run()
                newly = update_results()
                settled_since_retrain += (newly or 0)

                # ── Bias update: lightweight, every cycle ────────────────────────
                update_prediction_bias()

                send_results_recap()
                send_monthly_report()
                send_free_preview()
                send_avoid_list()
                send_daily_system()
                run_edge_fade_7()
                _nightly_pick_check()  # 1-3 AM ET: auto-check feedpick legs vs BDL

                # ── Dynamic game window — bot uses real game state, no clock guards ──
                _live_scores  = get_live_scores()
                _any_live     = any(g.get("status") == "in"  for g in _live_scores)
                _any_upcoming = any(g.get("status") == "pre" for g in _live_scores)
                _all_done     = bool(_live_scores) and all(
                    g.get("status") == "post" for g in _live_scores
                )

                # ── Live mid-game tracker + full game observer ────────────────
                # Fires when any game is live OR pre-game (not by clock time)
                if _any_live or _any_upcoming:
                    try:
                        _live_pick_tracker()
                    except Exception as _lpt_err:
                        print(f"[LiveTracker] unhandled error: {_lpt_err}")
                    try:
                        _watch_all_live_games()
                    except Exception as _obs_err:
                        print(f"[Observer] unhandled error: {_obs_err}")
                    try:
                        _cdn_live_tracker()
                    except Exception as _cdn_err:
                        print(f"[CDN] unhandled error: {_cdn_err}")

                # ── Auto-adjust: fires once when all tonight's games are Final ─
                # No clock guard — bot detects Final state from live data directly
                _adj_today = datetime.now(ET).strftime("%Y-%m-%d")
                if _all_done and _auto_adjust_done_date != _adj_today:
                    try:
                        _auto_adjust_done_date = _adj_today
                        _auto_adjust_model()
                        # Stamp to DB so a restart knows it already ran today
                        try:
                            import json as _json
                            _stamp_c = _db_conn()
                            if _stamp_c:
                                _stamp_cu = _stamp_c.cursor()
                                _stamp_cu.execute("""
                                    INSERT INTO learning_data (key, value, updated_at)
                                    VALUES ('last_auto_adjust_date', %s::jsonb, NOW())
                                    ON CONFLICT (key) DO UPDATE SET
                                        value = EXCLUDED.value, updated_at = NOW()
                                """, (_json.dumps(_adj_today),))
                                _stamp_c.commit()
                                _stamp_cu.close()
                                _stamp_c.close()
                        except Exception:
                            pass
                    except Exception as _adj_err:
                        print(f"[AutoAdjust] scheduler error: {_adj_err}")

                # ── Precision-timed Prop Wave — fires once at tip-2h ─────────────
                # Fetches all game props in one FanDuel batch, scores them, sends
                # per-game Elite Game Lines + SGPs, then builds the CGP.
                if _should_fire_prop_wave():
                    try:
                        _fire_prop_wave()
                    except Exception as _pw_err:
                        print(f"[PropWave] error: {_pw_err}")

                cycle += 1

                # ── Full retrain: volume-triggered OR time-based fallback ────────
                do_retrain = (settled_since_retrain >= RETRAIN_THRESHOLD
                              or cycle % RETRAIN_MAX_CYCLES == 0)
                if do_retrain:
                    print(f"  [retrain] Triggered — {settled_since_retrain} new settlements"
                          f" / cycle {cycle}")
                    retrain_from_results()
                    settled_since_retrain = 0

                save_memory_state()   # persist line_history + all critical state
                print(f"[{datetime.now()}] Cycle done. {n} picks, {newly or 0} settled. Sleeping 3 min...")
            except Exception as loop_err:
                import traceback as _tb
                _full_tb = _tb.format_exc()
                print(f"[LOOP ERROR] {loop_err} — recovering in 60s")
                print(f"[LOOP TRACEBACK]\n{_full_tb}")   # full stack visible in Railway logs
                try:
                    _err_short = str(loop_err)[:300]
                    # Extract last two lines of traceback for Telegram summary
                    _tb_lines = [l for l in _full_tb.strip().splitlines() if l.strip()]
                    _loc = " | ".join(_tb_lines[-3:])[:400] if len(_tb_lines) >= 2 else ""
                    send_telegram(
                        f"⚠️ Bot cycle error (auto-recovering):\n`{_err_short}`\n\n_{_loc}_",
                        ADMIN_ID
                    )
                except Exception:
                    pass
                try:
                    save_memory_state()   # always persist state before recovery sleep
                except Exception:
                    pass
                time.sleep(60)
                continue
            time.sleep(180)

if __name__ == "__main__":
    main()
