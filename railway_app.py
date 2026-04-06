import threading
import os
import json
import subprocess
import sys
from datetime import datetime
from flask import Flask, jsonify, request, redirect, send_from_directory, send_file

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BETS_FILE = os.path.join(BASE_DIR, "bets.json")
STATUS_FILE = os.path.join(BASE_DIR, "bot_status.json")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


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
        print("[DB] No DATABASE_URL — using JSON fallback")
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                game TEXT, player TEXT, pick TEXT, bet_type TEXT,
                line REAL, prediction REAL, odds REAL, prob REAL,
                edge REAL, confidence REAL, result TEXT,
                bet_time TIMESTAMP, created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'BALANCED'")
        cur.execute("ALTER TABLE bets ADD COLUMN IF NOT EXISTS script VARCHAR(20) DEFAULT 'NORMAL'")
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
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Tables ready ✓")
    except Exception as e:
        print(f"[DB] init error: {e}")
        try: conn.close()
        except Exception: pass


def _try_parse(v):
    try: return json.loads(v)
    except Exception: return v


def load_bets():
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT game, player, pick, bet_type, line, prediction, odds, prob, edge, confidence, result, bet_time, tier, script FROM bets ORDER BY created_at ASC")
            rows = cur.fetchall()
            cur.close(); conn.close()
            keys = ["game","player","pick","betType","line","prediction","odds","prob","edge","confidence","result","time","tier","script"]
            return [dict(zip(keys, r)) for r in rows]
        except Exception as e:
            print(f"[DB] load_bets error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        if os.path.exists(BETS_FILE):
            with open(BETS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return []


def load_status():
    conn = _db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM bot_status")
            rows = cur.fetchall()
            cur.close(); conn.close()
            result = {"lastRun": None, "picksToday": 0}
            for k, v in rows:
                result[k] = _try_parse(v)
            return result
        except Exception as e:
            print(f"[DB] load_status error: {e}")
            try: conn.close()
            except Exception: pass
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {"lastRun": None, "picksToday": 0}


_db_init()


def normalize_bet(b):
    return {
        "game": b.get("game", ""),
        "pick": b.get("pick", ""),
        "betType": b.get("betType", "MONEYLINE"),
        "line": b.get("line"),
        "prediction": b.get("prediction"),
        "odds": b.get("odds", 0),
        "prob": b.get("prob", 0),
        "edge": b.get("edge", 0),
        "time": b.get("time", ""),
        "result": b.get("result"),
        "confidence": b.get("confidence"),
        "signal": b.get("signal"),
        "bet_size": b.get("bet_size"),
        "sharp": b.get("sharp"),
        "movement": b.get("movement"),
    }


# ── CORS header for all responses ─────────────────────────────────────────────
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response


@app.route("/health", methods=["GET"])
def healthcheck():
    return jsonify({"ok": True})


# ── Bot status ─────────────────────────────────────────────────────────────────
@app.route("/api/bot/status", methods=["GET"])
def bot_status():
    status = load_status()
    return jsonify({
        "running": bot_thread.is_alive(),
        "lastRun": status.get("lastRun"),
        "picksToday": status.get("picksToday", 0),
    })


# ── Run bot once ───────────────────────────────────────────────────────────────
@app.route("/api/bot/run", methods=["POST", "OPTIONS"])
def bot_run():
    if request.method == "OPTIONS":
        return jsonify({}), 200

    before_keys = set()
    for b in load_bets():
        before_keys.add(f"{b.get('game')}|{b.get('pick')}|{b.get('betType','MONEYLINE')}")

    bot_script = os.path.join(BASE_DIR, "bot", "bot.py")
    python = sys.executable
    try:
        result = subprocess.run(
            [python, bot_script, "--once"],
            timeout=90,
            capture_output=True,
            text=True,
        )
        success = result.returncode == 0
    except subprocess.TimeoutExpired:
        success = False
    except Exception:
        success = False

    all_bets = load_bets()
    new_bets = [
        normalize_bet(b) for b in all_bets
        if f"{b.get('game')}|{b.get('pick')}|{b.get('betType','MONEYLINE')}" not in before_keys
    ]
    count = len(new_bets)

    settled = [b for b in all_bets if b.get("result") in ("WIN", "LOSS", "win", "loss")]
    wins = sum(1 for b in settled if b.get("result", "").upper() == "WIN")
    win_rate = round((wins / len(settled)) * 100) if settled else 0
    roi = round(count * 0.05, 2)

    message = (
        "Bot encountered an error" if not success
        else "No new edges found this scan" if count == 0
        else f"Found {count} new pick{'s' if count != 1 else ''}"
    )

    return jsonify({
        "success": success,
        "message": message,
        "count": count,
        "picksFound": count,
        "win_rate": win_rate,
        "roi": roi,
        "picks": new_bets,
    })


# ── Bets list ──────────────────────────────────────────────────────────────────
@app.route("/api/bets", methods=["GET"])
def get_bets():
    bets = [normalize_bet(b) for b in load_bets()]
    return jsonify({"bets": bets})


# ── Bet stats ──────────────────────────────────────────────────────────────────
@app.route("/api/bets/stats", methods=["GET"])
def get_bet_stats():
    bets = load_bets()
    wins = losses = pending = 0
    bankroll = 0.0
    history = []
    for b in bets:
        r = (b.get("result") or "").upper()
        if r == "WIN":
            wins += 1
            o = b.get("odds", 0) or 0
            bankroll += (o / 100) if o > 0 else (100 / abs(o)) if o != 0 else 0.91
            history.append(round(bankroll, 2))
        elif r == "LOSS":
            losses += 1
            bankroll -= 1
            history.append(round(bankroll, 2))
        else:
            pending += 1
    settled = wins + losses
    win_rate = round((wins / settled) * 100, 1) if settled else 0.0
    return jsonify({
        "totalBets": len(bets),
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "winRate": win_rate,
        "roi": round(bankroll, 2),
        "bankrollHistory": history,
    })


# ── Streak ─────────────────────────────────────────────────────────────────────
@app.route("/api/bets/streak", methods=["GET"])
def get_streak():
    bets = load_bets()
    settled = sorted(
        [b for b in bets if b.get("result") in ("win","loss","WIN","LOSS")],
        key=lambda b: b.get("time",""), reverse=True
    )[:10]
    wins = sum(1 for b in settled if b.get("result","").upper() == "WIN")
    losses = len(settled) - wins
    streak = 0
    streak_type = None
    for b in settled:
        r = "W" if b.get("result","").upper() == "WIN" else "L"
        if streak_type is None:
            streak_type = r
            streak = 1
        elif r == streak_type:
            streak += 1
        else:
            break
    return jsonify({
        "last10": [normalize_bet(b) for b in settled],
        "wins": wins,
        "losses": losses,
        "streak": streak,
        "streakType": streak_type,
    })


# ── Bets by type ───────────────────────────────────────────────────────────────
@app.route("/api/bets/by-type", methods=["GET"])
def get_bets_by_type():
    bets = load_bets()
    types = ["MONEYLINE", "SPREAD", "OVER", "UNDER"]
    result = {}
    for t in types:
        group = [b for b in bets if (b.get("betType") or "MONEYLINE") == t]
        w = sum(1 for b in group if (b.get("result") or "").upper() == "WIN")
        l = sum(1 for b in group if (b.get("result") or "").upper() == "LOSS")
        p = sum(1 for b in group if not b.get("result"))
        settled = w + l
        result[t] = {
            "wins": w, "losses": l, "pending": p,
            "winRate": round((w / settled) * 100, 1) if settled else 0
        }
    return jsonify(result)


# ── Schedule ───────────────────────────────────────────────────────────────────
@app.route("/api/schedule", methods=["GET"])
def get_schedule():
    import urllib.request
    bdl_key = os.environ.get("BDL_API_KEY", "")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        url = f"https://api.balldontlie.io/v1/games?dates[]={today}&per_page=15"
        req = urllib.request.Request(url, headers={"Authorization": bdl_key})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        games = [
            {
                "id": g["id"],
                "date": g.get("date", ""),
                "homeTeam": g.get("home_team", {}).get("full_name", ""),
                "awayTeam": g.get("visitor_team", {}).get("full_name", ""),
                "homeScore": g.get("home_team_score", 0),
                "awayScore": g.get("visitor_team_score", 0),
                "status": g.get("status", "Scheduled"),
            }
            for g in data.get("data", [])
        ]
        return jsonify({"games": games, "date": today})
    except Exception as e:
        return jsonify({"games": [], "date": today, "error": str(e)})


# ── Revenue (Stripe) ───────────────────────────────────────────────────────────
@app.route("/api/revenue", methods=["GET"])
def get_revenue():
    stripe_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not stripe_key:
        return jsonify({"subscribers": 0, "mrr": 0, "currency": "usd", "stripeReady": False})
    try:
        import stripe
        stripe.api_key = stripe_key.strip().replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace(" ", "")
        subs = stripe.Subscription.list(status="active", limit=100)
        subscribers = len(subs.data)
        mrr = 0
        for s in subs.data:
            item = s["items"]["data"][0] if s["items"]["data"] else None
            if item:
                amount = item["price"].get("unit_amount", 0) or 0
                interval = (item["price"].get("recurring") or {}).get("interval", "month")
                mrr += amount / 12 if interval == "year" else amount
        return jsonify({"subscribers": subscribers, "mrr": mrr / 100, "currency": "usd", "stripeReady": True})
    except Exception as e:
        return jsonify({"subscribers": 0, "mrr": 0, "currency": "usd", "stripeReady": False, "error": str(e)})


# ── Players ────────────────────────────────────────────────────────────────────
@app.route("/api/players", methods=["GET"])
def get_players():
    return jsonify({"players": []})


# ── Health for /api ────────────────────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({"ok": True})


# ── Stripe checkout ────────────────────────────────────────────────────────────
@app.route("/create-checkout-session", methods=["POST", "GET"])
def create_checkout():
    stripe_key = os.environ.get("STRIPE_SECRET_KEY", "")
    price_id = os.environ.get("STRIPE_PRICE_ID", "")
    if not stripe_key or not price_id:
        return jsonify({"error": "Stripe not configured"}), 500
    try:
        import stripe
        stripe.api_key = stripe_key.strip().replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace(" ", "")
        railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN",
                                       "asset-manager-production-0f7d.up.railway.app")
        domain = "https://" + railway_domain
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            subscription_data={"trial_period_days": 7},
            success_url=domain + "/dashboard",
            cancel_url=domain + "/",
        )
        return redirect(session.url, code=303)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/join", methods=["GET"])
def join():
    return create_checkout()


# ── Dashboard static files ─────────────────────────────────────────────────────
DASHBOARD_DIR = os.path.join(BASE_DIR, "artifacts", "betting-dashboard", "dist", "public")

@app.route("/assets/<path:filename>")
def root_assets(filename):
    return send_from_directory(os.path.join(DASHBOARD_DIR, "assets"), filename)

@app.route("/dashboard/assets/<path:filename>")
def dashboard_assets(filename):
    return send_from_directory(os.path.join(DASHBOARD_DIR, "assets"), filename)

@app.route("/dashboard/", defaults={"path": ""})
@app.route("/dashboard/<path:path>")
def dashboard(path):
    if os.path.exists(DASHBOARD_DIR):
        return send_from_directory(DASHBOARD_DIR, "index.html")
    return jsonify({"error": "Dashboard not built"}), 404

@app.route("/", methods=["GET"])
def root():
    if os.path.exists(os.path.join(DASHBOARD_DIR, "index.html")):
        return send_from_directory(DASHBOARD_DIR, "index.html")
    return jsonify({"status": "running", "bot": bot_thread.is_alive()})


# ── Start bot in background ────────────────────────────────────────────────────
def _start_bot():
    import time, traceback
    while True:
        try:
            import bot.bot as bot_module
            bot_module.main()
            print("[railway] Bot exited cleanly — stopping thread", flush=True)
            break
        except SystemExit:
            print("[railway] Bot SystemExit — stopping thread", flush=True)
            break
        except Exception as e:
            print(f"[railway] Bot crashed: {e}", flush=True)
            traceback.print_exc()
            print("[railway] Restarting bot in 30s...", flush=True)
            time.sleep(30)


bot_thread = threading.Thread(target=_start_bot, daemon=True, name="BettingBot")
bot_thread.start()
print("[railway] Betting bot thread started", flush=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
