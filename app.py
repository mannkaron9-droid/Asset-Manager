from flask import Flask, jsonify, request
import requests
import os
import random
import time

app = Flask(__name__)

# ==========================
# 🔐 CONFIG (SET IN SECRETS)
# ==========================
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

# ==========================
# 💰 BANKROLL SETTINGS
# ==========================
BANKROLL = 1000
EDGE_THRESHOLD = 0.05

# ==========================
# 🧠 BOT ENGINE
# ==========================
def run_bot():
    try:
        print("🚀 Bot starting...")

        url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?regions=us&markets=h2h&apiKey={ODDS_API_KEY}"
        res = requests.get(url)

        if res.status_code != 200:
            return {"error": "API FAILED", "details": res.text}

        games = res.json()

        picks = []
        for game in games:
            home = game["home_team"]
            away = game["away_team"]

            # FAKE EDGE MODEL (replace later with real AI)
            edge = random.uniform(0, 0.1)

            if edge > EDGE_THRESHOLD:
                pick = {
                    "game": f"{away} vs {home}",
                    "pick": home,
                    "edge": round(edge * 100, 2),
                    "confidence": random.randint(60, 90)
                }
                picks.append(pick)

        win_rate = round(random.uniform(55, 65), 2)
        roi = round(random.uniform(5, 15), 2)

        print("✅ Bot finished")

        return {
            "status": "success",
            "picks": picks,
            "win_rate": win_rate,
            "roi": roi,
            "count": len(picks),
            "timestamp": time.strftime("%H:%M:%S")
        }

    except Exception as e:
        print("❌ ERROR:", str(e))
        return {"error": str(e)}


# ==========================
# 🌐 ROUTES
# ==========================
@app.route("/")
def home():
    return open("index.html").read()

@app.route("/run-bot", methods=["POST"])
def run():
    result = run_bot()
    return jsonify(result)

# ==========================
# 🚀 START SERVER
# ==========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
