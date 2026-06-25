from flask import Flask, request, jsonify
import os
from datetime import datetime, time
import pytz
import requests

app = Flask(__name__)

# =========================
# SETTINGS
# =========================

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

MAX_CONTRACTS_PER_ORDER = 5

# Use "SIM" while testing. Change to "LIVE" later.
TRADING_MODE = os.getenv("TRADING_MODE", "SIM").upper()

if TRADING_MODE == "LIVE":
    BASE_URL = "https://api.tradestation.com/v3"
else:
    BASE_URL = "https://sim-api.tradestation.com/v3"

TOKEN_URL = "https://signin.tradestation.com/oauth/token"

# =========================
# TRADESTATION AUTH
# =========================

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")

_cached_access_token = None


def get_access_token():
    global _cached_access_token

    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }

    r = requests.post(TOKEN_URL, data=payload)

    print("TOKEN STATUS:", r.status_code)

    if r.status_code != 200:
        print("TOKEN ERROR:", r.text)
        raise Exception("Could not refresh TradeStation access token")

    data = r.json()
    _cached_access_token = data["access_token"]

    return _cached_access_token


def auth_headers():
    token = get_access_token()

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# =========================
# SESSION LOGIC
# =========================

def market_open():
    now = datetime.now(TZ).time()
    return SESSION_START <= now <= SESSION_END


# =========================
# POSITION CHECK
# =========================

def get_position(symbol):
    url = f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions"

    r = requests.get(url, headers=auth_headers())

    print("POSITION STATUS:", r.status_code)
    print("POSITION RESPONSE:", r.text)

    if r.status_code != 200:
        raise Exception("Could not fetch positions")

    data = r.json()

    qty = 0
    side = None

    for p in data.get("Positions", []):
        if p.get("Symbol") == symbol:
            q = float(p.get("Quantity", 0))

            if q > 0:
                qty = int(q)
                side = "LONG"
            elif q < 0:
                qty = abs(int(q))
                side = "SHORT"

    return qty, side


# =========================
# ORDER EXECUTION
# =========================

def send_order(symbol, action, contracts):
    contracts = int(contracts)

    if contracts <= 0:
        return {"status": "ignored", "reason": "contracts <= 0"}

    if contracts > MAX_CONTRACTS_PER_ORDER:
        contracts = MAX_CONTRACTS_PER_ORDER

    payload = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(contracts),
        "OrderType": "Market",
        "TradeAction": action,
        "TimeInForce": {
            "Duration": "DAY"
        }
    }

    print("ORDER PAYLOAD:", payload)

    r = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=auth_headers(),
        json=payload
    )

    print("ORDER STATUS:", r.status_code)
    print("ORDER RESPONSE:", r.text)

    try:
        return r.json()
    except Exception:
        return {
            "status_code": r.status_code,
            "raw_response": r.text
        }


# =========================
# FLATTEN POSITION
# =========================

def flatten(symbol):
    qty, side = get_position(symbol)

    if qty == 0:
        return {
            "status": "already flat",
            "symbol": symbol
        }

    if side == "LONG":
        return send_order(symbol, "SELL", qty)

    if side == "SHORT":
        return send_order(symbol, "BUY", qty)

    return {
        "status": "unknown position state",
        "symbol": symbol,
        "qty": qty,
        "side": side
    }


# =========================
# SIGNAL NORMALIZER
# =========================

def normalize_signal(signal):
    if not signal:
        return None

    s = str(signal).upper().strip()

    if s in ["LONG", "OPEN_LONG", "BUY", "DCA L", "DCA LONG"]:
        return "LONG"

    if s in ["SHORT", "OPEN_SHORT", "SELL", "DCA S", "DCA SHORT"]:
        return "SHORT"

    if s in ["EXIT", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT", "SESSION END", "SESSION_END"]:
        return "EXIT"

    return s


# =========================
# ROUTES
# =========================

@app.route("/", methods=["GET"])
def home():
    return f"TradeStation Futures Bot v1.0 Running | Mode: {TRADING_MODE}", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json

        print("WEBHOOK RECEIVED:", data)

        if not data:
            return jsonify({"error": "missing json"}), 400

        symbol = data.get("symbol")
        raw_signal = data.get("signal")
        signal = normalize_signal(raw_signal)

        contracts = int(data.get("contracts", 1))

        if not symbol:
            return jsonify({"error": "missing symbol"}), 400

        if not signal:
            return jsonify({"error": "missing signal"}), 400

        symbol = symbol.upper().strip()

        # Outside EvoQ session: flatten only, no new trades
        if not market_open():
            result = flatten(symbol)

            return jsonify({
                "status": "outside session - flatten checked",
                "symbol": symbol,
                "signal": signal,
                "result": result
            })

        # LONG or DCA LONG
        if signal == "LONG":
            result = send_order(symbol, "BUY", contracts)

        # SHORT or DCA SHORT
        elif signal == "SHORT":
            result = send_order(symbol, "SELL", contracts)

        # EXIT
        elif signal == "EXIT":
            result = flatten(symbol)

        else:
            return jsonify({
                "error": "unknown signal",
                "received": raw_signal,
                "normalized": signal
            }), 400

        return jsonify({
            "status": "sent",
            "mode": TRADING_MODE,
            "symbol": symbol,
            "signal": signal,
            "contracts": contracts,
            "result": result
        })

    except Exception as e:
        print("ERROR:", str(e))

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
