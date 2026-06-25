from flask import Flask, request, jsonify
import os
from datetime import datetime, time, timedelta
import pytz
import requests

app = Flask(__name__)

# =========================
# SETTINGS
# =========================

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

MAX_CONTRACTS_PER_ORDER = int(os.getenv("MAX_CONTRACTS_PER_ORDER", "5"))

TRADING_MODE = os.getenv("TRADING_MODE", "SIM").upper()

if TRADING_MODE == "LIVE":
    BASE_URL = "https://api.tradestation.com/v3"
else:
    BASE_URL = "https://sim-api.tradestation.com/v3"

TOKEN_URL = "https://signin.tradestation.com/oauth/token"

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")

MNQ_SYMBOL = os.getenv("MNQ_SYMBOL", "MNQU26")
MGC_SYMBOL = os.getenv("MGC_SYMBOL", "MGCQ26")

_cached_access_token = None
_token_expires_at = None


# =========================
# HELPERS
# =========================

def log(message):
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} CST] {message}")


def resolve_symbol(symbol):
    if not symbol:
        return None

    s = symbol.upper().strip()

    if s in ["MNQ", "MNQ1!", "@MNQ"]:
        return MNQ_SYMBOL

    if s in ["MGC", "MGC1!", "@MGC"]:
        return MGC_SYMBOL

    return s


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


def market_open():
    now = datetime.now(TZ).time()
    return SESSION_START <= now <= SESSION_END


# =========================
# AUTH
# =========================

def get_access_token():
    global _cached_access_token
    global _token_expires_at

    now = datetime.now(TZ)

    if _cached_access_token and _token_expires_at and now < _token_expires_at:
        return _cached_access_token

    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }

    r = requests.post(TOKEN_URL, data=payload)

    log(f"TOKEN STATUS: {r.status_code}")

    if r.status_code != 200:
        log(f"TOKEN ERROR: {r.text}")
        raise Exception("Could not refresh TradeStation access token")

    data = r.json()

    _cached_access_token = data["access_token"]

    expires_in = int(data.get("expires_in", 1200))
    _token_expires_at = now + timedelta(seconds=max(60, expires_in - 60))

    log("TOKEN REFRESHED")

    return _cached_access_token


def auth_headers():
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }


# =========================
# TRADESTATION
# =========================

def get_position(symbol):
    url = f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions"

    r = requests.get(url, headers=auth_headers())

    log(f"POSITION STATUS: {r.status_code}")
    log(f"POSITION RESPONSE: {r.text}")

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


def send_order(symbol, action, contracts):
    contracts = int(contracts)

    if contracts <= 0:
        return {
            "status": "ignored",
            "reason": "contracts <= 0"
        }

    if contracts > MAX_CONTRACTS_PER_ORDER:
        log(f"CONTRACTS CAPPED: requested {contracts}, capped {MAX_CONTRACTS_PER_ORDER}")
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

    log(f"ORDER PAYLOAD: {payload}")

    r = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=auth_headers(),
        json=payload
    )

    log(f"ORDER STATUS: {r.status_code}")
    log(f"ORDER RESPONSE: {r.text}")

    try:
        return r.json()
    except Exception:
        return {
            "status_code": r.status_code,
            "raw_response": r.text
        }


def flatten(symbol):
    qty, side = get_position(symbol)

    if qty == 0:
        return {
            "status": "already flat",
            "symbol": symbol
        }

    log(f"FLATTEN: {side} {qty} {symbol}")

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
# ROUTES
# =========================

@app.route("/", methods=["GET"])
def home():
    return f"TradeStation Futures Bot v1.1 Running | Mode: {TRADING_MODE}", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json

        log(f"WEBHOOK RECEIVED: {data}")

        if not data:
            return jsonify({"error": "missing json"}), 400

        raw_symbol = data.get("symbol")
        raw_signal = data.get("signal")

        symbol = resolve_symbol(raw_symbol)
        signal = normalize_signal(raw_signal)

        contracts = int(data.get("contracts", 1))

        if not symbol:
            return jsonify({"error": "missing symbol"}), 400

        if not signal:
            return jsonify({"error": "missing signal"}), 400

        log(f"PARSED: raw_symbol={raw_symbol}, symbol={symbol}, signal={signal}, contracts={contracts}")

        # Outside session:
        # - EXIT still flattens
        # - LONG/SHORT are ignored unless a position exists and needs flattening
        if not market_open():
            if signal == "EXIT":
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - exit flatten checked",
                    "symbol": symbol,
                    "signal": signal,
                    "result": result
                })

            qty, side = get_position(symbol)

            if qty > 0:
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - position flattened",
                    "symbol": symbol,
                    "signal": signal,
                    "current_position": {
                        "qty": qty,
                        "side": side
                    },
                    "result": result
                })

            return jsonify({
                "status": "outside session - ignored",
                "symbol": symbol,
                "signal": signal
            })

        result = None

        if signal == "LONG":
            qty, side = get_position(symbol)

            if side == "LONG":
                log(f"DCA LONG: current={qty}, adding={contracts}")
            else:
                log(f"OPEN LONG: buying={contracts}")

            result = send_order(symbol, "BUY", contracts)

        elif signal == "SHORT":
            qty, side = get_position(symbol)

            if side == "SHORT":
                log(f"DCA SHORT: current={qty}, adding={contracts}")
            else:
                log(f"OPEN SHORT: selling={contracts}")

            result = send_order(symbol, "SELL", contracts)

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
        log(f"ERROR: {str(e)}")

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "mode": TRADING_MODE,
            "account_configured": bool(ACCOUNT),
            "token_ok": bool(token),
            "mnq_symbol": MNQ_SYMBOL,
            "mgc_symbol": MGC_SYMBOL
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)