from flask import Flask, request, jsonify
import os
from datetime import datetime, time, timedelta
import pytz
import requests

app = Flask(__name__)

TZ = pytz.timezone("America/Chicago")
SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

TRADING_MODE = os.getenv("TRADING_MODE", "LIVE").upper()
BASE_URL = "https://api.tradestation.com/v3" if TRADING_MODE == "LIVE" else "https://sim-api.tradestation.com/v3"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")

MNQ_SYMBOL = os.getenv("MNQ_SYMBOL", "MNQU26")
MGC_SYMBOL = os.getenv("MGC_SYMBOL", "MGCQ26")
MAX_CONTRACTS_PER_ORDER = int(os.getenv("MAX_CONTRACTS_PER_ORDER", "5"))

_cached_access_token = None
_token_expires_at = None
tracked_stop_ids = {}


def log(msg):
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} CST] {msg}")


def market_open():
    now = datetime.now(TZ).time()
    return SESSION_START <= now <= SESSION_END


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

    if s in ["LONG", "BUY", "OPEN_LONG", "DCA L", "DCA LONG"]:
        return "LONG"

    if s in ["SHORT", "SELL", "OPEN_SHORT", "DCA S", "DCA SHORT"]:
        return "SHORT"

    if s in ["EXIT", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT", "SESSION END", "SESSION_END"]:
        return "EXIT"

    return s


def get_access_token():
    global _cached_access_token, _token_expires_at

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
        raise Exception("Could not refresh TradeStation token")

    data = r.json()
    _cached_access_token = data["access_token"]
    expires_in = int(data.get("expires_in", 1200))
    _token_expires_at = now + timedelta(seconds=max(60, expires_in - 60))

    return _cached_access_token


def auth_headers():
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }


def get_position(symbol):
    r = requests.get(
        f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions",
        headers=auth_headers()
    )

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


def extract_order_ids(response):
    ids = []

    try:
        for order in response.get("Orders", []):
            if "OrderID" in order:
                ids.append(str(order["OrderID"]))
    except Exception:
        pass

    return ids


def cancel_order(order_id):
    r = requests.delete(
        f"{BASE_URL}/orderexecution/orders/{order_id}",
        headers=auth_headers()
    )

    log(f"CANCEL ORDER {order_id} STATUS: {r.status_code}")
    log(f"CANCEL RESPONSE: {r.text}")

    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw": r.text}


def cancel_tracked_stops(symbol):
    ids = tracked_stop_ids.get(symbol, [])

    results = []

    for order_id in ids:
        results.append(cancel_order(order_id))

    tracked_stop_ids[symbol] = []

    return results


def send_market_order(symbol, action, contracts):
    contracts = int(contracts)

    payload = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(contracts),
        "OrderType": "Market",
        "TradeAction": action,
        "TimeInForce": {"Duration": "DAY"}
    }

    log(f"MARKET ORDER PAYLOAD: {payload}")

    r = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=auth_headers(),
        json=payload
    )

    log(f"MARKET ORDER STATUS: {r.status_code}")
    log(f"MARKET ORDER RESPONSE: {r.text}")

    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw": r.text}


def send_oso_entry_with_stop(symbol, side, contracts, stop_price):
    contracts = int(contracts)

    if contracts <= 0:
        return {"status": "ignored", "reason": "contracts <= 0"}

    if contracts > MAX_CONTRACTS_PER_ORDER:
        contracts = MAX_CONTRACTS_PER_ORDER

    if side == "LONG":
        entry_action = "BUY"
        stop_action = "SELL"
    elif side == "SHORT":
        entry_action = "SELL"
        stop_action = "BUY"
    else:
        raise Exception("Invalid side for OSO")

    payload = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(contracts),
        "OrderType": "Market",
        "TradeAction": entry_action,
        "TimeInForce": {"Duration": "DAY"},
        "OSOs": [
            {
                "Type": "NORMAL",
                "Orders": [
                    {
                        "AccountID": ACCOUNT,
                        "Symbol": symbol,
                        "Quantity": str(contracts),
                        "OrderType": "StopMarket",
                        "TradeAction": stop_action,
                        "StopPrice": str(stop_price),
                        "TimeInForce": {"Duration": "DAY"}
                    }
                ]
            }
        ]
    }

    log(f"OSO ENTRY PAYLOAD: {payload}")

    r = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=auth_headers(),
        json=payload
    )

    log(f"OSO ORDER STATUS: {r.status_code}")
    log(f"OSO ORDER RESPONSE: {r.text}")

    try:
        result = r.json()
    except Exception:
        result = {"status_code": r.status_code, "raw": r.text}

    ids = extract_order_ids(result)

    if ids:
        tracked_stop_ids.setdefault(symbol, [])
        tracked_stop_ids[symbol].extend(ids)

    return result


def flatten(symbol):
    cancel_result = cancel_tracked_stops(symbol)

    qty, side = get_position(symbol)

    if qty == 0:
        return {
            "status": "already flat",
            "symbol": symbol,
            "cancel_stops": cancel_result
        }

    if side == "LONG":
        result = send_market_order(symbol, "SELL", qty)
    elif side == "SHORT":
        result = send_market_order(symbol, "BUY", qty)
    else:
        result = {"status": "unknown position state"}

    return {
        "flatten_order": result,
        "cancel_stops": cancel_result
    }


@app.route("/", methods=["GET"])
def home():
    return f"TradeStation Futures Bot v2.1 OSO Running | Mode: {TRADING_MODE}", 200


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "mode": TRADING_MODE,
            "token_ok": bool(token),
            "account_configured": bool(ACCOUNT),
            "mnq_symbol": MNQ_SYMBOL,
            "mgc_symbol": MGC_SYMBOL,
            "version": "2.1-oso"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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

        stop_price = data.get("stop_price")

        if not symbol:
            return jsonify({"error": "missing symbol"}), 400

        if not signal:
            return jsonify({"error": "missing signal"}), 400

        log(f"PARSED: symbol={symbol}, signal={signal}, contracts={contracts}, stop_price={stop_price}")

        if not market_open():
            if signal == "EXIT":
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - exit flatten checked",
                    "symbol": symbol,
                    "result": result
                })

            qty, side = get_position(symbol)

            if qty > 0:
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - position flattened",
                    "symbol": symbol,
                    "result": result
                })

            return jsonify({
                "status": "outside session - ignored",
                "symbol": symbol
            })

        if signal == "LONG":
            if stop_price is None:
                return jsonify({
                    "error": "missing stop_price",
                    "message": "LONG entry requires stop_price for OSO bracket."
                }), 400

            result = send_oso_entry_with_stop(
                symbol=symbol,
                side="LONG",
                contracts=contracts,
                stop_price=stop_price
            )

        elif signal == "SHORT":
            if stop_price is None:
                return jsonify({
                    "error": "missing stop_price",
                    "message": "SHORT entry requires stop_price for OSO bracket."
                }), 400

            result = send_oso_entry_with_stop(
                symbol=symbol,
                side="SHORT",
                contracts=contracts,
                stop_price=stop_price
            )

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
