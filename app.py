from flask import Flask, request, jsonify
import os
from datetime import datetime, time, timedelta
import pytz
import requests
import math
import time as sleep_time

app = Flask(__name__)

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

TRADING_MODE = os.getenv("TRADING_MODE", "SIM").upper()
BASE_URL = "https://api.tradestation.com/v3" if TRADING_MODE == "LIVE" else "https://sim-api.tradestation.com/v3"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")

MNQ_SYMBOL = os.getenv("MNQ_SYMBOL", "MNQU26")
MGC_SYMBOL = os.getenv("MGC_SYMBOL", "MGCQ26")

MAX_CONTRACTS_PER_ORDER = int(os.getenv("MAX_CONTRACTS_PER_ORDER", "5"))

# Broker catastrophe stop. This is NOT your EvoQ stop.
BROKER_STOP_ENABLED = os.getenv("BROKER_STOP_ENABLED", "true").lower() == "true"
BROKER_STOP_MAX_LOSS_DOLLARS = float(os.getenv("BROKER_STOP_MAX_LOSS_DOLLARS", "4000"))

_cached_access_token = None
_token_expires_at = None

# Memory only. Good enough while service stays live.
protective_stop_order_ids = {}


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

    if s in ["LONG", "OPEN_LONG", "BUY", "DCA L", "DCA LONG"]:
        return "LONG"

    if s in ["SHORT", "OPEN_SHORT", "SELL", "DCA S", "DCA SHORT"]:
        return "SHORT"

    if s in ["EXIT", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT", "SESSION END", "SESSION_END"]:
        return "EXIT"

    return s


def futures_specs(symbol):
    s = symbol.upper()

    if s.startswith("MNQ"):
        return {
            "point_value": 2.0,
            "tick_size": 0.25
        }

    if s.startswith("MGC"):
        return {
            "point_value": 10.0,
            "tick_size": 0.10
        }

    return {
        "point_value": 1.0,
        "tick_size": 0.01
    }


def round_stop_price(price, side, tick_size):
    if side == "LONG":
        return math.floor(price / tick_size) * tick_size

    if side == "SHORT":
        return math.ceil(price / tick_size) * tick_size

    return price


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
    avg_price = None

    for p in data.get("Positions", []):
        if p.get("Symbol") == symbol:
            q = float(p.get("Quantity", 0))
            avg_price = float(p.get("AveragePrice", 0))

            if q > 0:
                qty = int(q)
                side = "LONG"

            elif q < 0:
                qty = abs(int(q))
                side = "SHORT"

    return qty, side, avg_price


def wait_for_position(symbol, tries=8, delay=0.75):
    for _ in range(tries):
        qty, side, avg_price = get_position(symbol)

        if qty > 0 and side and avg_price:
            return qty, side, avg_price

        sleep_time.sleep(delay)

    return get_position(symbol)


def extract_order_id(response):
    try:
        orders = response.get("Orders", [])
        if orders and "OrderID" in orders[0]:
            return str(orders[0]["OrderID"])
    except Exception:
        pass

    return None


def send_order(symbol, action, contracts, order_type="Market", stop_price=None):
    contracts = int(contracts)

    if contracts <= 0:
        return {"status": "ignored", "reason": "contracts <= 0"}

    if contracts > MAX_CONTRACTS_PER_ORDER:
        log(f"CONTRACTS CAPPED: requested {contracts}, capped {MAX_CONTRACTS_PER_ORDER}")
        contracts = MAX_CONTRACTS_PER_ORDER

    payload = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(contracts),
        "OrderType": order_type,
        "TradeAction": action,
        "TimeInForce": {
            "Duration": "DAY"
        }
    }

    if stop_price is not None:
        payload["StopPrice"] = str(round(stop_price, 2))

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


def cancel_order(order_id):
    if not order_id:
        return {"status": "no order id"}

    r = requests.delete(
        f"{BASE_URL}/orderexecution/orders/{order_id}",
        headers=auth_headers()
    )

    log(f"CANCEL STOP ORDER {order_id} STATUS: {r.status_code}")
    log(f"CANCEL RESPONSE: {r.text}")

    try:
        return r.json()
    except Exception:
        return {
            "status_code": r.status_code,
            "raw_response": r.text
        }


def cancel_protective_stop(symbol):
    order_id = protective_stop_order_ids.get(symbol)

    if not order_id:
        return {"status": "no protective stop tracked"}

    result = cancel_order(order_id)
    protective_stop_order_ids.pop(symbol, None)

    return result


def calculate_protective_stop(symbol, qty, side, avg_price):
    specs = futures_specs(symbol)

    point_value = specs["point_value"]
    tick_size = specs["tick_size"]

    price_distance = BROKER_STOP_MAX_LOSS_DOLLARS / (qty * point_value)

    if side == "LONG":
        raw_stop = avg_price - price_distance

    elif side == "SHORT":
        raw_stop = avg_price + price_distance

    else:
        return None

    return round_stop_price(raw_stop, side, tick_size)


def place_or_replace_protective_stop(symbol):
    if not BROKER_STOP_ENABLED:
        return {"status": "broker stop disabled"}

    qty, side, avg_price = wait_for_position(symbol)

    if qty == 0:
        cancel_protective_stop(symbol)
        return {"status": "no position for protective stop"}

    stop_price = calculate_protective_stop(symbol, qty, side, avg_price)

    if not stop_price:
        return {"status": "could not calculate stop"}

    cancel_protective_stop(symbol)

    if side == "LONG":
        action = "SELL"

    elif side == "SHORT":
        action = "BUY"

    else:
        return {"status": "unknown side"}

    log(f"PROTECTIVE STOP: {side} {qty} {symbol} avg={avg_price} stop={stop_price}")

    response = send_order(
        symbol=symbol,
        action=action,
        contracts=qty,
        order_type="StopMarket",
        stop_price=stop_price
    )

    stop_order_id = extract_order_id(response)

    if stop_order_id:
        protective_stop_order_ids[symbol] = stop_order_id

    return {
        "status": "protective stop placed",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "avg_price": avg_price,
        "stop_price": stop_price,
        "order_id": stop_order_id,
        "response": response
    }


def flatten(symbol):
    cancel_result = cancel_protective_stop(symbol)

    qty, side, avg_price = get_position(symbol)

    if qty == 0:
        return {
            "status": "already flat",
            "symbol": symbol,
            "cancel_stop": cancel_result
        }

    log(f"FLATTEN: {side} {qty} {symbol}")

    if side == "LONG":
        result = send_order(symbol, "SELL", qty)

    elif side == "SHORT":
        result = send_order(symbol, "BUY", qty)

    else:
        result = {
            "status": "unknown position state",
            "symbol": symbol,
            "qty": qty,
            "side": side
        }

    return {
        "flatten_order": result,
        "cancel_stop": cancel_result
    }


@app.route("/", methods=["GET"])
def home():
    return f"TradeStation Futures Bot v2.0 Running | Mode: {TRADING_MODE}", 200


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
            "mgc_symbol": MGC_SYMBOL,
            "broker_stop_enabled": BROKER_STOP_ENABLED,
            "broker_stop_max_loss_dollars": BROKER_STOP_MAX_LOSS_DOLLARS
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


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

        if not market_open():
            if signal == "EXIT":
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - exit flatten checked",
                    "symbol": symbol,
                    "signal": signal,
                    "result": result
                })

            qty, side, avg_price = get_position(symbol)

            if qty > 0:
                result = flatten(symbol)
                return jsonify({
                    "status": "outside session - position flattened",
                    "symbol": symbol,
                    "signal": signal,
                    "current_position": {
                        "qty": qty,
                        "side": side,
                        "avg_price": avg_price
                    },
                    "result": result
                })

            return jsonify({
                "status": "outside session - ignored",
                "symbol": symbol,
                "signal": signal
            })

        if signal == "LONG":
            qty, side, avg_price = get_position(symbol)

            if side == "LONG":
                log(f"DCA LONG: current={qty}, adding={contracts}")
            else:
                log(f"OPEN LONG: buying={contracts}")

            entry = send_order(symbol, "BUY", contracts)
            stop = place_or_replace_protective_stop(symbol)

            result = {
                "entry": entry,
                "protective_stop": stop
            }

        elif signal == "SHORT":
            qty, side, avg_price = get_position(symbol)

            if side == "SHORT":
                log(f"DCA SHORT: current={qty}, adding={contracts}")
            else:
                log(f"OPEN SHORT: selling={contracts}")

            entry = send_order(symbol, "SELL", contracts)
            stop = place_or_replace_protective_stop(symbol)

            result = {
                "entry": entry,
                "protective_stop": stop
            }

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