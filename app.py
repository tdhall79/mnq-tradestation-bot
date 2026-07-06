from flask import Flask, request, jsonify
import os
import json
import math
import time as sleep_time
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

BROKER_STOP_ENABLED = os.getenv("BROKER_STOP_ENABLED", "true").lower() == "true"
BROKER_STOP_MAX_LOSS_DOLLARS = float(os.getenv("BROKER_STOP_MAX_LOSS_DOLLARS", "250"))

STATE_FILE = os.getenv("STATE_FILE", "positions.json")

_cached_access_token = None
_token_expires_at = None


# =========================
# LOGGING / STATE
# =========================

def log(msg):
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} CST] {msg}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"strategies": {}, "stops": {}}

    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"strategies": {}, "stops": {}}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def strategy_key(symbol, strategy):
    return f"{symbol}:{strategy}"


def get_strategy_position(symbol, strategy):
    state = load_state()
    key = strategy_key(symbol, strategy)

    pos = state["strategies"].get(key, {
        "symbol": symbol,
        "strategy": strategy,
        "side": None,
        "qty": 0
    })

    return pos


def set_strategy_position(symbol, strategy, side, qty):
    state = load_state()
    key = strategy_key(symbol, strategy)

    state["strategies"][key] = {
        "symbol": symbol,
        "strategy": strategy,
        "side": side,
        "qty": qty
    }

    save_state(state)


def clear_strategy_position(symbol, strategy):
    set_strategy_position(symbol, strategy, None, 0)


def set_stop_id(symbol, order_id):
    state = load_state()
    state["stops"][symbol] = order_id
    save_state(state)


def get_stop_id(symbol):
    state = load_state()
    return state.get("stops", {}).get(symbol)


def clear_stop_id(symbol):
    state = load_state()
    state.get("stops", {}).pop(symbol, None)
    save_state(state)


# =========================
# PARSING
# =========================

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
        return {"point_value": 2.0, "tick_size": 0.25}

    if s.startswith("MGC"):
        return {"point_value": 10.0, "tick_size": 0.10}

    return {"point_value": 1.0, "tick_size": 0.01}


def round_stop_price(price, side, tick_size):
    if side == "LONG":
        return math.floor(price / tick_size) * tick_size

    if side == "SHORT":
        return math.ceil(price / tick_size) * tick_size

    return price


# =========================
# AUTH
# =========================

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


# =========================
# TRADESTATION
# =========================

def get_broker_position(symbol):
    r = requests.get(
        f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions",
        headers=auth_headers()
    )

    log(f"POSITION STATUS: {r.status_code}")
    log(f"POSITION RESPONSE: {r.text}")

    if r.status_code != 200:
        raise Exception("Could not fetch broker positions")

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
        qty, side, avg_price = get_broker_position(symbol)

        if qty > 0 and side and avg_price:
            return qty, side, avg_price

        sleep_time.sleep(delay)

    return get_broker_position(symbol)


def send_order(symbol, action, qty, order_type="Market", stop_price=None):
    qty = int(qty)

    if qty <= 0:
        return {"status": "ignored", "reason": "qty <= 0"}

    if qty > MAX_CONTRACTS_PER_ORDER:
        log(f"CONTRACTS CAPPED: requested={qty}, capped={MAX_CONTRACTS_PER_ORDER}")
        qty = MAX_CONTRACTS_PER_ORDER

    payload = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(qty),
        "OrderType": order_type,
        "TradeAction": action,
        "TimeInForce": {"Duration": "DAY"}
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
        return {"status_code": r.status_code, "raw_response": r.text}


def extract_order_id(response):
    try:
        orders = response.get("Orders", [])
        if orders and "OrderID" in orders[0]:
            return str(orders[0]["OrderID"])
    except Exception:
        pass

    return None


def cancel_order(order_id):
    if not order_id:
        return {"status": "no order id"}

    r = requests.delete(
        f"{BASE_URL}/orderexecution/orders/{order_id}",
        headers=auth_headers()
    )

    log(f"CANCEL ORDER {order_id} STATUS: {r.status_code}")
    log(f"CANCEL RESPONSE: {r.text}")

    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw_response": r.text}


def cancel_protective_stop(symbol):
    order_id = get_stop_id(symbol)

    if not order_id:
        return {"status": "no protective stop tracked"}

    result = cancel_order(order_id)
    clear_stop_id(symbol)

    return result


# =========================
# BROKER STOP
# =========================

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
        return {"status": "no broker position for stop"}

    stop_price = calculate_protective_stop(symbol, qty, side, avg_price)

    if not stop_price:
        return {"status": "could not calculate stop"}

    cancel_protective_stop(symbol)

    action = "SELL" if side == "LONG" else "BUY"

    log(f"PROTECTIVE STOP: {side} {qty} {symbol} avg={avg_price} stop={stop_price}")

    response = send_order(
        symbol=symbol,
        action=action,
        qty=qty,
        order_type="StopMarket",
        stop_price=stop_price
    )

    order_id = extract_order_id(response)

    if order_id:
        set_stop_id(symbol, order_id)

    return {
        "status": "protective stop placed",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "avg_price": avg_price,
        "stop_price": stop_price,
        "order_id": order_id,
        "response": response
    }


# =========================
# STRATEGY-AWARE EXECUTION
# =========================

def open_or_add_strategy(symbol, strategy, side, contracts):
    pos = get_strategy_position(symbol, strategy)

    current_side = pos.get("side")
    current_qty = int(pos.get("qty", 0))

    if current_qty > 0 and current_side and current_side != side:
        return {
            "status": "ignored",
            "reason": f"strategy already {current_side}, cannot open {side}",
            "strategy_position": pos
        }

    action = "BUY" if side == "LONG" else "SELL"

    log(f"{strategy} {side}: current={current_qty}, adding={contracts}")

    entry = send_order(symbol, action, contracts)

    new_qty = current_qty + int(contracts)
    set_strategy_position(symbol, strategy, side, new_qty)

    stop = place_or_replace_protective_stop(symbol)

    return {
        "entry": entry,
        "strategy_position": {
            "symbol": symbol,
            "strategy": strategy,
            "side": side,
            "qty": new_qty
        },
        "protective_stop": stop
    }


def exit_strategy(symbol, strategy):
    pos = get_strategy_position(symbol, strategy)

    side = pos.get("side")
    qty = int(pos.get("qty", 0))

    if qty <= 0 or not side:
        return {
            "status": "strategy already flat",
            "symbol": symbol,
            "strategy": strategy
        }

    action = "SELL" if side == "LONG" else "BUY"

    log(f"{strategy} EXIT: {side} {qty} {symbol}")

    exit_order = send_order(symbol, action, qty)

    clear_strategy_position(symbol, strategy)

    stop = place_or_replace_protective_stop(symbol)

    return {
        "exit_order": exit_order,
        "strategy_closed": {
            "symbol": symbol,
            "strategy": strategy,
            "side": side,
            "qty": qty
        },
        "protective_stop": stop
    }


def flatten_symbol(symbol):
    cancel_result = cancel_protective_stop(symbol)

    qty, side, avg_price = get_broker_position(symbol)

    if qty == 0:
        return {"status": "already flat", "symbol": symbol, "cancel_stop": cancel_result}

    action = "SELL" if side == "LONG" else "BUY"

    result = send_order(symbol, action, qty)

    state = load_state()

    for key, pos in list(state["strategies"].items()):
        if pos.get("symbol") == symbol:
            state["strategies"][key]["side"] = None
            state["strategies"][key]["qty"] = 0

    save_state(state)

    return {
        "flatten_order": result,
        "cancel_stop": cancel_result
    }


# =========================
# ROUTES
# =========================

@app.route("/", methods=["GET"])
def home():
    return f"TradeStation Futures Bot v3.0 Strategy-Aware Running | Mode: {TRADING_MODE}", 200


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "version": "3.0-strategy-aware",
            "mode": TRADING_MODE,
            "account_configured": bool(ACCOUNT),
            "token_ok": bool(token),
            "mnq_symbol": MNQ_SYMBOL,
            "mgc_symbol": MGC_SYMBOL,
            "broker_stop_enabled": BROKER_STOP_ENABLED,
            "broker_stop_max_loss_dollars": BROKER_STOP_MAX_LOSS_DOLLARS
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/state", methods=["GET"])
def state_view():
    return jsonify(load_state())


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True)

        log(f"WEBHOOK RECEIVED: {data}")

        if not data:
            return jsonify({"error": "missing or invalid json"}), 400

        raw_symbol = data.get("symbol")
        raw_signal = data.get("signal")

        strategy = data.get("strategy", "DEFAULT")
        strategy = str(strategy).upper().strip()

        symbol = resolve_symbol(raw_symbol)
        signal = normalize_signal(raw_signal)

        contracts = int(data.get("contracts", 1))

        if not symbol:
            return jsonify({"error": "missing symbol"}), 400

        if not signal:
            return jsonify({"error": "missing signal"}), 400

        log(f"PARSED: strategy={strategy}, symbol={symbol}, signal={signal}, contracts={contracts}")

        if not market_open():
            if signal == "EXIT":
                result = exit_strategy(symbol, strategy)
                return jsonify({
                    "status": "outside session - strategy exit processed",
                    "strategy": strategy,
                    "symbol": symbol,
                    "result": result
                })

            qty, side, avg_price = get_broker_position(symbol)

            if qty > 0:
                result = flatten_symbol(symbol)
                return jsonify({
                    "status": "outside session - symbol flattened",
                    "symbol": symbol,
                    "result": result
                })

            return jsonify({
                "status": "outside session - ignored",
                "strategy": strategy,
                "symbol": symbol
            })

        if signal == "LONG":
            result = open_or_add_strategy(symbol, strategy, "LONG", contracts)

        elif signal == "SHORT":
            result = open_or_add_strategy(symbol, strategy, "SHORT", contracts)

        elif signal == "EXIT":
            result = exit_strategy(symbol, strategy)

        else:
            return jsonify({
                "error": "unknown signal",
                "received": raw_signal,
                "normalized": signal
            }), 400

        return jsonify({
            "status": "sent",
            "mode": TRADING_MODE,
            "strategy": strategy,
            "symbol": symbol,
            "signal": signal,
            "contracts": contracts,
            "result": result
        })

    except Exception as e:
        log(f"ERROR: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
