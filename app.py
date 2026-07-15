from __future__ import annotations

from flask import Flask, jsonify, request
import json
import math
import os
import tempfile
import threading
import time as sleep_time
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

import pytz
import requests


app = Flask(__name__)

# =========================================================
# GENERAL SETTINGS
# =========================================================

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

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

MAX_CONTRACTS_PER_ORDER = int(
    os.getenv("MAX_CONTRACTS_PER_ORDER", "5")
)

BROKER_STOP_ENABLED = (
    os.getenv("BROKER_STOP_ENABLED", "true").lower() == "true"
)

# Total maximum loss across the entire broker position,
# not per contract.
BROKER_STOP_MAX_LOSS_DOLLARS = float(
    os.getenv("BROKER_STOP_MAX_LOSS_DOLLARS", "185")
)

# Use /var/data/positions.json when a Render persistent disk
# is mounted at /var/data.
STATE_FILE = Path(
    os.getenv("STATE_FILE", "/tmp/positions.json")
)

REQUEST_TIMEOUT = int(
    os.getenv("REQUEST_TIMEOUT_SECONDS", "15")
)

_cached_access_token: str | None = None
_token_expires_at: datetime | None = None

state_lock = threading.RLock()


# =========================================================
# LOGGING
# =========================================================

def log(message: str) -> None:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} CST] {message}", flush=True)


# =========================================================
# STATE STORAGE
# =========================================================

def empty_state() -> dict[str, Any]:
    return {
        "strategies": {},
        "stops": {}
    }


def load_state() -> dict[str, Any]:
    with state_lock:
        if not STATE_FILE.exists():
            return empty_state()

        try:
            with STATE_FILE.open("r", encoding="utf-8") as file:
                state = json.load(file)

            state.setdefault("strategies", {})
            state.setdefault("stops", {})

            return state

        except Exception as exc:
            log(f"STATE READ ERROR: {exc}")
            return empty_state()


def save_state(state: dict[str, Any]) -> None:
    with state_lock:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Atomic replacement prevents partially written JSON.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(STATE_FILE.parent),
            delete=False
        ) as temporary_file:
            json.dump(state, temporary_file, indent=2)
            temporary_path = Path(temporary_file.name)

        temporary_path.replace(STATE_FILE)


def strategy_key(symbol: str, strategy: str) -> str:
    return f"{symbol}:{strategy}"


def get_strategy_position(
    symbol: str,
    strategy: str
) -> dict[str, Any]:

    state = load_state()
    key = strategy_key(symbol, strategy)

    return state["strategies"].get(
        key,
        {
            "symbol": symbol,
            "strategy": strategy,
            "side": None,
            "qty": 0
        }
    )


def set_strategy_position(
    symbol: str,
    strategy: str,
    side: str | None,
    qty: int
) -> None:

    state = load_state()
    key = strategy_key(symbol, strategy)

    state["strategies"][key] = {
        "symbol": symbol,
        "strategy": strategy,
        "side": side,
        "qty": int(qty)
    }

    save_state(state)


def clear_strategy_position(
    symbol: str,
    strategy: str
) -> None:

    set_strategy_position(
        symbol=symbol,
        strategy=strategy,
        side=None,
        qty=0
    )


def clear_all_strategies_for_symbol(symbol: str) -> None:
    state = load_state()

    for key, position in state["strategies"].items():
        if position.get("symbol") == symbol:
            state["strategies"][key]["side"] = None
            state["strategies"][key]["qty"] = 0

    save_state(state)


def get_tracked_stop_id(symbol: str) -> str | None:
    state = load_state()
    return state["stops"].get(symbol)


def set_tracked_stop_id(
    symbol: str,
    order_id: str
) -> None:

    state = load_state()
    state["stops"][symbol] = order_id
    save_state(state)


def clear_tracked_stop_id(symbol: str) -> None:
    state = load_state()
    state["stops"].pop(symbol, None)
    save_state(state)


# =========================================================
# SIGNAL AND SYMBOL PARSING
# =========================================================

def market_open() -> bool:
    current_time = datetime.now(TZ).time()
    return SESSION_START <= current_time <= SESSION_END


def resolve_symbol(symbol: Any) -> str | None:
    if not symbol:
        return None

    normalized = str(symbol).upper().strip()

    if normalized in {"MNQ", "MNQ1!", "@MNQ"}:
        return MNQ_SYMBOL

    if normalized in {"MGC", "MGC1!", "@MGC"}:
        return MGC_SYMBOL

    # Full contract symbols pass through unchanged:
    # SILQ2026, QCU2026, MZSU2026, etc.
    return normalized


def normalize_signal(signal: Any) -> str | None:
    if not signal:
        return None

    normalized = str(signal).upper().strip()

    if normalized in {
        "LONG",
        "Long",
        "OPEN_LONG",
        "BUY",
        "DCA L",
        "DCA LONG"
    }:
        return "LONG"

    if normalized in {
        "SHORT",
        "Short",
        "OPEN_SHORT",
        "SELL",
        "DCA S",
        "DCA SHORT"
    }:
        return "SHORT"

    if normalized in {
        "EXIT",
        "CLOSE",
        "CLOSE_LONG",
        "CLOSE_SHORT",
        "SESSION END",
        "SESSION_END",
        "TP1",
        "SL"
    }:
        return "EXIT"

    return normalized


# =========================================================
# FUTURES CONTRACT SPECIFICATIONS
# =========================================================

def futures_specs(symbol: str) -> dict[str, float]:
    """
    point_value:
        Dollar P/L for a 1.00 displayed-price move,
        per contract.

    tick_size:
        Smallest valid displayed-price increment.
    """

    normalized = symbol.upper().strip()

    # Micro E-mini Nasdaq-100
    if normalized.startswith("MNQ"):
        return {
            "point_value": 2.0,
            "tick_size": 0.25
        }

    # Micro Gold: 10 troy ounces
    if normalized.startswith("MGC"):
        return {
            "point_value": 10.0,
            "tick_size": 0.10
        }

    # Micro Silver: 1,000 troy ounces
    # TradeStation commonly represents this product with SIL.
    if normalized.startswith("SIL"):
        return {
            "point_value": 1000.0,
            "tick_size": 0.01
        }

    # E-mini Copper: 12,500 pounds
    if normalized.startswith("QC"):
        return {
            "point_value": 12500.0,
            "tick_size": 0.002
        }

    # Micro Soybeans: 500 bushels
    # Displayed in cents per bushel.
    if normalized.startswith("MZS"):
        return {
            "point_value": 5.0,
            "tick_size": 0.50
        }

    # Micro Corn: 500 bushels
    if normalized.startswith("MZC"):
        return {
            "point_value": 5.0,
            "tick_size": 0.50
        }

    # Micro Chicago Wheat: 500 bushels
    if normalized.startswith("MZW"):
        return {
            "point_value": 5.0,
            "tick_size": 0.50
        }

    # Never guess a multiplier for an unknown contract.
    raise ValueError(
        f"No futures specifications configured for {symbol}. "
        "Order blocked to prevent an incorrect protective stop."
    )


def round_to_tick(
    price: float,
    side: str,
    tick_size: float
) -> float:

    tick_count = price / tick_size

    if side == "LONG":
        rounded_ticks = math.floor(tick_count)
    elif side == "SHORT":
        rounded_ticks = math.ceil(tick_count)
    else:
        raise ValueError(f"Unknown position side: {side}")

    rounded_price = rounded_ticks * tick_size

    # Avoid floating-point artifacts such as 6.12000000001.
    return round(rounded_price, 10)


# =========================================================
# OAUTH
# =========================================================

def validate_environment() -> None:
    missing = []

    values = {
        "TS_CLIENT_ID": CLIENT_ID,
        "TS_CLIENT_SECRET": CLIENT_SECRET,
        "TS_REFRESH_TOKEN": REFRESH_TOKEN,
        "TS_ACCOUNT": ACCOUNT
    }

    for name, value in values.items():
        if not value:
            missing.append(name)

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )


def get_access_token() -> str:
    global _cached_access_token
    global _token_expires_at

    validate_environment()

    now = datetime.now(TZ)

    if (
        _cached_access_token
        and _token_expires_at
        and now < _token_expires_at
    ):
        return _cached_access_token

    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }

    response = requests.post(
        TOKEN_URL,
        data=payload,
        timeout=REQUEST_TIMEOUT
    )

    log(f"TOKEN STATUS: {response.status_code}")

    if response.status_code != 200:
        log(f"TOKEN ERROR: {response.text}")
        raise RuntimeError(
            "Could not refresh TradeStation access token"
        )

    token_data = response.json()

    _cached_access_token = token_data["access_token"]

    expires_in = int(token_data.get("expires_in", 1200))

    # Refresh about one minute before official expiration.
    _token_expires_at = now + timedelta(
        seconds=max(60, expires_in - 60)
    )

    log("TOKEN REFRESHED")

    return _cached_access_token


def auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }


# =========================================================
# TRADESTATION POSITION LOOKUP
# =========================================================

def get_broker_position(
    symbol: str
) -> tuple[int, str | None, float | None]:

    response = requests.get(
        f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions",
        headers=auth_headers(),
        timeout=REQUEST_TIMEOUT
    )

    log(f"POSITION STATUS: {response.status_code}")
    log(f"POSITION RESPONSE: {response.text}")

    if response.status_code != 200:
        raise RuntimeError(
            "Could not fetch TradeStation positions"
        )

    position_data = response.json()

    for position in position_data.get("Positions", []):
        if position.get("Symbol", "").upper() != symbol.upper():
            continue

        quantity = abs(
            int(float(position.get("Quantity", 0)))
        )

        long_short = str(
            position.get("LongShort", "")
        ).upper()

        average_price_raw = position.get("AveragePrice")
        average_price = (
            float(average_price_raw)
            if average_price_raw not in (None, "")
            else None
        )

        if long_short.startswith("LONG"):
            return quantity, "LONG", average_price

        if long_short.startswith("SHORT"):
            return quantity, "SHORT", average_price

        # Fallback in case a signed quantity is returned.
        signed_quantity = float(position.get("Quantity", 0))

        if signed_quantity > 0:
            return quantity, "LONG", average_price

        if signed_quantity < 0:
            return quantity, "SHORT", average_price

    return 0, None, None


def wait_for_broker_position(
    symbol: str,
    tries: int = 10,
    delay: float = 0.75
) -> tuple[int, str | None, float | None]:

    last_result = (0, None, None)

    for _ in range(tries):
        last_result = get_broker_position(symbol)

        quantity, side, average_price = last_result

        if quantity > 0 and side and average_price:
            return last_result

        sleep_time.sleep(delay)

    return last_result


# =========================================================
# ORDER HELPERS
# =========================================================

def response_order_accepted(response_data: dict[str, Any]) -> bool:
    orders = response_data.get("Orders", [])

    if not orders:
        return False

    for order in orders:
        error = str(order.get("Error", "")).upper()

        if error in {"FAILED", "REJECTED", "ERROR"}:
            return False

        message = str(order.get("Message", "")).lower()

        if "failed" in message or "rejected" in message:
            return False

    return any(
        order.get("OrderID")
        or "sent order" in str(order.get("Message", "")).lower()
        for order in orders
    )


def extract_order_id(
    response_data: dict[str, Any]
) -> str | None:

    for order in response_data.get("Orders", []):
        order_id = order.get("OrderID")

        if order_id:
            return str(order_id)

    return None


def send_order(
    symbol: str,
    action: str,
    quantity: int,
    order_type: str = "Market",
    stop_price: float | None = None
) -> dict[str, Any]:

    quantity = int(quantity)

    if quantity <= 0:
        return {
            "accepted": False,
            "status": "ignored",
            "reason": "quantity must be positive"
        }

    if quantity > MAX_CONTRACTS_PER_ORDER:
        return {
            "accepted": False,
            "status": "blocked",
            "reason": (
                f"Requested {quantity} contracts exceeds "
                f"MAX_CONTRACTS_PER_ORDER="
                f"{MAX_CONTRACTS_PER_ORDER}"
            )
        }

    payload: dict[str, Any] = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(quantity),
        "OrderType": order_type,
        "TradeAction": action,
        "TimeInForce": {
            "Duration": "DAY"
        }
    }

    if stop_price is not None:
        payload["StopPrice"] = str(stop_price)

    log(f"ORDER PAYLOAD: {payload}")

    response = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=auth_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT
    )

    log(f"ORDER HTTP STATUS: {response.status_code}")
    log(f"ORDER RESPONSE: {response.text}")

    try:
        response_data = response.json()
    except ValueError:
        response_data = {
            "raw_response": response.text
        }

    accepted = (
        response.status_code == 200
        and response_order_accepted(response_data)
    )

    return {
        "accepted": accepted,
        "http_status": response.status_code,
        "payload": payload,
        "response": response_data
    }


def cancel_order(order_id: str) -> dict[str, Any]:
    response = requests.delete(
        f"{BASE_URL}/orderexecution/orders/{order_id}",
        headers=auth_headers(),
        timeout=REQUEST_TIMEOUT
    )

    log(
        f"CANCEL ORDER {order_id} "
        f"STATUS: {response.status_code}"
    )
    log(f"CANCEL RESPONSE: {response.text}")

    try:
        response_data = response.json()
    except ValueError:
        response_data = {
            "raw_response": response.text
        }

    return {
        "http_status": response.status_code,
        "response": response_data
    }


def cancel_protective_stop(symbol: str) -> dict[str, Any]:
    stop_order_id = get_tracked_stop_id(symbol)

    if not stop_order_id:
        return {
            "status": "no protective stop tracked"
        }

    result = cancel_order(stop_order_id)

    clear_tracked_stop_id(symbol)

    return result


# =========================================================
# PROTECTIVE BROKER STOP
# =========================================================

def calculate_protective_stop(
    symbol: str,
    quantity: int,
    side: str,
    average_price: float
) -> float:

    specifications = futures_specs(symbol)

    point_value = specifications["point_value"]
    tick_size = specifications["tick_size"]

    # This keeps the entire position near $185 total risk.
    price_distance = (
        BROKER_STOP_MAX_LOSS_DOLLARS
        / (quantity * point_value)
    )

    if side == "LONG":
        raw_stop = average_price - price_distance
    elif side == "SHORT":
        raw_stop = average_price + price_distance
    else:
        raise ValueError(f"Unknown side: {side}")

    return round_to_tick(
        price=raw_stop,
        side=side,
        tick_size=tick_size
    )


def place_or_replace_protective_stop(
    symbol: str
) -> dict[str, Any]:

    if not BROKER_STOP_ENABLED:
        return {
            "status": "broker stop disabled"
        }

    quantity, side, average_price = (
        wait_for_broker_position(symbol)
    )

    if quantity <= 0 or not side or average_price is None:
        cancel_result = cancel_protective_stop(symbol)

        return {
            "status": "no broker position for stop",
            "cancel_result": cancel_result
        }

    stop_price = calculate_protective_stop(
        symbol=symbol,
        quantity=quantity,
        side=side,
        average_price=average_price
    )

    cancel_result = cancel_protective_stop(symbol)

    stop_action = "SELL" if side == "LONG" else "BUY"

    log(
        f"PROTECTIVE STOP: {side} {quantity} {symbol} "
        f"avg={average_price} stop={stop_price} "
        f"total_risk=${BROKER_STOP_MAX_LOSS_DOLLARS:.2f}"
    )

    stop_result = send_order(
        symbol=symbol,
        action=stop_action,
        quantity=quantity,
        order_type="StopMarket",
        stop_price=stop_price
    )

    if stop_result["accepted"]:
        order_id = extract_order_id(
            stop_result["response"]
        )

        if order_id:
            set_tracked_stop_id(symbol, order_id)

    return {
        "status": (
            "protective stop submitted"
            if stop_result["accepted"]
            else "protective stop rejected"
        ),
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "average_price": average_price,
        "stop_price": stop_price,
        "total_risk_dollars": (
            BROKER_STOP_MAX_LOSS_DOLLARS
        ),
        "cancel_previous_stop": cancel_result,
        "stop_order": stop_result
    }


# =========================================================
# STRATEGY EXECUTION
# =========================================================

def open_or_add_strategy(
    symbol: str,
    strategy: str,
    requested_side: str,
    contracts: int
) -> dict[str, Any]:

    strategy_position = get_strategy_position(
        symbol,
        strategy
    )

    strategy_side = strategy_position.get("side")
    strategy_quantity = int(
        strategy_position.get("qty", 0)
    )

    # One strategy cannot reverse without first exiting.
    if (
        strategy_quantity > 0
        and strategy_side
        and strategy_side != requested_side
    ):
        return {
            "status": "blocked",
            "reason": (
                f"{strategy} is already "
                f"{strategy_side} {strategy_quantity}"
            ),
            "strategy_position": strategy_position
        }

    broker_quantity, broker_side, _ = (
        get_broker_position(symbol)
    )

    # Futures are netted. Do not let one strategy's entry
    # silently close or reverse another strategy.
    if (
        broker_quantity > 0
        and broker_side
        and broker_side != requested_side
    ):
        return {
            "status": "blocked",
            "reason": (
                f"Broker is already {broker_side} "
                f"{broker_quantity} {symbol}; "
                f"cannot open {requested_side} "
                f"for {strategy}"
            )
        }

    action = (
        "BUY"
        if requested_side == "LONG"
        else "SELL"
    )

    log(
        f"{strategy} {requested_side}: "
        f"current strategy qty={strategy_quantity}, "
        f"adding={contracts}"
    )

    entry_result = send_order(
        symbol=symbol,
        action=action,
        quantity=contracts
    )

    # Never update the ledger after a rejected order.
    if not entry_result["accepted"]:
        return {
            "status": "entry rejected",
            "entry_order": entry_result,
            "strategy_position_unchanged": (
                strategy_position
            )
        }

    new_strategy_quantity = (
        strategy_quantity + contracts
    )

    set_strategy_position(
        symbol=symbol,
        strategy=strategy,
        side=requested_side,
        qty=new_strategy_quantity
    )

    stop_result = place_or_replace_protective_stop(
        symbol
    )

    return {
        "status": "entry accepted",
        "entry_order": entry_result,
        "strategy_position": {
            "symbol": symbol,
            "strategy": strategy,
            "side": requested_side,
            "qty": new_strategy_quantity
        },
        "protective_stop": stop_result
    }


def exit_strategy(
    symbol: str,
    strategy: str
) -> dict[str, Any]:

    strategy_position = get_strategy_position(
        symbol,
        strategy
    )

    strategy_side = strategy_position.get("side")
    strategy_quantity = int(
        strategy_position.get("qty", 0)
    )

    if strategy_quantity <= 0 or not strategy_side:
        return {
            "status": "strategy already flat",
            "symbol": symbol,
            "strategy": strategy
        }

    broker_quantity, broker_side, _ = (
        get_broker_position(symbol)
    )

    log(
        f"{strategy} EXIT RECONCILIATION: "
        f"ledger={strategy_side} {strategy_quantity}; "
        f"broker={broker_side} {broker_quantity}"
    )

    # This directly prevents the SIL reversal you experienced.
    if broker_quantity <= 0 or not broker_side:
        clear_all_strategies_for_symbol(symbol)

        cancel_result = cancel_protective_stop(
            symbol
        )

        return {
            "status": (
                "broker already flat - "
                "no exit order sent"
            ),
            "symbol": symbol,
            "strategy": strategy,
            "all_symbol_ledgers_cleared": True,
            "cancel_protective_stop": cancel_result
        }

    # Never send an exit in the wrong direction.
    if broker_side != strategy_side:
        return {
            "status": "exit blocked",
            "reason": (
                "Broker side differs from "
                "strategy ledger"
            ),
            "symbol": symbol,
            "strategy": strategy,
            "strategy_side": strategy_side,
            "strategy_qty": strategy_quantity,
            "broker_side": broker_side,
            "broker_qty": broker_quantity
        }

    close_quantity = min(
        strategy_quantity,
        broker_quantity
    )

    close_action = (
        "SELL"
        if strategy_side == "LONG"
        else "BUY"
    )

    log(
        f"{strategy} EXIT: closing "
        f"{close_quantity} {symbol}"
    )

    exit_result = send_order(
        symbol=symbol,
        action=close_action,
        quantity=close_quantity
    )

    if not exit_result["accepted"]:
        return {
            "status": "exit rejected",
            "exit_order": exit_result,
            "strategy_position_unchanged": (
                strategy_position
            )
        }

    remaining_strategy_quantity = max(
        0,
        strategy_quantity - close_quantity
    )

    if remaining_strategy_quantity == 0:
        clear_strategy_position(
            symbol,
            strategy
        )
    else:
        set_strategy_position(
            symbol=symbol,
            strategy=strategy,
            side=strategy_side,
            qty=remaining_strategy_quantity
        )

    stop_result = place_or_replace_protective_stop(
        symbol
    )

    return {
        "status": "strategy exit accepted",
        "symbol": symbol,
        "strategy": strategy,
        "side": strategy_side,
        "requested_qty": strategy_quantity,
        "closed_qty": close_quantity,
        "strategy_qty_remaining": (
            remaining_strategy_quantity
        ),
        "exit_order": exit_result,
        "protective_stop": stop_result
    }


def flatten_symbol(symbol: str) -> dict[str, Any]:
    cancel_result = cancel_protective_stop(symbol)

    broker_quantity, broker_side, _ = (
        get_broker_position(symbol)
    )

    if broker_quantity <= 0 or not broker_side:
        clear_all_strategies_for_symbol(symbol)

        return {
            "status": "already flat",
            "symbol": symbol,
            "cancel_stop": cancel_result
        }

    close_action = (
        "SELL"
        if broker_side == "LONG"
        else "BUY"
    )

    flatten_result = send_order(
        symbol=symbol,
        action=close_action,
        quantity=broker_quantity
    )

    if flatten_result["accepted"]:
        clear_all_strategies_for_symbol(symbol)

    return {
        "status": (
            "flatten accepted"
            if flatten_result["accepted"]
            else "flatten rejected"
        ),
        "flatten_order": flatten_result,
        "cancel_stop": cancel_result
    }


# =========================================================
# ROUTES
# =========================================================

@app.route("/", methods=["GET"])
def home():
    return (
        "TradeStation Futures Bot v3.1 "
        f"Strategy-Aware Running | Mode: {TRADING_MODE}",
        200
    )


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "version": "3.1-strategy-aware",
            "mode": TRADING_MODE,
            "account_configured": bool(ACCOUNT),
            "token_ok": bool(token),
            "mnq_symbol": MNQ_SYMBOL,
            "mgc_symbol": MGC_SYMBOL,
            "broker_stop_enabled": (
                BROKER_STOP_ENABLED
            ),
            "broker_stop_total_risk_dollars": (
                BROKER_STOP_MAX_LOSS_DOLLARS
            ),
            "state_file": str(STATE_FILE)
        })

    except Exception as exc:
        return jsonify({
            "status": "error",
            "message": str(exc)
        }), 500


@app.route("/state", methods=["GET"])
def state_view():
    return jsonify(load_state())


@app.route("/flatten/<symbol>", methods=["POST"])
def manual_flatten(symbol: str):
    try:
        resolved_symbol = resolve_symbol(symbol)

        if not resolved_symbol:
            return jsonify({
                "error": "missing symbol"
            }), 400

        result = flatten_symbol(resolved_symbol)

        return jsonify({
            "symbol": resolved_symbol,
            "result": result
        })

    except Exception as exc:
        log(f"MANUAL FLATTEN ERROR: {exc}")

        return jsonify({
            "status": "error",
            "message": str(exc)
        }), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        # force=True avoids TradingView 415 errors when its
        # content-type header is missing or inconsistent.
        data = request.get_json(
            force=True,
            silent=True
        )

        log(f"WEBHOOK RECEIVED: {data}")

        if not data:
            return jsonify({
                "error": "missing or invalid JSON"
            }), 400

        raw_symbol = data.get("symbol")
        raw_signal = data.get("signal")

        strategy = str(
            data.get("strategy", "DEFAULT")
        ).upper().strip()

        symbol = resolve_symbol(raw_symbol)
        signal = normalize_signal(raw_signal)

        try:
            contracts = int(
                data.get("contracts", 1)
            )
        except (TypeError, ValueError):
            return jsonify({
                "error": "contracts must be an integer"
            }), 400

        if not symbol:
            return jsonify({
                "error": "missing symbol"
            }), 400

        if not signal:
            return jsonify({
                "error": "missing signal"
            }), 400

        log(
            f"PARSED: strategy={strategy}, "
            f"symbol={symbol}, signal={signal}, "
            f"contracts={contracts}"
        )

        # Outside session:
        # EXIT checks and closes only the named strategy.
        # Any late entry alert causes the symbol to be
        # flattened if a position remains.
        if not market_open():
            if signal == "EXIT":
                result = exit_strategy(
                    symbol,
                    strategy
                )

                return jsonify({
                    "status": (
                        "outside session - "
                        "strategy exit processed"
                    ),
                    "strategy": strategy,
                    "symbol": symbol,
                    "result": result
                })

            broker_quantity, broker_side, _ = (
                get_broker_position(symbol)
            )

            if broker_quantity > 0 and broker_side:
                result = flatten_symbol(symbol)

                return jsonify({
                    "status": (
                        "outside session - "
                        "symbol flattened"
                    ),
                    "symbol": symbol,
                    "result": result
                })

            return jsonify({
                "status": "outside session - ignored",
                "strategy": strategy,
                "symbol": symbol
            })

        if signal == "LONG":
            result = open_or_add_strategy(
                symbol=symbol,
                strategy=strategy,
                requested_side="LONG",
                contracts=contracts
            )

        elif signal == "SHORT":
            result = open_or_add_strategy(
                symbol=symbol,
                strategy=strategy,
                requested_side="SHORT",
                contracts=contracts
            )

        elif signal == "EXIT":
            result = exit_strategy(
                symbol=symbol,
                strategy=strategy
            )

        else:
            return jsonify({
                "error": "unknown signal",
                "received": raw_signal,
                "normalized": signal
            }), 400

        return jsonify({
            "status": "processed",
            "mode": TRADING_MODE,
            "strategy": strategy,
            "symbol": symbol,
            "signal": signal,
            "contracts": contracts,
            "result": result
        })

    except Exception as exc:
        log(f"ERROR: {exc}")

        return jsonify({
            "status": "error",
            "message": str(exc)
        }), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=10000
    )
