from __future__ import annotations

from datetime import datetime, time, timedelta
from pathlib import Path
from queue import Queue
from typing import Any
import json
import math
import os
import tempfile
import threading
import time as sleep_time

import pytz
import requests
from flask import Flask, jsonify, request


app = Flask(__name__)

# =========================================================
# CONFIGURATION
# =========================================================

VERSION = "6.0-safe-events"

TZ_NAME = os.getenv("BOT_TIMEZONE", "America/Chicago")
TZ = pytz.timezone(TZ_NAME)

SESSION_START = time(8, 31)
SESSION_END = time(14, 59)

TRADING_MODE = os.getenv("TRADING_MODE", "SIM").upper().strip()
BASE_URL = (
    "https://api.tradestation.com/v3"
    if TRADING_MODE == "LIVE"
    else "https://sim-api.tradestation.com/v3"
)
TOKEN_URL = "https://signin.tradestation.com/oauth/token"

CLIENT_ID = os.getenv("TS_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET", "").strip()
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN", "").strip()
ACCOUNT = os.getenv("TS_ACCOUNT", "").strip()

MNQ_SYMBOL = os.getenv("MNQ_SYMBOL", "MNQU26").upper().strip()
MGC_SYMBOL = os.getenv("MGC_SYMBOL", "MGCQ26").upper().strip()

ENABLE_SESSION_FILTER = (
    os.getenv("ENABLE_SESSION_FILTER", "true").lower().strip() == "true"
)
BROKER_STOP_ENABLED = (
    os.getenv("BROKER_STOP_ENABLED", "true").lower().strip() == "true"
)
AUTO_RECONCILE_ENABLED = (
    os.getenv("AUTO_RECONCILE_ENABLED", "true").lower().strip() == "true"
)
AUTO_RECONCILE_ON_START = False

DEFAULT_BROKER_STOP_DOLLARS = float(
    os.getenv("BROKER_STOP_MAX_LOSS_DOLLARS", "100")
)
MAX_CONTRACTS_PER_STRATEGY = int(
    os.getenv("MAX_CONTRACTS_PER_STRATEGY", "10")
)
MAX_NET_CONTRACTS = int(
    os.getenv("MAX_NET_CONTRACTS", "20")
)
REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("REQUEST_TIMEOUT_SECONDS", "15")
)
POSITION_SETTLE_SECONDS = float(
    os.getenv("POSITION_SETTLE_SECONDS", "8")
)
RECONCILE_INTERVAL_SECONDS = int(
    os.getenv("RECONCILE_INTERVAL_SECONDS", "30")
)
HEARTBEAT_INTERVAL_SECONDS = int(
    os.getenv("HEARTBEAT_INTERVAL_SECONDS", "600")
)
ORDER_COOLDOWN_SECONDS = int(
    os.getenv("ORDER_COOLDOWN_SECONDS", "15")
)
STALE_EVENT_TOLERANCE_MS = int(
    os.getenv("STALE_EVENT_TOLERANCE_MS", "2000")
)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

STATE_FILE = Path(
    os.getenv("STATE_FILE", "/tmp/strategy_state_v5_1.json")
)

# =========================================================
# GLOBALS
# =========================================================

_cached_access_token: str | None = None
_token_expires_at: datetime | None = None
token_lock = threading.RLock()

state_lock = threading.RLock()
symbol_locks: dict[str, threading.RLock] = {}
symbol_locks_guard = threading.Lock()

event_queue: Queue[dict[str, Any]] = Queue()
queued_event_ids: set[str] = set()
queued_event_ids_lock = threading.Lock()

shutdown_event = threading.Event()


# =========================================================
# LOGGING
# =========================================================

def now_local() -> datetime:
    return datetime.now(TZ)


def iso_now() -> str:
    return now_local().isoformat()


def log(message: str) -> None:
    stamp = now_local().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp} {TZ_NAME}] {message}", flush=True)


def log_separator(symbol: str) -> None:
    log("=" * 68)
    log(f"SYMBOL: {symbol}")
    log("=" * 68)


# =========================================================
# STATE
# =========================================================

def empty_state() -> dict[str, Any]:
    return {
        "version": VERSION,
        "strategies": {},
        "stops": {},
        "stop_details": {},
        "processed_events": {},
        "last_results": {},
        "symbol_session_dates": {},
        "symbol_runtime": {}
    }


def load_state() -> dict[str, Any]:
    with state_lock:
        if not STATE_FILE.exists():
            return empty_state()

        try:
            with STATE_FILE.open("r", encoding="utf-8") as file:
                state = json.load(file)

            default = empty_state()
            for key, value in default.items():
                state.setdefault(key, value)

            state["version"] = VERSION
            return state

        except Exception as exc:
            log(f"STATE READ ERROR: {exc}")
            return empty_state()


def save_state(state: dict[str, Any]) -> None:
    with state_lock:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(STATE_FILE.parent),
            delete=False
        ) as temporary:
            json.dump(state, temporary, indent=2, sort_keys=True)
            temporary_path = Path(temporary.name)

        temporary_path.replace(STATE_FILE)


def strategy_key(symbol: str, strategy: str) -> str:
    return f"{symbol}:{strategy}"


def get_symbol_lock(symbol: str) -> threading.RLock:
    with symbol_locks_guard:
        if symbol not in symbol_locks:
            symbol_locks[symbol] = threading.RLock()
        return symbol_locks[symbol]


def target_to_side_qty(target: int) -> tuple[str, int]:
    if target > 0:
        return "LONG", target
    if target < 0:
        return "SHORT", abs(target)
    return "FLAT", 0


def get_strategy_record(
    state: dict[str, Any],
    symbol: str,
    strategy: str
) -> dict[str, Any]:
    return state["strategies"].get(
        strategy_key(symbol, strategy),
        {
            "symbol": symbol,
            "strategy": strategy,
            "target": 0,
            "side": "FLAT",
            "qty": 0,
            "broker_stop_dollars": DEFAULT_BROKER_STOP_DOLLARS,
            "last_event_time_ms": 0,
            "last_event_id": None,
            "session_date": None,
            "updated_at": None
        }
    )


def set_strategy_target(
    state: dict[str, Any],
    *,
    symbol: str,
    strategy: str,
    target: int,
    stop_dollars: float,
    event_time_ms: int,
    event_id: str,
    session_date: str
) -> None:
    side, qty = target_to_side_qty(target)

    state["strategies"][strategy_key(symbol, strategy)] = {
        "symbol": symbol,
        "strategy": strategy,
        "target": int(target),
        "side": side,
        "qty": qty,
        "broker_stop_dollars": float(stop_dollars),
        "last_event_time_ms": int(event_time_ms),
        "last_event_id": event_id,
        "session_date": session_date,
        "updated_at": iso_now()
    }


def clear_symbol_targets(
    state: dict[str, Any],
    symbol: str,
    session_date: str
) -> None:
    for record in state["strategies"].values():
        if record.get("symbol") != symbol:
            continue

        record["target"] = 0
        record["side"] = "FLAT"
        record["qty"] = 0
        record["session_date"] = session_date
        record["updated_at"] = iso_now()


def desired_net_target(state: dict[str, Any], symbol: str) -> int:
    return sum(
        int(record.get("target", 0))
        for record in state["strategies"].values()
        if record.get("symbol") == symbol
    )


def strategy_snapshot(
    state: dict[str, Any],
    symbol: str
) -> list[dict[str, Any]]:
    records = [
        dict(record)
        for record in state["strategies"].values()
        if record.get("symbol") == symbol
    ]
    return sorted(records, key=lambda item: item.get("strategy", ""))


def known_symbols(state: dict[str, Any]) -> list[str]:
    symbols = {
        record.get("symbol")
        for record in state["strategies"].values()
        if record.get("symbol")
    }
    return sorted(symbols)


def get_stop_id(state: dict[str, Any], symbol: str) -> str | None:
    value = state["stops"].get(symbol)
    return str(value) if value else None


def set_stop_id(
    state: dict[str, Any],
    symbol: str,
    order_id: str,
    *,
    side: str | None = None,
    quantity: int | None = None,
    average_price: float | None = None,
    stop_price: float | None = None,
    risk_dollars: float | None = None
) -> None:
    state["stops"][symbol] = order_id

    if all(
        value is not None
        for value in (
            side,
            quantity,
            average_price,
            stop_price,
            risk_dollars
        )
    ):
        state["stop_details"][symbol] = {
            "order_id": str(order_id),
            "side": str(side),
            "quantity": int(quantity),
            "average_price": float(average_price),
            "stop_price": float(stop_price),
            "risk_dollars": float(risk_dollars),
            "updated_at": iso_now()
        }


def clear_stop_id(state: dict[str, Any], symbol: str) -> None:
    state["stops"].pop(symbol, None)
    state["stop_details"].pop(symbol, None)


def stop_is_unchanged(
    state: dict[str, Any],
    symbol: str,
    *,
    side: str,
    quantity: int,
    average_price: float,
    stop_price: float,
    risk_dollars: float
) -> bool:
    order_id = get_stop_id(state, symbol)
    details = state["stop_details"].get(symbol)

    if not order_id or not isinstance(details, dict):
        return False

    return (
        str(details.get("order_id")) == str(order_id)
        and details.get("side") == side
        and int(details.get("quantity", -1)) == int(quantity)
        and abs(
            float(details.get("average_price", -1.0))
            - float(average_price)
        ) < 0.000001
        and abs(
            float(details.get("stop_price", -1.0))
            - float(stop_price)
        ) < 0.000001
        and abs(
            float(details.get("risk_dollars", -1.0))
            - float(risk_dollars)
        ) < 0.000001
    )


def runtime_record(
    state: dict[str, Any],
    symbol: str
) -> dict[str, Any]:
    return state["symbol_runtime"].setdefault(
        symbol,
        {
            "last_order_at": None,
            "last_order_epoch": 0.0,
            "last_reconcile_at": None,
            "last_error": None,
            "last_drift_signature": None,
            "last_heartbeat_epoch": 0.0
        }
    )


def mark_event_processed(
    state: dict[str, Any],
    event_id: str,
    status: str
) -> None:
    state["processed_events"][event_id] = {
        "processed_at": iso_now(),
        "status": status
    }

    if len(state["processed_events"]) > 1500:
        keys = list(state["processed_events"].keys())
        for key in keys[:-1000]:
            state["processed_events"].pop(key, None)


# =========================================================
# PARSING AND SESSION
# =========================================================

def market_open() -> bool:
    if not ENABLE_SESSION_FILTER:
        return True

    current = now_local().time()
    return SESSION_START <= current <= SESSION_END


def resolve_symbol(value: Any) -> str | None:
    if value is None:
        return None

    symbol = str(value).upper().strip()

    if symbol in {"MNQ", "MNQ1!", "@MNQ"}:
        return MNQ_SYMBOL

    if symbol in {"MGC", "MGC1!", "@MGC"}:
        return MGC_SYMBOL

    return symbol or None


def normalize_signal(value: Any) -> str | None:
    if value is None:
        return None

    signal = str(value).upper().strip()

    if signal in {"LONG", "OPEN_LONG", "BUY"}:
        return "LONG"

    if signal in {"SHORT", "OPEN_SHORT", "SELL"}:
        return "SHORT"

    if signal in {
        "EXIT", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT",
        "SESSION_END", "SESSION END"
    }:
        return "EXIT"

    return signal


def session_date_from_event(event_time_ms: int) -> str:
    event_time = datetime.fromtimestamp(event_time_ms / 1000, TZ)
    return event_time.date().isoformat()


def parse_event(data: dict[str, Any]) -> dict[str, Any]:
    symbol = resolve_symbol(data.get("symbol"))
    signal = normalize_signal(data.get("signal"))
    strategy = str(data.get("strategy", "")).upper().strip()
    event_id = str(data.get("event_id", "")).strip()

    if not symbol:
        raise ValueError("Missing symbol")

    if signal not in {"LONG", "SHORT", "EXIT"}:
        raise ValueError(f"Unknown signal: {signal}")

    if not strategy:
        raise ValueError("Missing strategy")

    if not event_id:
        raise ValueError("Missing event_id")

    try:
        contracts = int(data.get("contracts", 1))
    except (TypeError, ValueError) as exc:
        raise ValueError("contracts must be an integer") from exc

    if contracts < 1 or contracts > MAX_CONTRACTS_PER_STRATEGY:
        raise ValueError(
            f"contracts must be between 1 and "
            f"{MAX_CONTRACTS_PER_STRATEGY}"
        )

    try:
        stop_dollars = float(
            data.get(
                "broker_stop_dollars",
                DEFAULT_BROKER_STOP_DOLLARS
            )
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "broker_stop_dollars must be numeric"
        ) from exc

    try:
        event_time_ms = int(data.get("event_time_ms"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Missing or invalid event_time_ms") from exc

    return {
        "symbol": symbol,
        "signal": signal,
        "strategy": strategy,
        "event_id": event_id,
        "event_time_ms": event_time_ms,
        "bar_time_ms": int(data.get("bar_time_ms", 0) or 0),
        "contracts": contracts,
        "broker_stop_dollars": max(1.0, stop_dollars),
        "session_date": session_date_from_event(event_time_ms),
        "received_at": iso_now()
    }


# =========================================================
# TRADESTATION AUTH
# =========================================================

def validate_environment() -> None:
    missing = [
        name
        for name, value in {
            "TS_CLIENT_ID": CLIENT_ID,
            "TS_CLIENT_SECRET": CLIENT_SECRET,
            "TS_REFRESH_TOKEN": REFRESH_TOKEN,
            "TS_ACCOUNT": ACCOUNT
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing environment variables: " + ", ".join(missing)
        )


def get_access_token() -> str:
    global _cached_access_token, _token_expires_at

    validate_environment()

    with token_lock:
        now = now_local()

        if (
            _cached_access_token
            and _token_expires_at
            and now < _token_expires_at
        ):
            return _cached_access_token

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN
            },
            timeout=REQUEST_TIMEOUT_SECONDS
        )

        log(f"TOKEN STATUS: {response.status_code}")

        if response.status_code != 200:
            raise RuntimeError(
                f"Token refresh failed: {response.text}"
            )

        payload = response.json()
        _cached_access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 1200))
        _token_expires_at = now + timedelta(
            seconds=max(60, expires_in - 60)
        )

        return _cached_access_token


def headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }


# =========================================================
# BROKER POSITION AND ORDER HELPERS
# =========================================================

def broker_position(
    symbol: str
) -> tuple[int, float | None]:
    response = requests.get(
        f"{BASE_URL}/brokerage/accounts/{ACCOUNT}/positions",
        headers=headers(),
        timeout=REQUEST_TIMEOUT_SECONDS
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Position request failed: {response.status_code} "
            f"{response.text}"
        )

    for position in response.json().get("Positions", []):
        if str(position.get("Symbol", "")).upper() != symbol.upper():
            continue

        raw_qty = float(position.get("Quantity", 0) or 0)
        qty = abs(int(raw_qty))
        long_short = str(position.get("LongShort", "")).upper()
        average_price_raw = position.get("AveragePrice")
        average_price = (
            float(average_price_raw)
            if average_price_raw not in (None, "")
            else None
        )

        if long_short.startswith("LONG") or raw_qty > 0:
            return qty, average_price

        if long_short.startswith("SHORT") or raw_qty < 0:
            return -qty, average_price

    return 0, None


def accepted_order_response(payload: dict[str, Any]) -> bool:
    orders = payload.get("Orders", [])

    for order in orders:
        error = str(order.get("Error", "")).upper()
        message = str(order.get("Message", "")).lower()

        if error in {"FAILED", "REJECTED", "ERROR"}:
            return False

        if "failed" in message or "rejected" in message:
            return False

    return bool(orders)


def extract_order_id(payload: dict[str, Any]) -> str | None:
    for order in payload.get("Orders", []):
        order_id = order.get("OrderID")
        if order_id:
            return str(order_id)
    return None


def submit_order(
    *,
    symbol: str,
    action: str,
    quantity: int,
    order_type: str = "Market",
    stop_price: float | None = None
) -> dict[str, Any]:
    if quantity <= 0:
        return {
            "accepted": False,
            "reason": "Quantity must be positive"
        }

    payload: dict[str, Any] = {
        "AccountID": ACCOUNT,
        "Symbol": symbol,
        "Quantity": str(quantity),
        "OrderType": order_type,
        "TradeAction": action,
        "TimeInForce": {"Duration": "DAY"}
    }

    if stop_price is not None:
        payload["StopPrice"] = str(stop_price)

    log(f"ORDER PAYLOAD: {payload}")

    response = requests.post(
        f"{BASE_URL}/orderexecution/orders",
        headers=headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS
    )

    log(f"ORDER HTTP: {response.status_code}")
    log(f"ORDER RESPONSE: {response.text}")

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {"raw_response": response.text}

    return {
        "accepted": (
            response.status_code == 200
            and accepted_order_response(response_payload)
        ),
        "http_status": response.status_code,
        "payload": payload,
        "response": response_payload
    }


def cancel_order(order_id: str) -> dict[str, Any]:
    response = requests.delete(
        f"{BASE_URL}/orderexecution/orders/{order_id}",
        headers=headers(),
        timeout=REQUEST_TIMEOUT_SECONDS
    )

    log(
        f"CANCEL ORDER {order_id}: "
        f"{response.status_code} {response.text}"
    )

    return {
        "http_status": response.status_code,
        "response": response.text
    }


def cancel_tracked_stop(
    state: dict[str, Any],
    symbol: str
) -> dict[str, Any]:
    order_id = get_stop_id(state, symbol)

    if not order_id:
        return {"status": "no tracked stop"}

    result = cancel_order(order_id)
    clear_stop_id(state, symbol)
    save_state(state)
    return result


def wait_for_position(
    symbol: str,
    expected: int,
    timeout_seconds: float
) -> tuple[int, float | None]:
    deadline = sleep_time.monotonic() + timeout_seconds
    latest = broker_position(symbol)

    while sleep_time.monotonic() < deadline:
        if latest[0] == expected:
            return latest

        sleep_time.sleep(0.5)
        latest = broker_position(symbol)

    return latest


# =========================================================
# FUTURES STOP CALCULATION
# =========================================================

def futures_specs(symbol: str) -> dict[str, float]:
    value = symbol.upper()

    if value.startswith("MNQ"):
        return {"point_value": 2.0, "tick_size": 0.25}

    if value.startswith("MGC"):
        return {"point_value": 10.0, "tick_size": 0.10}

    raise ValueError(
        f"Futures specifications are not configured for {symbol}"
    )


def round_stop_to_tick(
    price: float,
    side: str,
    tick_size: float
) -> float:
    tick_value = price / tick_size

    if side == "LONG":
        ticks = math.floor(tick_value)
    else:
        ticks = math.ceil(tick_value)

    return round(ticks * tick_size, 10)


def effective_net_stop_dollars(
    state: dict[str, Any],
    symbol: str,
    desired_target: int
) -> float:
    if desired_target == 0:
        return 0.0

    same_direction_records = []

    for record in state["strategies"].values():
        if record.get("symbol") != symbol:
            continue

        target = int(record.get("target", 0))

        if desired_target > 0 and target > 0:
            same_direction_records.append(record)

        if desired_target < 0 and target < 0:
            same_direction_records.append(record)

    if not same_direction_records:
        return DEFAULT_BROKER_STOP_DOLLARS * abs(desired_target)

    total_risk = sum(
        float(
            record.get(
                "broker_stop_dollars",
                DEFAULT_BROKER_STOP_DOLLARS
            )
        )
        for record in same_direction_records
    )

    return max(1.0, total_risk)


def place_protective_stop(
    state: dict[str, Any],
    symbol: str,
    desired_target: int
) -> dict[str, Any]:
    if not BROKER_STOP_ENABLED:
        return {"status": "disabled"}

    if desired_target == 0:
        return cancel_tracked_stop(state, symbol)

    actual, average_price = broker_position(symbol)

    if actual != desired_target or average_price is None:
        return {
            "status": "not placed",
            "reason": "Broker position is not synchronized",
            "actual": actual,
            "desired": desired_target
        }

    specs = futures_specs(symbol)
    quantity = abs(actual)
    side = "LONG" if actual > 0 else "SHORT"
    risk_dollars = effective_net_stop_dollars(
        state,
        symbol,
        desired_target
    )
    distance = risk_dollars / (
        quantity * specs["point_value"]
    )

    raw_stop = (
        average_price - distance
        if side == "LONG"
        else average_price + distance
    )
    stop_price = round_stop_to_tick(
        raw_stop,
        side,
        specs["tick_size"]
    )
    action = "SELL" if side == "LONG" else "BUY"

    if stop_is_unchanged(
        state,
        symbol,
        side=side,
        quantity=quantity,
        average_price=average_price,
        stop_price=stop_price,
        risk_dollars=risk_dollars
    ):
        return {
            "status": "unchanged",
            "side": side,
            "quantity": quantity,
            "average_price": average_price,
            "stop_price": stop_price,
            "risk_dollars": risk_dollars,
            "order_id": get_stop_id(state, symbol)
        }

    cancel_result = cancel_tracked_stop(state, symbol)

    result = submit_order(
        symbol=symbol,
        action=action,
        quantity=quantity,
        order_type="StopMarket",
        stop_price=stop_price
    )

    if result["accepted"]:
        order_id = extract_order_id(result["response"])
        if order_id:
            set_stop_id(
                state,
                symbol,
                order_id,
                side=side,
                quantity=quantity,
                average_price=average_price,
                stop_price=stop_price,
                risk_dollars=risk_dollars
            )
            save_state(state)

    return {
        "status": (
            "submitted"
            if result["accepted"]
            else "rejected"
        ),
        "side": side,
        "quantity": quantity,
        "average_price": average_price,
        "stop_price": stop_price,
        "risk_dollars": risk_dollars,
        "cancel_previous": cancel_result,
        "order": result
    }


# =========================================================
# TARGET ENGINE
# =========================================================

def reset_for_new_session(
    state: dict[str, Any],
    symbol: str,
    event_session_date: str
) -> bool:
    previous_date = state["symbol_session_dates"].get(symbol)

    if previous_date == event_session_date:
        return False

    log(
        f"NEW SESSION: {symbol}; "
        f"{previous_date} -> {event_session_date}. "
        f"Clearing prior-day strategy targets."
    )

    clear_symbol_targets(state, symbol, event_session_date)
    state["symbol_session_dates"][symbol] = event_session_date
    save_state(state)
    return True


def order_cooldown_active(
    state: dict[str, Any],
    symbol: str
) -> bool:
    runtime = runtime_record(state, symbol)
    elapsed = sleep_time.time() - float(
        runtime.get("last_order_epoch", 0.0)
    )
    return elapsed < ORDER_COOLDOWN_SECONDS


def stamp_order_time(
    state: dict[str, Any],
    symbol: str
) -> None:
    runtime = runtime_record(state, symbol)
    runtime["last_order_at"] = iso_now()
    runtime["last_order_epoch"] = sleep_time.time()
    save_state(state)


def clear_targets_after_external_flat(
    state: dict[str, Any],
    symbol: str,
    *,
    reason: str
) -> None:
    today = now_local().date().isoformat()
    clear_symbol_targets(state, symbol, today)
    state["symbol_session_dates"][symbol] = today
    clear_stop_id(state, symbol)
    state["last_results"][symbol] = {
        "status": "targets cleared",
        "symbol": symbol,
        "reason": reason,
        "time": iso_now()
    }
    save_state(state)


def reconcile_symbol(
    symbol: str,
    *,
    reason: str,
    force: bool = False,
    allow_market_order: bool = False
) -> dict[str, Any]:
    """
    Event-driven safety model:

    - TradingView events may change the broker position.
    - Periodic reconciliation NEVER submits a market order.
    - If a tracked stop or manual action flattens the broker, old strategy
      targets are cleared so the bot cannot automatically re-enter.
    - Periodic reconciliation only verifies state and maintains an
      unchanged protective stop.
    """
    with get_symbol_lock(symbol):
        state = load_state()
        desired = desired_net_target(state, symbol)

        if abs(desired) > MAX_NET_CONTRACTS:
            result = {
                "status": "blocked",
                "reason": (
                    f"Desired net {desired} exceeds "
                    f"MAX_NET_CONTRACTS={MAX_NET_CONTRACTS}"
                ),
                "symbol": symbol,
                "time": iso_now()
            }
            state["last_results"][symbol] = result
            save_state(state)
            log(f"BLOCKED {symbol}: {result['reason']}")
            return result

        actual_before, _ = broker_position(symbol)
        delta = desired - actual_before
        runtime = runtime_record(state, symbol)
        runtime["last_reconcile_at"] = iso_now()

        # PERIODIC / STARTUP MONITORING: never place market orders.
        if not allow_market_order:
            # A real broker flatten after the order cooldown means the stop
            # filled or the user manually closed. Clear stale targets.
            if (
                desired != 0
                and actual_before == 0
                and not order_cooldown_active(state, symbol)
            ):
                clear_targets_after_external_flat(
                    state,
                    symbol,
                    reason=(
                        "broker flat while strategy targets were active; "
                        "treated as stop/manual close"
                    )
                )
                log(
                    f"EXTERNAL FLAT {symbol}: old desired {desired:+d}; "
                    "targets cleared; waiting for a new webhook"
                )
                return {
                    "status": "external flat accepted",
                    "symbol": symbol,
                    "old_desired": desired,
                    "actual": 0,
                    "reason": reason,
                    "time": iso_now()
                }

            # When synchronized, only ensure the current stop exists.
            if delta == 0:
                stop_result = place_protective_stop(
                    state,
                    symbol,
                    desired
                )
                result = {
                    "status": "synchronized",
                    "symbol": symbol,
                    "desired": desired,
                    "actual_before": actual_before,
                    "actual_after": actual_before,
                    "delta": 0,
                    "protective_stop": stop_result,
                    "reason": reason,
                    "time": iso_now()
                }
                state["last_results"][symbol] = result
                save_state(state)

                # Quiet heartbeat, at most once per configured interval.
                now_epoch = sleep_time.time()
                last_heartbeat = float(
                    runtime.get("last_heartbeat_epoch", 0.0)
                )
                if now_epoch - last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
                    log(
                        f"HEARTBEAT {symbol}: desired={desired:+d} "
                        f"actual={actual_before:+d} stop="
                        f"{stop_result.get('status')}"
                    )
                    runtime["last_heartbeat_epoch"] = now_epoch
                    save_state(state)

                return result

            # Non-flat mismatch: report once per changed mismatch, but do not
            # trade. A fresh TradingView event is required to change exposure.
            signature = f"{desired}:{actual_before}"
            if runtime.get("last_drift_signature") != signature:
                log(
                    f"POSITION DRIFT {symbol}: desired={desired:+d} "
                    f"actual={actual_before:+d}; NO ORDER SENT; "
                    "waiting for a new webhook or manual reconcile"
                )
                runtime["last_drift_signature"] = signature

            result = {
                "status": "drift detected - no automatic order",
                "symbol": symbol,
                "desired": desired,
                "actual": actual_before,
                "delta": delta,
                "reason": reason,
                "time": iso_now()
            }
            state["last_results"][symbol] = result
            save_state(state)
            return result

        # EVENT / EXPLICIT MANUAL RECONCILE: position changes are allowed.
        log(
            f"TARGET CHANGE {symbol}: desired={desired:+d} "
            f"actual={actual_before:+d} delta={delta:+d} | {reason}"
        )

        if delta == 0:
            stop_result = place_protective_stop(
                state,
                symbol,
                desired
            )
            result = {
                "status": "synchronized",
                "symbol": symbol,
                "desired": desired,
                "actual_before": actual_before,
                "actual_after": actual_before,
                "delta": 0,
                "protective_stop": stop_result,
                "reason": reason,
                "time": iso_now()
            }
            runtime["last_drift_signature"] = None
            state["last_results"][symbol] = result
            save_state(state)
            return result

        cancel_result = cancel_tracked_stop(state, symbol)

        action = "BUY" if delta > 0 else "SELL"
        order_result = submit_order(
            symbol=symbol,
            action=action,
            quantity=abs(delta)
        )
        stamp_order_time(state, symbol)

        if not order_result["accepted"]:
            actual_after_rejection, _ = broker_position(symbol)
            restore_stop = place_protective_stop(
                state,
                symbol,
                actual_after_rejection
            )
            result = {
                "status": "order rejected",
                "symbol": symbol,
                "desired": desired,
                "actual_before": actual_before,
                "actual_after": actual_after_rejection,
                "delta": delta,
                "order": order_result,
                "cancel_previous_stop": cancel_result,
                "restored_stop": restore_stop,
                "reason": reason,
                "time": iso_now()
            }
            runtime["last_error"] = result
            state["last_results"][symbol] = result
            save_state(state)
            return result

        actual_after, _ = wait_for_position(
            symbol,
            desired,
            POSITION_SETTLE_SECONDS
        )

        stop_target = desired if actual_after == desired else actual_after
        stop_result = place_protective_stop(
            state,
            symbol,
            stop_target
        )

        status = (
            "synchronized"
            if actual_after == desired
            else "order accepted; broker not yet synchronized"
        )

        result = {
            "status": status,
            "symbol": symbol,
            "desired": desired,
            "actual_before": actual_before,
            "actual_after": actual_after,
            "delta": delta,
            "order": order_result,
            "cancel_previous_stop": cancel_result,
            "protective_stop": stop_result,
            "reason": reason,
            "time": iso_now()
        }

        runtime["last_reconcile_at"] = iso_now()
        runtime["last_drift_signature"] = None
        runtime["last_error"] = (
            None if status == "synchronized" else result
        )
        state["last_results"][symbol] = result
        save_state(state)

        log(
            f"ORDER COMPLETE {symbol}: status={status} "
            f"broker={actual_after:+d}"
        )
        return result

def apply_event(event: dict[str, Any]) -> dict[str, Any]:
    symbol = event["symbol"]

    with get_symbol_lock(symbol):
        state = load_state()

        if event["event_id"] in state["processed_events"]:
            return {
                "status": "duplicate ignored",
                "event_id": event["event_id"]
            }

        reset_for_new_session(
            state,
            symbol,
            event["session_date"]
        )

        current = get_strategy_record(
            state,
            symbol,
            event["strategy"]
        )
        previous_event_time = int(
            current.get("last_event_time_ms", 0)
        )

        if (
            previous_event_time
            and event["event_time_ms"] + STALE_EVENT_TOLERANCE_MS
            < previous_event_time
        ):
            result = {
                "status": "stale event ignored",
                "event_id": event["event_id"],
                "event_time_ms": event["event_time_ms"],
                "previous_event_time_ms": previous_event_time,
                "strategy": event["strategy"],
                "symbol": symbol
            }
            mark_event_processed(
                state,
                event["event_id"],
                result["status"]
            )
            state["last_results"][symbol] = result
            save_state(state)
            return result

        if event["signal"] == "LONG":
            target = event["contracts"]
        elif event["signal"] == "SHORT":
            target = -event["contracts"]
        else:
            target = 0

        old_target = int(current.get("target", 0))

        set_strategy_target(
            state,
            symbol=symbol,
            strategy=event["strategy"],
            target=target,
            stop_dollars=event["broker_stop_dollars"],
            event_time_ms=event["event_time_ms"],
            event_id=event["event_id"],
            session_date=event["session_date"]
        )
        save_state(state)

        result = reconcile_symbol(
            symbol,
            reason=(
                f"event {event['strategy']} "
                f"{event['signal']} "
                f"{old_target:+d}->{target:+d}"
            ),
            force=True,
            allow_market_order=True
        )

        state = load_state()
        result["event_id"] = event["event_id"]
        result["strategy"] = event["strategy"]
        result["signal"] = event["signal"]
        result["old_strategy_target"] = old_target
        result["new_strategy_target"] = target
        result["targets"] = strategy_snapshot(state, symbol)

        mark_event_processed(
            state,
            event["event_id"],
            result["status"]
        )
        state["last_results"][symbol] = result
        save_state(state)

        return result


# =========================================================
# BACKGROUND WORKERS
# =========================================================

def event_worker() -> None:
    while not shutdown_event.is_set():
        event = event_queue.get()

        try:
            log(
                f"EVENT START: {event['event_id']} | "
                f"{event['strategy']} {event['signal']} "
                f"{event['symbol']}"
            )
            result = apply_event(event)
            log(
                f"EVENT END: {event['event_id']} | "
                f"{result.get('status')}"
            )

        except Exception as exc:
            log(
                f"EVENT ERROR: {event.get('event_id')} | {exc}"
            )

            try:
                state = load_state()
                symbol = event.get("symbol", "UNKNOWN")
                state["last_results"][symbol] = {
                    "status": "worker error",
                    "message": str(exc),
                    "event_id": event.get("event_id"),
                    "time": iso_now()
                }
                save_state(state)
            except Exception as state_exc:
                log(f"STATE ERROR AFTER WORKER ERROR: {state_exc}")

        finally:
            with queued_event_ids_lock:
                queued_event_ids.discard(event.get("event_id", ""))
            event_queue.task_done()


def process_event_direct(event: dict[str, Any]) -> None:
    try:
        log(
            f"EVENT START: {event['event_id']} | "
            f"{event['strategy']} {event['signal']} "
            f"{event['symbol']}"
        )
        result = apply_event(event)
        log(
            f"EVENT END: {event['event_id']} | "
            f"{result.get('status')}"
        )

    except Exception as exc:
        log(
            f"EVENT ERROR: {event.get('event_id')} | {exc}"
        )

        try:
            state = load_state()
            symbol = event.get("symbol", "UNKNOWN")
            state["last_results"][symbol] = {
                "status": "direct worker error",
                "message": str(exc),
                "event_id": event.get("event_id"),
                "time": iso_now()
            }
            save_state(state)
        except Exception as state_exc:
            log(f"STATE ERROR AFTER EVENT ERROR: {state_exc}")

    finally:
        with queued_event_ids_lock:
            queued_event_ids.discard(event.get("event_id", ""))


def reconciler_worker() -> None:
    # Give Gunicorn and OAuth a moment to initialize.
    sleep_time.sleep(5)

    first_pass = True

    while not shutdown_event.is_set():
        try:
            if AUTO_RECONCILE_ENABLED and (
                market_open() or TRADING_MODE == "SIM"
            ):
                state = load_state()
                symbols = known_symbols(state)

                for symbol in symbols:
                    if first_pass and not AUTO_RECONCILE_ON_START:
                        continue

                    try:
                        reconcile_symbol(
                            symbol,
                            reason=(
                                "startup monitor"
                                if first_pass
                                else "periodic monitor"
                            ),
                            force=False,
                            allow_market_order=False
                        )
                    except Exception as exc:
                        log(
                            f"RECONCILE ERROR {symbol}: {exc}"
                        )

            first_pass = False

        except Exception as exc:
            log(f"RECONCILER ERROR: {exc}")

        shutdown_event.wait(RECONCILE_INTERVAL_SECONDS)


event_worker_thread: threading.Thread | None = None
reconciler_thread: threading.Thread | None = None
worker_start_lock = threading.Lock()


def ensure_background_workers() -> None:
    global reconciler_thread

    with worker_start_lock:
        if (
            reconciler_thread is None
            or not reconciler_thread.is_alive()
        ):
            log("STARTING MONITOR WORKER")
            reconciler_thread = threading.Thread(
                target=reconciler_worker,
                name="monitor-worker",
                daemon=True
            )
            reconciler_thread.start()


ensure_background_workers()


# =========================================================
# ROUTES
# =========================================================

@app.route("/", methods=["GET"])
def home():
    return (
        f"TradeStation Futures Bot {VERSION} | "
        f"Mode: {TRADING_MODE}",
        200
    )


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "version": VERSION,
            "mode": TRADING_MODE,
            "token_ok": bool(token),
            "account_configured": bool(ACCOUNT),
            "state_file": str(STATE_FILE),
            "legacy_queue_size": event_queue.qsize(),
            "active_event_count": len(queued_event_ids),
            "session_filter_enabled": ENABLE_SESSION_FILTER,
            "market_open": market_open(),
            "auto_reconcile_enabled": AUTO_RECONCILE_ENABLED,
            "broker_stop_enabled": BROKER_STOP_ENABLED,
            "event_processing": "direct serialized threads",
            "reconciler_alive": (
                reconciler_thread is not None
                and reconciler_thread.is_alive()
            )
        })

    except Exception as exc:
        return jsonify({
            "status": "error",
            "version": VERSION,
            "message": str(exc)
        }), 500


@app.route("/state", methods=["GET"])
def state_view():
    state = load_state()
    symbols_payload: dict[str, Any] = {}

    for symbol in known_symbols(state):
        desired = desired_net_target(state, symbol)

        try:
            actual, average_price = broker_position(symbol)
            synchronized = actual == desired
            broker_error = None
        except Exception as exc:
            actual = None
            average_price = None
            synchronized = False
            broker_error = str(exc)

        desired_side, desired_qty = target_to_side_qty(desired)
        actual_side, actual_qty = (
            target_to_side_qty(actual)
            if actual is not None
            else ("UNKNOWN", 0)
        )

        symbols_payload[symbol] = {
            "strategies": strategy_snapshot(state, symbol),
            "desired_broker": {
                "signed": desired,
                "side": desired_side,
                "qty": desired_qty
            },
            "actual_broker": {
                "signed": actual,
                "side": actual_side,
                "qty": actual_qty,
                "average_price": average_price,
                "error": broker_error
            },
            "synchronized": synchronized,
            "tracked_stop_order_id": get_stop_id(
                state,
                symbol
            ),
            "runtime": state["symbol_runtime"].get(
                symbol,
                {}
            ),
            "last_result": state["last_results"].get(
                symbol
            )
        }

    return jsonify({
        "version": VERSION,
        "mode": TRADING_MODE,
        "time": iso_now(),
        "symbols": symbols_payload
    })


@app.route("/reconcile/<symbol>", methods=["POST"])
def manual_reconcile(symbol: str):
    resolved = resolve_symbol(symbol)

    if not resolved:
        return jsonify({"error": "Invalid symbol"}), 400

    try:
        return jsonify(
            reconcile_symbol(
                resolved,
                reason="manual reconcile",
                force=True,
                allow_market_order=True
            )
        )
    except Exception as exc:
        return jsonify({
            "status": "error",
            "message": str(exc)
        }), 500


@app.route("/flatten/<symbol>", methods=["POST"])
def flatten(symbol: str):
    resolved = resolve_symbol(symbol)

    if not resolved:
        return jsonify({"error": "Invalid symbol"}), 400

    with get_symbol_lock(resolved):
        try:
            state = load_state()
            today = now_local().date().isoformat()
            clear_symbol_targets(state, resolved, today)
            state["symbol_session_dates"][resolved] = today
            save_state(state)

            result = reconcile_symbol(
                resolved,
                reason="manual flatten",
                force=True,
                allow_market_order=True
            )

            return jsonify(result)

        except Exception as exc:
            return jsonify({
                "status": "error",
                "message": str(exc)
            }), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)

    log(
        "WEBHOOK "
        f"{data.get('strategy') if isinstance(data, dict) else '?'} "
        f"{data.get('signal') if isinstance(data, dict) else '?'} "
        f"{data.get('symbol') if isinstance(data, dict) else '?'}"
    )

    if not isinstance(data, dict):
        return jsonify({
            "error": "Missing or invalid JSON"
        }), 400

    if WEBHOOK_SECRET:
        supplied = str(data.get("secret", "")).strip()
        if supplied != WEBHOOK_SECRET:
            return jsonify({
                "error": "Invalid webhook secret"
            }), 401

    try:
        event = parse_event(data)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    if ENABLE_SESSION_FILTER and not market_open():
        if event["signal"] != "EXIT":
            return jsonify({
                "status": "session closed",
                "event_id": event["event_id"]
            }), 200

    state = load_state()

    if event["event_id"] in state["processed_events"]:
        return jsonify({
            "status": "duplicate already processed",
            "event_id": event["event_id"]
        }), 200

    ensure_background_workers()

    with queued_event_ids_lock:
        if event["event_id"] in queued_event_ids:
            return jsonify({
                "status": "duplicate already queued",
                "event_id": event["event_id"]
            }), 200

        queued_event_ids.add(event["event_id"])

    event_thread = threading.Thread(
        target=process_event_direct,
        args=(event,),
        name=f"event-{event['strategy']}",
        daemon=True
    )
    event_thread.start()

    log(
        f"EVENT DISPATCHED: {event['event_id']} | "
        f"thread_alive={event_thread.is_alive()}"
    )

    return jsonify({
        "status": "accepted",
        "event_id": event["event_id"],
        "processing": "direct background thread"
    }), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
