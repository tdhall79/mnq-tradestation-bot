from __future__ import annotations

from flask import Flask, jsonify, request
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from queue import Queue
from typing import Any
import hashlib
import json
import math
import os
import tempfile
import threading
import time as sleep_time

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
BASE_URL = (
    "https://api.tradestation.com/v3"
    if TRADING_MODE == "LIVE"
    else "https://sim-api.tradestation.com/v3"
)
TOKEN_URL = "https://signin.tradestation.com/oauth/token"

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TS_REFRESH_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")

MNQ_SYMBOL = os.getenv("MNQ_SYMBOL", "MNQU26")
MGC_SYMBOL = os.getenv("MGC_SYMBOL", "MGCQ26")

MAX_CONTRACTS_PER_ORDER = int(
    os.getenv("MAX_CONTRACTS_PER_ORDER", "10")
)

BROKER_STOP_ENABLED = (
    os.getenv("BROKER_STOP_ENABLED", "true").lower() == "true"
)

DEFAULT_BROKER_STOP_DOLLARS = float(
    os.getenv("BROKER_STOP_MAX_LOSS_DOLLARS", "100")
)

ENABLE_SESSION_FILTER = (
    os.getenv("ENABLE_SESSION_FILTER", "").strip().lower()
)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

REQUEST_TIMEOUT = int(
    os.getenv("REQUEST_TIMEOUT_SECONDS", "15")
)

# Mount a Render persistent disk at /var/data and set:
# STATE_FILE=/var/data/strategy_state.json
STATE_FILE = Path(
    os.getenv("STATE_FILE", "/tmp/strategy_state.json")
)

_cached_access_token: str | None = None
_token_expires_at: datetime | None = None

state_lock = threading.RLock()
symbol_locks: dict[str, threading.RLock] = {}
symbol_locks_guard = threading.Lock()

event_queue: Queue[dict[str, Any]] = Queue()
queued_event_ids: set[str] = set()
queued_event_ids_lock = threading.Lock()


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
        "stops": {},
        "processed_events": {},
        "last_results": {},
        "symbol_session_dates": {}
    }


def load_state() -> dict[str, Any]:
    with state_lock:
        if not STATE_FILE.exists():
            return empty_state()

        try:
            with STATE_FILE.open("r", encoding="utf-8") as file:
                state = json.load(file)

            for key, default in empty_state().items():
                state.setdefault(key, default)

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
        ) as temporary_file:
            json.dump(state, temporary_file, indent=2)
            temporary_path = Path(temporary_file.name)

        temporary_path.replace(STATE_FILE)


def strategy_key(symbol: str, strategy: str) -> str:
    return f"{symbol}:{strategy}"


def get_symbol_lock(symbol: str) -> threading.RLock:
    with symbol_locks_guard:
        if symbol not in symbol_locks:
            symbol_locks[symbol] = threading.RLock()
        return symbol_locks[symbol]


def signed_target(side: str | None, qty: int) -> int:
    if not side or qty <= 0:
        return 0
    return qty if side == "LONG" else -qty


def target_to_side_qty(target: int) -> tuple[str | None, int]:
    if target > 0:
        return "LONG", target
    if target < 0:
        return "SHORT", abs(target)
    return None, 0


def get_strategy_target(
    state: dict[str, Any],
    symbol: str,
    strategy: str
) -> dict[str, Any]:
    key = strategy_key(symbol, strategy)
    return state["strategies"].get(
        key,
        {
            "symbol": symbol,
            "strategy": strategy,
            "target": 0,
            "side": None,
            "qty": 0,
            "broker_stop_dollars": DEFAULT_BROKER_STOP_DOLLARS,
            "updated_at": None
        }
    )


def set_strategy_target(
    state: dict[str, Any],
    symbol: str,
    strategy: str,
    target: int,
    broker_stop_dollars: float
) -> None:
    side, qty = target_to_side_qty(target)

    state["strategies"][strategy_key(symbol, strategy)] = {
        "symbol": symbol,
        "strategy": strategy,
        "target": int(target),
        "side": side,
        "qty": qty,
        "broker_stop_dollars": float(broker_stop_dollars),
        "updated_at": datetime.now(TZ).isoformat()
    }


def clear_symbol_targets(
    state: dict[str, Any],
    symbol: str
) -> None:
    for item in state["strategies"].values():
        if item.get("symbol") == symbol:
            item["target"] = 0
            item["side"] = None
            item["qty"] = 0
            item["updated_at"] = datetime.now(TZ).isoformat()


def desired_net_target(
    state: dict[str, Any],
    symbol: str
) -> int:
    total = 0
    for item in state["strategies"].values():
        if item.get("symbol") != symbol:
            continue

        if "target" in item:
            total += int(item.get("target", 0))
        else:
            total += signed_target(
                item.get("side"),
                int(item.get("qty", 0))
            )

    return total


def target_snapshot(
    state: dict[str, Any],
    symbol: str
) -> list[dict[str, Any]]:
    snapshot = []

    for item in state["strategies"].values():
        if item.get("symbol") == symbol:
            snapshot.append(item.copy())

    return sorted(
        snapshot,
        key=lambda item: str(item.get("strategy", ""))
    )


def effective_net_stop_dollars(
    state: dict[str, Any],
    symbol: str,
    desired_target: int
) -> float:
    """
    Convert each strategy's total risk into risk per contract,
    average it across active strategy contracts, then apply that
    per-contract risk to the absolute net broker quantity.

    Example:
      2 active strategies, each 1 contract and $100 total risk.
      Net position 2 -> $200 total broker-stop risk.
      One long and one short -> net 0 -> no broker stop.
    """
    net_qty = abs(desired_target)

    if net_qty == 0:
        return 0.0

    total_risk = 0.0
    total_contracts = 0

    for item in state["strategies"].values():
        if item.get("symbol") != symbol:
            continue

        target = int(item.get("target", 0))
        qty = abs(target)

        if qty <= 0:
            continue

        strategy_total_risk = float(
            item.get(
                "broker_stop_dollars",
                DEFAULT_BROKER_STOP_DOLLARS
            )
        )

        risk_per_contract = strategy_total_risk / qty
        total_risk += risk_per_contract * qty
        total_contracts += qty

    if total_contracts <= 0:
        return DEFAULT_BROKER_STOP_DOLLARS * net_qty

    average_risk_per_contract = total_risk / total_contracts
    return max(1.0, average_risk_per_contract * net_qty)


def get_tracked_stop_id(
    state: dict[str, Any],
    symbol: str
) -> str | None:
    value = state["stops"].get(symbol)
    return str(value) if value else None


def set_tracked_stop_id(
    state: dict[str, Any],
    symbol: str,
    order_id: str
) -> None:
    state["stops"][symbol] = order_id


def clear_tracked_stop_id(
    state: dict[str, Any],
    symbol: str
) -> None:
    state["stops"].pop(symbol, None)


def event_already_processed(
    state: dict[str, Any],
    event_id: str
) -> bool:
    return event_id in state["processed_events"]


def mark_event_processed(
    state: dict[str, Any],
    event_id: str,
    result: dict[str, Any]
) -> None:
    state["processed_events"][event_id] = {
        "processed_at": datetime.now(TZ).isoformat(),
        "status": result.get("status")
    }

    # Keep the state file bounded.
    if len(state["processed_events"]) > 1000:
        oldest = list(state["processed_events"].keys())[:-750]
        for key in oldest:
            state["processed_events"].pop(key, None)


# =========================================================
# SESSION, SIGNAL, AND SYMBOL PARSING
# =========================================================

def market_open() -> bool:
    if ENABLE_SESSION_FILTER == "false":
        return True

    if ENABLE_SESSION_FILTER == "true":
        current_time = datetime.now(TZ).time()
        return SESSION_START <= current_time <= SESSION_END

    # Default:
    # SIM = 24 hours
    # LIVE = configured session only
    if TRADING_MODE == "SIM":
        return True

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

    return normalized


def normalize_signal(signal: Any) -> str | None:
    if not signal:
        return None

    normalized = str(signal).upper().strip()

    if normalized in {
        "LONG", "OPEN_LONG", "BUY", "DCA L", "DCA LONG"
    }:
        return "LONG"

    if normalized in {
        "SHORT", "OPEN_SHORT", "SELL", "DCA S", "DCA SHORT"
    }:
        return "SHORT"

    if normalized in {
        "EXIT", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT",
        "SESSION END", "SESSION_END", "TP1", "SL"
    }:
        return "EXIT"

    return normalized


# =========================================================
# FUTURES SPECIFICATIONS
# =========================================================

def futures_specs(symbol: str) -> dict[str, float]:
    normalized = symbol.upper().strip()

    if normalized.startswith("MNQ"):
        return {"point_value": 2.0, "tick_size": 0.25}

    if normalized.startswith("MGC"):
        return {"point_value": 10.0, "tick_size": 0.10}

    if normalized.startswith("SIL"):
        return {"point_value": 1000.0, "tick_size": 0.01}

    if normalized.startswith("QC"):
        return {"point_value": 12500.0, "tick_size": 0.002}

    if normalized.startswith(("MZS", "MZC", "MZW")):
        return {"point_value": 5.0, "tick_size": 0.50}

    raise ValueError(
        f"No futures specifications configured for {symbol}."
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
        raise ValueError(f"Unknown side: {side}")

    return round(rounded_ticks * tick_size, 10)


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
            "Missing environment variables: " + ", ".join(missing)
        )


def get_access_token() -> str:
    global _cached_access_token, _token_expires_at

    validate_environment()
    now = datetime.now(TZ)

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

    for position in response.json().get("Positions", []):
        if position.get("Symbol", "").upper() != symbol.upper():
            continue

        raw_qty = float(position.get("Quantity", 0))
        qty = abs(int(raw_qty))

        long_short = str(
            position.get("LongShort", "")
        ).upper()

        avg_raw = position.get("AveragePrice")
        avg_price = (
            float(avg_raw)
            if avg_raw not in (None, "")
            else None
        )

        if long_short.startswith("LONG") or raw_qty > 0:
            return qty, "LONG", avg_price

        if long_short.startswith("SHORT") or raw_qty < 0:
            return qty, "SHORT", avg_price

    return 0, None, None


def broker_signed_position(symbol: str) -> tuple[int, float | None]:
    qty, side, avg_price = get_broker_position(symbol)

    if side == "LONG":
        return qty, avg_price

    if side == "SHORT":
        return -qty, avg_price

    return 0, None


def wait_for_signed_position(
    symbol: str,
    desired_target: int,
    tries: int = 10,
    delay: float = 0.50
) -> tuple[int, float | None]:
    last = (0, None)

    for _ in range(tries):
        last = broker_signed_position(symbol)

        if last[0] == desired_target:
            return last

        sleep_time.sleep(delay)

    return last


# =========================================================
# ORDER HELPERS
# =========================================================

def response_order_accepted(
    response_data: dict[str, Any]
) -> bool:
    orders = response_data.get("Orders", [])

    if not orders:
        return False

    for order in orders:
        error = str(order.get("Error", "")).upper()
        message = str(order.get("Message", "")).lower()

        if error in {"FAILED", "REJECTED", "ERROR"}:
            return False

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
                f"Requested {quantity} exceeds "
                f"MAX_CONTRACTS_PER_ORDER={MAX_CONTRACTS_PER_ORDER}"
            )
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
        headers=auth_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT
    )

    log(f"ORDER HTTP STATUS: {response.status_code}")
    log(f"ORDER RESPONSE: {response.text}")

    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw_response": response.text}

    return {
        "accepted": (
            response.status_code == 200
            and response_order_accepted(response_data)
        ),
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

    log(f"CANCEL ORDER {order_id} STATUS: {response.status_code}")
    log(f"CANCEL RESPONSE: {response.text}")

    try:
        response_data = response.json()
    except ValueError:
        response_data = {"raw_response": response.text}

    return {
        "http_status": response.status_code,
        "response": response_data
    }


def cancel_protective_stop(
    state: dict[str, Any],
    symbol: str
) -> dict[str, Any]:
    order_id = get_tracked_stop_id(state, symbol)

    if not order_id:
        return {"status": "no protective stop tracked"}

    result = cancel_order(order_id)
    clear_tracked_stop_id(state, symbol)
    save_state(state)

    return result


# =========================================================
# NET BROKER STOP
# =========================================================

def calculate_stop_price(
    symbol: str,
    quantity: int,
    side: str,
    average_price: float,
    total_risk_dollars: float
) -> float:
    specs = futures_specs(symbol)

    distance = (
        total_risk_dollars
        / (quantity * specs["point_value"])
    )

    raw_stop = (
        average_price - distance
        if side == "LONG"
        else average_price + distance
    )

    return round_to_tick(
        raw_stop,
        side,
        specs["tick_size"]
    )


def place_net_protective_stop(
    state: dict[str, Any],
    symbol: str,
    desired_target: int
) -> dict[str, Any]:
    if not BROKER_STOP_ENABLED:
        return {"status": "broker stop disabled"}

    if desired_target == 0:
        return cancel_protective_stop(state, symbol)

    actual_target, average_price = wait_for_signed_position(
        symbol,
        desired_target
    )

    if actual_target != desired_target or average_price is None:
        return {
            "status": "stop not placed",
            "reason": "broker position did not reach desired target",
            "desired_target": desired_target,
            "actual_target": actual_target
        }

    side = "LONG" if desired_target > 0 else "SHORT"
    quantity = abs(desired_target)

    total_risk = effective_net_stop_dollars(
        state,
        symbol,
        desired_target
    )

    stop_price = calculate_stop_price(
        symbol,
        quantity,
        side,
        average_price,
        total_risk
    )

    cancel_result = cancel_protective_stop(state, symbol)

    stop_action = "SELL" if side == "LONG" else "BUY"

    log(
        f"NET STOP: {side} {quantity} {symbol} "
        f"avg={average_price} stop={stop_price} "
        f"total_risk=${total_risk:.2f}"
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
            set_tracked_stop_id(state, symbol, order_id)
            save_state(state)

    return {
        "status": (
            "protective stop submitted"
            if stop_result["accepted"]
            else "protective stop rejected"
        ),
        "side": side,
        "quantity": quantity,
        "average_price": average_price,
        "stop_price": stop_price,
        "total_risk_dollars": total_risk,
        "cancel_previous_stop": cancel_result,
        "stop_order": stop_result
    }


# =========================================================
# TARGET-POSITION EXECUTION
# =========================================================

def reset_for_new_session_if_needed(
    state: dict[str, Any],
    symbol: str
) -> None:
    today = datetime.now(TZ).date().isoformat()
    previous = state["symbol_session_dates"].get(symbol)

    if previous == today:
        return

    log(
        f"NEW SESSION RESET: {symbol}; "
        f"previous={previous}, current={today}"
    )

    clear_symbol_targets(state, symbol)
    state["symbol_session_dates"][symbol] = today
    save_state(state)


def apply_target_event(event: dict[str, Any]) -> dict[str, Any]:
    symbol = event["symbol"]
    strategy = event["strategy"]
    signal = event["signal"]
    contracts = event["contracts"]
    stop_dollars = event["broker_stop_dollars"]
    event_id = event["event_id"]

    with get_symbol_lock(symbol):
        state = load_state()

        if event_already_processed(state, event_id):
            return {
                "status": "duplicate ignored",
                "event_id": event_id
            }

        reset_for_new_session_if_needed(state, symbol)

        current = get_strategy_target(
            state,
            symbol,
            strategy
        )

        if signal == "LONG":
            new_target = contracts
        elif signal == "SHORT":
            new_target = -contracts
        elif signal == "EXIT":
            new_target = 0
        else:
            raise ValueError(f"Unknown signal: {signal}")

        old_target = int(current.get("target", 0))

        set_strategy_target(
            state,
            symbol,
            strategy,
            new_target,
            stop_dollars
        )
        save_state(state)

        desired_target = desired_net_target(state, symbol)
        actual_target, _ = broker_signed_position(symbol)
        delta = desired_target - actual_target

        log(
            f"TARGET UPDATE: strategy={strategy} "
            f"{old_target}->{new_target}; "
            f"symbol={symbol}; desired_net={desired_target}; "
            f"actual_net={actual_target}; delta={delta}"
        )

        # Remove the old net stop before changing broker exposure.
        cancel_result = cancel_protective_stop(
            state,
            symbol
        )

        order_result: dict[str, Any] | None = None

        if delta != 0:
            action = "BUY" if delta > 0 else "SELL"

            order_result = send_order(
                symbol=symbol,
                action=action,
                quantity=abs(delta)
            )

            if not order_result["accepted"]:
                result = {
                    "status": "broker adjustment rejected",
                    "event_id": event_id,
                    "strategy": strategy,
                    "signal": signal,
                    "old_strategy_target": old_target,
                    "new_strategy_target": new_target,
                    "desired_net_target": desired_target,
                    "actual_net_target": actual_target,
                    "required_delta": delta,
                    "order": order_result,
                    "cancel_previous_stop": cancel_result,
                    "targets": target_snapshot(state, symbol)
                }

                state["last_results"][symbol] = result
                mark_event_processed(state, event_id, result)
                save_state(state)
                return result

        stop_result = place_net_protective_stop(
            state,
            symbol,
            desired_target
        )

        final_actual, _ = broker_signed_position(symbol)

        result = {
            "status": (
                "target synchronized"
                if final_actual == desired_target
                else "target pending or mismatched"
            ),
            "event_id": event_id,
            "strategy": strategy,
            "signal": signal,
            "old_strategy_target": old_target,
            "new_strategy_target": new_target,
            "desired_net_target": desired_target,
            "actual_net_before": actual_target,
            "required_delta": delta,
            "actual_net_after": final_actual,
            "order": order_result,
            "cancel_previous_stop": cancel_result,
            "protective_stop": stop_result,
            "targets": target_snapshot(state, symbol)
        }

        state["last_results"][symbol] = result
        mark_event_processed(state, event_id, result)
        save_state(state)

        return result


# =========================================================
# BACKGROUND WORKER
# =========================================================

def worker_loop() -> None:
    while True:
        event = event_queue.get()

        try:
            log(
                f"PROCESSING EVENT: {event['event_id']} "
                f"{event['strategy']} {event['signal']} "
                f"{event['symbol']}"
            )

            result = apply_target_event(event)
            log(
                f"EVENT RESULT: {event['event_id']} "
                f"{result.get('status')}"
            )

        except Exception as exc:
            log(
                f"EVENT ERROR: {event.get('event_id')} {exc}"
            )

            try:
                state = load_state()
                symbol = str(event.get("symbol", "UNKNOWN"))
                state["last_results"][symbol] = {
                    "status": "worker error",
                    "event_id": event.get("event_id"),
                    "message": str(exc),
                    "time": datetime.now(TZ).isoformat()
                }
                save_state(state)
            except Exception as state_exc:
                log(f"FAILED TO SAVE WORKER ERROR: {state_exc}")

        finally:
            with queued_event_ids_lock:
                queued_event_ids.discard(
                    str(event.get("event_id", ""))
                )

            event_queue.task_done()


worker_thread = threading.Thread(
    target=worker_loop,
    name="trade-worker",
    daemon=True
)
worker_thread.start()


# =========================================================
# EVENT VALIDATION
# =========================================================

def fallback_event_id(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":")
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]
    # Include the current second so separate legitimate signals with
    # identical payloads on different seconds are not collapsed.
    second = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
    return f"fallback:{second}:{digest}"


def parse_event(data: dict[str, Any]) -> dict[str, Any]:
    raw_symbol = data.get("symbol")
    raw_signal = data.get("signal")

    symbol = resolve_symbol(raw_symbol)
    signal = normalize_signal(raw_signal)

    strategy = str(
        data.get("strategy", "DEFAULT")
    ).upper().strip()

    if not symbol:
        raise ValueError("missing symbol")

    if not signal:
        raise ValueError("missing signal")

    if signal not in {"LONG", "SHORT", "EXIT"}:
        raise ValueError(f"unknown signal: {signal}")

    try:
        contracts = int(data.get("contracts", 1))
    except (TypeError, ValueError) as exc:
        raise ValueError("contracts must be an integer") from exc

    if contracts <= 0:
        raise ValueError("contracts must be positive")

    if contracts > MAX_CONTRACTS_PER_ORDER:
        raise ValueError(
            f"contracts exceeds MAX_CONTRACTS_PER_ORDER="
            f"{MAX_CONTRACTS_PER_ORDER}"
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

    event_id = str(
        data.get("event_id") or fallback_event_id(data)
    ).strip()

    return {
        "event_id": event_id,
        "strategy": strategy,
        "symbol": symbol,
        "signal": signal,
        "contracts": contracts,
        "broker_stop_dollars": max(1.0, stop_dollars),
        "received_at": datetime.now(TZ).isoformat()
    }


# =========================================================
# ROUTES
# =========================================================

@app.route("/", methods=["GET"])
def home():
    return (
        "TradeStation Futures Bot v5.0 "
        f"Target-Position Engine | Mode: {TRADING_MODE}",
        200
    )


@app.route("/health", methods=["GET"])
def health():
    try:
        token = get_access_token()

        return jsonify({
            "status": "ok",
            "version": "5.0-target-position",
            "mode": TRADING_MODE,
            "account_configured": bool(ACCOUNT),
            "token_ok": bool(token),
            "state_file": str(STATE_FILE),
            "queue_size": event_queue.qsize(),
            "broker_stop_enabled": BROKER_STOP_ENABLED,
            "session_filter": ENABLE_SESSION_FILTER or "default"
        })

    except Exception as exc:
        return jsonify({
            "status": "error",
            "message": str(exc)
        }), 500


@app.route("/state", methods=["GET"])
def state_view():
    state = load_state()

    symbols = sorted({
        item.get("symbol")
        for item in state["strategies"].values()
        if item.get("symbol")
    })

    broker_positions: dict[str, Any] = {}

    for symbol in symbols:
        try:
            actual, avg_price = broker_signed_position(symbol)
            broker_positions[symbol] = {
                "signed_position": actual,
                "average_price": avg_price,
                "desired_target": desired_net_target(
                    state,
                    symbol
                )
            }
        except Exception as exc:
            broker_positions[symbol] = {
                "error": str(exc)
            }

    return jsonify({
        "state": state,
        "broker_positions": broker_positions
    })


@app.route("/reconcile/<symbol>", methods=["POST"])
def manual_reconcile(symbol: str):
    resolved = resolve_symbol(symbol)

    if not resolved:
        return jsonify({"error": "missing symbol"}), 400

    event = {
        "event_id": (
            f"manual-reconcile:"
            f"{datetime.now(TZ).isoformat()}"
        ),
        "strategy": "MANUAL_RECONCILE",
        "symbol": resolved,
        "signal": "EXIT",
        "contracts": 1,
        "broker_stop_dollars": (
            DEFAULT_BROKER_STOP_DOLLARS
        ),
        "received_at": datetime.now(TZ).isoformat()
    }

    # Do not change any target; just synchronize actual broker
    # position to the current desired net target.
    with get_symbol_lock(resolved):
        state = load_state()
        desired = desired_net_target(state, resolved)
        actual, _ = broker_signed_position(resolved)
        delta = desired - actual

        cancel_result = cancel_protective_stop(
            state,
            resolved
        )

        order_result = None

        if delta != 0:
            order_result = send_order(
                resolved,
                "BUY" if delta > 0 else "SELL",
                abs(delta)
            )

        stop_result = place_net_protective_stop(
            state,
            resolved,
            desired
        )

        final_actual, _ = broker_signed_position(resolved)

    return jsonify({
        "status": "reconciled",
        "symbol": resolved,
        "desired": desired,
        "actual_before": actual,
        "delta": delta,
        "actual_after": final_actual,
        "order": order_result,
        "cancel_previous_stop": cancel_result,
        "protective_stop": stop_result
    })


@app.route("/flatten/<symbol>", methods=["POST"])
def manual_flatten(symbol: str):
    resolved = resolve_symbol(symbol)

    if not resolved:
        return jsonify({"error": "missing symbol"}), 400

    with get_symbol_lock(resolved):
        state = load_state()
        clear_symbol_targets(state, resolved)
        save_state(state)

        actual, _ = broker_signed_position(resolved)
        cancel_result = cancel_protective_stop(
            state,
            resolved
        )

        order_result = None

        if actual != 0:
            order_result = send_order(
                resolved,
                "SELL" if actual > 0 else "BUY",
                abs(actual)
            )

        final_actual, _ = wait_for_signed_position(
            resolved,
            0
        )

    return jsonify({
        "status": "flatten processed",
        "symbol": resolved,
        "actual_before": actual,
        "actual_after": final_actual,
        "order": order_result,
        "cancel_stop": cancel_result
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(
        force=True,
        silent=True
    )

    log(f"WEBHOOK RECEIVED: {data}")

    if not data:
        return jsonify({
            "error": "missing or invalid JSON"
        }), 400

    if WEBHOOK_SECRET:
        supplied_secret = str(
            data.get("secret", "")
        ).strip()

        if supplied_secret != WEBHOOK_SECRET:
            return jsonify({
                "error": "invalid webhook secret"
            }), 401

    try:
        event = parse_event(data)
    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400

    state = load_state()

    if event_already_processed(
        state,
        event["event_id"]
    ):
        return jsonify({
            "status": "duplicate already processed",
            "event_id": event["event_id"]
        }), 200

    with queued_event_ids_lock:
        if event["event_id"] in queued_event_ids:
            return jsonify({
                "status": "duplicate already queued",
                "event_id": event["event_id"]
            }), 200

        queued_event_ids.add(event["event_id"])

    event_queue.put(event)

    # Return immediately so TradingView does not time out while
    # TradeStation orders, fills, position polling, and stop
    # replacement continue in the background.
    return jsonify({
        "status": "accepted",
        "event_id": event["event_id"],
        "queue_size": event_queue.qsize()
    }), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
