import os
import time
import threading
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import databento as db
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
DATASET = os.getenv("DATABENTO_DATASET", "GLBX.MDP3")
SCHEMA = os.getenv("DATABENTO_SCHEMA", "ohlcv-1s")

# v = volume-ranked continuous front contract
# c = calendar front contract
ROLL_RULE = os.getenv("ROLL_RULE", "v")

SYMBOLS = {
    "NQ": f"NQ.{ROLL_RULE}.0",
    "ES": f"ES.{ROLL_RULE}.0",
    "MNQ": f"MNQ.{ROLL_RULE}.0",
    "MES": f"MES.{ROLL_RULE}.0",
}

app = FastAPI(title="EdgeOS Databento Live Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

state_lock = threading.Lock()

state: Dict[str, Any] = {
    "ok": False,
    "source": "Databento GLBX.MDP3",
    "dataset": DATASET,
    "schema": SCHEMA,
    "stype_in": "continuous",
    "roll_rule": ROLL_RULE,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "last_heartbeat": None,
    "last_error": None,
    "symbols": {},
}

for clean_symbol, requested_symbol in SYMBOLS.items():
    state["symbols"][clean_symbol] = {
        "ok": False,
        "symbol": clean_symbol,
        "requested_symbol": requested_symbol,
        "status": "waiting_for_live_trade",
        "price": None,
        "last": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "vwap": None,
        "volume": 0,
        "bar": None,
        "ts_event": None,
        "ts_recv": None,
        "age_seconds": None,
        "proxy_age_seconds": None,
        "updated_at": None,
        "ohlcv_source": "databento_ohlcv_1s_session_accumulated",
    }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ns_to_iso(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    try:
        return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def ns_age_seconds(ns: Optional[int]) -> Optional[float]:
    if ns is None:
        return None
    try:
        return max(0.0, time.time() - (ns / 1_000_000_000))
    except Exception:
        return None


def get_attr(record: Any, name: str, default=None):
    if hasattr(record, name):
        return getattr(record, name)
    if isinstance(record, dict):
        return record.get(name, default)
    return default


def convert_price(value):
    """
    Databento price fields are often integer nanos.
    Example: 30531750000000 means 30531.75
    """
    if value is None:
        return None

    try:
        v = float(value)

        if abs(v) > 1_000_000:
            return v / 1_000_000_000

        return v
    except Exception:
        return None


def identify_symbol(record: Any) -> Optional[str]:
    """
    Databento records may expose symbols differently depending on schema/client.
    This tries multiple ways to map Databento records back to NQ/ES/MNQ/MES.
    """
    raw_text = str(record)

    for clean_symbol, requested_symbol in SYMBOLS.items():
        if requested_symbol in raw_text:
            return clean_symbol

        if f"{clean_symbol}." in raw_text:
            return clean_symbol

    direct_symbol = get_attr(record, "symbol", None)

    if direct_symbol:
        direct_symbol = str(direct_symbol)

        for clean_symbol, requested_symbol in SYMBOLS.items():
            if direct_symbol == requested_symbol:
                return clean_symbol

            if direct_symbol.startswith(f"{clean_symbol}."):
                return clean_symbol

    return None


def update_from_ohlcv(record: Any):
    clean_symbol = identify_symbol(record)

    if clean_symbol is None:
        return

    ts_event = get_attr(record, "ts_event", None)
    ts_recv = get_attr(record, "ts_recv", None)

    bar_open = convert_price(get_attr(record, "open", None))
    bar_high = convert_price(get_attr(record, "high", None))
    bar_low = convert_price(get_attr(record, "low", None))
    bar_close = convert_price(get_attr(record, "close", None))

    bar_volume_raw = get_attr(record, "volume", 0)

    try:
        bar_volume = int(bar_volume_raw or 0)
    except Exception:
        bar_volume = 0

    if bar_close is None:
        return

    last_price = bar_close

    with state_lock:
        symbol_state = state["symbols"][clean_symbol]

        previous_open = symbol_state.get("open")
        previous_high = symbol_state.get("high")
        previous_low = symbol_state.get("low")
        previous_vwap = symbol_state.get("vwap")
        previous_volume = int(symbol_state.get("volume") or 0)

        session_open = previous_open if previous_open is not None else bar_open

        session_high_candidates = [
            x for x in [previous_high, bar_high, last_price]
            if x is not None
        ]

        session_low_candidates = [
            x for x in [previous_low, bar_low, last_price]
            if x is not None
        ]

        session_high = max(session_high_candidates) if session_high_candidates else last_price
        session_low = min(session_low_candidates) if session_low_candidates else last_price

        new_total_volume = previous_volume + bar_volume

        if bar_high is not None and bar_low is not None and bar_close is not None:
            typical_price = (bar_high + bar_low + bar_close) / 3
        else:
            typical_price = last_price

        if previous_vwap is not None and previous_volume > 0 and bar_volume > 0:
            session_vwap = (
                (previous_vwap * previous_volume) + (typical_price * bar_volume)
            ) / new_total_volume
        elif bar_volume > 0:
            session_vwap = typical_price
        else:
            session_vwap = previous_vwap or typical_price

        symbol_state.update(
            {
                "ok": True,
                "symbol": clean_symbol,
                "requested_symbol": SYMBOLS[clean_symbol],
                "status": "live",
                "price": last_price,
                "last": last_price,
                "open": session_open,
                "high": session_high,
                "low": session_low,
                "close": bar_close,
                "vwap": session_vwap,
                "volume": new_total_volume,
                "bar": {
                    "open": bar_open,
                    "high": bar_high,
                    "low": bar_low,
                    "close": bar_close,
                    "volume": bar_volume,
                },
                "ts_event": ns_to_iso(ts_event),
                "ts_recv": ns_to_iso(ts_recv),
                "age_seconds": ns_age_seconds(ts_event),
                "updated_at": now_iso(),
                "ohlcv_source": "databento_ohlcv_1s_session_accumulated",
            }
        )

        state["ok"] = True
        state["last_heartbeat"] = now_iso()
        state["last_error"] = None


def live_worker():
    if not DATABENTO_API_KEY:
        with state_lock:
            state["ok"] = False
            state["last_error"] = "Missing DATABENTO_API_KEY"
        return

    while True:
        try:
            with state_lock:
                state["last_error"] = None

            client = db.Live(key=DATABENTO_API_KEY)

            client.subscribe(
                dataset=DATASET,
                schema=SCHEMA,
                stype_in="continuous",
                symbols=list(SYMBOLS.values()),
            )

            client.add_callback(update_from_ohlcv)
            client.start()
            client.block_for_close()

        except Exception as error:
            with state_lock:
                state["ok"] = False
                state["last_error"] = {
                    "message": str(error),
                    "trace": traceback.format_exc()[-2000:],
                    "time": now_iso(),
                }

            time.sleep(5)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=live_worker, daemon=True)
    thread.start()


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "EdgeOS Databento Proxy",
        "endpoints": ["/health", "/snapshot", "/debug"],
    }


@app.get("/health")
def health():
    with state_lock:
        valid_symbols = [
            symbol for symbol, data in state["symbols"].items()
            if data.get("ok") is True
        ]

        waiting_symbols = [
            symbol for symbol, data in state["symbols"].items()
            if data.get("ok") is not True
        ]

        return {
            "ok": len(valid_symbols) > 0,
            "source": state["source"],
            "dataset": state["dataset"],
            "schema": state["schema"],
            "stype_in": state["stype_in"],
            "roll_rule": state["roll_rule"],
            "valid_symbols": valid_symbols,
            "waiting_symbols": waiting_symbols,
            "last_heartbeat": state["last_heartbeat"],
            "last_error": state["last_error"],
            "server_time": now_iso(),
        }


@app.get("/snapshot")
def snapshot():
    with state_lock:
        symbols = {}

        for symbol, data in state["symbols"].items():
            item = dict(data)

            updated_at = item.get("updated_at")

            if updated_at:
                try:
                    updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                    item["proxy_age_seconds"] = max(
                        0.0,
                        (datetime.now(timezone.utc) - updated_dt).total_seconds()
                    )
                except Exception:
                    item["proxy_age_seconds"] = None
            else:
                item["proxy_age_seconds"] = None

            symbols[symbol] = item

        valid_symbols = [
            symbol for symbol, data in symbols.items()
            if data.get("ok") is True
        ]

        waiting_symbols = [
            symbol for symbol, data in symbols.items()
            if data.get("ok") is not True
        ]

        return {
            "ok": len(valid_symbols) > 0,
            "source": "Databento GLBX.MDP3",
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": "continuous",
            "roll_rule": ROLL_RULE,
            "server_time": now_iso(),
            "valid_symbols": valid_symbols,
            "waiting_symbols": waiting_symbols,
            "symbols": symbols,
            "last_error": state["last_error"],
            "note": "Live last price comes from Databento OHLCV-1s close. Session OHLCV accumulates from received 1-second bars after proxy start.",
        }


@app.get("/debug")
def debug():
    with state_lock:
        return {
            "env": {
                "has_databento_key": bool(DATABENTO_API_KEY),
                "dataset": DATASET,
                "schema": SCHEMA,
                "roll_rule": ROLL_RULE,
                "symbols": SYMBOLS,
            },
            "state": state,
            "server_time": now_iso(),
        }
