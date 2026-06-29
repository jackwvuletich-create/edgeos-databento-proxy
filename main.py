main_py = r'''import os
import time
import threading
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

import databento as db
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
DATASET = os.getenv("DATABENTO_DATASET", "GLBX.MDP3")

# Use raw live trades because this is confirmed working on your Databento stream.
# The proxy builds rolling 1m and 5m OHLCV bars from these live trade ticks.
SCHEMA = "trades"

# Use calendar front contract because your old working proxy used ES.c.0 / NQ.c.0.
ROLL_RULE = "c"

SYMBOLS = {
    "NQ": f"NQ.{ROLL_RULE}.0",
    "ES": f"ES.{ROLL_RULE}.0",
    "MNQ": f"MNQ.{ROLL_RULE}.0",
    "MES": f"MES.{ROLL_RULE}.0",
}

MAX_BARS = int(os.getenv("MAX_BARS", "300"))
REALTIME_STALE_SECONDS = float(os.getenv("REALTIME_STALE_SECONDS", "90"))
MIN_LIVE_BARS_READY = int(os.getenv("MIN_LIVE_BARS_READY", "30"))

app = FastAPI(title="EdgeOS Databento Live Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

state_lock = threading.Lock()


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


def iso_age_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
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
    Databento trade prices are often integer nanos.
    Example: 29545500000000 means 29545.5
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


def floor_to_minute(ts_seconds: float) -> int:
    return int(ts_seconds // 60) * 60


def floor_to_5min(ts_seconds: float) -> int:
    return int(ts_seconds // 300) * 300


def bucket_iso(bucket_seconds: int) -> str:
    return datetime.fromtimestamp(bucket_seconds, tz=timezone.utc).isoformat()


def make_empty_symbol_state(clean_symbol: str, requested_symbol: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "symbol": clean_symbol,
        "requested_symbol": requested_symbol,
        "databento_symbol": requested_symbol,
        "status": "waiting_for_live_trade",

        # latest/current price data
        "price": None,
        "last": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "vwap": None,
        "volume": 0,

        # last trade represented as a tiny one-tick bar for backwards compatibility
        "bar": None,
        "raw": None,

        # timestamps
        "ts_event": None,
        "ts_recv": None,
        "age_seconds": None,
        "proxy_age_seconds": None,
        "received_at": None,
        "updated_at": None,

        # rolling OHLCV
        "current_1m_bar": None,
        "current_5m_bar": None,
        "bars_1m": [],
        "bars_5m": [],

        # transparency
        "ohlcv_source": "databento_live_trades",
        "bar_source": "waiting_for_live_data",
        "bars_are_realtime": False,
        "historical_lag_warning": False,
        "cold_start_partial": True,
        "last_tick_timestamp": None,
        "last_bar_timestamp": None,
    }


state: Dict[str, Any] = {
    "ok": False,
    "source": "Databento GLBX.MDP3",
    "dataset": DATASET,
    "schema": SCHEMA,
    "stype_in": "continuous",
    "roll_rule": ROLL_RULE,
    "started_at": now_iso(),
    "last_heartbeat": None,
    "last_error": None,
    "record_count": 0,
    "last_any_record_at": None,
    "last_record_at": None,
    "symbols": {
        clean_symbol: make_empty_symbol_state(clean_symbol, requested_symbol)
        for clean_symbol, requested_symbol in SYMBOLS.items()
    },
}

debug_records: List[str] = []
instrument_to_symbol: Dict[int, str] = {}


def save_debug(record_text: str):
    debug_records.append(record_text[:1500])
    if len(debug_records) > 20:
        debug_records.pop(0)


def get_instrument_id(record: Any) -> Optional[int]:
    try:
        if hasattr(record, "instrument_id"):
            return int(record.instrument_id)

        if hasattr(record, "hd") and hasattr(record.hd, "instrument_id"):
            return int(record.hd.instrument_id)

        return None
    except Exception:
        return None


def identify_symbol(record: Any) -> Optional[str]:
    """
    Prefer symbol text detection, then instrument-id memory.
    This preserves the behavior that made your old trades proxy work.
    """
    raw_text = str(record)
    instrument_id = get_instrument_id(record)

    for clean_symbol, requested_symbol in SYMBOLS.items():
        if requested_symbol in raw_text:
            if instrument_id is not None:
                instrument_to_symbol[instrument_id] = clean_symbol
            return clean_symbol

        if f"{clean_symbol}." in raw_text:
            if instrument_id is not None:
                instrument_to_symbol[instrument_id] = clean_symbol
            return clean_symbol

    direct_symbol = get_attr(record, "symbol", None)
    if direct_symbol:
        direct_symbol = str(direct_symbol)
        for clean_symbol, requested_symbol in SYMBOLS.items():
            if direct_symbol == requested_symbol or direct_symbol.startswith(f"{clean_symbol}."):
                if instrument_id is not None:
                    instrument_to_symbol[instrument_id] = clean_symbol
                return clean_symbol

    if instrument_id is not None:
        return instrument_to_symbol.get(instrument_id)

    return None


def append_limited(bar_list: List[Dict[str, Any]], bar: Dict[str, Any]):
    bar_list.append(bar)
    if len(bar_list) > MAX_BARS:
        del bar_list[0:len(bar_list) - MAX_BARS]


def update_bar(existing: Optional[Dict[str, Any]], bucket_seconds: int, price: float, size: int) -> Dict[str, Any]:
    if existing is None or existing.get("bucket") != bucket_seconds:
        return {
            "bucket": bucket_seconds,
            "timestamp": bucket_iso(bucket_seconds),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": int(size or 0),
            "source": "live_tick_built_from_databento_trades",
            "complete": False,
        }

    existing["high"] = max(x for x in [existing.get("high"), price] if x is not None)
    existing["low"] = min(x for x in [existing.get("low"), price] if x is not None)
    existing["close"] = price
    existing["volume"] = int(existing.get("volume") or 0) + int(size or 0)
    existing["complete"] = False
    return existing


def finalize_bar(bar: Dict[str, Any]) -> Dict[str, Any]:
    completed = dict(bar)
    completed["complete"] = True
    return completed


def bars_with_current(completed: List[Dict[str, Any]], current: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = list(completed)
    if current:
        output.append(dict(current))
    return output[-MAX_BARS:]


def update_realtime_flags(symbol_state: Dict[str, Any]):
    proxy_age = iso_age_seconds(symbol_state.get("updated_at"))
    count_1m = len(symbol_state.get("bars_1m") or [])
    has_current = symbol_state.get("current_1m_bar") is not None

    is_realtime = proxy_age is not None and proxy_age <= REALTIME_STALE_SECONDS and has_current
    cold_start_partial = count_1m < MIN_LIVE_BARS_READY

    if is_realtime and not cold_start_partial:
        bar_source = "live_tick_built"
    elif is_realtime and cold_start_partial:
        bar_source = "mixed"
    else:
        bar_source = "waiting_for_live_data"

    symbol_state["proxy_age_seconds"] = proxy_age
    symbol_state["bars_are_realtime"] = bool(is_realtime)
    symbol_state["cold_start_partial"] = bool(cold_start_partial)
    symbol_state["historical_lag_warning"] = False
    symbol_state["bar_source"] = bar_source


def update_from_trade(record: Any):
    try:
        record_text = str(record)

        with state_lock:
            state["record_count"] += 1
            state["last_any_record_at"] = now_iso()

        save_debug(record_text)

        clean_symbol = identify_symbol(record)
        if clean_symbol is None:
            return

        ts_event = get_attr(record, "ts_event", None)
        ts_recv = get_attr(record, "ts_recv", None)

        trade_price = convert_price(get_attr(record, "price", None))
        if trade_price is None:
            trade_price = convert_price(get_attr(record, "px", None))
        if trade_price is None:
            trade_price = convert_price(get_attr(record, "close", None))
        if trade_price is None:
            return

        try:
            trade_size = int(get_attr(record, "size", 0) or 0)
        except Exception:
            trade_size = 0

        event_seconds = ts_event / 1_000_000_000 if ts_event is not None else time.time()
        one_min_bucket = floor_to_minute(event_seconds)
        five_min_bucket = floor_to_5min(event_seconds)

        with state_lock:
            symbol_state = state["symbols"][clean_symbol]

            previous_open = symbol_state.get("open")
            previous_high = symbol_state.get("high")
            previous_low = symbol_state.get("low")
            previous_vwap = symbol_state.get("vwap")
            previous_volume = int(symbol_state.get("volume") or 0)

            session_open = previous_open if previous_open is not None else trade_price
            session_high = max(x for x in [previous_high, trade_price] if x is not None)
            session_low = min(x for x in [previous_low, trade_price] if x is not None)

            new_total_volume = previous_volume + trade_size

            if previous_vwap is not None and previous_volume > 0 and trade_size > 0:
                session_vwap = (
                    (previous_vwap * previous_volume) +
                    (trade_price * trade_size)
                ) / max(new_total_volume, 1)
            elif trade_size > 0:
                session_vwap = trade_price
            else:
                session_vwap = previous_vwap or trade_price

            current_1m = symbol_state.get("current_1m_bar")
            if current_1m is not None and current_1m.get("bucket") != one_min_bucket:
                append_limited(symbol_state["bars_1m"], finalize_bar(current_1m))
                current_1m = None

            current_1m = update_bar(current_1m, one_min_bucket, trade_price, trade_size)

            current_5m = symbol_state.get("current_5m_bar")
            if current_5m is not None and current_5m.get("bucket") != five_min_bucket:
                append_limited(symbol_state["bars_5m"], finalize_bar(current_5m))
                current_5m = None

            current_5m = update_bar(current_5m, five_min_bucket, trade_price, trade_size)

            last_tick_timestamp = ns_to_iso(ts_event) or now_iso()
            last_bar_timestamp = current_1m.get("timestamp") if current_1m else None
            instrument_id = get_instrument_id(record)

            symbol_state.update(
                {
                    "ok": True,
                    "symbol": clean_symbol,
                    "requested_symbol": SYMBOLS[clean_symbol],
                    "databento_symbol": SYMBOLS[clean_symbol],
                    "instrument_id": instrument_id,
                    "status": "live",
                    "price": trade_price,
                    "last": trade_price,
                    "open": session_open,
                    "high": session_high,
                    "low": session_low,
                    "close": trade_price,
                    "vwap": session_vwap,
                    "volume": new_total_volume,
                    "bar": {
                        "open": trade_price,
                        "high": trade_price,
                        "low": trade_price,
                        "close": trade_price,
                        "volume": trade_size,
                        "timestamp": last_tick_timestamp,
                        "source": "databento_live_trade",
                    },
                    "raw": record_text[:1000],
                    "current_1m_bar": current_1m,
                    "current_5m_bar": current_5m,
                    "ts_event": ns_to_iso(ts_event),
                    "ts_recv": ns_to_iso(ts_recv),
                    "age_seconds": ns_age_seconds(ts_event),
                    "received_at": now_iso(),
                    "updated_at": now_iso(),
                    "ohlcv_source": "databento_live_trades",
                    "last_tick_timestamp": last_tick_timestamp,
                    "last_bar_timestamp": last_bar_timestamp,
                }
            )

            update_realtime_flags(symbol_state)

            state["ok"] = True
            state["last_heartbeat"] = now_iso()
            state["last_record_at"] = state["last_heartbeat"]
            state["last_error"] = None

    except Exception as error:
        with state_lock:
            state["last_error"] = {
                "message": str(error),
                "trace": traceback.format_exc()[-2000:],
                "time": now_iso(),
            }


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

            client.add_callback(update_from_trade)

            state["live_client_started"] = True

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
        "active_file": "main.py",
        "endpoints": ["/health", "/snapshot", "/debug"],
    }


@app.get("/health")
def health():
    with state_lock:
        valid_symbols = [s for s, d in state["symbols"].items() if d.get("ok") is True]
        waiting_symbols = [s for s, d in state["symbols"].items() if d.get("ok") is not True]

        return {
            "ok": len(valid_symbols) > 0,
            "service": "edgeos-databento-live-proxy",
            "source": state["source"],
            "dataset": state["dataset"],
            "schema": state["schema"],
            "stype_in": state["stype_in"],
            "roll_rule": state["roll_rule"],
            "hasKey": bool(DATABENTO_API_KEY),
            "liveClientStarted": state.get("live_client_started", state["last_heartbeat"] is not None),
            "valid_symbols": valid_symbols,
            "waiting_symbols": waiting_symbols,
            "lastRecordAt": state["last_record_at"],
            "lastAnyRecordAt": state["last_any_record_at"],
            "recordCount": state["record_count"],
            "last_heartbeat": state["last_heartbeat"],
            "error": state["last_error"],
            "last_error": state["last_error"],
            "time": now_iso(),
            "server_time": now_iso(),
            "bar_builder": {
                "enabled": True,
                "input_schema": SCHEMA,
                "builds": ["1m", "5m"],
                "max_bars": MAX_BARS,
                "stale_after_seconds": REALTIME_STALE_SECONDS,
                "min_live_bars_ready": MIN_LIVE_BARS_READY,
            },
        }


@app.get("/snapshot")
def snapshot():
    with state_lock:
        symbols = {}

        for symbol, data in state["symbols"].items():
            item = dict(data)

            update_realtime_flags(item)

            item["bars_1m"] = bars_with_current(
                item.get("bars_1m") or [],
                item.get("current_1m_bar"),
            )

            item["bars_5m"] = bars_with_current(
                item.get("bars_5m") or [],
                item.get("current_5m_bar"),
            )

            item["bars_1m_count"] = len(item["bars_1m"])
            item["bars_5m_count"] = len(item["bars_5m"])

            symbols[symbol] = item

        valid_symbols = [s for s, d in symbols.items() if d.get("ok") is True]
        waiting_symbols = [s for s, d in symbols.items() if d.get("ok") is not True]

        return {
            "ok": len(valid_symbols) > 0,
            "mode": "live-stream",
            "source": "Databento Live GLBX.MDP3",
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": "continuous",
            "roll_rule": ROLL_RULE,
            "generatedAt": now_iso(),
            "server_time": now_iso(),
            "valid_symbols": valid_symbols,
            "waiting_symbols": waiting_symbols,

            # New shape
            "symbols": symbols,

            # Backward-compatible shape for existing EdgeOS code
            "data": symbols,

            "status": {
                "live_client_started": state.get("live_client_started", state["last_heartbeat"] is not None),
                "last_record_at": state["last_record_at"],
                "last_any_record_at": state["last_any_record_at"],
                "record_count": state["record_count"],
                "error": state["last_error"],
            },
            "last_error": state["last_error"],
            "note": (
                "Live price comes from Databento live trades. "
                "Rolling 1m and 5m bars are built inside the Railway proxy from actual live TradeMsg records. "
                "Historical REST bars are not labeled as real-time."
            ),
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
            "instrument_to_symbol": instrument_to_symbol,
            "latest_prices": {
                symbol: data for symbol, data in state["symbols"].items() if data.get("ok") is True
            },
            "last_records": debug_records,
            "server_time": now_iso(),
        }
'''

path = Path("/mnt/data/main.py")
path.write_text(main_py)
print(f"Created {path} ({path.stat().st_size} bytes)")
