import os
import time
import threading
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

import databento as db
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
DATASET = os.getenv("DATABENTO_DATASET", "GLBX.MDP3")
SCHEMA = "trades"
ROLL_RULE = "c"

API_CONTRACT_VERSION = "candle_api_v1"
BUILDER_VERSION = "candle_builder_v1.0.0"
SUPPORTED_TIMEFRAMES = ("1m", "5m", "15m")

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
        return datetime.fromtimestamp(
            ns / 1_000_000_000,
            tz=timezone.utc,
        ).isoformat()
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
        return max(
            0.0,
            (datetime.now(timezone.utc) - dt).total_seconds(),
        )
    except Exception:
        return None


def get_attr(record: Any, name: str, default=None):
    if hasattr(record, name):
        return getattr(record, name)

    if isinstance(record, dict):
        return record.get(name, default)

    return default


def convert_price(value):
    if value is None:
        return None

    try:
        converted = float(value)

        if abs(converted) > 1_000_000:
            return converted / 1_000_000_000

        return converted
    except Exception:
        return None


def floor_to_bucket(
    ts_seconds: float,
    bucket_size_seconds: int,
) -> int:
    return int(ts_seconds // bucket_size_seconds) * bucket_size_seconds


def floor_to_minute(ts_seconds: float) -> int:
    return floor_to_bucket(ts_seconds, 60)


def floor_to_5min(ts_seconds: float) -> int:
    return floor_to_bucket(ts_seconds, 300)


def floor_to_15min(ts_seconds: float) -> int:
    return floor_to_bucket(ts_seconds, 900)


def bucket_iso(bucket_seconds: int) -> str:
    return datetime.fromtimestamp(
        bucket_seconds,
        tz=timezone.utc,
    ).isoformat()


def make_empty_symbol_state(
    clean_symbol: str,
    requested_symbol: str,
) -> Dict[str, Any]:
    return {
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
        "current_1m_bar": None,
        "current_5m_bar": None,
        "current_15m_bar": None,
        "bars_1m": [],
        "bars_5m": [],
        "bars_15m": [],
        "ohlcv_source": "databento_live_trades",
        "bar_source": "waiting_for_live_data",
        "bars_are_realtime": False,
        "historical_lag_warning": False,
        "cold_start_partial": True,
        "first_trade_at": None,
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
    "worker_running": False,
    "reconnect_count": 0,
    "symbols": {
        clean_symbol: make_empty_symbol_state(
            clean_symbol,
            requested_symbol,
        )
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

        if hasattr(record, "hd") and hasattr(
            record.hd,
            "instrument_id",
        ):
            return int(record.hd.instrument_id)

        return None
    except Exception:
        return None


def identify_symbol(record: Any) -> Optional[str]:
    raw_text = str(record)
    instrument_id = get_instrument_id(record)

    for clean_symbol, requested_symbol in SYMBOLS.items():
        if requested_symbol in raw_text:
            if instrument_id is not None:
                instrument_to_symbol[instrument_id] = clean_symbol

            return clean_symbol

    for clean_symbol, requested_symbol in SYMBOLS.items():
        if f"stype_in_symbol='{requested_symbol}'" in raw_text:
            if instrument_id is not None:
                instrument_to_symbol[instrument_id] = clean_symbol

            return clean_symbol

    direct_symbol = get_attr(record, "symbol", None)

    if direct_symbol:
        direct_symbol = str(direct_symbol)

        for clean_symbol, requested_symbol in SYMBOLS.items():
            if direct_symbol == requested_symbol:
                if instrument_id is not None:
                    instrument_to_symbol[instrument_id] = clean_symbol

                return clean_symbol

    if instrument_id is not None:
        return instrument_to_symbol.get(instrument_id)

    return None


def append_limited(
    bar_list: List[Dict[str, Any]],
    bar: Dict[str, Any],
):
    bar_list.append(bar)

    if len(bar_list) > MAX_BARS:
        del bar_list[0 : len(bar_list) - MAX_BARS]


def update_bar(
    existing: Optional[Dict[str, Any]],
    bucket_seconds: int,
    timeframe: str,
    symbol: str,
    price: float,
    size: int,
    event_iso: str,
) -> Dict[str, Any]:
    bucket_sizes = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
    }

    bucket_size = bucket_sizes[timeframe]

    if (
        existing is None
        or existing.get("bucket") != bucket_seconds
    ):
        return {
            "bucket": bucket_seconds,
            "symbol": symbol,
            "timeframe": timeframe,
            "start_at": bucket_iso(bucket_seconds),
            "end_at": bucket_iso(
                bucket_seconds + bucket_size
            ),
            "timestamp": bucket_iso(bucket_seconds),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": max(0, int(size or 0)),
            "trade_count": 1,
            "source": "proxy_live",
            "is_complete": False,
            "complete": False,
            "last_update_at": event_iso,
        }

    existing["high"] = max(
        existing["high"],
        price,
    )
    existing["low"] = min(
        existing["low"],
        price,
    )
    existing["close"] = price
    existing["volume"] = (
        int(existing.get("volume") or 0)
        + max(0, int(size or 0))
    )
    existing["trade_count"] = (
        int(existing.get("trade_count") or 0) + 1
    )
    existing["is_complete"] = False
    existing["complete"] = False
    existing["last_update_at"] = event_iso

    return existing


def finalize_bar(
    bar: Dict[str, Any],
) -> Dict[str, Any]:
    completed = dict(bar)
    completed["is_complete"] = True
    completed["complete"] = True

    return completed


def bars_with_current(
    completed: List[Dict[str, Any]],
    current: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    output = [dict(bar) for bar in completed]

    if current:
        output.append(dict(current))

    return output[-MAX_BARS:]


def data_quality_for_symbol(
    symbol_state: Dict[str, Any],
) -> str:
    last_tick_age = iso_age_seconds(
        symbol_state.get("last_tick_timestamp")
    )

    if (
        not state.get("worker_running")
        or state.get("last_error")
    ):
        return "DISCONNECTED"

    if symbol_state.get("last_tick_timestamp") is None:
        return "WARMING_UP"

    if last_tick_age is None:
        return "DEGRADED"

    if last_tick_age > REALTIME_STALE_SECONDS:
        return "STALE"

    if symbol_state.get("current_1m_bar") is None:
        return "WARMING_UP"

    return "HEALTHY"


def update_realtime_flags(
    symbol_state: Dict[str, Any],
):
    proxy_age = iso_age_seconds(
        symbol_state.get("updated_at")
    )
    count_1m = len(
        symbol_state.get("bars_1m") or []
    )
    has_current = (
        symbol_state.get("current_1m_bar") is not None
    )

    is_realtime = (
        proxy_age is not None
        and proxy_age <= REALTIME_STALE_SECONDS
        and has_current
    )
    cold_start_partial = (
        count_1m < MIN_LIVE_BARS_READY
    )

    if is_realtime and not cold_start_partial:
        bar_source = "live_tick_built"
    elif is_realtime and cold_start_partial:
        bar_source = "mixed"
    else:
        bar_source = "waiting_for_live_data"

    symbol_state["proxy_age_seconds"] = proxy_age
    symbol_state["bars_are_realtime"] = bool(
        is_realtime
    )
    symbol_state["cold_start_partial"] = bool(
        cold_start_partial
    )
    symbol_state["historical_lag_warning"] = False
    symbol_state["bar_source"] = bar_source
    symbol_state["data_quality"] = (
        data_quality_for_symbol(symbol_state)
    )


def update_from_trade(record: Any):
    record_text = str(record)
    save_debug(record_text)

    with state_lock:
        state["record_count"] = (
            int(state.get("record_count") or 0) + 1
        )
        state["last_any_record_at"] = now_iso()

    clean_symbol = identify_symbol(record)

    if clean_symbol is None:
        return

    ts_event = get_attr(
        record,
        "ts_event",
        None,
    )
    ts_recv = get_attr(
        record,
        "ts_recv",
        None,
    )

    trade_price = convert_price(
        get_attr(record, "price", None)
    )

    if trade_price is None:
        trade_price = convert_price(
            get_attr(record, "px", None)
        )

    if trade_price is None:
        trade_price = convert_price(
            get_attr(record, "close", None)
        )

    if trade_price is None or trade_price <= 0:
        return

    try:
        trade_size = int(
            get_attr(record, "size", 0) or 0
        )
    except Exception:
        trade_size = 0

    trade_size = max(0, trade_size)

    event_seconds = (
        ts_event / 1_000_000_000
        if ts_event is not None
        else time.time()
    )
    event_iso = ns_to_iso(ts_event) or now_iso()

    buckets = {
        "1m": floor_to_minute(event_seconds),
        "5m": floor_to_5min(event_seconds),
        "15m": floor_to_15min(event_seconds),
    }

    with state_lock:
        symbol_state = state["symbols"][clean_symbol]

        previous_open = symbol_state.get("open")
        previous_high = symbol_state.get("high")
        previous_low = symbol_state.get("low")
        previous_vwap = symbol_state.get("vwap")
        previous_volume = int(
            symbol_state.get("volume") or 0
        )

        session_open = (
            previous_open
            if previous_open is not None
            else trade_price
        )
        session_high = max(
            value
            for value in [
                previous_high,
                trade_price,
            ]
            if value is not None
        )
        session_low = min(
            value
            for value in [
                previous_low,
                trade_price,
            ]
            if value is not None
        )

        new_total_volume = (
            previous_volume + trade_size
        )

        if (
            previous_vwap is not None
            and previous_volume > 0
            and trade_size > 0
        ):
            session_vwap = (
                (
                    previous_vwap * previous_volume
                )
                + (
                    trade_price * trade_size
                )
            ) / max(new_total_volume, 1)
        elif trade_size > 0:
            session_vwap = trade_price
        else:
            session_vwap = (
                previous_vwap or trade_price
            )

        for timeframe in SUPPORTED_TIMEFRAMES:
            current_key = (
                f"current_{timeframe}_bar"
            )
            completed_key = (
                f"bars_{timeframe}"
            )
            current_bar = symbol_state.get(
                current_key
            )
            bucket = buckets[timeframe]

            if (
                current_bar is not None
                and current_bar.get("bucket")
                != bucket
            ):
                append_limited(
                    symbol_state[completed_key],
                    finalize_bar(current_bar),
                )
                current_bar = None

            symbol_state[current_key] = update_bar(
                current_bar,
                bucket,
                timeframe,
                clean_symbol,
                trade_price,
                trade_size,
                event_iso,
            )

        last_tick_timestamp = event_iso

        last_completed_1m = (
            symbol_state["bars_1m"][-1]
            if symbol_state.get("bars_1m")
            else None
        )

        last_bar_timestamp = (
            last_completed_1m.get("start_at")
            if last_completed_1m
            else symbol_state[
                "current_1m_bar"
            ].get("start_at")
        )

        symbol_state.update(
            {
                "ok": True,
                "symbol": clean_symbol,
                "requested_symbol": SYMBOLS[
                    clean_symbol
                ],
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
                "ts_event": ns_to_iso(ts_event),
                "ts_recv": ns_to_iso(ts_recv),
                "age_seconds": ns_age_seconds(
                    ts_event
                ),
                "updated_at": now_iso(),
                "ohlcv_source": (
                    "databento_live_trades"
                ),
                "first_trade_at": (
                    symbol_state.get(
                        "first_trade_at"
                    )
                    or last_tick_timestamp
                ),
                "last_tick_timestamp": (
                    last_tick_timestamp
                ),
                "last_bar_timestamp": (
                    last_bar_timestamp
                ),
            }
        )

        update_realtime_flags(symbol_state)

        state["ok"] = True
        state["last_heartbeat"] = now_iso()
        state["last_record_at"] = (
            state["last_heartbeat"]
        )
        state["last_error"] = None


def live_worker():
    if not DATABENTO_API_KEY:
        with state_lock:
            state["ok"] = False
            state["worker_running"] = False
            state["last_error"] = (
                "Missing DATABENTO_API_KEY"
            )

        return

    while True:
        try:
            with state_lock:
                state["last_error"] = None
                state["worker_running"] = True

            client = db.Live(
                key=DATABENTO_API_KEY
            )

            client.subscribe(
                dataset=DATASET,
                schema=SCHEMA,
                stype_in="continuous",
                symbols=list(SYMBOLS.values()),
            )

            client.add_callback(
                update_from_trade
            )
            client.start()
            client.block_for_close()

            with state_lock:
                state["worker_running"] = False
                state["last_error"] = {
                    "message": (
                        "Databento live client closed"
                    ),
                    "time": now_iso(),
                }

        except Exception as error:
            with state_lock:
                state["ok"] = False
                state["worker_running"] = False
                state["reconnect_count"] = (
                    int(
                        state.get(
                            "reconnect_count"
                        )
                        or 0
                    )
                    + 1
                )
                state["last_error"] = {
                    "message": str(error),
                    "trace": traceback.format_exc()[
                        -2000:
                    ],
                    "time": now_iso(),
                }

            time.sleep(5)


@app.on_event("startup")
def startup_event():
    print(
        {
            "active_file": "main.py",
            "builder_version": BUILDER_VERSION,
            "api_contract_version": (
                API_CONTRACT_VERSION
            ),
            "symbols": SYMBOLS,
            "supported_timeframes": (
                SUPPORTED_TIMEFRAMES
            ),
            "has_databento_key": bool(
                DATABENTO_API_KEY
            ),
            "dataset": DATASET,
            "schema": SCHEMA,
        }
    )

    thread = threading.Thread(
        target=live_worker,
        daemon=True,
    )
    thread.start()


def serialize_bar(
    bar: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not bar:
        return None

    return {
        "symbol": bar.get("symbol"),
        "timeframe": bar.get("timeframe"),
        "start_at": (
            bar.get("start_at")
            or bar.get("timestamp")
        ),
        "end_at": bar.get("end_at"),
        "open": bar.get("open"),
        "high": bar.get("high"),
        "low": bar.get("low"),
        "close": bar.get("close"),
        "volume": int(
            bar.get("volume") or 0
        ),
        "trade_count": int(
            bar.get("trade_count") or 0
        ),
        "is_complete": bool(
            bar.get(
                "is_complete",
                bar.get("complete", False),
            )
        ),
        "last_update_at": bar.get(
            "last_update_at"
        ),
    }


@app.get("/")
def root():
    return {
        "ok": True,
        "service": (
            "EdgeOS Databento Proxy"
        ),
        "active_file": "main.py",
        "api_contract_version": (
            API_CONTRACT_VERSION
        ),
        "builder_version": BUILDER_VERSION,
        "endpoints": [
            "/health",
            "/snapshot",
            "/debug",
            "/bars/status",
            (
                "/bars/1m?"
                "symbol=NQ&limit=300"
            ),
            (
                "/bars/5m?"
                "symbol=NQ&limit=300"
            ),
            (
                "/bars/15m?"
                "symbol=NQ&limit=300"
            ),
        ],
    }


@app.get("/health")
def health():
    with state_lock:
        symbols_copy = {
            symbol: dict(symbol_state)
            for symbol, symbol_state
            in state["symbols"].items()
        }

        state_copy = {
            "source": state["source"],
            "dataset": state["dataset"],
            "schema": state["schema"],
            "stype_in": state["stype_in"],
            "roll_rule": state["roll_rule"],
            "last_heartbeat": (
                state["last_heartbeat"]
            ),
            "last_error": state["last_error"],
            "worker_running": (
                state["worker_running"]
            ),
            "reconnect_count": (
                state["reconnect_count"]
            ),
        }

    per_symbol = {}
    valid_symbols = []
    waiting_symbols = []

    for symbol, symbol_state in (
        symbols_copy.items()
    ):
        update_realtime_flags(symbol_state)

        if symbol_state.get("ok") is True:
            valid_symbols.append(symbol)
        else:
            waiting_symbols.append(symbol)

        per_symbol[symbol] = {
            "data_quality": symbol_state.get(
                "data_quality"
            ),
            "last_tick_ts": symbol_state.get(
                "last_tick_timestamp"
            ),
            "last_tick_age_s": iso_age_seconds(
                symbol_state.get(
                    "last_tick_timestamp"
                )
            ),
            "completed_1m": len(
                symbol_state.get(
                    "bars_1m"
                )
                or []
            ),
            "completed_5m": len(
                symbol_state.get(
                    "bars_5m"
                )
                or []
            ),
            "completed_15m": len(
                symbol_state.get(
                    "bars_15m"
                )
                or []
            ),
            "forming_1m": (
                symbol_state.get(
                    "current_1m_bar"
                )
                is not None
            ),
            "forming_5m": (
                symbol_state.get(
                    "current_5m_bar"
                )
                is not None
            ),
            "forming_15m": (
                symbol_state.get(
                    "current_15m_bar"
                )
                is not None
            ),
        }

    freshest_tick_ages = [
        item["last_tick_age_s"]
        for item in per_symbol.values()
        if item["last_tick_age_s"]
        is not None
    ]

    receiving_trades = bool(
        freshest_tick_ages
        and min(freshest_tick_ages)
        <= REALTIME_STALE_SECONDS
    )

    websocket_connected = bool(
        state_copy["worker_running"]
        and not state_copy["last_error"]
    )

    return {
        "ok": len(valid_symbols) > 0,
        "source": state_copy["source"],
        "dataset": state_copy["dataset"],
        "schema": state_copy["schema"],
        "stype_in": state_copy["stype_in"],
        "roll_rule": state_copy["roll_rule"],
        "api_contract_version": (
            API_CONTRACT_VERSION
        ),
        "builder_version": BUILDER_VERSION,
        "supported_timeframes": list(
            SUPPORTED_TIMEFRAMES
        ),
        "websocket_connected": (
            websocket_connected
        ),
        "receiving_trades": receiving_trades,
        "valid_symbols": valid_symbols,
        "waiting_symbols": waiting_symbols,
        "last_heartbeat": (
            state_copy["last_heartbeat"]
        ),
        "last_error": state_copy["last_error"],
        "reconnect_count": (
            state_copy["reconnect_count"]
        ),
        "server_time": now_iso(),
        "per_symbol": per_symbol,
        "bar_builder": {
            "enabled": True,
            "input_schema": SCHEMA,
            "builds": list(
                SUPPORTED_TIMEFRAMES
            ),
            "max_bars": MAX_BARS,
            "stale_after_seconds": (
                REALTIME_STALE_SECONDS
            ),
            "min_live_bars_ready": (
                MIN_LIVE_BARS_READY
            ),
        },
    }


@app.get("/bars/status")
def bars_status():
    with state_lock:
        symbols_copy = {
            symbol: dict(symbol_state)
            for symbol, symbol_state
            in state["symbols"].items()
        }

        worker_running = bool(
            state.get("worker_running")
        )
        last_error = state.get("last_error")
        reconnect_count = int(
            state.get("reconnect_count") or 0
        )

    per_symbol = {}
    last_tick_ages = []

    for symbol, symbol_state in (
        symbols_copy.items()
    ):
        update_realtime_flags(symbol_state)

        last_tick_age = iso_age_seconds(
            symbol_state.get(
                "last_tick_timestamp"
            )
        )

        if last_tick_age is not None:
            last_tick_ages.append(
                last_tick_age
            )

        per_symbol[symbol] = {
            "completed_1m": len(
                symbol_state.get(
                    "bars_1m"
                )
                or []
            ),
            "completed_5m": len(
                symbol_state.get(
                    "bars_5m"
                )
                or []
            ),
            "completed_15m": len(
                symbol_state.get(
                    "bars_15m"
                )
                or []
            ),
            "forming_1m": (
                symbol_state.get(
                    "current_1m_bar"
                )
                is not None
            ),
            "forming_5m": (
                symbol_state.get(
                    "current_5m_bar"
                )
                is not None
            ),
            "forming_15m": (
                symbol_state.get(
                    "current_15m_bar"
                )
                is not None
            ),
            "last_tick_ts": (
                symbol_state.get(
                    "last_tick_timestamp"
                )
            ),
            "last_tick_age_s": (
                last_tick_age
            ),
            "data_quality": (
                symbol_state.get(
                    "data_quality"
                )
            ),
        }

    receiving_trades = bool(
        last_tick_ages
        and min(last_tick_ages)
        <= REALTIME_STALE_SECONDS
    )

    websocket_connected = bool(
        worker_running and not last_error
    )

    return {
        "ok": True,
        "api_contract_version": (
            API_CONTRACT_VERSION
        ),
        "builder_version": BUILDER_VERSION,
        "service": (
            "EdgeOS Databento Proxy"
        ),
        "supported_symbols": list(
            SYMBOLS.keys()
        ),
        "supported_timeframes": list(
            SUPPORTED_TIMEFRAMES
        ),
        "websocket_connected": (
            websocket_connected
        ),
        "receiving_trades": receiving_trades,
        "last_tick_ts": state.get(
            "last_record_at"
        ),
        "last_tick_age_s": iso_age_seconds(
            state.get("last_record_at")
        ),
        "server_time": now_iso(),
        "reconnect_count": reconnect_count,
        "last_error": last_error,
        "per_symbol": per_symbol,
    }


@app.get("/bars/{interval}")
def bars(
    interval: str,
    symbol: str = Query("NQ"),
    limit: int = Query(
        300,
        ge=1,
        le=2000,
    ),
):
    interval = interval.lower().strip()
    symbol = symbol.upper().strip()

    if interval not in SUPPORTED_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported interval "
                f"'{interval}'. Supported: "
                f"{list(SUPPORTED_TIMEFRAMES)}"
            ),
        )

    if symbol not in SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported symbol "
                f"'{symbol}'. Supported: "
                f"{list(SYMBOLS.keys())}"
            ),
        )

    with state_lock:
        symbol_state = dict(
            state["symbols"][symbol]
        )

        completed = [
            dict(bar)
            for bar in state[
                "symbols"
            ][symbol].get(
                f"bars_{interval}",
                [],
            )
        ]

        forming = state[
            "symbols"
        ][symbol].get(
            f"current_{interval}_bar"
        )

        forming = (
            dict(forming)
            if forming
            else None
        )

        worker_running = bool(
            state.get("worker_running")
        )
        last_error = state.get("last_error")

    update_realtime_flags(symbol_state)
    completed = completed[-limit:]

    serialized_bars = [
        serialize_bar(bar)
        for bar in completed
    ]
    serialized_forming = serialize_bar(
        forming
    )

    last_bar = (
        serialized_bars[-1]
        if serialized_bars
        else None
    )

    last_bar_ts = (
        last_bar.get("end_at")
        if last_bar
        else None
    )
    last_bar_age_s = iso_age_seconds(
        last_bar_ts
    )

    last_tick_ts = symbol_state.get(
        "last_tick_timestamp"
    )
    last_tick_age_s = iso_age_seconds(
        last_tick_ts
    )

    websocket_connected = bool(
        worker_running and not last_error
    )
    receiving_trades = bool(
        last_tick_age_s is not None
        and last_tick_age_s
        <= REALTIME_STALE_SECONDS
    )

    data_quality = (
        symbol_state.get("data_quality")
        or "DISCONNECTED"
    )
    healthy = bool(
        data_quality == "HEALTHY"
        and receiving_trades
    )

    return {
        "ok": True,
        "api_contract_version": (
            API_CONTRACT_VERSION
        ),
        "builder_version": BUILDER_VERSION,
        "symbol": symbol,
        "provider_symbol": SYMBOLS[symbol],
        "timeframe": interval,
        "bars": serialized_bars,
        "forming_bar": serialized_forming,
        "total_cached": len(
            symbol_state.get(
                f"bars_{interval}"
            )
            or []
        ),
        "returned_count": len(
            serialized_bars
        ),
        "last_bar_ts": last_bar_ts,
        "last_bar_age_s": last_bar_age_s,
        "last_tick_ts": last_tick_ts,
        "last_tick_age_s": last_tick_age_s,
        "bar_source_primary": "proxy_live",
        "websocket_connected": (
            websocket_connected
        ),
        "receiving_trades": receiving_trades,
        "healthy": healthy,
        "data_quality": data_quality,
        "session_id": None,
        "server_time": now_iso(),
        "last_error": last_error,
    }


@app.get("/snapshot")
def snapshot():
    with state_lock:
        symbols = {}

        for symbol, data in (
            state["symbols"].items()
        ):
            item = dict(data)
            update_realtime_flags(item)

            item["bars_1m"] = (
                bars_with_current(
                    item.get("bars_1m") or [],
                    item.get(
                        "current_1m_bar"
                    ),
                )
            )

            item["bars_5m"] = (
                bars_with_current(
                    item.get("bars_5m") or [],
                    item.get(
                        "current_5m_bar"
                    ),
                )
            )

            item["bars_15m"] = (
                bars_with_current(
                    item.get("bars_15m") or [],
                    item.get(
                        "current_15m_bar"
                    ),
                )
            )

            item["bars_1m_count"] = len(
                item["bars_1m"]
            )
            item["bars_5m_count"] = len(
                item["bars_5m"]
            )
            item["bars_15m_count"] = len(
                item["bars_15m"]
            )

            symbols[symbol] = item

        valid_symbols = [
            symbol
            for symbol, data in symbols.items()
            if data.get("ok") is True
        ]

        waiting_symbols = [
            symbol
            for symbol, data in symbols.items()
            if data.get("ok") is not True
        ]

        return {
            "ok": len(valid_symbols) > 0,
            "mode": "live-stream",
            "source": (
                "Databento Live GLBX.MDP3"
            ),
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": "continuous",
            "roll_rule": ROLL_RULE,
            "generatedAt": now_iso(),
            "server_time": now_iso(),
            "valid_symbols": valid_symbols,
            "waiting_symbols": (
                waiting_symbols
            ),
            "symbols": symbols,
            "data": symbols,
            "status": {
                "live_client_started": (
                    state["worker_running"]
                ),
                "last_record_at": (
                    state["last_record_at"]
                ),
                "last_any_record_at": (
                    state[
                        "last_any_record_at"
                    ]
                ),
                "error": state["last_error"],
            },
            "last_error": state["last_error"],
            "note": (
                "Live price and rolling 1m, "
                "5m, and 15m candles are "
                "built inside the Railway "
                "proxy from Databento trade "
                "records. The canonical "
                "/bars API separates "
                "completed bars from "
                "forming_bar."
            ),
        }


@app.get("/debug")
def debug():
    with state_lock:
        return {
            "env": {
                "has_databento_key": bool(
                    DATABENTO_API_KEY
                ),
                "dataset": DATASET,
                "schema": SCHEMA,
                "roll_rule": ROLL_RULE,
                "symbols": SYMBOLS,
                "api_contract_version": (
                    API_CONTRACT_VERSION
                ),
                "builder_version": (
                    BUILDER_VERSION
                ),
                "supported_timeframes": (
                    SUPPORTED_TIMEFRAMES
                ),
            },
            "state": state,
            "record_count": state.get(
                "record_count"
            ),
            "last_any_record_at": state.get(
                "last_any_record_at"
            ),
            "last_record_at": state.get(
                "last_record_at"
            ),
            "instrument_to_symbol": (
                instrument_to_symbol
            ),
            "last_records": debug_records,
            "server_time": now_iso(),
        }
