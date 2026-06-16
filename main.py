import os
import time
import threading
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import databento as db
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="EdgeOS Databento Live Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")

SYMBOL_MAP = {
    "ES": "ES.c.0",
    "NQ": "NQ.c.0",
    "MES": "MES.c.0",
    "MNQ": "MNQ.c.0",
}

latest_prices: Dict[str, Dict[str, Any]] = {}
instrument_to_symbol: Dict[int, str] = {}

debug_records: List[str] = []

status: Dict[str, Any] = {
    "live_client_started": False,
    "last_record_at": None,
    "last_any_record_at": None,
    "record_count": 0,
    "error": None,
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_price(value):
    if value is None:
        return None

    try:
        number = float(value)
    except Exception:
        return None

    if number > 1_000_000:
        return number / 1_000_000_000

    return number


def save_debug(record_text: str):
    debug_records.append(record_text[:1500])

    if len(debug_records) > 20:
        debug_records.pop(0)


def get_instrument_id(record):
    try:
        if hasattr(record, "instrument_id"):
            return int(record.instrument_id)

        if hasattr(record, "hd") and hasattr(record.hd, "instrument_id"):
            return int(record.hd.instrument_id)

        text = str(record)
        match = re.search(r"instrument_id[=:]\s*(\d+)", text)
        if match:
            return int(match.group(1))

    except Exception:
        return None

    return None


def detect_symbol_from_text(record_text: str):
    for edge_symbol, db_symbol in SYMBOL_MAP.items():
        if db_symbol in record_text:
            return edge_symbol

    for edge_symbol in SYMBOL_MAP.keys():
        if f"symbol='{edge_symbol}" in record_text or f"symbol={edge_symbol}" in record_text:
            return edge_symbol

    return None


def detect_price(record):
    for attr in ["price", "px", "close", "last"]:
        if hasattr(record, attr):
            price = normalize_price(getattr(record, attr))
            if price:
                return price

    text = str(record)

    for pattern in [
        r"price[=:]\s*([0-9]+)",
        r"px[=:]\s*([0-9]+)",
        r"close[=:]\s*([0-9]+)",
    ]:
        match = re.search(pattern, text)
        if match:
            return normalize_price(match.group(1))

    return None


def handle_record(record):
    try:
        record_text = str(record)

        status["record_count"] += 1
        status["last_any_record_at"] = now_iso()
        save_debug(record_text)

        instrument_id = get_instrument_id(record)
        detected_symbol = detect_symbol_from_text(record_text)

        if instrument_id and detected_symbol:
            instrument_to_symbol[instrument_id] = detected_symbol

        if not detected_symbol and instrument_id:
            detected_symbol = instrument_to_symbol.get(instrument_id)

        price = detect_price(record)

        if detected_symbol and price:
            latest_prices[detected_symbol] = {
                "ok": True,
                "symbol": detected_symbol,
                "databento_symbol": SYMBOL_MAP[detected_symbol],
                "instrument_id": instrument_id,
                "price": price,
                "received_at": now_iso(),
                "raw": record_text[:1000],
            }

            status["last_record_at"] = now_iso()

    except Exception as error:
        status["error"] = str(error)


def start_live_client():
    if not DATABENTO_API_KEY:
        status["error"] = "Missing DATABENTO_API_KEY"
        return

    try:
        client = db.Live(key=DATABENTO_API_KEY)

        client.subscribe(
            dataset="GLBX.MDP3",
            schema="trades",
            stype_in="continuous",
            symbols=list(SYMBOL_MAP.values()),
        )

        client.add_callback(handle_record)

        status["live_client_started"] = True
        status["error"] = None

        client.start()
        client.block_for_close()

    except Exception as error:
        status["live_client_started"] = False
        status["error"] = str(error)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=start_live_client, daemon=True)
    thread.start()


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "edgeos-databento-live-proxy",
        "endpoints": ["/health", "/snapshot", "/debug"],
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "edgeos-databento-live-proxy",
        "hasKey": bool(DATABENTO_API_KEY),
        "liveClientStarted": status["live_client_started"],
        "lastRecordAt": status["last_record_at"],
        "lastAnyRecordAt": status["last_any_record_at"],
        "recordCount": status["record_count"],
        "error": status["error"],
        "time": now_iso(),
    }


@app.get("/snapshot")
def snapshot():
    generated_at = now_iso()
    data = {}

    for edge_symbol, db_symbol in SYMBOL_MAP.items():
        latest = latest_prices.get(edge_symbol)

        if not latest:
            data[edge_symbol] = {
                "ok": False,
                "symbol": edge_symbol,
                "databento_symbol": db_symbol,
                "price": None,
                "reason": "waiting_for_live_trade",
            }
            continue

        received_at = latest.get("received_at")
        age_seconds = None

        try:
            received_dt = datetime.fromisoformat(received_at)
            age_seconds = time.time() - received_dt.timestamp()
        except Exception:
            pass

        data[edge_symbol] = {
            **latest,
            "age_seconds": age_seconds,
        }

    return {
        "ok": True,
        "mode": "live-stream",
        "source": "Databento Live GLBX.MDP3",
        "generatedAt": generated_at,
        "status": status,
        "data": data,
    }


@app.get("/debug")
def debug():
    return {
        "ok": True,
        "status": status,
        "instrument_to_symbol": instrument_to_symbol,
        "latest_prices": latest_prices,
        "last_records": debug_records,
    }
