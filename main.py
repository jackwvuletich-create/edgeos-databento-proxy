import os
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict

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
    "ES": "ES.FUT",
    "NQ": "NQ.FUT",
    "MES": "MES.FUT",
    "MNQ": "MNQ.FUT",
}

latest_prices: Dict[str, Dict[str, Any]] = {}
status: Dict[str, Any] = {
    "live_client_started": False,
    "last_record_at": None,
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

    # Databento prices often come through as fixed-point integers.
    # This protects against huge raw values.
    if number > 1_000_000:
        return number / 1_000_000_000

    return number


def handle_record(record):
    try:
        record_text = str(record)

        matched_symbol = None
        for edge_symbol, db_symbol in SYMBOL_MAP.items():
            if db_symbol in record_text or edge_symbol in record_text:
                matched_symbol = edge_symbol
                break

        price = None

        if hasattr(record, "price"):
            price = normalize_price(record.price)
        elif hasattr(record, "px"):
            price = normalize_price(record.px)

        if matched_symbol and price:
            latest_prices[matched_symbol] = {
                "ok": True,
                "symbol": matched_symbol,
                "databento_symbol": SYMBOL_MAP[matched_symbol],
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
            stype_in="parent",
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
        "endpoints": ["/health", "/snapshot"],
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "edgeos-databento-live-proxy",
        "hasKey": bool(DATABENTO_API_KEY),
        "liveClientStarted": status["live_client_started"],
        "lastRecordAt": status["last_record_at"],
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
