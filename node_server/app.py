"""Fog node microservice providing ingestion, analysis, and Prometheus metrics."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Info, generate_latest

logger = logging.getLogger("fog-node")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

NODE_NAME = os.getenv("NODE_NAME", "unknown")
NODE_ROLE = os.getenv("NODE_ROLE", "fog")
POLL_INTERVAL = float(os.getenv("METRIC_REFRESH_SECONDS", "5"))

fog_temperature = Gauge("fog_temperature", "Latest recorded temperature per fog node", ["node"])
fog_humidity = Gauge("fog_humidity", "Latest recorded humidity per fog node", ["node"])
fog_air_quality = Gauge("fog_air_quality", "Latest recorded air quality score per fog node", ["node"])
fog_power = Gauge("fog_power", "Latest recorded power draw per fog node", ["node"])
fog_comfort_index = Gauge("fog_comfort_index", "Derived comfort index per fog node", ["node"])
fog_alert_level = Gauge("fog_alert_level", "Alert level per fog node (0=normal, 1=warning, 2=danger)", ["node"])
fog_alerts_total = Counter("fog_alerts_total", "Total alerts emitted per fog node", ["node"])
fog_analysis_info = Info("fog_analysis_info", "Last analysis outcome per fog node", ["node"])

ALERT_LEVELS: Dict[str, float] = {"normal": 0.0, "warning": 1.0, "danger": 2.0}
DEFAULT_ANALYSIS = "Awaiting data"


class SensorReading(BaseModel):
    temperature: Optional[float] = Field(default=None)
    humidity: Optional[float] = Field(default=None)
    air_quality: Optional[float] = Field(default=None, alias="airQuality")
    power: Optional[float] = Field(default=None)
    extra: Dict[str, float] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    def normalise_payload(cls, values: Any):  # type: ignore[override]
        if not isinstance(values, dict):
            raise ValueError("Sensor reading must be provided as an object")
        data: Dict[str, float] = {}
        extras: Dict[str, float] = {}
        for key, raw in values.items():
            key_lower = key.lower()
            try:
                value = float(raw)
            except (TypeError, ValueError) as error:  # noqa: PERF203 - clarity first
                raise ValueError(f"Metric '{key}' must be numeric") from error
            if key_lower in {"temp", "temperature"}:
                data["temperature"] = value
            elif key_lower == "humidity":
                data["humidity"] = value
            elif key_lower in {"air_quality", "airquality", "aqi"}:
                data["air_quality"] = value
            elif key_lower == "power":
                data["power"] = value
            else:
                extras[key] = value
        data["extra"] = extras
        return data


app = FastAPI(title=f"Fog Node {NODE_NAME}")
_latest_reading: Optional[SensorReading] = None
_last_analysis: str = DEFAULT_ANALYSIS
_last_status: str = "unknown"
_last_comfort_index: Optional[float] = None
_lock = threading.Lock()
_stop_event = threading.Event()


def _comfort_index(reading: SensorReading) -> Optional[float]:
    values = []
    if reading.temperature is not None:
        values.append(max(min(reading.temperature, 120.0), -40.0))
    if reading.humidity is not None:
        values.append(100.0 - max(min(reading.humidity, 100.0), 0.0))
    if reading.air_quality is not None:
        values.append(max(min(reading.air_quality, 100.0), 0.0))
    if reading.power is not None:
        values.append(100.0 - max(min(reading.power, 100.0), 0.0))
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _classify_status(reading: SensorReading) -> str:
    danger_flags = [
        reading.temperature is not None and reading.temperature >= 90.0,
        reading.humidity is not None and reading.humidity >= 85.0,
        reading.air_quality is not None and reading.air_quality <= 30.0,
        reading.power is not None and reading.power >= 85.0,
    ]
    if any(danger_flags):
        return "danger"
    warning_flags = [
        reading.temperature is not None and reading.temperature >= 80.0,
        reading.humidity is not None and reading.humidity >= 70.0,
        reading.air_quality is not None and reading.air_quality <= 50.0,
        reading.power is not None and reading.power >= 65.0,
    ]
    if any(warning_flags):
        return "warning"
    return "normal"


def _analysis_message(status: str, reading: SensorReading) -> str:
    if status == "normal":
        return "All metrics normal"
    messages = []
    if reading.temperature is not None:
        if status == "danger" and reading.temperature >= 90.0:
            messages.append("Temperature critical")
        elif status == "warning" and reading.temperature >= 80.0:
            messages.append("Temperature elevated")
    if reading.humidity is not None:
        if status == "danger" and reading.humidity >= 85.0:
            messages.append("Humidity critical")
        elif status == "warning" and reading.humidity >= 70.0:
            messages.append("Humidity elevated")
    if reading.air_quality is not None:
        if status == "danger" and reading.air_quality <= 30.0:
            messages.append("Air quality poor")
        elif status == "warning" and reading.air_quality <= 50.0:
            messages.append("Air quality degrading")
    if reading.power is not None:
        if status == "danger" and reading.power >= 85.0:
            messages.append("Power draw critical")
        elif status == "warning" and reading.power >= 65.0:
            messages.append("Power draw high")
    return "; ".join(messages) if messages else "Investigate sensor readings"


def _metrics_payload(reading: SensorReading, comfort_index: Optional[float], status: str) -> Dict[str, Any]:
    return {
        "temperature": reading.temperature,
        "humidity": reading.humidity,
        "airQuality": reading.air_quality,
        "power": reading.power,
        "comfortIndex": comfort_index,
        "alertStatus": status,
        "extra": reading.extra,
    }


def _evaluate(reading: SensorReading, *, increment_alerts: bool) -> Dict[str, Any]:
    status = _classify_status(reading)
    if increment_alerts and status == "danger":
        fog_alerts_total.labels(node=NODE_NAME).inc()
    message = _analysis_message(status, reading)
    comfort_index = _comfort_index(reading)
    metrics = _metrics_payload(reading, comfort_index, status)
    return {
        "message": message,
        "status": status,
        "comfort_index": comfort_index,
        "metrics": metrics,
    }


def _refresh_metrics() -> None:
    with _lock:
        reading = _latest_reading
        message = _last_analysis
        status = _last_status
        comfort_index = _last_comfort_index
    labels = {"node": NODE_NAME}
    fog_analysis_info.labels(**labels).info({"message": message})
    fog_alert_level.labels(**labels).set(ALERT_LEVELS.get(status, -1.0))
    if reading is None:
        fog_temperature.labels(**labels).set(0)
        fog_humidity.labels(**labels).set(0)
        fog_air_quality.labels(**labels).set(0)
        fog_power.labels(**labels).set(0)
        fog_comfort_index.labels(**labels).set(0)
        return
    fog_temperature.labels(**labels).set(reading.temperature if reading.temperature is not None else 0)
    fog_humidity.labels(**labels).set(reading.humidity if reading.humidity is not None else 0)
    fog_air_quality.labels(**labels).set(reading.air_quality if reading.air_quality is not None else 0)
    fog_power.labels(**labels).set(reading.power if reading.power is not None else 0)
    fog_comfort_index.labels(**labels).set(comfort_index if comfort_index is not None else 0)


def _metrics_loop() -> None:
    while not _stop_event.is_set():
        _refresh_metrics()
        _stop_event.wait(POLL_INTERVAL)


@app.on_event("startup")
async def on_startup() -> None:
    thread = threading.Thread(target=_metrics_loop, name="metrics-updater", daemon=True)
    thread.start()
    logger.info("Fog node %s initialised (role=%s)", NODE_NAME, NODE_ROLE)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    _stop_event.set()
    _refresh_metrics()


@app.get("/health")
async def health() -> Dict[str, Any]:
    with _lock:
        status = _last_status
        records = 0 if _latest_reading is None else 1
    return {
        "status": status or "unknown",
        "node": NODE_NAME,
        "records": records,
        "timestamp": time.time(),
    }


@app.post("/ingest")
async def ingest(payload: SensorReading) -> JSONResponse:
    global _latest_reading, _last_analysis, _last_status, _last_comfort_index
    result = _evaluate(payload, increment_alerts=True)
    with _lock:
        _latest_reading = payload
        _last_analysis = result["message"]
        _last_status = result["status"]
        _last_comfort_index = result["comfort_index"]
    _refresh_metrics()
    logger.info("Ingested metrics for %s: %s", NODE_NAME, result["metrics"])
    body = {
        "node": NODE_NAME,
        "analysis": result["message"],
        "status": result["status"],
        "stored": True,
        "metrics": result["metrics"],
        "updatedAt": time.time(),
    }
    return JSONResponse(body)


@app.get("/analyze")
async def analyze() -> Dict[str, Any]:
    global _last_analysis, _last_status, _last_comfort_index
    with _lock:
        reading = _latest_reading
    if reading is None:
        raise HTTPException(status_code=404, detail="No data ingested yet")
    result = _evaluate(reading, increment_alerts=False)
    with _lock:
        _last_analysis = result["message"]
        _last_status = result["status"]
        _last_comfort_index = result["comfort_index"]
    _refresh_metrics()
    return {
        "node": NODE_NAME,
        "analysis": result["message"],
        "status": result["status"],
        "metrics": result["metrics"],
        "generatedAt": time.time(),
    }


@app.get("/metrics")
async def metrics() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
