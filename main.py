"""Dynamic fog-cloud orchestration API.

This module exposes a FastAPI service that can:
* parse user requests describing how many fog/cloud nodes are required,
* create Docker containers to represent those nodes,
* accept simulated data-transfer events between any two nodes, and
* export metrics via Prometheus and JSON APIs so Grafana can visualise system activity.

It is intentionally lightweight so the prototype can evolve quickly.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import threading
import time
import webbrowser
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import docker
from docker.errors import APIError, ImageNotFound, NotFound
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

logger = logging.getLogger("dynamic-orchestrator")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

CONTAINER_LABEL = "dynamic.mockfog.node"

# Metrics describing orchestrated state and simulated transfers.
transfer_counter = Counter(
    "mockfog_transfers_total",
    "Total number of simulated node transfers",
    ["source", "destination"],
)
transfer_latency = Histogram(
    "mockfog_transfer_latency_seconds",
    "Simulated transfer latency distribution",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)
node_gauge = Gauge(
    "mockfog_nodes_total",
    "Current number of orchestrated nodes by tier",
    ["role"],
)


@dataclass
class TransferEvent:
    """In-memory record of a simulated data transfer."""

    timestamp: float
    source: str
    destination: str
    payload: Dict[str, Any]
    latency_seconds: float


class CreateNodesRequest(BaseModel):
    """Payload describing the desired fog/cloud node counts."""

    application: str = Field(min_length=1)
    fog_nodes: int = Field(default=0, ge=0, alias="fogNodes")
    cloud_nodes: int = Field(default=0, ge=0, alias="cloudNodes")

    model_config = ConfigDict(populate_by_name=True)


class CreateNodesResponse(BaseModel):
    """Response describing the created (or reused) nodes."""

    application: str
    nodes: Dict[str, str]
    grafana_url: str


class TransferRequest(BaseModel):
    """Payload describing a simulated transfer between two nodes."""

    source: str = Field(min_length=2)
    destination: str = Field(min_length=2)


class TransferResponse(BaseModel):
    """Response confirming the simulated transfer."""

    source: str
    destination: str
    latency_ms: float
    timestamp: float
    metrics: Dict[str, Any] = Field(default_factory=dict)
    analysis: Optional[str] = None
    alertStatus: Optional[str] = None


class NodesResponse(BaseModel):
    """Response describing the currently tracked nodes."""

    nodes: Dict[str, str]


class NodeDetail(BaseModel):
    """Detail about an orchestrated node container."""

    id: str
    role: str
    status: str
    ip: Optional[str] = None
    image: Optional[str] = None
    endpoint: Optional[str] = None


class NodeStatusResponse(BaseModel):
    """Detailed status for all managed nodes."""

    nodes: Dict[str, NodeDetail]


@dataclass
class NodeRecord:
    """Internal representation of a managed node."""

    id: str
    role: str


class NodeIngestRequest(BaseModel):
    """Payload describing sensor data destined for a fog node."""

    node: str = Field(min_length=2)
    data: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validate_data(self):  # type: ignore[override]
        if not self.data:
            raise ValueError("Sensor data payload cannot be empty")
        return self


class NodeIngestResponse(BaseModel):
    """Acknowledgement after pushing sensor data into a fog node."""

    node: str
    analysis: str
    status: str = Field(default="unknown")
    stored: bool = True
    updatedAt: Optional[float] = None
    updatedAtIso: Optional[str] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)


class FogAnalysisResponse(BaseModel):
    """Summary of analysis results from all fog nodes."""

    results: Dict[str, Dict[str, Any]]


class NodeDataResponse(BaseModel):
    """Snapshot of the stored data for a specific node."""

    node: str
    metrics: Dict[str, Any] = Field(default_factory=dict)
    recognised: Dict[str, Any] = Field(default_factory=dict)
    extra: Dict[str, Any] = Field(default_factory=dict)
    comfortIndex: Optional[float] = None
    alertStatus: Optional[str] = None
    analysis: Optional[str] = None
    updatedAt: Optional[float] = None
    updatedAtIso: Optional[str] = None
    history: List[Dict[str, Any]] = Field(default_factory=list)


class CleanupResponse(BaseModel):
    """Outcome after stopping containers and clearing in-memory data."""

    message: str
    stopped: int = 0
    removed: int = 0
    errors: int = 0
    clearedNodes: int = 0
    clearedSummaries: int = 0

class GrafanaTarget(BaseModel):
    """Grafana JSON datasource target descriptor."""

    refId: str
    target: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class GrafanaQueryRequest(BaseModel):
    """Subset of Grafana query request payload we care about."""

    targets: List[GrafanaTarget] = Field(default_factory=list)


ALIAS_MAP: Dict[str, set[str]] = {
    "temperature": {"temperature", "temp"},
    "humidity": {"humidity"},
    "pressure": {"pressure"},
    "air_quality": {"air_quality", "airquality", "aqi"},
    "power": {"power"},
}
ALIAS_LOOKUP: Dict[str, str] = {
    alias: canonical for canonical, aliases in ALIAS_MAP.items() for alias in aliases
}
NODE_HISTORY_LIMIT = 50
SUMMARY_HISTORY_LIMIT = 200

store_lock = threading.Lock()
node_store: Dict[str, Dict[str, Any]] = {}
summary_events: Deque[Dict[str, Any]] = deque(maxlen=SUMMARY_HISTORY_LIMIT)
ingest_counter = 0
analysis_counter = 0
app_started_at = time.time()


def _iso_timestamp(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _normalise_metrics(data: Dict[str, Any]) -> tuple[Dict[str, float], Dict[str, float]]:
    recognised: Dict[str, float] = {}
    extras: Dict[str, float] = {}
    for key, value in data.items():
        key_str = str(key).strip()
        if not key_str or value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            logger.debug("Skipping non-numeric metric %s=%s", key, value)
            continue
        canonical = ALIAS_LOOKUP.get(key_str.lower())
        if canonical:
            recognised[canonical] = numeric
        else:
            extras[key_str] = numeric
    return recognised, extras


def _comfort_index(temperature: Optional[float], humidity: Optional[float]) -> Optional[float]:
    if temperature is None or humidity is None:
        return None
    value = 100.0 - abs(temperature - 72.0) - abs(humidity - 50.0) / 2.0
    return round(value, 2)


def _alert_from_metrics(comfort_index: Optional[float]) -> str:
    if comfort_index is None:
        return "Warning"
    if comfort_index < 60.0:
        return "Danger"
    if comfort_index < 80.0:
        return "Warning"
    return "OK"


def _ingest_message(alert_status: str, comfort_index: Optional[float]) -> str:
    if comfort_index is None:
        return f"Metrics ingested ({alert_status})"
    return f"Comfort index {comfort_index} ({alert_status})"


def _health_emoji(status: str) -> str:
    mapping = {
        "ok": "",
        "normal": "",
        "warning": "",
        "danger": "",
        "high temp": "",
        "low humidity": "",
        "no data": "",
    }
    return mapping.get(status.lower(), "")


class NodeOrchestrator:
    """Provision fog and cloud nodes using the Docker Engine API."""

    def __init__(
        self,
        image: str = "mockfog_fog_node:latest",
        network: Optional[str] = None,
    ) -> None:
        self._client = docker.from_env()
        self._image = image
        self._network = network or os.getenv("ORCHESTRATOR_NETWORK")
        self._node_server_dir = Path(__file__).resolve().parent / "node_server"
        self._lock = threading.Lock()
        self._state: Dict[str, NodeRecord] = {}

    def create_nodes(self, application: str, fog_count: int, cloud_count: int) -> Dict[str, str]:
        fog_count = self._validate_count(fog_count, "fog")
        cloud_count = self._validate_count(cloud_count, "cloud")
        with self._lock:
            self._ensure_image()
            mapping: Dict[str, NodeRecord] = {}
            mapping.update(self._ensure_group("fog", fog_count))
            mapping.update(self._ensure_group("cloud", cloud_count))
            self._state.update(mapping)
            self._state = self._ordered_records(self._state)
            self._update_metrics()
            logger.info(
                "Provisioned application=%s fog=%s cloud=%s",
                application,
                fog_count,
                cloud_count,
            )
            return self._ordered_id_mapping(self._state)

    def current_nodes(self) -> Dict[str, str]:
        with self._lock:
            containers = self._client.containers.list(all=True, filters={"label": CONTAINER_LABEL})
            records: Dict[str, NodeRecord] = {}
            for container in containers:
                role = self._role_for_container(container)
                records[container.name] = NodeRecord(id=container.id, role=role)
            self._state = self._ordered_records(records)
            self._update_metrics()
            return self._ordered_id_mapping(self._state)

    def node_details(self) -> Dict[str, Dict[str, Optional[str]]]:
        with self._lock:
            containers = self._client.containers.list(all=True, filters={"label": CONTAINER_LABEL})
            details: Dict[str, Dict[str, Optional[str]]] = {}
            records: Dict[str, NodeRecord] = {}
            for container in containers:
                try:
                    container.reload()
                except APIError:  # pragma: no cover - docker engine interaction
                    logger.warning("Unable to refresh container %s state", container.name)
                status = getattr(container, "status", "unknown") or "unknown"
                role = self._role_for_container(container)
                ip_address: Optional[str] = None
                try:
                    networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                    for network in networks.values():
                        if network.get("IPAddress"):
                            ip_address = network.get("IPAddress")
                            break
                except AttributeError:
                    ip_address = None
                image_tag = None
                try:
                    if container.image and container.image.tags:
                        image_tag = container.image.tags[0]
                    elif container.image:
                        image_tag = container.image.short_id
                except AttributeError:
                    image_tag = None
                endpoint = f"http://{container.name}:8000" if role == "fog" else None
                details[container.name] = {
                    "id": container.id,
                    "role": role,
                    "status": status,
                    "ip": ip_address,
                    "image": image_tag,
                    "endpoint": endpoint,
                }
                records[container.name] = NodeRecord(id=container.id, role=role)
            ordered_names = sorted(details.keys(), key=self._sort_key)
            ordered_details = {name: details[name] for name in ordered_names}
            ordered_records = {name: records[name] for name in ordered_names if name in records}
            self._state = ordered_records
            self._update_metrics()
            return ordered_details

    def stop_all(self) -> Dict[str, int]:
        summary = {"stopped": 0, "removed": 0, "errors": 0}
        with self._lock:
            containers = self._client.containers.list(all=True, filters={"label": CONTAINER_LABEL})
            for container in containers:
                try:
                    container.reload()
                except APIError:
                    logger.warning("Unable to refresh container %s state before stop", container.name)
                try:
                    if getattr(container, "status", "") == "running":
                        container.stop(timeout=10)
                        summary["stopped"] += 1
                except APIError as error:
                    logger.warning("Unable to stop container %s: %s", container.name, error)
                    summary["errors"] += 1
                try:
                    container.remove(force=True)
                    summary["removed"] += 1
                except APIError as error:
                    logger.warning("Unable to remove container %s: %s", container.name, error)
                    summary["errors"] += 1
            self._state.clear()
            self._update_metrics()
        return summary

    def has_node(self, name: str) -> bool:
        with self._lock:
            return name in self._state

    def fog_nodes(self) -> List[str]:
        with self._lock:
            return [name for name, record in self._state.items() if record.role == "fog" or name.startswith("f")]

    def _ensure_group(self, tier: str, count: int) -> Dict[str, NodeRecord]:
        mapping: Dict[str, NodeRecord] = {}
        for index in range(1, count + 1):
            node_name = self._node_name(tier, index)
            mapping[node_name] = self._ensure_container(node_name, tier)
        return mapping

    def _ensure_container(self, node_name: str, tier: str) -> NodeRecord:
        labels = {CONTAINER_LABEL: "true", "mockfog.role": tier}
        try:
            container = self._client.containers.get(node_name)
            container.reload()
            if container.status != "running":
                container.start()
                container.reload()
            self._ensure_network_attachment(container)
            logger.info("Reusing container %s (id=%s tier=%s)", node_name, container.id, tier)
            return NodeRecord(id=container.id, role=tier)
        except NotFound:
            environment = {
                "NODE_NAME": node_name,
                "NODE_ROLE": tier,
            }
            run_kwargs = {
                "image": self._image,
                "name": node_name,
                "detach": True,
                "labels": labels,
                "environment": environment,
                "restart_policy": {"Name": "unless-stopped"},
            }
            if self._network:
                run_kwargs["network"] = self._network
            try:
                container = self._client.containers.run(**run_kwargs)
            except APIError as error:  # pragma: no cover - docker engine interaction
                message = getattr(error, "explanation", str(error))
                logger.error("Failed to start container %s: %s", node_name, message)
                raise RuntimeError(f"Unable to start container {node_name}: {message}") from error
            logger.info(
                "Created container %s (id=%s tier=%s image=%s)",
                node_name,
                container.id,
                tier,
                self._image,
            )
            return NodeRecord(id=container.id, role=tier)

    def _ensure_image(self) -> None:
        build_label = "mockfog.build.digest"
        if not self._node_server_dir.exists():
            raise RuntimeError(f"Node server directory {self._node_server_dir} not found")
        digest = self._calculate_directory_digest()
        needs_build = False
        try:
            image = self._client.images.get(self._image)
            labels = getattr(image, "labels", {}) or {}
            if labels.get(build_label) != digest:
                needs_build = True
        except ImageNotFound:
            needs_build = True
        if not needs_build:
            logger.debug("Fog node image %s up-to-date (digest=%s)", self._image, digest[:12])
            return
        logger.info("Building fog node image %s (digest=%s)", self._image, digest[:12])
        self._client.images.build(
            path=str(self._node_server_dir),
            tag=self._image,
            rm=True,
            forcerm=True,
            labels={build_label: digest},
        )

    def _calculate_directory_digest(self) -> str:
        digest = hashlib.sha256()
        for path in sorted(self._node_server_dir.rglob("*")):
            if path.is_dir() or path.name.endswith(".pyc") or "__pycache__" in path.parts:
                continue
            relative = path.relative_to(self._node_server_dir).as_posix().encode()
            digest.update(relative)
            with path.open("rb") as handle:
                digest.update(handle.read())
        return digest.hexdigest()

    @staticmethod
    def _node_name(tier: str, index: int) -> str:
        prefix = "f" if tier == "fog" else "c"
        return f"{prefix}{index}"

    @staticmethod
    def _validate_count(value: int, label: str) -> int:
        if value is None:
            raise ValueError(f"{label} node count must be provided")
        if value < 0:
            raise ValueError(f"{label} node count must be non-negative")
        return int(value)

    def _ensure_network_attachment(self, container) -> None:
        if not self._network:
            return
        container.reload()
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        if self._network in networks:
            return
        try:
            network = self._client.networks.get(self._network)
            network.connect(container)
            logger.info("Attached container %s to network %s", container.name, self._network)
        except NotFound:
            logger.warning("Declared network %s not found; continuing without attachment", self._network)
        except APIError as error:  # pragma: no cover - docker engine interaction
            logger.warning("Failed to attach container %s to network %s: %s", container.name, self._network, error)

    def _role_for_container(self, container) -> str:
        labels = container.labels or {}
        role = labels.get("mockfog.role")
        if role:
            return role
        if container.name.startswith("f"):
            return "fog"
        if container.name.startswith("c"):
            return "cloud"
        return "unknown"

    def _ordered_records(self, mapping: Dict[str, NodeRecord]) -> Dict[str, NodeRecord]:
        return {name: mapping[name] for name in sorted(mapping.keys(), key=self._sort_key)}

    def _ordered_id_mapping(self, mapping: Dict[str, NodeRecord]) -> Dict[str, str]:
        return {name: mapping[name].id for name in mapping}

    def _update_metrics(self) -> None:
        fog_nodes = sum(1 for record in self._state.values() if record.role == "fog")
        cloud_nodes = sum(1 for record in self._state.values() if record.role == "cloud")
        node_gauge.labels(role="fog").set(fog_nodes)
        node_gauge.labels(role="cloud").set(cloud_nodes)

    @staticmethod
    def _sort_key(name: str) -> tuple[int, int, str]:
        if name.startswith("f"):
            role_rank = 0
        elif name.startswith("c"):
            role_rank = 1
        else:
            role_rank = 2
        digits = "".join(ch for ch in name if ch.isdigit())
        try:
            index = int(digits)
        except ValueError:
            index = 0
        return (role_rank, index, name)


class TransferService:
    """Record simulated transfers and feed Prometheus metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: List[TransferEvent] = []

    def record(self, source: str, destination: str, payload: Dict[str, Any]) -> TransferEvent:
        latency_seconds = random.uniform(0.02, 0.5)
        transfer_counter.labels(source=source, destination=destination).inc()
        transfer_latency.observe(latency_seconds)
        payload_copy = dict(payload) if payload else {}
        event = TransferEvent(
            timestamp=time.time(),
            source=source,
            destination=destination,
            payload=payload_copy,
            latency_seconds=latency_seconds,
        )
        with self._lock:
            self._events.append(event)
            # Keep the log from growing indefinitely in the prototype.
            if len(self._events) > 5000:
                self._events = self._events[-1000:]
        metrics_preview = {key: payload_copy[key] for key in list(payload_copy)[:4]}
        logger.info(
            "Transfer recorded source=%s destination=%s latency=%.3fs metrics=%s",
            source,
            destination,
            latency_seconds,
            metrics_preview,
        )
        return event

    def recent(self) -> List[TransferEvent]:
        with self._lock:
            return list(self._events)


GRAFANA_URL = os.getenv(
    "GRAFANA_URL",
    "http://localhost:3000/d/dynamic-fog/mockfog-dynamic-fog?orgId=1",
)

app = FastAPI(title="Dynamic Fog Orchestrator", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

orchestrator = NodeOrchestrator()
transfer_service = TransferService()


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/create", response_model=CreateNodesResponse)
async def create_nodes(payload: CreateNodesRequest) -> CreateNodesResponse:
    logger.info(
        "Received create request application=%s fog=%s cloud=%s",
        payload.application,
        payload.fog_nodes,
        payload.cloud_nodes,
    )
    try:
        nodes = await asyncio.to_thread(
            orchestrator.create_nodes,
            payload.application,
            payload.fog_nodes,
            payload.cloud_nodes,
        )
    except Exception as error:  # pragma: no cover - docker interactions are runtime specific
        logger.exception("Unable to provision nodes")
        raise HTTPException(status_code=500, detail=str(error)) from error
    return CreateNodesResponse(application=payload.application, nodes=nodes, grafana_url=GRAFANA_URL)


@app.get("/nodes", response_model=NodesResponse)
async def list_nodes() -> NodesResponse:
    nodes = await asyncio.to_thread(orchestrator.current_nodes)
    return NodesResponse(nodes=nodes)


@app.get("/nodes/status", response_model=NodeStatusResponse)
async def node_status() -> NodeStatusResponse:
    details = await asyncio.to_thread(orchestrator.node_details)
    return NodeStatusResponse(nodes=details)


@app.get("/nodes/{node}/data", response_model=NodeDataResponse)
async def node_stored_data(node: str) -> NodeDataResponse:
    label = node.strip()
    if not label:
        raise HTTPException(status_code=400, detail="Node label is required")
    with store_lock:
        entry = node_store.get(label)
        if not entry:
            raise HTTPException(status_code=404, detail=f"No stored data for node {label}")
        history = list(entry.get("history", []))
        payload = NodeDataResponse(
            node=label,
            metrics=dict(entry.get("metrics", {})),
            recognised=dict(entry.get("recognised", {})),
            extra=dict(entry.get("extra", {})),
            comfortIndex=entry.get("comfortIndex"),
            alertStatus=entry.get("alertStatus"),
            analysis=entry.get("analysis"),
            updatedAt=entry.get("updatedAt"),
            updatedAtIso=entry.get("updatedAtIso"),
            history=history,
        )
    return payload


@app.post("/transfer", response_model=TransferResponse)
async def transfer(payload: TransferRequest) -> TransferResponse:
    nodes = await asyncio.to_thread(orchestrator.current_nodes)
    if payload.source not in nodes:
        raise HTTPException(status_code=404, detail=f"Source node {payload.source} not found")
    if payload.destination not in nodes:
        raise HTTPException(status_code=404, detail=f"Destination node {payload.destination} not found")
    with store_lock:
        source_entry = node_store.get(payload.source)
        if not source_entry or not source_entry.get("metrics"):
            raise HTTPException(
                status_code=400,
                detail=f"Source node {payload.source} has no stored metrics to transfer",
            )
        transfer_metrics = dict(source_entry.get("metrics", {}))
        transfer_recognised = dict(source_entry.get("recognised", {}))
        transfer_extra = dict(source_entry.get("extra", {}))
        comfort_index = source_entry.get("comfortIndex")
        alert_status = source_entry.get("alertStatus", "No Data")
        analysis_message = source_entry.get("analysis")
        timestamp = time.time()
        timestamp_iso = _iso_timestamp(timestamp)

        dest_entry = node_store.get(payload.destination, {})
        history = list(dest_entry.get("history", []))
        transfer_event_record = {
            "node": payload.destination,
            "sourceNode": payload.source,
            "analysis": analysis_message or "Data transferred",
            "alertStatus": alert_status,
            "comfortIndex": comfort_index,
            "timestamp": timestamp,
            "timestampIso": timestamp_iso,
            "source": "transfer",
        }
        history.append(transfer_event_record)
        if len(history) > NODE_HISTORY_LIMIT:
            history = history[-NODE_HISTORY_LIMIT:]
        dest_entry.update(
            {
                "node": payload.destination,
                "metrics": transfer_metrics,
                "recognised": transfer_recognised,
                "extra": transfer_extra,
                "comfortIndex": comfort_index,
                "alertStatus": alert_status,
                "analysis": analysis_message,
                "updatedAt": timestamp,
                "updatedAtIso": timestamp_iso,
                "history": history,
            }
        )
        node_store[payload.destination] = dest_entry
        summary_events.append(transfer_event_record)

    event = transfer_service.record(payload.source, payload.destination, transfer_metrics)
    return TransferResponse(
        source=event.source,
        destination=event.destination,
        latency_ms=round(event.latency_seconds * 1000.0, 3),
        timestamp=event.timestamp,
        metrics=transfer_metrics,
        analysis=analysis_message,
        alertStatus=alert_status,
    )


@app.get("/transfers")
async def transfers() -> List[TransferResponse]:
    events = await asyncio.to_thread(transfer_service.recent)
    return [
        TransferResponse(
            source=event.source,
            destination=event.destination,
            latency_ms=round(event.latency_seconds * 1000.0, 3),
            timestamp=event.timestamp,
            metrics=dict(event.payload),
        )
        for event in events
    ]


@app.post("/ingest", response_model=NodeIngestResponse)
async def ingest_sensor(payload: NodeIngestRequest) -> NodeIngestResponse:
    global ingest_counter
    node = payload.node.strip()
    if not node:
        raise HTTPException(status_code=400, detail="Node label is required")
    if not node.startswith("f"):
        raise HTTPException(status_code=400, detail="Data ingestion is only supported for fog nodes")
    if not orchestrator.has_node(node):
        raise HTTPException(status_code=404, detail=f"Node {node} not found")

    recognised, extras = _normalise_metrics(payload.data)
    temperature = recognised.get("temperature")
    humidity = recognised.get("humidity")
    comfort_value = _comfort_index(temperature, humidity)
    alert_status = _alert_from_metrics(comfort_value)
    analysis_message = _ingest_message(alert_status, comfort_value)
    snapshot = {
        "temperature": temperature,
        "humidity": humidity,
        "pressure": recognised.get("pressure"),
        "airQuality": recognised.get("air_quality"),
        "power": recognised.get("power"),
        "comfortIndex": comfort_value,
        "alertStatus": alert_status,
        "extra": extras,
    }
    timestamp = time.time()
    timestamp_iso = _iso_timestamp(timestamp)
    event_record = {
        "node": node,
        "analysis": analysis_message,
        "alertStatus": alert_status,
        "comfortIndex": comfort_value,
        "timestamp": timestamp,
        "timestampIso": timestamp_iso,
        "source": "ingest",
    }

    with store_lock:
        entry = node_store.get(node, {})
        history = list(entry.get("history", []))
        history.append(event_record)
        if len(history) > NODE_HISTORY_LIMIT:
            history = history[-NODE_HISTORY_LIMIT:]
        entry.update(
            {
                "node": node,
                "metrics": snapshot,
                "recognised": recognised,
                "extra": extras,
                "comfortIndex": comfort_value,
                "alertStatus": alert_status,
                "analysis": analysis_message,
                "updatedAt": timestamp,
                "updatedAtIso": timestamp_iso,
                "history": history,
            }
        )
        node_store[node] = entry
        summary_events.append(event_record)
        ingest_counter += 1

    logger.info("Ingested metrics for %s: %s", node, snapshot)
    return NodeIngestResponse(
        node=node,
        analysis=analysis_message,
        status=alert_status,
        stored=True,
        updatedAt=timestamp,
        updatedAtIso=timestamp_iso,
        metrics=snapshot,
    )

@app.get("/analyze", response_model=FogAnalysisResponse)
async def analyze_fog_nodes() -> FogAnalysisResponse:
    global analysis_counter
    nodes = orchestrator.fog_nodes()
    if not nodes:
        return FogAnalysisResponse(results={})

    now = time.time()
    now_iso = _iso_timestamp(now)
    results: Dict[str, Dict[str, Any]] = {}
    with store_lock:
        for node in nodes:
            entry = node_store.get(node)
            if not entry:
                results[node] = {
                    "analysis": "No data ingested yet",
                    "status": "No Data",
                    "alertStatus": "No Data",
                    "comfortIndex": None,
                    "metrics": {},
                    "generatedAt": now,
                    "generatedAtIso": now_iso,
                }
                continue

            metrics = dict(entry.get("metrics", {}))
            temperature = metrics.get("temperature")
            humidity = metrics.get("humidity")
            if temperature is not None and temperature > 100.0:
                alert_status = "High Temp"
                analysis_message = "Temperature above safe threshold"
            elif humidity is not None and humidity < 30.0:
                alert_status = "Low Humidity"
                analysis_message = "Humidity below optimal range"
            else:
                alert_status = "Normal"
                analysis_message = "Operating within expected parameters"

            comfort_value = entry.get("comfortIndex")
            payload = {
                "analysis": analysis_message,
                "status": alert_status,
                "alertStatus": alert_status,
                "comfortIndex": comfort_value,
                "metrics": metrics,
                "generatedAt": now,
                "generatedAtIso": now_iso,
            }
            results[node] = payload

            history = list(entry.get("history", []))
            event_record = {
                "node": node,
                "analysis": analysis_message,
                "alertStatus": alert_status,
                "comfortIndex": comfort_value,
                "timestamp": now,
                "timestampIso": now_iso,
                "source": "analyze",
            }
            history.append(event_record)
            if len(history) > NODE_HISTORY_LIMIT:
                history = history[-NODE_HISTORY_LIMIT:]
            entry.update(
                {
                    "history": history,
                    "analysis": analysis_message,
                    "alertStatus": alert_status,
                    "comfortIndex": comfort_value,
                    "updatedAt": now,
                    "updatedAtIso": now_iso,
                }
            )
            node_store[node] = entry
            summary_events.append(event_record)

        analysis_counter += 1

    return FogAnalysisResponse(results=results)


@app.post("/cleanup", response_model=CleanupResponse)
async def cleanup_environment() -> CleanupResponse:
    global ingest_counter, analysis_counter
    summary = orchestrator.stop_all()
    with store_lock:
        cleared_nodes = len(node_store)
        cleared_summaries = len(summary_events)
        node_store.clear()
        summary_events.clear()
    ingest_counter = 0
    analysis_counter = 0
    message = "Stopped containers and cleared node data."
    return CleanupResponse(
        message=message,
        stopped=summary.get("stopped", 0),
        removed=summary.get("removed", 0),
        errors=summary.get("errors", 0),
        clearedNodes=cleared_nodes,
        clearedSummaries=cleared_summaries,
    )

@app.get("/api/live_data")
async def api_live_data() -> Dict[str, Any]:
    nodes = orchestrator.fog_nodes()
    with store_lock:
        items: List[Dict[str, Any]] = []
        node_snapshot: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            entry = node_store.get(node, {})
            metrics = dict(entry.get("metrics", {}))
            updated_epoch = entry.get("updatedAt")
            updated_iso = entry.get("updatedAtIso") or _iso_timestamp(updated_epoch)
            record = {
                "node": node,
                "temperature": metrics.get("temperature"),
                "humidity": metrics.get("humidity"),
                "pressure": metrics.get("pressure"),
                "airQuality": metrics.get("airQuality"),
                "power": metrics.get("power"),
                "comfortIndex": metrics.get("comfortIndex"),
                "alertStatus": entry.get("alertStatus", "No Data"),
                "updatedAt": updated_epoch,
                "updatedAtIso": updated_iso,
                "extra": entry.get("extra", {}),
            }
            node_snapshot[node] = {
                "metrics": metrics,
                "analysis": entry.get("analysis"),
                "alertStatus": entry.get("alertStatus", "No Data"),
                "updatedAt": updated_epoch,
                "updatedAtIso": updated_iso,
            }
            items.append(record)
    current = time.time()
    return {
        "nodes": node_snapshot,
        "items": items,
        "timestamp": current,
        "timestampIso": _iso_timestamp(current),
    }

@app.get("/api/health_map")
async def api_health_map() -> Dict[str, Any]:
    nodes = orchestrator.fog_nodes()
    with store_lock:
        health: Dict[str, str] = {}
        items: List[Dict[str, Any]] = []
        for node in nodes:
            entry = node_store.get(node)
            status = entry.get("alertStatus") if entry else "No Data"
            icon = _health_emoji(status)
            updated = entry.get("updatedAt") if entry else None
            updated_iso = entry.get("updatedAtIso") if entry else None
            if not updated_iso:
                updated_iso = _iso_timestamp(updated)
            health[node] = icon
            items.append(
                {
                    "node": node,
                    "status": status,
                    "icon": icon,
                    "updatedAt": updated,
                    "updatedAtIso": updated_iso,
                }
            )
    current = time.time()
    return {
        "health": health,
        "items": items,
        "timestamp": current,
        "timestampIso": _iso_timestamp(current),
    }

@app.get("/api/analysis_summary")
async def api_analysis_summary() -> Dict[str, Any]:
    with store_lock:
        events = list(summary_events)
    events.sort(key=lambda entry: entry.get("timestamp", 0), reverse=True)
    current = time.time()
    return {
        "events": events,
        "count": len(events),
        "timestamp": current,
        "timestampIso": _iso_timestamp(current),
    }

@app.get("/api/system_overview")
async def api_system_overview() -> Dict[str, Any]:
    managed_nodes = await asyncio.to_thread(orchestrator.current_nodes)
    with store_lock:
        active = len(node_store)
        status_buckets = {"OK": 0, "Warning": 0, "Danger": 0, "Other": 0}
        for entry in node_store.values():
            status = str(entry.get("alertStatus", "Other")).lower()
            if status in {"ok", "normal"}:
                status_buckets["OK"] += 1
            elif status in {"warning", "low humidity"}:
                status_buckets["Warning"] += 1
            elif status in {"danger", "high temp"}:
                status_buckets["Danger"] += 1
            else:
                status_buckets["Other"] += 1
        ingests = ingest_counter
        analyses = analysis_counter
    total_nodes = len(managed_nodes)
    inactive = max(total_nodes - active, 0)
    uptime = time.time() - app_started_at
    generated = time.time()
    overview = {
        "totalNodes": total_nodes,
        "activeNodes": active,
        "inactiveNodes": inactive,
        "ingestCount": ingests,
        "analysisCount": analyses,
        "uptimeSeconds": round(uptime, 2),
        "alertBuckets": status_buckets,
        "generatedAt": generated,
        "generatedAtIso": _iso_timestamp(generated),
    }
    current = time.time()
    return {
        "overview": overview,
        "timestamp": current,
        "timestampIso": _iso_timestamp(current),
    }

@app.get("/")
async def root() -> Dict[str, Any]:
    """Health check endpoint consumed by Grafana's JSON datasource."""

    return {
        "status": "ok",
        "uptimeSeconds": round(time.time() - app_started_at, 2),
    }


@app.post("/metrics")
async def grafana_metrics() -> List[Dict[str, str]]:
    """Expose available targets to the Grafana JSON datasource."""

    return [
        {"value": "live_data"},
        {"value": "health_map"},
        {"value": "analysis_summary"},
        {"value": "system_overview"},
        {"value": "alert_buckets"},
    ]


@app.post("/metric-payload-options")
async def grafana_metric_payload_options() -> List[Dict[str, Any]]:
    """Simple placeholder to satisfy Grafana JSON datasource contract."""

    return []


@app.post("/query")
async def grafana_query(payload: GrafanaQueryRequest) -> List[Dict[str, Any]]:
    """Translate Grafana JSON datasource queries into orchestrator responses."""

    frames: List[Dict[str, Any]] = []
    live_cache: Optional[Dict[str, Any]] = None
    health_cache: Optional[Dict[str, Any]] = None
    summary_cache: Optional[Dict[str, Any]] = None
    overview_cache: Optional[Dict[str, Any]] = None

    for target in payload.targets:
        key = (target.target or "").strip().lower()
        if not key:
            continue

        if key == "live_data":
            if live_cache is None:
                live_cache = await api_live_data()
            items = live_cache.get("items", [])
            rows = []
            for item in items:
                rows.append(
                    [
                        item.get("node"),
                        item.get("temperature"),
                        item.get("humidity"),
                        item.get("pressure"),
                        item.get("airQuality"),
                        item.get("power"),
                        item.get("comfortIndex"),
                        item.get("alertStatus"),
                        item.get("updatedAtIso"),
                        item.get("updatedAt"),
                    ]
                )
            frames.append(
                {
                    "refId": target.refId,
                    "columns": [
                        {"text": "Node", "type": "string"},
                        {"text": "Temperature", "type": "number"},
                        {"text": "Humidity", "type": "number"},
                        {"text": "Pressure", "type": "number"},
                        {"text": "Air Quality", "type": "number"},
                        {"text": "Power", "type": "number"},
                        {"text": "Comfort Index", "type": "number"},
                        {"text": "Alert Status", "type": "string"},
                        {"text": "Updated At (UTC)", "type": "string"},
                        {"text": "Updated Epoch", "type": "number"},
                    ],
                    "rows": rows,
                    "type": "table",
                }
            )
        elif key == "health_map":
            if health_cache is None:
                health_cache = await api_health_map()
            items = health_cache.get("items", [])
            rows = []
            for item in items:
                rows.append(
                    [
                        item.get("node"),
                        item.get("status"),
                        item.get("icon"),
                        item.get("updatedAtIso"),
                        item.get("updatedAt"),
                    ]
                )
            frames.append(
                {
                    "refId": target.refId,
                    "columns": [
                        {"text": "Node", "type": "string"},
                        {"text": "Status", "type": "string"},
                        {"text": "Icon", "type": "string"},
                        {"text": "Updated At (UTC)", "type": "string"},
                        {"text": "Updated Epoch", "type": "number"},
                    ],
                    "rows": rows,
                    "type": "table",
                }
            )
        elif key == "analysis_summary":
            if summary_cache is None:
                summary_cache = await api_analysis_summary()
            events = summary_cache.get("events", [])
            rows = []
            for event in events[:50]:
                rows.append(
                    [
                        event.get("timestampIso"),
                        event.get("timestamp"),
                        event.get("node"),
                        event.get("alertStatus"),
                        event.get("comfortIndex"),
                        event.get("analysis"),
                        event.get("source"),
                    ]
                )
            frames.append(
                {
                    "refId": target.refId,
                    "columns": [
                        {"text": "Timestamp (UTC)", "type": "string"},
                        {"text": "Timestamp", "type": "number"},
                        {"text": "Node", "type": "string"},
                        {"text": "Alert Status", "type": "string"},
                        {"text": "Comfort Index", "type": "number"},
                        {"text": "Analysis", "type": "string"},
                        {"text": "Source", "type": "string"},
                    ],
                    "rows": rows,
                    "type": "table",
                }
            )
        elif key == "system_overview":
            if overview_cache is None:
                overview_cache = await api_system_overview()
            overview = overview_cache.get("overview", {})
            rows = [
                ["Total Nodes", overview.get("totalNodes")],
                ["Active Nodes", overview.get("activeNodes")],
                ["Inactive Nodes", overview.get("inactiveNodes")],
                ["Ingest Events", overview.get("ingestCount")],
                ["Analysis Events", overview.get("analysisCount")],
                ["Uptime Seconds", overview.get("uptimeSeconds")],
            ]
            frames.append(
                {
                    "refId": target.refId,
                    "columns": [
                        {"text": "Metric", "type": "string"},
                        {"text": "Value", "type": "number"},
                    ],
                    "rows": rows,
                    "type": "table",
                }
            )
        elif key == "alert_buckets":
            if overview_cache is None:
                overview_cache = await api_system_overview()
            buckets = overview_cache.get("overview", {}).get("alertBuckets", {})
            rows = [
                [
                    buckets.get("OK", 0),
                    buckets.get("Warning", 0),
                    buckets.get("Danger", 0),
                    buckets.get("Other", 0),
                ]
            ]
            frames.append(
                {
                    "refId": target.refId,
                    "columns": [
                        {"text": "OK", "type": "number"},
                        {"text": "Warning", "type": "number"},
                        {"text": "Danger", "type": "number"},
                        {"text": "Other", "type": "number"},
                    ],
                    "rows": rows,
                    "type": "table",
                }
            )

    return frames


@app.get("/dashboard/start")
async def dashboard_start() -> Dict[str, Any]:
    url = GRAFANA_URL
    try:
        await asyncio.to_thread(webbrowser.open, url, 2)
        message = "Grafana dashboard opened"
    except Exception as error:  # pragma: no cover - environment dependent
        logger.warning("Failed to open Grafana dashboard automatically: %s", error)
        message = "Grafana dashboard open attempt failed"
        return {"message": message, "url": url, "error": str(error)}
    return {"message": message, "url": url}


@app.get("/prometheus/fog_nodes")
async def prometheus_fog_targets() -> List[Dict[str, object]]:
    nodes = orchestrator.fog_nodes()
    return [
        {
            "targets": [f"{node}:8000"],
            "labels": {
                "node": node,
                "role": "fog",
            },
        }
        for node in nodes
    ]


@app.get("/metrics")
async def metrics() -> Response:
    data = await asyncio.to_thread(generate_latest)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

