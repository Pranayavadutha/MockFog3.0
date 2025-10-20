"""Interactive CLI for the dynamic fog-cloud orchestrator API."""

from __future__ import annotations

import json
import os
import sys
import webbrowser
from typing import Dict, Optional, Tuple

import requests

DEFAULT_API_URL = "http://127.0.0.1:8000"
API_URL = os.getenv("ORCHESTRATOR_URL", DEFAULT_API_URL).rstrip("/")
MENU_OPTIONS = {
    "1": "Configure application and create nodes",
    "2": "View node status",
    "3": "Ingest JSON data into a fog node",
    "4": "Send data between nodes",
    "5": "Run fog analysis",
    "6": "Show visualization dashboard",
    "7": "View stored data for a node",
    "8": "Stop containers and clear node data",
    "9": "Exit",
}


def _print_header() -> None:
    banner = "=" * 60
    print(banner)
    print("Dynamic Fog-Cloud Orchestrator CLI")
    print(f"API endpoint: {API_URL}")
    print(banner)


def _print_menu() -> None:
    print("\nSelect an option:")
    for key in sorted(MENU_OPTIONS):
        print(f"  {key}. {MENU_OPTIONS[key]}")


def _prompt_int(message: str) -> int:
    while True:
        raw = input(message).strip()
        try:
            value = int(raw)
            if value < 0:
                raise ValueError
            return value
        except ValueError:
            print("Please enter a non-negative integer value.")


def _prompt_non_empty(message: str) -> str:
    while True:
        value = input(message).strip()
        if value:
            return value
        print("Input cannot be empty.")


def _handle_response(response: requests.Response) -> Dict:
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        detail = ""
        try:
            detail = response.json().get("detail", "")
        except Exception:
            detail = response.text
        print(f"Request failed: {error}. {detail}")
        return {}
    try:
        return response.json()
    except ValueError:
        print("API returned an unexpected response format.")
        return {}


def _node_sort_tuple(name: str, detail: Dict | None = None) -> Tuple[int, int, str]:
    detail = detail or {}
    role = detail.get("role", "")
    if role == "fog" or name.startswith("f"):
        role_rank = 0
    elif role == "cloud" or name.startswith("c"):
        role_rank = 1
    else:
        role_rank = 2
    digits = "".join(ch for ch in name if ch.isdigit())
    try:
        index = int(digits)
    except ValueError:
        index = 0
    return (role_rank, index, name)


def configure_nodes() -> None:
    app_name = _prompt_non_empty("Enter application name: ")
    fog_nodes = _prompt_int("Enter number of Fog nodes: ")
    cloud_nodes = _prompt_int("Enter number of Cloud nodes: ")

    payload = {
        "application": app_name,
        "fog_nodes": fog_nodes,
        "cloud_nodes": cloud_nodes,
    }
    try:
        response = requests.post(f"{API_URL}/create", json=payload, timeout=30)
    except requests.RequestException as error:
        print(f"Network error while creating nodes: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    nodes_dict = data.get("nodes", {}) or {}
    fog_labels = [name for name in nodes_dict if name.startswith("f")]
    cloud_labels = [name for name in nodes_dict if name.startswith("c")]
    other_labels = [name for name in nodes_dict if name not in fog_labels + cloud_labels]
    if fog_labels:
        print(f"Fog nodes: {', '.join(fog_labels)}")
    if cloud_labels:
        print(f"Cloud nodes: {', '.join(cloud_labels)}")
    if other_labels:
        print(f"Other nodes: {', '.join(other_labels)}")
    if not nodes_dict:
        print(f"Configured application '{app_name}'. No nodes created.")
    else:
        print(f"Configured application '{app_name}'.")
    grafana_url = data.get("grafana_url")
    if grafana_url:
        print(f"Grafana dashboard available at: {grafana_url}")


def view_node_status() -> None:
    try:
        response = requests.get(f"{API_URL}/nodes/status", timeout=15)
    except requests.RequestException as error:
        print(f"Network error while retrieving status: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    nodes = data.get("nodes", {})
    if not nodes:
        print("No nodes currently managed.")
        return

    ordered_items = sorted(nodes.items(), key=lambda item: _node_sort_tuple(item[0], item[1]))
    for name, detail in ordered_items:
        detail = detail or {}
        role = detail.get("role", "unknown")
        status = detail.get("status", "unknown")
        ip_address = detail.get("ip", "-")
        print(f"- {name}: tier={role} status={status} ip={ip_address}")


def send_transfer() -> None:
    source = _prompt_non_empty("Source node label: ")
    destination = _prompt_non_empty("Destination node label: ")
    body = {
        "source": source,
        "destination": destination,
    }
    try:
        response = requests.post(f"{API_URL}/transfer", json=body, timeout=15)
    except requests.RequestException as error:
        print(f"Network error while sending transfer: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    latency = data.get("latency_ms", 0)
    print(f"Transfer completed from {source} to {destination} with latency {latency} ms")

    metrics = data.get("metrics", {}) or {}
    if metrics:
        print("Transferred metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

    analysis = data.get("analysis")
    if analysis:
        print(f"Analysis: {analysis}")
    alert_status = data.get("alertStatus")
    if alert_status:
        print(f"Alert status: {alert_status}")


def ingest_fog_data() -> None:
    node = _prompt_non_empty("Fog node label (e.g., f1): ")
    raw_payload = _prompt_non_empty("Enter metrics as JSON (e.g., {\"temperature\":85,\"humidity\":60,\"air_quality\":90,\"power\":45}): ")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as error:
        print(f"Invalid JSON payload: {error}")
        return
    if not isinstance(payload, dict):
        print("Metrics payload must be a JSON object.")
        return

    body = {
        "node": node,
        "data": payload,
    }
    try:
        response = requests.post(f"{API_URL}/ingest", json=body, timeout=15)
    except requests.RequestException as error:
        print(f"Network error while ingesting data: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    analysis = data.get("analysis", "No analysis available")
    print(f"Ingestion accepted. {node} analysis: {analysis}")
    metrics = data.get("metrics", {})
    if isinstance(metrics, dict) and metrics:
        print("Latest metrics:")
        for key, value in metrics.items():
            if key == "extra":
                if value:
                    print("  extra metrics:")
                    for extra_key, extra_value in value.items():
                        print(f"    {extra_key}: {extra_value}")
                continue
            print(f"  {key}: {value}")
    print("View the Grafana dashboard for live updates.")


def run_fog_analysis() -> None:
    try:
        response = requests.get(f"{API_URL}/analyze", timeout=15)
    except requests.RequestException as error:
        print(f"Network error while running analysis: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    results = data.get("results", {})
    if not results:
        print("No fog analysis results available.")
        return

    for name, message in sorted(results.items(), key=lambda item: _node_sort_tuple(item[0])):
        print(f"- {name}: {message}")
    print("Grafana dashboard updated with the latest decisions.")


def view_node_data() -> None:
    node = _prompt_non_empty("Node label to inspect (e.g., f1): ")
    try:
        response = requests.get(f"{API_URL}/nodes/{node}/data", timeout=15)
    except requests.RequestException as error:
        print(f"Network error while retrieving stored data: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    print(f"Stored metrics for {node}:")
    metrics = data.get("metrics", {}) or {}
    if metrics:
        for key, value in metrics.items():
            print(f"  {key}: {value}")
    else:
        print("  No primary metrics stored.")

    extra = data.get("extra", {}) or {}
    if extra:
        print("  Extra metrics:")
        for key, value in extra.items():
            print(f"    {key}: {value}")

    analysis = data.get("analysis")
    alert_status = data.get("alertStatus")
    updated_iso = data.get("updatedAtIso") or data.get("updatedAt")
    if analysis:
        print(f"Analysis: {analysis}")
    if alert_status:
        print(f"Alert status: {alert_status}")
    if updated_iso:
        print(f"Last updated: {updated_iso}")

    history = data.get("history", []) or []
    if history:
        print("Recent events:")
        for entry in history[-10:]:
            stamp = entry.get("timestampIso") or entry.get("timestamp")
            source = entry.get("source", "event")
            analysis_msg = entry.get("analysis", "")
            alert = entry.get("alertStatus", "")
            details = entry.get("comfortIndex")
            detail_str = f" comfortIndex={details}" if details is not None else ""
            print(f"  [{stamp}] {source}: {analysis_msg} (alert: {alert}{detail_str})")


def cleanup_environment() -> None:
    try:
        response = requests.post(f"{API_URL}/cleanup", timeout=30)
    except requests.RequestException as error:
        print(f"Network error while stopping containers: {error}")
        return

    data = _handle_response(response)
    if not data:
        return

    message = data.get("message") or "Cleanup completed."
    print(message)
    print(
        "Containers stopped: {stopped}, removed: {removed}".format(
            stopped=data.get("stopped", 0),
            removed=data.get("removed", 0),
        )
    )
    print(
        "Cleared nodes: {nodes}, cleared summary events: {events}".format(
            nodes=data.get("clearedNodes", 0),
            events=data.get("clearedSummaries", 0),
        )
    )
    errors = data.get("errors", 0)
    if errors:
        print(f"Encountered {errors} errors while stopping containers. Check server logs for details.")


def show_visualization() -> None:
    try:
        response = requests.get(f"{API_URL}/dashboard/start", timeout=10)
    except requests.RequestException as error:
        print(f"Unable to reach orchestrator API: {error}")
        print("Open Grafana manually at http://localhost:3000/d/dynamic-fog/mockfog-dynamic-fog?orgId=1")
        return

    data = _handle_response(response)
    url = data.get("url") if data else None
    if not url:
        url = "http://localhost:3000"
    message = data.get("message") if data else None
    if message:
        print(message)
    print(f"Grafana dashboard: {url}")
    try:
        webbrowser.open(url, new=2)
        print("Opened Grafana in your default browser.")
    except Exception as error:
        print(f"Could not open browser automatically: {error}")
        print("Please open the link manually.")


def main() -> None:
    _print_header()
    while True:
        _print_menu()
        choice = input("Enter choice: ").strip()
        if choice == "1":
            configure_nodes()
        elif choice == "2":
            view_node_status()
        elif choice == "3":
            ingest_fog_data()
        elif choice == "4":
            send_transfer()
        elif choice == "5":
            run_fog_analysis()
        elif choice == "6":
            show_visualization()
        elif choice == "7":
            view_node_data()
        elif choice == "8":
            cleanup_environment()
        elif choice == "9":
            print("Exiting orchestrator CLI. Goodbye!")
            break
        else:
            print("Invalid choice. Please select one of the listed options.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
