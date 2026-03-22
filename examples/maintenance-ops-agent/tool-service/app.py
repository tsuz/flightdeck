"""
Maintenance ops tool consumer — mock sensor data, maintenance history,
parts inventory, CMMS work orders, and technician alerts.
Uses the FlightDeck SDK.
"""

import json
import os
import uuid
from datetime import datetime, timezone

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

AGENT_NAME = os.environ["AGENT_NAME"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# ── Mock Data ─────────────────────────────────────────────────────────────────

SENSORS = {
    "TEMP-001": {
        "sensor_id": "TEMP-001",
        "type": "temperature",
        "unit": "°C",
        "asset_id": "PUMP-101",
        "asset_name": "Coolant Circulation Pump #1",
        "location": "Building A, Floor 2, Bay 7",
        "threshold_warning": 85,
        "threshold_critical": 95,
        "current_reading": 97.3,
        "status": "CRITICAL",
        "recent_readings": [
            {"timestamp": "2026-03-22T08:00:00Z", "value": 72.1},
            {"timestamp": "2026-03-22T09:00:00Z", "value": 78.4},
            {"timestamp": "2026-03-22T10:00:00Z", "value": 84.2},
            {"timestamp": "2026-03-22T10:30:00Z", "value": 89.7},
            {"timestamp": "2026-03-22T10:45:00Z", "value": 93.1},
            {"timestamp": "2026-03-22T11:00:00Z", "value": 97.3},
        ],
    },
    "VIB-003": {
        "sensor_id": "VIB-003",
        "type": "vibration",
        "unit": "mm/s RMS",
        "asset_id": "COMPRESSOR-A3",
        "asset_name": "Air Compressor Unit A3",
        "location": "Building B, Compressor Room",
        "threshold_warning": 7.0,
        "threshold_critical": 11.0,
        "current_reading": 9.8,
        "status": "WARNING",
        "recent_readings": [
            {"timestamp": "2026-03-22T06:00:00Z", "value": 4.2},
            {"timestamp": "2026-03-22T07:00:00Z", "value": 4.5},
            {"timestamp": "2026-03-22T08:00:00Z", "value": 5.8},
            {"timestamp": "2026-03-22T09:00:00Z", "value": 7.2},
            {"timestamp": "2026-03-22T10:00:00Z", "value": 8.9},
            {"timestamp": "2026-03-22T11:00:00Z", "value": 9.8},
        ],
    },
    "PRESS-002": {
        "sensor_id": "PRESS-002",
        "type": "pressure",
        "unit": "bar",
        "asset_id": "PUMP-101",
        "asset_name": "Coolant Circulation Pump #1",
        "location": "Building A, Floor 2, Bay 7",
        "threshold_warning": 3.0,
        "threshold_critical": 2.0,
        "current_reading": 2.4,
        "status": "WARNING",
        "recent_readings": [
            {"timestamp": "2026-03-22T08:00:00Z", "value": 4.8},
            {"timestamp": "2026-03-22T09:00:00Z", "value": 4.1},
            {"timestamp": "2026-03-22T10:00:00Z", "value": 3.5},
            {"timestamp": "2026-03-22T10:30:00Z", "value": 3.0},
            {"timestamp": "2026-03-22T10:45:00Z", "value": 2.7},
            {"timestamp": "2026-03-22T11:00:00Z", "value": 2.4},
        ],
    },
}

MAINTENANCE_HISTORY = {
    "PUMP-101": {
        "asset_id": "PUMP-101",
        "asset_name": "Coolant Circulation Pump #1",
        "commissioned": "2019-06-15",
        "assigned_technician": "TECH-001",
        "technician_name": "Mike Torres",
        "technician_email": "m.torres@plant.local",
        "records": [
            {"date": "2026-01-10", "type": "preventive", "description": "Quarterly inspection — all parameters normal", "parts": [], "hours": 2, "cost": 320},
            {"date": "2025-10-05", "type": "corrective", "description": "Replaced mechanical seal — minor leak detected", "parts": ["SEAL-42X62"], "hours": 4, "cost": 890},
            {"date": "2025-06-20", "type": "corrective", "description": "Replaced bearing — elevated vibration", "parts": ["BRG-6205"], "hours": 6, "cost": 1250},
            {"date": "2025-03-15", "type": "preventive", "description": "Annual overhaul — replaced bearing and seal", "parts": ["BRG-6205", "SEAL-42X62"], "hours": 8, "cost": 2100},
            {"date": "2024-09-10", "type": "corrective", "description": "Bearing failure — unplanned downtime 14 hours", "parts": ["BRG-6205"], "hours": 10, "cost": 4500},
            {"date": "2024-06-01", "type": "corrective", "description": "Replaced bearing — noise complaint from operator", "parts": ["BRG-6205"], "hours": 5, "cost": 1100},
        ],
        "total_downtime_hours_12mo": 22,
        "total_cost_12mo": 4560,
    },
    "COMPRESSOR-A3": {
        "asset_id": "COMPRESSOR-A3",
        "asset_name": "Air Compressor Unit A3",
        "commissioned": "2021-02-01",
        "assigned_technician": "TECH-002",
        "technician_name": "Sara Chen",
        "technician_email": "s.chen@plant.local",
        "records": [
            {"date": "2026-02-20", "type": "preventive", "description": "Quarterly inspection — normal", "parts": [], "hours": 3, "cost": 480},
            {"date": "2025-11-15", "type": "preventive", "description": "Belt replacement — scheduled", "parts": ["BELT-V68"], "hours": 2, "cost": 340},
            {"date": "2025-08-10", "type": "corrective", "description": "Intake filter clogged — reduced output", "parts": ["FILTER-AF200"], "hours": 1, "cost": 180},
        ],
        "total_downtime_hours_12mo": 6,
        "total_cost_12mo": 1000,
    },
}

PARTS_INVENTORY = {
    "BRG-6205": {
        "part_number": "BRG-6205",
        "description": "Deep groove ball bearing 6205-2RS",
        "compatible_assets": ["PUMP-101", "PUMP-102", "PUMP-103"],
        "in_stock": 3,
        "warehouse": "Warehouse A, Shelf B-12",
        "unit_cost": 45.00,
        "lead_time_days": 0,
        "status": "in_stock",
    },
    "SEAL-42X62": {
        "part_number": "SEAL-42X62",
        "description": "Mechanical shaft seal 42x62x8mm",
        "compatible_assets": ["PUMP-101", "PUMP-102"],
        "in_stock": 1,
        "warehouse": "Warehouse A, Shelf C-04",
        "unit_cost": 128.00,
        "lead_time_days": 0,
        "status": "in_stock",
    },
    "IMPELLER-P1": {
        "part_number": "IMPELLER-P1",
        "description": "Pump impeller — cast iron, 180mm",
        "compatible_assets": ["PUMP-101"],
        "in_stock": 0,
        "warehouse": "N/A",
        "unit_cost": 520.00,
        "lead_time_days": 14,
        "status": "out_of_stock",
        "supplier": "FlowTech Parts Inc.",
    },
    "BELT-V68": {
        "part_number": "BELT-V68",
        "description": "V-belt 68 inch industrial grade",
        "compatible_assets": ["COMPRESSOR-A3", "COMPRESSOR-A4"],
        "in_stock": 5,
        "warehouse": "Warehouse B, Shelf A-01",
        "unit_cost": 32.00,
        "lead_time_days": 0,
        "status": "in_stock",
    },
    "FILTER-AF200": {
        "part_number": "FILTER-AF200",
        "description": "Air intake filter 200mm",
        "compatible_assets": ["COMPRESSOR-A3", "COMPRESSOR-A4"],
        "in_stock": 8,
        "warehouse": "Warehouse B, Shelf A-03",
        "unit_cost": 22.00,
        "lead_time_days": 0,
        "status": "in_stock",
    },
}

# In-memory work order store
WORK_ORDERS = {}


# ── Tool implementations ──────────────────────────────────────────────────────


def get_sensor_details(input_data):
    sensor_id = input_data.get("sensor_id", "")
    sensor = SENSORS.get(sensor_id)
    if not sensor:
        available = list(SENSORS.keys())
        return {"error": f"Sensor '{sensor_id}' not found. Available: {available}"}
    return sensor


def query_maintenance_history(input_data):
    asset_id = input_data.get("asset_id", "")
    history = MAINTENANCE_HISTORY.get(asset_id)
    if not history:
        return {"error": f"No maintenance history for asset '{asset_id}'"}

    last_months = input_data.get("last_months", 12)
    # Return full history (mock doesn't filter by date)
    return history


def check_parts_inventory(input_data):
    part_number = input_data.get("part_number", "")
    asset_id = input_data.get("asset_id", "")

    if part_number:
        part = PARTS_INVENTORY.get(part_number)
        if not part:
            return {"error": f"Part '{part_number}' not found"}
        return part

    if asset_id:
        compatible = [p for p in PARTS_INVENTORY.values() if asset_id in p["compatible_assets"]]
        return {"asset_id": asset_id, "compatible_parts": compatible, "count": len(compatible)}

    return {"error": "Provide either 'part_number' or 'asset_id'"}


def create_work_order(input_data):
    wo_id = f"WO-{uuid.uuid4().hex[:8].upper()}"
    asset_id = input_data.get("asset_id", "")

    # Look up technician from maintenance history
    history = MAINTENANCE_HISTORY.get(asset_id, {})
    technician_id = history.get("assigned_technician", "TECH-UNASSIGNED")
    technician_name = history.get("technician_name", "Unassigned")

    work_order = {
        "work_order_id": wo_id,
        "status": "DRAFT",
        "asset_id": asset_id,
        "asset_name": history.get("asset_name", asset_id),
        "priority": input_data.get("priority", "medium"),
        "work_type": input_data.get("work_type", "corrective"),
        "description": input_data.get("description", ""),
        "parts_needed": input_data.get("parts_needed", []),
        "estimated_hours": input_data.get("estimated_hours", 0),
        "assigned_technician": technician_id,
        "technician_name": technician_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "maintenance-ops-agent",
    }

    WORK_ORDERS[wo_id] = work_order
    print(f"[CMMS] Created draft work order {wo_id} for {asset_id} ({input_data.get('priority', 'medium')})")

    return work_order


def approve_work_order(input_data):
    wo_id = input_data.get("work_order_id", "")
    work_order = WORK_ORDERS.get(wo_id)

    if not work_order:
        return {"error": f"Work order '{wo_id}' not found"}

    if work_order["status"] != "DRAFT":
        return {"error": f"Work order '{wo_id}' is already {work_order['status']}"}

    work_order["status"] = "APPROVED"
    work_order["approved_at"] = datetime.now(timezone.utc).isoformat()
    work_order["approved_by"] = "operator"

    print(f"[CMMS] Work order {wo_id} APPROVED")
    return {"work_order_id": wo_id, "status": "APPROVED", "message": f"Work order {wo_id} has been approved and submitted to the CMMS."}


def send_technician_alert(input_data):
    wo_id = input_data.get("work_order_id", "")
    technician_id = input_data.get("technician_id", "")
    urgency = input_data.get("urgency", "next_shift")
    message = input_data.get("message", "")

    work_order = WORK_ORDERS.get(wo_id)
    if not work_order:
        return {"error": f"Work order '{wo_id}' not found"}

    if work_order["status"] != "APPROVED":
        return {"error": f"Work order '{wo_id}' must be APPROVED before sending alerts (current: {work_order['status']})"}

    # Look up technician email from maintenance history
    tech_email = "unknown@plant.local"
    tech_name = technician_id
    for history in MAINTENANCE_HISTORY.values():
        if history.get("assigned_technician") == technician_id:
            tech_email = history.get("technician_email", tech_email)
            tech_name = history.get("technician_name", tech_name)
            break

    alert = {
        "alert_id": f"ALERT-{uuid.uuid4().hex[:6].upper()}",
        "work_order_id": wo_id,
        "sent_to": tech_email,
        "technician_name": tech_name,
        "technician_id": technician_id,
        "urgency": urgency,
        "subject": f"[{urgency.upper()}] Work Order {wo_id} — {work_order['asset_name']}",
        "body": f"Priority: {work_order['priority'].upper()}\n"
                f"Asset: {work_order['asset_name']} ({work_order['asset_id']})\n"
                f"Type: {work_order['work_type']}\n"
                f"Description: {work_order['description']}\n"
                f"Parts: {', '.join(work_order['parts_needed']) if work_order['parts_needed'] else 'None listed'}\n"
                f"Est. Hours: {work_order['estimated_hours']}\n"
                f"\n{message}" if message else "",
        "status": "sent",
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"[EMAIL] Alert sent to {tech_name} ({tech_email}) for {wo_id} [{urgency}]")
    return alert


TOOLS = {
    "get_sensor_details": get_sensor_details,
    "query_maintenance_history": query_maintenance_history,
    "check_parts_inventory": check_parts_inventory,
    "create_work_order": create_work_order,
    "approve_work_order": approve_work_order,
    "send_technician_alert": send_technician_alert,
}


def process(key, value, ctx):
    request = json.loads(value)
    tool_name = request.get("name", "")

    handler = TOOLS.get(tool_name)
    if handler is None:
        ctx.error(f"Unknown tool: {tool_name}")
        return

    try:
        result = handler(request.get("input", {}))
        ctx.success(result)
    except Exception as e:
        ctx.error(str(e))


if __name__ == "__main__":
    runner = ToolConsumerRunner(
        ToolConsumerConfig(
            agent_name=AGENT_NAME,
            brokers=KAFKA_BOOTSTRAP,
            process_fn=process,
        )
    )
    runner.start()
