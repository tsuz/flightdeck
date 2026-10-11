"""
Facilities tool consumer — mock ticketing, room bookings, and office status.
"""

import json
import os
import random
import string

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

AGENT_NAME = os.environ["AGENT_NAME"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

OFFICE_STATUS = {
    "sf": {"open": True, "advisories": [], "hours": "07:00-19:00 PT", "address": "123 Market St, San Francisco"},
    "nyc": {"open": True, "advisories": ["HVAC maintenance on Floor 4 from 18:00-22:00 ET"], "hours": "07:00-19:00 ET", "address": "500 Broadway, New York"},
    "london": {"open": True, "advisories": [], "hours": "07:00-19:00 GMT", "address": "10 Finsbury Square, London"},
    "tokyo": {"open": False, "advisories": ["Closed for Golden Week — reopens 2026-05-07"], "hours": "08:00-19:00 JST", "address": "1-1-1 Shibuya, Tokyo"},
}

ROOM_AVAILABILITY = {
    ("Yosemite", "2026-04-30", "14:00"): True,
    ("Yosemite", "2026-04-30", "10:00"): False,
    ("Tahoe-2", "2026-04-30", "14:00"): True,
    ("Tahoe-2", "2026-04-30", "10:00"): True,
}

ETA_BY_CATEGORY = {
    "hvac": "4-8 business hours",
    "lighting": "1-2 business days",
    "furniture": "3-5 business days",
    "plumbing": "2-4 business hours",
    "other": "1-3 business days",
}


def _make_ticket_id():
    suffix = "".join(random.choices(string.digits, k=5))
    return f"FAC-2026-{suffix}"


def submit_ticket(input_data):
    location = input_data.get("location", "")
    category = input_data.get("category", "other")
    description = input_data.get("description", "")

    if not location or not description:
        return {"error": "location and description are required"}

    ticket_id = _make_ticket_id()
    eta = ETA_BY_CATEGORY.get(category, "1-3 business days")

    return {
        "ticket_id": ticket_id,
        "status": "created",
        "location": location,
        "category": category,
        "description": description,
        "eta": eta,
        "assigned_to": "facilities-team",
    }


def book_room(input_data):
    room = input_data.get("room", "")
    date = input_data.get("date", "")
    start_time = input_data.get("start_time", "")
    duration = input_data.get("duration_minutes", 30)

    available = ROOM_AVAILABILITY.get((room, date, start_time), True)

    if available:
        return {
            "status": "booked",
            "room": room,
            "date": date,
            "start_time": start_time,
            "duration_minutes": duration,
            "confirmation": f"BK-{random.randint(10000, 99999)}",
        }
    return {
        "status": "unavailable",
        "room": room,
        "date": date,
        "start_time": start_time,
        "alternatives": [
            {"room": "Tahoe-2", "date": date, "start_time": start_time, "available": True},
            {"room": room, "date": date, "start_time": "15:00", "available": True},
        ],
    }


def check_office_status(input_data):
    office = input_data.get("office", "")
    if office in OFFICE_STATUS:
        return {"office": office, **OFFICE_STATUS[office]}
    return {"office": office, "error": "Unknown office code"}


TOOLS = {
    "submit_ticket": submit_ticket,
    "book_room": book_room,
    "check_office_status": check_office_status,
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
