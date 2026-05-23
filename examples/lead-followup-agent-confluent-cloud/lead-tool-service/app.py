"""
Lead CRM tool consumer — rewritten using the flightdeck SDK.

Compare with app.py to see how the SDK eliminates all Kafka boilerplate.
The developer only writes tool functions and a dispatch handler.
"""

import json
import os
import time
from copy import deepcopy
from datetime import datetime, timezone

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

# ── Mock CRM Database ────────────────────────────────────────────────────────

LEADS = {
    "lead-001": {
        "lead_id": "lead-001",
        "name": "Jane Chen",
        "email": "jane.chen@acmecorp.com",
        "company": "Acme Corp",
        "industry": "ecommerce",
        "deal_value": 45000,
        "status": "dormant",
        "last_contact": "2025-08-12",
        "created": "2025-03-01",
        "activities": [
            {"date": "2025-03-01", "type": "note", "summary": "Inbound lead from website demo request"},
        ],
    },
    "lead-002": {
        "lead_id": "lead-002",
        "name": "Marcus Johnson",
        "email": "m.johnson@brighthealth.io",
        "company": "BrightHealth",
        "industry": "healthcare",
        "deal_value": 120000,
        "status": "dormant",
        "last_contact": "2025-06-20",
        "created": "2025-02-14",
        "activities": [],
    },
}


# ── Tool implementations ────────────────────────────────────────────────────


def search_leads(input_data):
    results = []
    status = input_data.get("status")
    industry = input_data.get("industry")
    limit = input_data.get("limit", 10)

    for lead in LEADS.values():
        if status and lead["status"] != status:
            continue
        if industry and lead["industry"] != industry:
            continue
        results.append({
            "lead_id": lead["lead_id"],
            "name": lead["name"],
            "company": lead["company"],
            "status": lead["status"],
        })

    return {"leads": results[:limit], "count": len(results[:limit])}


def get_lead_details(input_data):
    lead = LEADS.get(input_data.get("lead_id", ""))
    if not lead:
        return {"error": f"Lead '{input_data.get('lead_id')}' not found"}
    return deepcopy(lead)


def update_lead_status(input_data):
    lead = LEADS.get(input_data.get("lead_id", ""))
    if not lead:
        return {"error": f"Lead '{input_data.get('lead_id')}' not found"}
    old_status = lead["status"]
    lead["status"] = input_data.get("status", "")
    return {"lead_id": lead["lead_id"], "old_status": old_status, "new_status": lead["status"]}


def log_activity(input_data):
    lead = LEADS.get(input_data.get("lead_id", ""))
    if not lead:
        return {"error": f"Lead '{input_data.get('lead_id')}' not found"}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    activity = {"date": today, "type": input_data.get("type", "note"), "summary": input_data.get("summary", "")}
    lead["activities"].append(activity)
    lead["last_contact"] = today
    return {"lead_id": lead["lead_id"], "activity": activity}


def draft_followup_email(input_data):
    lead = LEADS.get(input_data.get("lead_id", ""))
    if not lead:
        return {"error": f"Lead '{input_data.get('lead_id')}' not found"}
    return {
        "to": lead["email"],
        "name": lead["name"],
        "company": lead["company"],
        "reason": input_data.get("reason", "general follow-up"),
    }


TOOLS = {
    "search_leads": search_leads,
    "get_lead_details": get_lead_details,
    "update_lead_status": update_lead_status,
    "log_activity": log_activity,
    "draft_followup_email": draft_followup_email,
}


# ── Process function (the only thing the SDK needs) ─────────────────────────


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


# ── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    runner = ToolConsumerRunner(
        ToolConsumerConfig(
            agent_name=os.environ["AGENT_NAME"],
            brokers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            process_fn=process,
        )
    )
    runner.start()
