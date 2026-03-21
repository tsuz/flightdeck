"""
Lead CRM tool consumer — reads tool-use requests from Kafka,
executes them against an in-memory CRM database, and produces results.

This is a demo with mock data. In production, replace the in-memory
store with calls to your actual CRM (Salesforce, HubSpot, etc.).

Env vars:
    KAFKA_BOOTSTRAP_SERVERS  — Kafka broker
    AGENT_NAME               — agent name (for topic prefix)
"""

import json
import os
import signal
import time
from copy import deepcopy
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
AGENT_NAME = os.environ["AGENT_NAME"]

INPUT_TOPIC = f"{AGENT_NAME}-tool-use"
OUTPUT_TOPIC = f"{AGENT_NAME}-tool-use-result"

# ── Kafka clients ─────────────────────────────────────────────────────────────

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "tool-execution-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
})

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
            {"date": "2025-03-05", "type": "call", "summary": "Intro call — interested in enterprise plan, needs budget approval"},
            {"date": "2025-04-10", "type": "email", "summary": "Sent pricing proposal for 50 seats"},
            {"date": "2025-05-15", "type": "call", "summary": "Follow-up — budget delayed to Q3, asked to check back later"},
            {"date": "2025-08-12", "type": "email", "summary": "Q3 check-in email — no response"},
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
        "activities": [
            {"date": "2025-02-14", "type": "note", "summary": "Referred by existing customer (MedTech Solutions)"},
            {"date": "2025-02-20", "type": "meeting", "summary": "Discovery meeting — needs HIPAA-compliant solution, 200+ users"},
            {"date": "2025-03-15", "type": "email", "summary": "Sent technical architecture doc and compliance certifications"},
            {"date": "2025-04-02", "type": "call", "summary": "Technical review with their CTO — positive, pending legal review"},
            {"date": "2025-05-10", "type": "email", "summary": "Legal review taking longer than expected, will follow up in June"},
            {"date": "2025-06-20", "type": "call", "summary": "Left voicemail — legal team still reviewing, contact went on vacation"},
        ],
    },
    "lead-003": {
        "lead_id": "lead-003",
        "name": "Sarah Kim",
        "email": "sarah.kim@fintechly.com",
        "company": "Fintechly",
        "industry": "fintech",
        "deal_value": 75000,
        "status": "dormant",
        "last_contact": "2025-07-03",
        "created": "2025-05-20",
        "activities": [
            {"date": "2025-05-20", "type": "note", "summary": "Met at FinTech Summit conference booth"},
            {"date": "2025-05-28", "type": "email", "summary": "Post-conference follow-up with product deck"},
            {"date": "2025-06-10", "type": "meeting", "summary": "Product demo — very engaged, wants to pilot with 10 users"},
            {"date": "2025-06-25", "type": "email", "summary": "Sent pilot proposal and onboarding timeline"},
            {"date": "2025-07-03", "type": "call", "summary": "Company pivoting strategy, all new vendor evaluations paused"},
        ],
    },
    "lead-004": {
        "lead_id": "lead-004",
        "name": "David Park",
        "email": "dpark@retailnext.co",
        "company": "RetailNext",
        "industry": "ecommerce",
        "deal_value": 30000,
        "status": "contacted",
        "last_contact": "2026-03-10",
        "created": "2026-01-15",
        "activities": [
            {"date": "2026-01-15", "type": "note", "summary": "Inbound from Google Ads campaign"},
            {"date": "2026-01-20", "type": "call", "summary": "Quick intro — small team, interested in starter plan"},
            {"date": "2026-03-10", "type": "email", "summary": "Sent updated pricing for Q1 promotion"},
        ],
    },
    "lead-005": {
        "lead_id": "lead-005",
        "name": "Lisa Nguyen",
        "email": "lisa@cloudbridge.dev",
        "company": "CloudBridge",
        "industry": "saas",
        "deal_value": 95000,
        "status": "dormant",
        "last_contact": "2025-09-15",
        "created": "2025-06-01",
        "activities": [
            {"date": "2025-06-01", "type": "note", "summary": "Outbound — identified as high-potential account from target list"},
            {"date": "2025-06-15", "type": "email", "summary": "Cold outreach — personalized based on their recent Series B"},
            {"date": "2025-07-01", "type": "call", "summary": "Connected — interested but evaluating 3 vendors"},
            {"date": "2025-07-20", "type": "meeting", "summary": "Full demo with engineering and product leads — strong fit"},
            {"date": "2025-08-10", "type": "email", "summary": "Sent competitive comparison doc they requested"},
            {"date": "2025-09-01", "type": "call", "summary": "Went with competitor — but said to check back in 6 months if not satisfied"},
            {"date": "2025-09-15", "type": "note", "summary": "Marked as dormant — revisit in March 2026"},
        ],
    },
}


# ── Tool implementations ──────────────────────────────────────────────────────


def search_leads(input_data):
    results = []
    last_before = input_data.get("last_contact_before")
    status = input_data.get("status")
    industry = input_data.get("industry")
    company = input_data.get("company", "").lower()
    limit = input_data.get("limit", 10)

    for lead in LEADS.values():
        if status and lead["status"] != status:
            continue
        if industry and lead["industry"] != industry:
            continue
        if company and company not in lead["company"].lower():
            continue
        if last_before and lead["last_contact"] > last_before:
            continue
        results.append({
            "lead_id": lead["lead_id"],
            "name": lead["name"],
            "company": lead["company"],
            "industry": lead["industry"],
            "deal_value": lead["deal_value"],
            "status": lead["status"],
            "last_contact": lead["last_contact"],
        })

    results = results[:limit]
    return {"leads": results, "count": len(results)}


def get_lead_details(input_data):
    lead_id = input_data.get("lead_id", "")
    lead = LEADS.get(lead_id)
    if not lead:
        return {"error": f"Lead '{lead_id}' not found"}
    return deepcopy(lead)


def update_lead_status(input_data):
    lead_id = input_data.get("lead_id", "")
    new_status = input_data.get("status", "")
    lead = LEADS.get(lead_id)
    if not lead:
        return {"error": f"Lead '{lead_id}' not found"}

    old_status = lead["status"]
    lead["status"] = new_status
    print(f"[CRM] Lead {lead_id} status: {old_status} → {new_status}")
    return {"lead_id": lead_id, "old_status": old_status, "new_status": new_status}


def log_activity(input_data):
    lead_id = input_data.get("lead_id", "")
    activity_type = input_data.get("type", "note")
    summary = input_data.get("summary", "")
    lead = LEADS.get(lead_id)
    if not lead:
        return {"error": f"Lead '{lead_id}' not found"}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    activity = {"date": today, "type": activity_type, "summary": summary}
    lead["activities"].append(activity)
    lead["last_contact"] = today
    print(f"[CRM] Lead {lead_id} activity: [{activity_type}] {summary}")
    return {"lead_id": lead_id, "activity": activity, "total_activities": len(lead["activities"])}


def draft_followup_email(input_data):
    lead_id = input_data.get("lead_id", "")
    reason = input_data.get("reason", "general follow-up")
    tone = input_data.get("tone", "professional")
    lead = LEADS.get(lead_id)
    if not lead:
        return {"error": f"Lead '{lead_id}' not found"}

    last_activity = lead["activities"][-1] if lead["activities"] else None
    last_context = last_activity["summary"] if last_activity else "no prior activity"

    # Return structured data for the LLM to craft the actual email
    return {
        "lead_id": lead_id,
        "to": lead["email"],
        "name": lead["name"],
        "company": lead["company"],
        "last_contact": lead["last_contact"],
        "last_activity_summary": last_context,
        "reason": reason,
        "tone": tone,
        "deal_value": lead["deal_value"],
        "suggestion": f"Reference the previous interaction ({last_context}) and introduce the new reason for reaching out ({reason}). Keep it {tone}.",
    }


# ── Tool registry ─────────────────────────────────────────────────────────────

TOOLS = {
    "search_leads": search_leads,
    "get_lead_details": get_lead_details,
    "update_lead_status": update_lead_status,
    "log_activity": log_activity,
    "draft_followup_email": draft_followup_email,
}

# ── Main consumer loop ────────────────────────────────────────────────────────

running = True


def shutdown(signum, frame):
    global running
    print("\nShutdown signal received")
    running = False


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


def process_record(msg):
    session_id = msg.key().decode() if msg.key() else "unknown"
    item = json.loads(msg.value())

    tool_name = item.get("name", "")
    tool_use_id = item.get("tool_use_id", "")
    tool_input = item.get("input", {})
    total_tools = item.get("total_tools", 1)

    print(f"[{session_id}] Executing tool: {tool_name} (tool_use_id={tool_use_id})")

    handler = TOOLS.get(tool_name)
    start_ms = time.time() * 1000

    if handler is None:
        result = {"error": f"Unknown tool: {tool_name}"}
        status = "error"
    else:
        try:
            result = handler(tool_input)
            status = "success"
        except Exception as e:
            result = {"error": str(e)}
            status = "error"

    latency_ms = int(time.time() * 1000 - start_ms)

    tool_result = {
        "session_id": session_id,
        "tool_use_id": tool_use_id,
        "name": tool_name,
        "result": result,
        "latency_ms": latency_ms,
        "status": status,
        "total_tools": total_tools,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print(f"[{session_id}] {tool_name} → {status} ({latency_ms}ms)")

    producer.produce(
        OUTPUT_TOPIC,
        key=session_id.encode(),
        value=json.dumps(tool_result).encode(),
    )
    producer.flush()


def main():
    print(f"Lead Tool Consumer starting")
    print(f"  Kafka:       {KAFKA_BOOTSTRAP}")
    print(f"  Agent:       {AGENT_NAME}")
    print(f"  Input topic: {INPUT_TOPIC}")
    print(f"  Output topic:{OUTPUT_TOPIC}")
    print(f"  Tools:       {list(TOOLS.keys())}")
    print(f"  Mock leads:  {len(LEADS)}")

    consumer.subscribe([INPUT_TOPIC])
    print(f"Subscribed — waiting for tool requests...")

    while running:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            process_record(msg)
            consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"Failed to process record: {e}")

    print("Shutting down...")
    consumer.close()
    producer.flush()


if __name__ == "__main__":
    main()
