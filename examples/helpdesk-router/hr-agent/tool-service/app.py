"""
HR tool consumer — mock PTO, policy, and benefits data.
"""

import json
import os

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

AGENT_NAME = os.environ["AGENT_NAME"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

PTO = {
    "jsmith": {"vacation_days_remaining": 11.5, "sick_days_remaining": 8, "pending_requests": [
        {"start": "2026-06-15", "end": "2026-06-19", "days": 5, "status": "approved"},
    ], "accrual_rate": "1.5 days/month"},
    "achen": {"vacation_days_remaining": 17, "sick_days_remaining": 8, "pending_requests": [], "accrual_rate": "1.25 days/month"},
    "rpatel": {"vacation_days_remaining": 4, "sick_days_remaining": 6, "pending_requests": [
        {"start": "2026-05-12", "end": "2026-05-13", "days": 2, "status": "pending_manager_approval"},
    ], "accrual_rate": "1.5 days/month"},
}

POLICIES = {
    "pto": {
        "name": "Paid Time Off Policy (POL-HR-001)",
        "summary": "Full-time employees accrue 1.5 vacation days per month (18/year). Sick time is 8 days/year, refreshed each January 1. PTO requests must be submitted 2+ weeks in advance for any window 5 days or longer. Carryover capped at 10 vacation days; sick time does not carry over.",
        "last_updated": "2026-01-15",
    },
    "parental_leave": {
        "name": "Parental Leave Policy (POL-HR-007)",
        "summary": "All new parents (birth, adoption, or foster) receive 16 weeks of fully paid leave. Must be taken within 12 months of the qualifying event. Eligible after 6 months of full-time employment.",
        "last_updated": "2025-09-01",
    },
    "remote_work": {
        "name": "Remote Work Policy (POL-HR-012)",
        "summary": "Hybrid by default — 2 days/week in-office for employees within 50 miles of an office. Fully remote available with manager + VP approval. Coworking space stipends available for fully remote employees ($300/month).",
        "last_updated": "2025-11-20",
    },
    "expense_reimbursement": {
        "name": "Expense Reimbursement Policy (POL-FIN-002)",
        "summary": "Owned by Finance, summarized here for HR context. See Finance specialist for details. Highlights: home office stipend $1500/year; coworking $300/month for fully remote employees; conference attendance with manager approval.",
        "last_updated": "2026-02-10",
    },
    "code_of_conduct": {
        "name": "Code of Conduct (POL-HR-000)",
        "summary": "Defines expected behavior, anti-harassment policy, and reporting channels. All employees must acknowledge annually.",
        "last_updated": "2026-01-01",
    },
}

BENEFITS = {
    "jsmith": {"health": "BlueCross PPO — Family", "dental": "Delta Dental Premium", "vision": "VSP Standard", "401k": {"contribution_pct": 8, "match_pct": 4, "vested_pct": 100}},
    "achen": {"health": "BlueCross HMO — Single", "dental": "Delta Dental Standard", "vision": None, "401k": {"contribution_pct": 6, "match_pct": 4, "vested_pct": 50}},
    "rpatel": {"health": "BlueCross PPO — Single+Spouse", "dental": "Delta Dental Premium", "vision": "VSP Premium", "401k": {"contribution_pct": 12, "match_pct": 4, "vested_pct": 100}},
}


def get_pto_balance(input_data):
    username = input_data.get("username", "")
    if username in PTO:
        return {"username": username, **PTO[username]}
    return {"username": username, "error": "No PTO record on file"}


def lookup_policy(input_data):
    topic = input_data.get("topic", "")
    if topic in POLICIES:
        return {"topic": topic, **POLICIES[topic]}
    return {"topic": topic, "error": f"No policy on file for topic '{topic}'"}


def get_benefits_summary(input_data):
    username = input_data.get("username", "")
    if username in BENEFITS:
        return {"username": username, **BENEFITS[username]}
    return {"username": username, "error": "No benefits record on file"}


TOOLS = {
    "get_pto_balance": get_pto_balance,
    "lookup_policy": lookup_policy,
    "get_benefits_summary": get_benefits_summary,
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
