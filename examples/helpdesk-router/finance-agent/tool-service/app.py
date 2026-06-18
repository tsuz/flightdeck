"""
Finance & Expenses tool consumer — mock expenses, reimbursements, corporate cards.
"""

import json
import os

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

AGENT_NAME = os.environ["AGENT_NAME"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

EXPENSES = {
    "EXP-2026-0421": {
        "expense_id": "EXP-2026-0421",
        "username": "jsmith",
        "amount_usd": 482.15,
        "category": "travel",
        "submitted": "2026-04-08",
        "status": "approved",
        "approver": "mlee (manager)",
        "payment_date": "2026-04-25",
        "payment_method": "ACH to checking ending 4421",
        "description": "March client visit — flight + 2 nights hotel",
    },
    "EXP-2026-0512": {
        "expense_id": "EXP-2026-0512",
        "username": "jsmith",
        "amount_usd": 132.00,
        "category": "meals",
        "submitted": "2026-04-15",
        "status": "pending_approval",
        "approver": "mlee (manager)",
        "payment_date": None,
        "description": "Team dinner — 4 attendees",
        "note": "Awaiting manager review since 2026-04-15. Average approval time: 3 business days.",
    },
    "EXP-2026-0533": {
        "expense_id": "EXP-2026-0533",
        "username": "achen",
        "amount_usd": 89.99,
        "category": "software",
        "submitted": "2026-04-20",
        "status": "rejected",
        "approver": "mlee (manager)",
        "rejection_reason": "Personal subscription — not on the approved software list. See SoftwarePolicy doc.",
        "description": "Notion Pro annual",
    },
}

REIMBURSEMENTS = {
    "jsmith": {
        "total_pending_usd": 132.00,
        "expected_payment_date": "2026-05-02",
        "blocking_issues": [],
        "items": [{"expense_id": "EXP-2026-0512", "amount_usd": 132.00, "status": "pending_approval"}],
    },
    "achen": {
        "total_pending_usd": 0,
        "expected_payment_date": None,
        "blocking_issues": ["Most recent submission (EXP-2026-0533) was rejected — see expense for reason"],
        "items": [],
    },
    "rpatel": {
        "total_pending_usd": 0,
        "expected_payment_date": None,
        "blocking_issues": [],
        "items": [],
    },
}

CARDS = {
    "jsmith": {"status": "active", "last4": "8821", "month_to_date_spend_usd": 1247.50, "monthly_limit_usd": 5000, "issued": "2024-08-15"},
    "achen": {"status": "active", "last4": "1129", "month_to_date_spend_usd": 312.00, "monthly_limit_usd": 3000, "issued": "2025-02-01"},
    "rpatel": {"status": "suspended", "last4": "5530", "month_to_date_spend_usd": 0, "monthly_limit_usd": 5000, "issued": "2023-11-10", "suspension_reason": "Reported lost on 2026-04-22 — replacement card shipped 2026-04-23"},
}

EXPENSE_POLICY = {
    "meals": {"limit_per_person_usd": 75, "requires_receipt_above_usd": 25, "notes": "Team dinners require >=2 attendees and a one-line business justification."},
    "travel": {"limit_per_night_hotel_usd": 350, "flight_class": "economy (premium economy on flights >6h)", "notes": "Book through Navan whenever possible. Pre-approval required for flights over $1500."},
    "software": {"approved_list_url": "internal://approved-software", "notes": "Only software on the approved list is reimbursable. Submit a SoftwareRequest ticket to add new items."},
    "conferences": {"limit_per_event_usd": 2500, "requires_pre_approval": True, "notes": "Pre-approval from manager + VP required. Includes registration, travel, and lodging."},
    "coworking": {"monthly_limit_usd": 300, "eligibility": "Fully remote employees only (per Remote Work Policy)", "notes": "Submit monthly with receipt. Hybrid employees not eligible."},
}


def lookup_expense(input_data):
    expense_id = input_data.get("expense_id", "")
    username = input_data.get("username", "")

    if expense_id:
        if expense_id in EXPENSES:
            return EXPENSES[expense_id]
        return {"expense_id": expense_id, "error": "Not found"}

    if username:
        matches = [e for e in EXPENSES.values() if e["username"] == username]
        return {"username": username, "expenses": matches, "count": len(matches)}

    return {"error": "Provide either expense_id or username"}


def get_reimbursement_status(input_data):
    username = input_data.get("username", "")
    if username in REIMBURSEMENTS:
        return {"username": username, **REIMBURSEMENTS[username]}
    return {"username": username, "error": "No reimbursement record on file"}


def corporate_card_info(input_data):
    username = input_data.get("username", "")
    if username in CARDS:
        return {"username": username, **CARDS[username]}
    return {"username": username, "error": "No corporate card on file"}


def lookup_expense_policy(input_data):
    category = input_data.get("category", "")
    if category in EXPENSE_POLICY:
        return {"category": category, **EXPENSE_POLICY[category]}
    return {"category": category, "error": "Unknown expense category"}


TOOLS = {
    "lookup_expense": lookup_expense,
    "get_reimbursement_status": get_reimbursement_status,
    "corporate_card_info": corporate_card_info,
    "lookup_expense_policy": lookup_expense_policy,
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
