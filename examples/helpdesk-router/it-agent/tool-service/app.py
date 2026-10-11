"""
IT Support tool consumer — mock device, account, and VPN data.
"""

import json
import os

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

AGENT_NAME = os.environ["AGENT_NAME"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# ── Mock data ────────────────────────────────────────────────────────────────

DEVICES = {
    "jsmith": {
        "model": "MacBook Pro 14\" M3",
        "os": "macOS 15.2",
        "last_checkin": "2026-04-30T08:14:00Z",
        "encrypted": True,
        "pending_updates": ["macOS 15.3 (security)", "Slack 4.41.0"],
        "mdm_compliant": True,
    },
    "achen": {
        "model": "MacBook Air 13\" M2",
        "os": "macOS 14.6",
        "last_checkin": "2026-04-29T19:02:00Z",
        "encrypted": True,
        "pending_updates": ["macOS 15.0 (major)", "macOS 15.2 (security)"],
        "mdm_compliant": True,
    },
    "rpatel": {
        "model": "Dell XPS 15",
        "os": "Windows 11 23H2",
        "last_checkin": "2026-04-30T07:45:00Z",
        "encrypted": True,
        "pending_updates": [],
        "mdm_compliant": True,
    },
}

VPN = {
    "jsmith": {"active": True, "current_region": "us-west-2", "last_connect": "2026-04-30T08:30:00Z", "recent_failures": 0},
    "achen": {"active": True, "current_region": None, "last_connect": "2026-04-29T17:11:00Z", "recent_failures": 4, "note": "4 consecutive failed connection attempts in the last hour"},
    "rpatel": {"active": True, "current_region": "us-east-1", "last_connect": "2026-04-30T07:50:00Z", "recent_failures": 0},
}

ACCOUNTS = {
    "jsmith": {"status": "active", "last_login": "2026-04-30T08:30:00Z", "lockout_reason": None, "mfa_enrolled": True},
    "achen": {"status": "locked", "last_login": "2026-04-28T11:22:00Z", "lockout_reason": "5 consecutive failed password attempts", "mfa_enrolled": True},
    "rpatel": {"status": "active", "last_login": "2026-04-30T07:50:00Z", "lockout_reason": None, "mfa_enrolled": True},
}


def lookup_device(input_data):
    username = input_data.get("username", "")
    if username in DEVICES:
        return {"username": username, **DEVICES[username]}
    return {"username": username, "error": "No device on file for this user"}


def check_vpn_status(input_data):
    username = input_data.get("username", "")
    if username in VPN:
        return {"username": username, **VPN[username]}
    return {"username": username, "error": "No VPN account on file"}


def check_account_status(input_data):
    username = input_data.get("username", "")
    if username in ACCOUNTS:
        return {"username": username, **ACCOUNTS[username]}
    return {"username": username, "error": "No account on file"}


def reset_password(input_data):
    username = input_data.get("username", "")
    if username not in ACCOUNTS:
        return {"username": username, "error": "No account on file"}
    return {
        "username": username,
        "status": "reset_email_sent",
        "sent_to": f"{username}@company.example",
        "expires_in_minutes": 30,
        "note": "User must click the link in the email within 30 minutes. If they don't see it, ask them to check spam.",
    }


TOOLS = {
    "lookup_device": lookup_device,
    "check_vpn_status": check_vpn_status,
    "check_account_status": check_account_status,
    "reset_password": reset_password,
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
