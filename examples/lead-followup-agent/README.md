# Lead Follow-Up Agent

An AI sales assistant that helps re-engage dormant leads. It searches your CRM, presents candidates for approval, drafts personalized follow-up emails, and tracks all activity.

## How it works

1. Ask the agent to find old leads (e.g. "find leads we haven't contacted since July")
2. The agent searches the CRM and presents a list for your review
3. You approve which leads to follow up with
4. For each approved lead, the agent reviews their full history, drafts a personalized email, and waits for your approval before recording the activity
5. All actions are logged in the CRM so the next session picks up where you left off

## Tools

| Tool | Description |
|------|-------------|
| `search_leads` | Find leads by status, last contact date, industry, or company |
| `get_lead_details` | Full lead profile including contact info, deal value, and activity history |
| `update_lead_status` | Change lead status (e.g. dormant → pending_followup → contacted) |
| `log_activity` | Record an interaction (email, call, meeting, note) |
| `draft_followup_email` | Generate context for a personalized re-engagement email |

## Configuration

| Parameter | Value | Why |
|-----------|-------|-----|
| `SYSTEM_PROMPT_FILE` | `system-prompt.txt` | Instructs the agent to follow the approval workflow and always use tools |
| `TOOLS_JSON_FILE` | `tools.json` | CRM tools for lead management |
| `MEMOIR_ENABLED` | `false` | Disabled — all lead state is tracked in the CRM via tools |

## Services

| Service | Description |
|---------|-------------|
| `kafka` | Message broker |
| `api` | Chat API (REST + WebSocket) |
| `processing` | Kafka Streams pipeline with memoir enabled |
| `think-consumer` | Claude API caller with CRM tools |
| `lead-tool-service` | Python Kafka consumer with mock CRM data (5 sample leads) |
| `frontend` | Web UI |

## Mock CRM Data

The lead-tool-service includes 5 sample leads for demonstration:

| Lead | Company | Industry | Deal Value | Status | Last Contact |
|------|---------|----------|-----------|--------|-------------|
| Jane Chen | Acme Corp | ecommerce | $45,000 | dormant | 2025-08-12 |
| Marcus Johnson | BrightHealth | healthcare | $120,000 | dormant | 2025-06-20 |
| Sarah Kim | Fintechly | fintech | $75,000 | dormant | 2025-07-03 |
| David Park | RetailNext | ecommerce | $30,000 | contacted | 2026-03-10 |
| Lisa Nguyen | CloudBridge | saas | $95,000 | dormant | 2025-09-15 |

Each lead has a realistic activity history with calls, emails, meetings, and notes.

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Open [http://localhost](http://localhost) in your browser.

## Example prompts

- "Find all dormant leads we haven't contacted since August 2025"
- "Show me leads in the healthcare industry"
- "Get the full history for Jane Chen"
- "Let's follow up with Jane Chen and Lisa Nguyen"
- "Draft a follow-up email for Jane — mention our new Q1 pricing"
- "What leads are pending follow-up?"
- "Mark Lisa Nguyen as contacted and log that we sent the re-engagement email"
