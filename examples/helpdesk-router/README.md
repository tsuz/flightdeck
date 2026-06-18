# Helpdesk Router — Multi-Agent Routing for Scaling

Five FlightDeck agents on a shared Kafka cluster: a thin **router** that classifies the user's request and forwards it to exactly **one** of four specialist agents (IT, HR, Facilities, Finance). Each specialist owns its own prompt, its own tools, and its own deployment.

The point of this example is **architectural scaling** — the router pattern is how you grow an AI agent product past a single-domain MVP without piling every tool and policy into one monolithic prompt.

## How it works

```
User: "Where's my reimbursement for the March travel?"
                    |
                    v
             [Router Agent]
             route_to(specialist="finance", question="...")
                    |
                    v
             [Finance Agent]
             lookup_expense, get_reimbursement_status,
             corporate_card_info, lookup_expense_policy
                    |
                    v
             Specialist response returned to user
```

For each user turn, **exactly one specialist activates**. The router has no domain knowledge of its own — its only skill is classification.

## Agents

### Router (`AGENT_NAME=router`)
User-facing agent with the frontend. One tool: `route_to(specialist, question)`. System prompt is purely about classification.

### IT Agent (`AGENT_NAME=it-agent`)
| Tool | Returns |
|------|---------|
| `lookup_device` | Model, OS, last check-in, pending updates |
| `check_vpn_status` | VPN active state, region, recent failures |
| `check_account_status` | Lockout state, last login, MFA enrollment |
| `reset_password` | Triggers reset email, returns confirmation |

### HR Agent (`AGENT_NAME=hr-agent`)
| Tool | Returns |
|------|---------|
| `get_pto_balance` | Vacation/sick day balances and pending requests |
| `lookup_policy` | PTO, parental leave, remote work, expense, code-of-conduct policies |
| `get_benefits_summary` | Health, dental, vision, 401k enrollment |

### Facilities Agent (`AGENT_NAME=facilities-agent`)
| Tool | Returns |
|------|---------|
| `submit_ticket` | Creates HVAC/lighting/furniture/plumbing tickets |
| `book_room` | Books a conference room or returns alternatives |
| `check_office_status` | Open/closed status and advisories per office |

### Finance Agent (`AGENT_NAME=finance-agent`)
| Tool | Returns |
|------|---------|
| `lookup_expense` | Expense by ID or by user — amount, status, approver |
| `get_reimbursement_status` | Total pending, expected payment date, blocking issues |
| `corporate_card_info` | Card status, MTD spend, monthly limit |
| `lookup_expense_policy` | Per-category limits and documentation requirements |

## The demo arc — "What happens when your AI agent grows up?"

Run the prompts below in order. Each one is designed to make a specific scaling argument visible on the dashboard.

### Act 1 — Clean routes (the obvious cases)

Show that classification works for the easy 90%.

| Prompt | Expected route |
|---|---|
| `My laptop won't connect to the office wifi. My username is jsmith.` | IT |
| `How many vacation days do I have left? I'm achen.` | HR |
| `The AC in conference room Yosemite is broken — it's freezing.` | Facilities |
| `Where's my reimbursement for expense EXP-2026-0421?` | Finance |

### Act 2 — Ambiguous routes (showing the router's reasoning)

These are the moments worth pausing on. Click into the router's pipeline view to show the audience how it picked.

| Prompt | Tension |
|---|---|
| `I need a new keyboard for my standing desk.` | IT (peripheral) vs Facilities (desk equipment) — router will pick one |
| `Can I expense a coworking space while I work remotely next month?` | Finance (expense limits) vs HR (remote work policy). Both have relevant tools — but `lookup_expense_policy("coworking")` is the right answer, so Finance should win |

### Act 3 — Multi-turn route stickiness

Same conversation, two consecutive turns. The router should stay with the same specialist for the follow-up.

1. `Where's my reimbursement? I'm jsmith.` → Finance
2. `And what about EXP-2026-0512 specifically?` → still Finance (no re-route needed)

### Act 4 — Out-of-scope refusal

Show that the router declines instead of hallucinating.

- `What's the weather in Tokyo?` → router replies directly: "I can only help with IT, HR, Facilities, or Finance/Expenses. Try: …"

### Act 5 — Scaling payoff (the talking points)

This is where the demo lands for a technical audience. Each is something the dashboard makes visible.

1. **Heterogeneous models per domain.** Run an HR question (`How many PTO days does jsmith have?`) and an IT question (`achen says her account is locked, can you investigate?`). Show the dashboard: HR ran on Haiku for ~$0.0001 in <1s; IT ran on Sonnet for ~$0.005 in 3-4s. → *Pay for intelligence only where you need it.* (Set via `IT_CLAUDE_MODEL=claude-sonnet-4-5` in `.env`.)

2. **Per-agent cost attribution.** Click through each agent in the dashboard — costs are tracked separately. → *You can see which domain is expensive and tune that one prompt without touching others.*

3. **Independent deployability.** To add a fifth specialist (e.g. Legal), copy any specialist directory, add a service block to `docker-compose.yml`, and add one entry to the router's `SPECIALIST_MAP`. **No edits to existing agents.** → *New domains are deployment changes, not rewrites.*

4. **Blast-radius isolation.** Stop the HR agent (`docker compose stop hr-agent-think`). HR queries fail; IT, Facilities, and Finance keep working. → *One specialist's bug doesn't take down the others.*

5. **Per-agent horizontal scaling.** Each specialist is its own Kafka consumer group. You can run `docker compose up --scale it-agent-think=3` to fan out IT-only without scaling HR/Facilities/Finance. → *Scale the busy domains independently.*

## Mock data

Three test users are seeded across all specialists so the demo is consistent:

| Username | Role in demo |
|---|---|
| `jsmith` | "Happy path" employee — active, has pending reimbursement, recent travel expense |
| `achen` | "Problem" employee — account locked, VPN failures, rejected expense |
| `rpatel` | "Edge cases" — low PTO balance, pending leave request, suspended/lost corp card |

## Services

| Service | Agent | Description |
|---------|-------|-------------|
| `router-api` | router | Chat API (user-facing) |
| `router-processing` | router | Kafka Streams pipeline |
| `router-think` | router | Claude API — classifies and routes |
| `router-tool-service` | router | Bridges router → specialists via Kafka topics |
| `frontend` | router | Web UI |
| `it-agent-processing/think/tool-service` | it-agent | IT specialist (devices, VPN, accounts) |
| `hr-agent-processing/think/tool-service` | hr-agent | HR specialist (PTO, policies, benefits) |
| `facilities-agent-processing/think/tool-service` | facilities-agent | Facilities specialist (rooms, tickets, offices) |
| `finance-agent-processing/think/tool-service` | finance-agent | Finance specialist (expenses, reimbursements, cards) |

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Open [http://localhost](http://localhost) in your browser.

## Adding a fifth specialist (live demo trick)

The compelling moment is showing how cheap it is to extend. To add a Legal specialist:

1. `cp -r finance-agent legal-agent`
2. Replace `system-prompt.txt`, `tools.json`, and `tool-service/app.py` with Legal-flavored versions.
3. Add a `legal-agent-processing/think/tool-service` block to `docker-compose.yml` (copy any specialist's three blocks, find/replace).
4. Update the router's `SPECIALIST_MAP` env: add `legal=legal-agent`.
5. Update the router's `tools.json` `route_to.specialist.enum` to include `legal`.
6. Update the router's `system-prompt.txt` to mention Legal in the specialist list.
7. `docker compose up --build legal-agent-processing legal-agent-think legal-agent-tool-service router-think router-tool-service`

Other agents are untouched. That's the architectural argument in one command.
