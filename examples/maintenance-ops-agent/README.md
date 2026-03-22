# Maintenance Ops Agent

An AI agent that triages sensor anomaly events, diagnoses equipment issues using maintenance history and parts inventory, creates work orders in a mock CMMS, and sends technician alerts — with human approval before any action is taken.

## How it works

```
Operator: "Sensor TEMP-001 triggered a critical alert"
    |
    v
1. get_sensor_details("TEMP-001")
   → 97.3°C (critical threshold: 95°C), asset: PUMP-101
    |
2. query_maintenance_history("PUMP-101")
   → bearing replaced 3 times in 18 months — recurring failure pattern
    |
3. check_parts_inventory(asset_id="PUMP-101")
   → bearing BRG-6205: 3 in stock, seal SEAL-42X62: 1 in stock
    |
4. create_work_order(...)
   → draft WO with diagnosis, parts, estimated hours
    |
5. Agent presents draft → WAITS for operator approval
    |
6. Operator: "approved"
    |
7. approve_work_order("WO-XXXXXXXX")
   → submitted to CMMS
    |
8. send_technician_alert("WO-XXXXXXXX", "TECH-001", "immediate")
   → email sent to Mike Torres
```

## Tools

| Tool | Description | Approval Required |
|------|-------------|:-:|
| `get_sensor_details` | Sensor readings, thresholds, asset info | No |
| `query_maintenance_history` | Past work orders, repairs, downtime | No |
| `check_parts_inventory` | Stock levels, lead time, warehouse location | No |
| `create_work_order` | Creates a DRAFT work order in the CMMS | No (draft only) |
| `approve_work_order` | Submits the work order | **Yes** |
| `send_technician_alert` | Emails the assigned technician | **Yes** (after approval) |

## Mock Data

### Sensors

| Sensor | Type | Asset | Current Reading | Status |
|--------|------|-------|----------------|--------|
| TEMP-001 | Temperature | PUMP-101 | 97.3°C (critical: 95°C) | CRITICAL |
| VIB-003 | Vibration | COMPRESSOR-A3 | 9.8 mm/s (warning: 7.0) | WARNING |
| PRESS-002 | Pressure | PUMP-101 | 2.4 bar (warning: 3.0) | WARNING |

### Assets

| Asset | Equipment | Assigned Tech | Pattern |
|-------|-----------|--------------|---------|
| PUMP-101 | Coolant Circulation Pump #1 | Mike Torres (TECH-001) | Bearing BRG-6205 replaced 3 times in 18 months — recurring root cause issue |
| COMPRESSOR-A3 | Air Compressor Unit A3 | Sara Chen (TECH-002) | Clean history, routine maintenance |

### Parts

| Part | Description | In Stock | Lead Time |
|------|-------------|----------|-----------|
| BRG-6205 | Ball bearing 6205-2RS | 3 | In stock |
| SEAL-42X62 | Mechanical shaft seal | 1 | In stock |
| IMPELLER-P1 | Pump impeller 180mm | 0 | 14 days |
| BELT-V68 | V-belt 68 inch | 5 | In stock |
| FILTER-AF200 | Air intake filter | 8 | In stock |

## Configuration

| Parameter | Value | Why |
|-----------|-------|-----|
| `SYSTEM_PROMPT_FILE` | `system-prompt.txt` | Maintenance triage workflow with approval gates |
| `TOOLS_JSON_FILE` | `tools.json` | Sensor, maintenance, inventory, CMMS, and alerting tools |
| `MEMOIR_ENABLED` | `false` | Equipment state lives in the CMMS, not in agent memory |

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Open [http://localhost](http://localhost) in your browser.

## Example prompts

**Sensor anomaly triage:**
- "Sensor TEMP-001 just triggered a critical temperature alert on PUMP-101"
- "VIB-003 is showing elevated vibration on the air compressor — investigate"
- "Both TEMP-001 and PRESS-002 are alarming on PUMP-101 — could be related?"

**Proactive investigation:**
- "Show me the maintenance history for PUMP-101 — I'm seeing a pattern"
- "Check parts availability for PUMP-101 repairs"
- "The bearing on PUMP-101 keeps failing — what's going on?"

**After diagnosis:**
- "Approved — create the work order and notify the technician"
- "Change the priority to critical and send it as immediate"
- "Hold off on this one — let's monitor for another hour"
