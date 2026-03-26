
# ETL Agent — Autonomous Data Engineering
**Snowflake Native · Domain-Agnostic · Powered by CoCo (Cortex Code)**

Cortex Code Prompt : cortex code --skill SKILL.md

A fully autonomous, self-configuring ETL pipeline system built 100% inside Snowflake.
Users submit pipeline configurations via a Streamlit UI. CoCo reads the config,
generates production-grade SQL, gets human approval, then deploys a Snowflake Task
automatically. Works for any domain — Finance, Healthcare, Retail, Logistics.

## What problem it solves
Data engineers spend weeks writing the same ETL patterns over and over.
Every new pipeline needs the same boilerplate — watermark logic, quality checks,
audit logging, error handling, retry logic. This agent eliminates all of that.
You describe what you want in a form. The agent writes the code.

## How it works

```
Business User fills in Streamlit form
        ↓
Pipeline config stored in ETL_CONFIGS table (PIPELINE_PARAMS VARIANT)
        ↓
CoCo reads the config + DDL of source and target tables
        ↓
CoCo generates production SQL (INCREMENTAL / FULL_LOAD / CDC)
        ↓
Human reviews and approves (mandatory gate)
        ↓
CoCo deploys Snowflake Task — pipeline runs on schedule automatically
        ↓
ETL_TASK_RUN_LOG records every run with quality checks + audit trail
```

## 3 Pipeline Strategies

| Strategy | When to use | How it works |
|---|---|---|
| INCREMENTAL | Source has a watermark column (UPDATED_AT) | Only new/changed rows since last run |
| FULL_LOAD | Daily snapshot tables | Truncate and reload everything |
| CDC / MERGE | Source has delete flags | MERGE with UPDATE + DELETE branches |

## Tech Stack
| Tool | Purpose |
|---|---|
| Snowflake Tasks | Scheduled pipeline execution |
| Snowflake Stored Procedures | Quality checks, hooks, state management |
| PIPELINE_PARAMS VARIANT | Domain-agnostic config storage |
| Snowflake CoCo | AI that reads configs and generates SQL |
| Streamlit in Snowflake | Pipeline submission and monitoring UI |
| GitHub Actions | Auto-deploy SQL changes |

## Project Structure
```
etl-agent-copilot/
├── sql/
│   ├── 01_setup.sql        ← Control plane: ETL_AGENT_DB, tables, views, procedures
│   ├── 02_mock_data.sql    ← Finance demo data: RAW + CLEAN tables
│   └── 03_tasks.sql        ← Task templates + quality check procedures
├── scripts/
│   ├── deploy_sql.py       ← Runs all SQL files against Snowflake
│   └── submit_pipeline.py  ← Submits sample pipeline configs to ETL_CONFIGS
├── streamlit/
│   └── app.py              ← Pipeline configurator UI
├── SKILL.md                ← CoCo agent brain — paste into CoCo
├── .env.example            ← Credentials template
├── requirements.txt        ← Python dependencies
├── .gitignore              ← Blocks secrets
└── .github/workflows/
    └── deploy.yml          ← Auto-deploy on git push
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up credentials
cp .env.example .env
# Edit .env with your Snowflake details

# 3. Deploy infrastructure to Snowflake
python scripts/deploy_sql.py

# 4. Submit a sample pipeline config
python scripts/submit_pipeline.py

# 5. Open Snowsight → paste SKILL.md into CoCo → agent processes the queue
```

## How to use CoCo with this project
1. Open Snowsight → any Worksheet → click CoCo icon
2. Paste the entire contents of `SKILL.md` into CoCo
3. CoCo will read the pending pipeline queue and start processing
4. Review the generated SQL when prompted
5. Type `Y` to approve and deploy, `N` to reject

## Built for
Snowflake CoCo (Cortex Code) Hackathon
=======
# autonomous-finance-etl
Snowflake autonomous ETL agent - CoCo Hackathon
