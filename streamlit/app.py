"""
ETL Agent — Autonomous Pipeline Configurator
Run locally in VS Code: streamlit run streamlit_app.py
Auth: .env locally | st.secrets on Streamlit Community Cloud
"""

import streamlit as st
import snowflake.connector
import datetime
import json
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        cfg = st.secrets["snowflake"]
        return snowflake.connector.connect(
            account=cfg["account"], user=cfg["user"], password=cfg["password"],
            warehouse=cfg.get("warehouse", "ETL_AGENT_WH"),
            database=cfg.get("database", "ETL_AGENT_DB"),
            schema=cfg.get("schema", "ETL_META"),
            role=cfg.get("role", "ETL_UI_ROLE"),
        )
    except (KeyError, FileNotFoundError):
        return snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_AGENT_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "ETL_AGENT_DB"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "ETL_META"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ETL_UI_ROLE"),
        )


def run_sql(sql, params=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    return cur


def query_df(sql):
    import pandas as pd
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ETL Agent",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap');
:root {
    --bg:      #080c10; --surface:  #0d1117; --surface2: #161b22;
    --border:  #21262d; --border2:  #30363d;
    --accent:  #00d4aa; --accent2:  #0ea5e9;
    --amber:   #f59e0b; --coral:    #f87171;
    --text:    #cdd9e5; --muted:    #768390; --tag:      #1c2128;
}
html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
}
#MainMenu, footer, header, [data-testid="stToolbar"] { visibility: hidden; }
[data-testid="stSidebar"] { display: none; }

.page-header {
    padding: 32px 0 24px; border-bottom: 1px solid var(--border); margin-bottom: 32px;
}
.page-header h1 {
    font-family: 'IBM Plex Mono', monospace; font-size: 20px; font-weight: 600;
    color: var(--accent); letter-spacing: -0.3px; margin: 0 0 4px 0;
}
.page-header p { font-size: 13px; color: var(--muted); margin: 0; }

.section-header {
    display: flex; align-items: center; gap: 10px;
    margin: 28px 0 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border);
}
.section-num {
    font-family: 'IBM Plex Mono', monospace; font-size: 10px;
    background: var(--tag); color: var(--accent);
    border: 1px solid var(--border2); padding: 2px 7px;
    border-radius: 4px; letter-spacing: 0.8px;
}
.section-title {
    font-family: 'IBM Plex Mono', monospace; font-size: 12px;
    font-weight: 600; color: var(--text); letter-spacing: 0.6px; text-transform: uppercase;
}
.section-desc { font-size: 12px; color: var(--muted); margin-left: auto; }

.card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 18px 20px; margin-bottom: 16px;
}
.card-accent { border-left: 3px solid var(--accent); }

.strat-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px; min-height: 110px;
}
.strat-card.active {
    border-color: var(--accent); background: rgba(0,212,170,0.05);
}
.strat-name {
    font-family: 'IBM Plex Mono', monospace; font-size: 12px;
    font-weight: 600; margin-bottom: 4px;
}
.strat-card.active .strat-name { color: var(--accent); }
.strat-card:not(.active) .strat-name { color: var(--text); }
.strat-desc { font-size: 11px; color: var(--muted); line-height: 1.5; }
.strat-sql {
    font-family: 'IBM Plex Mono', monospace; font-size: 10px;
    margin-top: 8px; line-height: 1.4;
}
.strat-card.active .strat-sql { color: var(--muted); }
.strat-card:not(.active) .strat-sql { color: var(--border2); }

.mapping-row {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 6px; padding: 10px 12px; margin-bottom: 8px;
    display: flex; align-items: center; gap: 8px;
    font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: var(--text);
}

.pill {
    display: inline-block; padding: 3px 10px; border-radius: 100px;
    font-family: 'IBM Plex Mono', monospace; font-size: 10px;
    font-weight: 500; letter-spacing: 0.5px;
}
.pill-pending  { background: rgba(245,158,11,0.12); color:#f59e0b; border:1px solid rgba(245,158,11,0.25); }
.pill-approved { background: rgba(0,212,170,0.10);  color:#00d4aa; border:1px solid rgba(0,212,170,0.25); }
.pill-deployed { background: rgba(14,165,233,0.10); color:#0ea5e9; border:1px solid rgba(14,165,233,0.25); }
.pill-rejected { background: rgba(248,113,113,0.10);color:#f87171; border:1px solid rgba(248,113,113,0.25); }
.pill-failed   { background: rgba(248,113,113,0.10);color:#f87171; border:1px solid rgba(248,113,113,0.25); }

.alert {
    padding: 12px 16px; border-radius: 6px; font-size: 13px;
    line-height: 1.6; margin-top: 12px; font-family: 'IBM Plex Mono', monospace;
}
.alert-warn    { background:rgba(245,158,11,0.07);  border:1px solid rgba(245,158,11,0.2);  color:#f59e0b; }
.alert-success { background:rgba(0,212,170,0.07);   border:1px solid rgba(0,212,170,0.2);   color:#00d4aa; }
.alert-info    { background:rgba(14,165,233,0.07);  border:1px solid rgba(14,165,233,0.2);  color:#0ea5e9; }
.alert-error   { background:rgba(248,113,113,0.07); border:1px solid rgba(248,113,113,0.2); color:#f87171; }

[data-testid="stTextInput"] input,
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextArea"] textarea,
[data-testid="stNumberInput"] input {
    background: var(--surface) !important; border: 1px solid var(--border2) !important;
    border-radius: 6px !important; color: var(--text) !important;
    font-family: 'IBM Plex Mono', monospace !important; font-size: 13px !important;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 2px rgba(0,212,170,0.12) !important;
}
label, [data-testid="stWidgetLabel"] {
    color: var(--muted) !important; font-size: 12px !important;
    font-family: 'IBM Plex Mono', monospace !important; letter-spacing: 0.3px !important;
}
[data-testid="stButton"] button {
    font-family: 'IBM Plex Mono', monospace !important; font-size: 12px !important;
    font-weight: 600 !important; border-radius: 6px !important; letter-spacing: 0.4px !important;
}
[data-testid="stButton"] button[kind="primary"] {
    background: var(--accent) !important; color: #080c10 !important; border: none !important;
    padding: 10px 28px !important;
}
[data-testid="stButton"] button[kind="secondary"] {
    background: var(--tag) !important; color: var(--text) !important;
    border: 1px solid var(--border2) !important;
}
hr { border-color: var(--border) !important; margin: 24px 0 !important; }

.queue-table { width:100%; border-collapse:collapse; font-size:12px; }
.queue-table th {
    text-align:left; padding:8px 10px; border-bottom:1px solid var(--border);
    color:var(--muted); font-family:'IBM Plex Mono',monospace;
    font-size:10px; letter-spacing:0.8px; text-transform:uppercase; font-weight:500;
}
.queue-table td {
    padding:10px 10px; border-bottom:1px solid rgba(33,38,45,0.6);
    color:var(--text); font-family:'IBM Plex Mono',monospace;
}
.queue-table tr:hover td { background:rgba(0,212,170,0.03); }
.queue-table tr:last-child td { border-bottom:none; }

.nl-hint {
    font-size:11px; color:var(--muted); margin-top:6px; line-height:1.8;
    font-family:'IBM Plex Mono',monospace;
}
.nl-hint code { background:var(--tag); padding:1px 5px; border-radius:3px; color:var(--accent2); }

.json-preview {
    background:var(--surface2); border:1px solid var(--border); border-radius:6px;
    padding:14px; font-family:'IBM Plex Mono',monospace; font-size:11px; color:var(--muted);
    line-height:1.7; white-space:pre-wrap; max-height:300px; overflow-y:auto;
}
</style>
""", unsafe_allow_html=True)


# ── Constants ─────────────────────────────────────────────────────────────────
STRATEGIES = {
    "INCREMENTAL": {
        "label": "Incremental INSERT",
        "desc": "Append new rows using a watermark column.",
        "sql": "INSERT INTO target\nSELECT ... WHERE col > last_run",
    },
    "FULL_LOAD": {
        "label": "Full Load",
        "desc": "Truncate target and reload all rows.",
        "sql": "INSERT OVERWRITE INTO target\nSELECT ... FROM source",
    },
    "CDC": {
        "label": "CDC / Merge",
        "desc": "Upsert and delete via MERGE on a key.",
        "sql": "MERGE INTO target USING source\nON key WHEN MATCHED ...",
    },
}

SCHEDULES  = ["5 MINUTE","15 MINUTE","30 MINUTE","1 HOUR","6 HOUR","12 HOUR","1 DAY"]
WAREHOUSES = ["ETL_AGENT_WH","COMPUTE_WH","TRANSFORM_WH","ANALYTICS_WH"]
DOMAINS    = ["finance","healthcare","retail","logistics","hr","marketing","operations","other"]
WRITE_MODES = ["append","overwrite","upsert"]

STATUS_CSS = {
    "PENDING":"pill-pending","APPROVED":"pill-approved","DEPLOYED":"pill-deployed",
    "REJECTED":"pill-rejected","FAILED":"pill-failed",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def pill(status):
    css = STATUS_CSS.get(status, "pill-pending")
    return f'<span class="pill {css}">{status}</span>'


def section(num, title, desc=""):
    desc_html = f'<span class="section-desc">{desc}</span>' if desc else ""
    st.markdown(f"""
    <div class="section-header">
        <span class="section-num">{num:02d}</span>
        <span class="section-title">{title}</span>
        {desc_html}
    </div>""", unsafe_allow_html=True)


def generate_pipeline_id(name):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    slug = "".join(c if c.isalnum() else "_" for c in name.upper())[:24]
    return f"ETL_{slug}_{ts}"


def ensure_table():
    run_sql("""
        CREATE TABLE IF NOT EXISTS ETL_AGENT_DB.ETL_META.ETL_CONFIGS (
            PIPELINE_ID     VARCHAR(64)   NOT NULL PRIMARY KEY,
            PIPELINE_NAME   VARCHAR(256)  NOT NULL,
            DOMAIN          VARCHAR(64),
            PIPELINE_PARAMS VARIANT       NOT NULL,
            SOURCE_TABLE    VARCHAR(512),
            TARGET_TABLE    VARCHAR(512),
            LOAD_STRATEGY   VARCHAR(32),
            GENERATED_SQL   TEXT,
            TASK_NAME       VARCHAR(256),
            STATUS          VARCHAR(32)   NOT NULL DEFAULT 'PENDING',
            CREATED_BY      VARCHAR(256),
            CREATED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            APPROVED_BY     VARCHAR(256),
            APPROVED_AT     TIMESTAMP_NTZ,
            DEPLOYED_AT     TIMESTAMP_NTZ,
            ERROR_MESSAGE   TEXT
        )
    """)


def fetch_queue():
    try:
        return query_df("""
            SELECT PIPELINE_ID, PIPELINE_NAME, DOMAIN, SOURCE_TABLE,
                   TARGET_TABLE, LOAD_STRATEGY, STATUS, CREATED_AT
            FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
            ORDER BY CREATED_AT DESC LIMIT 50
        """)
    except Exception:
        return None


def submit_pipeline(params: dict, pid: str):
    run_sql("""
        INSERT INTO ETL_AGENT_DB.ETL_META.ETL_CONFIGS (
            PIPELINE_ID, PIPELINE_NAME, DOMAIN, PIPELINE_PARAMS,
            SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY,
            STATUS, CREATED_BY
        ) 
        SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, 'PENDING', CURRENT_USER()
    """, [
        pid,
        params["identity"]["pipeline_name"],
        params["identity"]["domain"],
        json.dumps(params),
        params["source"]["table"],
        params["target"]["table"],
        params["strategy"]["type"],
    ])


# ── Session state ─────────────────────────────────────────────────────────────
for k, v in {
    "strategy": "INCREMENTAL", "mappings": [],
    "submitted": False, "last_pid": None, "show_json": False,
    "mapping_src": "", "mapping_tgt": "", "mapping_expr": "",
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── DB init ───────────────────────────────────────────────────────────────────
try:
    ensure_table()
    db_ok = True
except Exception as e:
    db_ok = False
    db_error = str(e)

if not db_ok:
    st.markdown(f"""
    <div class="alert alert-error">
        Cannot connect to Snowflake.<br><strong>Error:</strong> {db_error}<br><br>
        Check your <code>.env</code>: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD.
    </div>""", unsafe_allow_html=True)
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="page-header">
    <h1>⚡ ETL Agent</h1>
    <p>Autonomous pipeline configurator — define once, agent builds and deploys.</p>
</div>""", unsafe_allow_html=True)

form_col, queue_col = st.columns([1.1, 1], gap="large")


# ═══════════════════════════════════════════════════════════════════════════════
# LEFT COLUMN — FORM
# ═══════════════════════════════════════════════════════════════════════════════
with form_col:

    if st.session_state.submitted and st.session_state.last_pid:
        st.markdown(f"""
        <div class="alert alert-success">
            Pipeline <strong>{st.session_state.last_pid}</strong> submitted.<br>
            Status: PENDING — run <code>cortex code --skill SKILL.md</code> to process.
        </div>""", unsafe_allow_html=True)
        if st.button("Submit another pipeline", type="secondary"):
            st.session_state.submitted = False
            st.session_state.last_pid  = None
            st.session_state.mappings  = []
            st.rerun()
        st.stop()

    # ── SECTION 1: IDENTITY ───────────────────────────────────────────────────
    section(1, "Identity", "Name this pipeline")
    c1, c2 = st.columns(2)
    with c1:
        pipeline_name = st.text_input("Pipeline name",
            placeholder="e.g. transactions_raw_to_clean")
    with c2:
        domain = st.selectbox("Domain", DOMAINS)
    owner_email = st.text_input("Owner email",
        placeholder="analyst@company.com",
        help="Used for failure notifications.")

    # ── SECTION 2: SOURCE ─────────────────────────────────────────────────────
    section(2, "Source", "Where data comes from")
    source_table = st.text_input("Source table (fully qualified)",
        placeholder="FINANCE_DB.RAW.TRANSACTIONS_RAW",
        help="GET_DDL() will be called on this table.")
    source_filter = st.text_input("Permanent source filter (optional)",
        placeholder="e.g.  STATUS != 'TEST'  or  REGION = 'US'",
        help="Applied as a permanent WHERE clause on every run.")

    dedup_on = st.checkbox("Deduplicate source rows before loading")
    dedup_key = dedup_orderby = ""
    if dedup_on:
        dc1, dc2 = st.columns(2)
        with dc1:
            dedup_key = st.text_input("Dedup key column(s)",
                placeholder="e.g. TRANSACTION_ID")
        with dc2:
            dedup_orderby = st.text_input("Keep row order by",
                placeholder="e.g. UPDATED_AT DESC")

    # ── SECTION 3: TARGET ─────────────────────────────────────────────────────
    section(3, "Target", "Where data goes")
    target_table = st.text_input("Target table (fully qualified)",
        placeholder="FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN",
        help="Must exist before deployment. GET_DDL() called here too.")
    write_mode = st.selectbox("Write mode", WRITE_MODES,
        help="append=INSERT · overwrite=INSERT OVERWRITE · upsert=MERGE")

    # ── SECTION 4: LOAD STRATEGY ──────────────────────────────────────────────
    section(4, "Load strategy")
    strat_cols = st.columns(3)
    for i, (key, meta) in enumerate(STRATEGIES.items()):
        with strat_cols[i]:
            active = st.session_state.strategy == key
            st.markdown(f"""
            <div class="strat-card {'active' if active else ''}">
                <div class="strat-name">{meta['label']}</div>
                <div class="strat-desc">{meta['desc']}</div>
                <div class="strat-sql">{meta['sql']}</div>
            </div>""", unsafe_allow_html=True)
            if st.button(
                "Selected" if active else "Select",
                key=f"s_{key}",
                type="primary" if active else "secondary",
            ):
                st.session_state.strategy = key
                st.rerun()

    strategy = st.session_state.strategy
    watermark_col = watermark_initial = merge_keys = delete_flag_col = ""
    st.markdown("<br>", unsafe_allow_html=True)

    if strategy == "INCREMENTAL":
        ic1, ic2 = st.columns(2)
        with ic1:
            watermark_col = st.text_input("Watermark column",
                placeholder="e.g. UPDATED_AT")
        with ic2:
            watermark_initial = st.text_input("Initial load from date",
                value="2020-01-01", help="First run loads rows after this date.")
    elif strategy == "CDC":
        cc1, cc2 = st.columns(2)
        with cc1:
            merge_keys = st.text_input("Merge key column(s)",
                placeholder="e.g. TRANSACTION_ID  or  ORDER_ID, LINE_ID")
        with cc2:
            delete_flag_col = st.text_input("Delete flag column (optional)",
                placeholder="e.g. IS_DELETED",
                help="BOOLEAN col — rows where TRUE are deleted from target.")

    # ── SECTION 5: TRANSFORMATIONS ────────────────────────────────────────────
    section(5, "Transformations", "What to do with the data")
    nl_rules = st.text_area(
        "Natural language transformation rules",
        placeholder=(
            "e.g. Parse tx_payload JSON into amount, currency, merchant_name. "
            "Cast tx_date to DATE using format YYYY-MM-DD. "
            "Mask account_number using SHA2. "
            "Drop rows where status = 'CANCELLED'. "
            "Coalesce tx_amount with 0."
        ),
        height=120,
    )
    st.markdown("""
    <div class="nl-hint">
    Patterns: <code>parse JSON field X into A,B,C</code> · <code>cast COL to DATE</code> ·
    <code>mask COL with SHA2</code> · <code>rename A to B</code> ·
    <code>coalesce COL with 0</code> · <code>drop rows where COL='X'</code> ·
    <code>decode 'A' to 'Active','S' to 'Suspended'</code> ·
    <code>trim/uppercase/lowercase COL</code> · <code>flag COL>N as 'HIGH'</code>
    </div>""", unsafe_allow_html=True)

    # Column mappings
    st.markdown("""
    <div style="margin-top:20px;margin-bottom:8px;font-family:'IBM Plex Mono',monospace;
                font-size:11px;color:#768390;letter-spacing:0.5px;text-transform:uppercase">
        Explicit column mappings (optional)
    </div>""", unsafe_allow_html=True)

    ma, mb, mc, md = st.columns([2, 2, 3, 1])
    with ma:
        new_src = st.text_input("Source col", key="map_src_input",
                                placeholder="SRC_COL",
                                value=st.session_state.mapping_src)
    with mb:
        new_tgt = st.text_input("Target col", key="map_tgt_input",
                                placeholder="TGT_COL",
                                value=st.session_state.mapping_tgt)
    with mc:
        new_expr = st.text_input("Expression (blank = direct copy)",
                                 key="map_expr_input",
                                 placeholder="e.g. ROUND(src.TX_AMT, 2)",
                                 value=st.session_state.mapping_expr)
    with md:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Add", type="secondary", key="add_mapping"):
            if new_src.strip() and new_tgt.strip():
                st.session_state.mappings.append({
                    "source_col": new_src.strip().upper(),
                    "target_col": new_tgt.strip().upper(),
                    "expression": new_expr.strip() or None,
                })
                st.session_state.mapping_src = ""
                st.session_state.mapping_tgt = ""
                st.session_state.mapping_expr = ""
                st.rerun()

    for idx, m in enumerate(st.session_state.mappings):
        expr_part = f" → <code>{m['expression']}</code>" if m["expression"] else ""
        ca, cb = st.columns([6, 1])
        with ca:
            st.markdown(f"""
            <div class="mapping-row">
                <span style="color:#768390">{idx+1}.</span>
                <span style="color:#0ea5e9">{m['source_col']}</span>
                <span style="color:#30363d;margin:0 6px">→</span>
                <span style="color:#00d4aa">{m['target_col']}</span>
                {expr_part}
            </div>""", unsafe_allow_html=True)
        with cb:
            if st.button("✕", key=f"del_{idx}", type="secondary"):
                st.session_state.mappings.pop(idx)
                st.rerun()

    # ── SECTION 6: EXECUTION ──────────────────────────────────────────────────
    section(6, "Execution", "How and when to run")
    ex1, ex2 = st.columns(2)
    with ex1:
        schedule  = st.selectbox("Task schedule", SCHEDULES, index=3)
        warehouse = st.selectbox("Warehouse", WAREHOUSES)
    with ex2:
        timeout_min = st.number_input("Timeout (minutes)",
            min_value=5, max_value=480, value=30, step=5)
        max_retries = st.number_input("Max retries on failure",
            min_value=0, max_value=5, value=2, step=1)

    pre_load_sql = st.text_input("Pre-load SQL hook (optional)",
        placeholder="e.g. TRUNCATE TABLE FINANCE_DB.STAGE.TXN_STAGE",
        help="Runs before the main INSERT/MERGE.")
    post_load_sql = st.text_input("Post-load SQL hook (optional)",
        placeholder="e.g. CALL FINANCE_DB.PROCS.REFRESH_AGGREGATES()",
        help="Runs after a successful load.")

    # ── SECTION 7: QUALITY & ALERTING ────────────────────────────────────────
    section(7, "Quality & Alerting", "Thresholds and notifications")
    qa1, qa2 = st.columns(2)
    with qa1:
        error_tolerance = st.number_input("Error tolerance (%)",
            min_value=0.0, max_value=100.0, value=1.0, step=0.5, format="%.1f",
            help="Fail task if more than this % of rows have cast errors.")
        min_rows = st.number_input("Minimum rows expected",
            min_value=0, value=0, step=100,
            help="Fail task if loaded rows < this. 0 = no check.")
    with qa2:
        null_check_cols = st.text_input("Null-check columns (comma-separated)",
            placeholder="e.g. TRANSACTION_ID, AMOUNT",
            help="Fail task if any of these columns contain NULL in loaded rows.")
        notification_email = st.text_input("Failure notification email",
            placeholder="alerts@company.com")

    # ── VALIDATION ────────────────────────────────────────────────────────────
    errors = []
    if not pipeline_name.strip():  errors.append("Pipeline name is required.")
    if not source_table.strip():   errors.append("Source table is required.")
    if not target_table.strip():   errors.append("Target table is required.")
    if not nl_rules.strip():       errors.append("Transformation rules are required.")
    if strategy == "INCREMENTAL" and not watermark_col.strip():
        errors.append("Watermark column is required for Incremental strategy.")
    if strategy == "CDC" and not merge_keys.strip():
        errors.append("Merge key column(s) required for CDC strategy.")
    if dedup_on and not dedup_key.strip():
        errors.append("Dedup key column is required when deduplication is enabled.")

    if errors and any([pipeline_name, source_table, target_table]):
        st.markdown(
            '<div class="alert alert-warn">' +
            "".join(f"<div>· {e}</div>" for e in errors) +
            "</div>", unsafe_allow_html=True,
        )

    # ── Build params dict ─────────────────────────────────────────────────────
    null_cols_list  = [c.strip().upper() for c in null_check_cols.split(",") if c.strip()]
    merge_keys_list = [k.strip().upper() for k in merge_keys.split(",") if k.strip()]

    params = {
        "identity": {
            "pipeline_name": pipeline_name.strip(),
            "domain": domain,
            "owner_email": owner_email.strip() or None,
        },
        "source": {
            "table": source_table.strip().upper(),
            "filter": source_filter.strip() or None,
            "deduplicate": dedup_on,
            "dedup_key": dedup_key.strip().upper() or None,
            "dedup_order_by": dedup_orderby.strip() or None,
        },
        "target": {
            "table": target_table.strip().upper(),
            "write_mode": write_mode,
        },
        "strategy": {
            "type": strategy,
            "watermark_col": watermark_col.strip().upper() or None,
            "watermark_initial": watermark_initial.strip() or "2020-01-01",
            "merge_keys": merge_keys_list,
            "delete_flag_col": delete_flag_col.strip().upper() or None,
        },
        "transformations": {
            "nl_rules": nl_rules.strip(),
            "column_mappings": st.session_state.mappings,
        },
        "execution": {
            "warehouse": warehouse,
            "schedule": schedule,
            "timeout_minutes": int(timeout_min),
            "max_retries": int(max_retries),
        },
        "quality": {
            "error_tolerance_pct": float(error_tolerance),
            "null_check_cols": null_cols_list,
            "min_rows_expected": int(min_rows),
            "notification_email": notification_email.strip() or None,
        },
        "hooks": {
            "pre_load_sql": pre_load_sql.strip() or None,
            "post_load_sql": post_load_sql.strip() or None,
        },
    }

    st.markdown("<br>", unsafe_allow_html=True)
    preview_col, submit_col = st.columns([1, 1])
    with preview_col:
        if st.button("Preview JSON config", type="secondary"):
            st.session_state.show_json = not st.session_state.show_json
    with submit_col:
        submit_clicked = st.button("Submit pipeline →", type="primary",
                                   disabled=bool(errors))

    if st.session_state.show_json:
        st.markdown(
            f'<div class="json-preview">{json.dumps(params, indent=2)}</div>',
            unsafe_allow_html=True,
        )

    if submit_clicked and not errors:
        try:
            pid = generate_pipeline_id(pipeline_name.strip())
            submit_pipeline(params, pid)
            st.session_state.submitted = True
            st.session_state.last_pid  = pid
            st.rerun()
        except Exception as ex:
            st.markdown(
                f'<div class="alert alert-error">Submission failed: {ex}</div>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# RIGHT COLUMN — QUEUE + MONITORING
# ═══════════════════════════════════════════════════════════════════════════════
with queue_col:

    st.markdown("""
    <div style="padding-top:4px;margin-bottom:20px">
        <span style="font-family:'IBM Plex Mono',monospace;font-size:12px;font-weight:600;
                     color:#768390;letter-spacing:0.8px;text-transform:uppercase">
            Pipeline Queue
        </span>
    </div>""", unsafe_allow_html=True)

    if st.button("↻ Refresh", type="secondary"):
        st.rerun()

    df = fetch_queue()

    if df is None or df.empty:
        st.markdown("""
        <div class="alert alert-info" style="margin-top:16px">
            No pipelines yet. Fill the form and submit your first pipeline.
        </div>""", unsafe_allow_html=True)
    else:
        rows_html = ""
        for _, row in df.iterrows():
            created     = str(row["CREATED_AT"])[:16]
            strat_label = {"FULL_LOAD":"Full","INCREMENTAL":"Incr","CDC":"CDC"
                           }.get(row["LOAD_STRATEGY"], row["LOAD_STRATEGY"] or "—")
            src_short   = str(row["SOURCE_TABLE"]).split(".")[-1] if row["SOURCE_TABLE"] else "—"
            rows_html += f"""
            <tr>
                <td title="{row['PIPELINE_ID']}" style="max-width:130px;overflow:hidden;
                    text-overflow:ellipsis;white-space:nowrap">{row['PIPELINE_NAME']}</td>
                <td style="color:#768390">{row['DOMAIN'] or '—'}</td>
                <td style="color:#768390" title="{row['SOURCE_TABLE']}">{src_short}</td>
                <td style="color:#768390">{strat_label}</td>
                <td>{pill(row['STATUS'])}</td>
                <td style="color:#768390;font-size:11px">{created}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card" style="padding:0;overflow:hidden">
            <table class="queue-table">
                <thead><tr>
                    <th>Name</th><th>Domain</th><th>Source</th>
                    <th>Strategy</th><th>Status</th><th>Created</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>""", unsafe_allow_html=True)

        counts = df["STATUS"].value_counts().to_dict()
        badges = "".join(
            f'<span class="pill {STATUS_CSS.get(s,"pill-pending")}" '
            f'style="margin-right:8px">{n} {s}</span>'
            for s, n in counts.items()
        )
        st.markdown(f"<div style='margin-top:10px'>{badges}</div>",
                    unsafe_allow_html=True)

        # Reset panel
        failed_df = df[df["STATUS"].isin(["FAILED", "REJECTED"])]
        if not failed_df.empty:
            st.markdown("""
            <div style="margin-top:24px;font-family:'IBM Plex Mono',monospace;font-size:10px;
                        color:#768390;letter-spacing:0.8px;text-transform:uppercase;
                        margin-bottom:8px">Reset pipeline</div>""",
                        unsafe_allow_html=True)
            pid_to_reset = st.selectbox(
                "Select pipeline to reset",
                failed_df["PIPELINE_ID"].tolist(),
                label_visibility="collapsed",
            )
            if st.button("Reset to PENDING", type="secondary"):
                try:
                    run_sql("CALL ETL_AGENT_DB.ETL_META.SP_RESET_PIPELINE(%s)",
                            [pid_to_reset])
                    st.rerun()
                except Exception as ex:
                    st.markdown(
                        f'<div class="alert alert-error">Reset failed: {ex}</div>',
                        unsafe_allow_html=True,
                    )

    # ── Agent run box ─────────────────────────────────────────────────────────
    st.markdown("<div style='margin-top:32px'></div>", unsafe_allow_html=True)
    st.markdown("""
    <div style="font-family:'IBM Plex Mono',monospace;font-size:10px;color:#768390;
                letter-spacing:0.8px;text-transform:uppercase;margin-bottom:10px">
        Run the agent
    </div>
    <div class="card card-accent">
        <div style="font-family:'IBM Plex Mono',monospace;font-size:12px;
                    color:#00d4aa;margin-bottom:8px;font-weight:600">
            Process pending pipelines
        </div>
        <div style="font-family:'IBM Plex Mono',monospace;font-size:12px;
                    background:#080c10;border:1px solid #21262d;border-radius:5px;
                    padding:10px 12px;color:#cdd9e5">
            cortex code --skill SKILL.md
        </div>
        <div style="font-size:11px;color:#768390;margin-top:10px;line-height:1.7;
                    font-family:'IBM Plex Mono',monospace">
            1. Reads all PENDING pipelines (FIFO)<br>
            2. Calls GET_DDL() on source + target<br>
            3. Generates transformation SQL<br>
            4. Shows SQL — asks Y / N / E<br>
            5. Deploys Snowflake Task on Y
        </div>
    </div>
    <div class="alert alert-warn" style="margin-top:12px;font-size:11px">
        <strong>Human-in-the-loop enforced.</strong> Every pipeline requires
        explicit terminal approval before any SQL runs in Snowflake.
    </div>""", unsafe_allow_html=True)