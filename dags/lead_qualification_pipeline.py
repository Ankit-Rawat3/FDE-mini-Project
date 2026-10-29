"""
dags/lead_qualification_pipeline.py
------------------------------------
"""

import os
import json
import requests
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================
# CENTRALIZED CONFIGURATION (Update here!)
# ==========================================
CENTRAL_CSV_PATH = "include/data/leads_testing.csv"
DUCKDB_PATH = "include/lead_qualification.duckdb"
# ==========================================

default_args = {
    'owner': 'sales_ops',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def get_project_paths():
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(dag_dir)
    db_path = DUCKDB_PATH if os.path.isabs(DUCKDB_PATH) else os.path.join(project_root, DUCKDB_PATH)
    csv_path = CENTRAL_CSV_PATH if os.path.isabs(CENTRAL_CSV_PATH) else os.path.join(project_root, CENTRAL_CSV_PATH)
    return project_root, db_path, csv_path

def check_pipeline_health(**context):
    _, db_path, csv_path = get_project_paths()
    exists = os.path.exists(csv_path)
    health_status = {
        "timestamp": datetime.utcnow().isoformat(),
        "storage_check": "OK" if exists else "WARNING",
        "central_dataset": csv_path,
        "duckdb_target": db_path,
        "exists": exists
    }
    print(f"[Health Check] Status: {health_status}")

def ingest_leads_to_db(**context):
    _, db_path, csv_path = get_project_paths()
    
    if not os.path.exists(csv_path):
        alt_path = os.path.join("/usr/local/airflow", CENTRAL_CSV_PATH)
        if os.path.exists(alt_path):
            csv_path = alt_path
        else:
            raise FileNotFoundError(f"[Ingest Error] Centralized CSV file not found at: {csv_path}")

    print(f"[Ingest] Reading centralized CSV file: {csv_path}")
    df = pd.read_csv(csv_path)
    df['parsed_date'] = pd.to_datetime(df['last_interaction_date'], errors='coerce')
    df['ingested_at'] = datetime.utcnow().isoformat()

    # Store raw leads in DuckDB (eliminating heavy XCom usage)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute("DROP TABLE IF EXISTS raw_leads")
    con.execute("CREATE TABLE raw_leads AS SELECT * FROM df")
    con.close()
    print(f"[Ingest] Successfully stored {len(df)} records into DuckDB table 'raw_leads'.")

def evaluate_and_score_leads(**context):
    _, db_path, _ = get_project_paths()
    
    con = duckdb.connect(db_path)
    df = con.execute("SELECT * FROM raw_leads").fetchdf()
    
    qualified_count, review_count, rejected_count = 0, 0, 0
    scored_results = []
    rejection_summary = {}

    if not df.empty:
        ref_date = pd.to_datetime(df['parsed_date'], errors='coerce').max()
        ref_date = ref_date if pd.notna(ref_date) else datetime.now()

        for _, row in df.iterrows():
            score = 0.0
            reasoning = []
            
            size = row.get('company_size')
            if pd.isna(size) or str(size).strip().upper() in ['NA', '', 'NONE']:
                reasoning.append("Missing company size")
            else:
                try:
                    s_val = float(size)
                    score += 3.0 if 11 <= s_val <= 5000 else 1.5
                except ValueError:
                    reasoning.append("Invalid company size format")

            industry = str(row.get('industry', '')).strip().title()
            tier1 = ['Saas', 'Software', 'Finance', 'Fintech', 'Healthcare', 'Cybersecurity', 'Analytics']
            tier2 = ['Retail', 'Logistics', 'Manufacturing', 'Edtech', 'Consulting', 'Insurance']
            
            if not industry or industry == 'Nan':
                reasoning.append("Missing industry")
            elif industry in tier1:
                score += 3.0
            elif industry in tier2:
                score += 2.0
            else:
                score += 1.0

            source = str(row.get('source', '')).strip().title()
            if any(hi in source for hi in ['Inbound Demo Request', 'Referral', 'Sales Call']):
                score += 2.0
            elif any(mi in source for mi in ['Content Download', 'Webinar Attendee']):
                score += 1.0
            else:
                score += 0.5

            try:
                dt = pd.to_datetime(str(row.get('last_interaction_date', '')).strip())
                days_ago = (ref_date - dt).days
                if days_ago <= 60:
                    score += 2.0
                elif days_ago <= 180:
                    score += 1.0
                else:
                    reasoning.append(f"Stale engagement ({days_ago} days ago)")
            except Exception:
                reasoning.append("Invalid or missing date")

            has_missing = pd.isna(row.get('company_size')) or pd.isna(row.get('industry')) or pd.isna(row.get('source')) or pd.isna(row.get('last_interaction_date'))
            
            if score >= 7.0 and not has_missing:
                decision, priority = "QUALIFIED", "HIGH"
                qualified_count += 1
            elif score >= 4.0:
                decision, priority = "REVIEW", "MEDIUM"
                review_count += 1
            else:
                decision, priority = "REJECTED", "LOW"
                rejected_count += 1

            for r in reasoning:
                rejection_summary[r] = rejection_summary.get(r, 0) + 1

            # Generate outreach message if qualified
            name = row.get('name', 'Valued Customer')
            company = row.get('company', 'Your Company')
            ind = industry
            src = source.lower()
            hook = "Following up on your recent demo request," if 'demo' in src else "I came across your profile and"
            outreach_msg = f"Hi {name}, {hook} I noticed {company} is making strong moves in {ind}. We help growing teams automate workflow bottlenecks. Open to a 10-minute intro call?" if decision == "QUALIFIED" else ""

            scored_results.append({
                "name": name if pd.notna(name) else "Unknown",
                "company": company if pd.notna(company) else "Unknown",
                "industry": industry,
                "source": source,
                "company_size": row.get('company_size') if pd.notna(row.get('company_size')) else "N/A",
                "score": round(score, 1),
                "decision": decision,
                "priority": priority,
                "reasoning": "; ".join(reasoning) if reasoning else "Met target SMB/Enterprise profile criteria",
                "outreach_message": outreach_msg
            })

    scored_df = pd.DataFrame(scored_results)
    con.execute("DROP TABLE IF EXISTS scored_leads")
    con.execute("CREATE TABLE scored_leads AS SELECT * FROM scored_df")

    pipeline_summary = [{
        "total_leads": len(df),
        "qualified_count": qualified_count,
        "review_count": review_count,
        "rejected_count": rejected_count,
        "top_disqualification_reasons": json.dumps(rejection_summary),
        "processed_timestamp": datetime.now().isoformat()
    }]
    summary_df = pd.DataFrame(pipeline_summary)
    con.execute("DROP TABLE IF EXISTS pipeline_summary")
    con.execute("CREATE TABLE pipeline_summary AS SELECT * FROM summary_df")
    
    con.close()
    print(f"[Score] Scored {len(scored_df)} leads and saved state to DuckDB.")

def export_report_to_json(**context):
    project_root, db_path, _ = get_project_paths()
    
    con = duckdb.connect(db_path)
    scored_df = con.execute("SELECT * FROM scored_leads").fetchdf()
    summary_df = con.execute("SELECT * FROM pipeline_summary").fetchdf()
    con.close()

    summary_data = summary_df.to_dict(orient='records')[0] if not summary_df.empty else {}
    if "top_disqualification_reasons" in summary_data and isinstance(summary_data["top_disqualification_reasons"], str):
        try:
            summary_data["top_disqualification_reasons"] = json.loads(summary_data["top_disqualification_reasons"])
        except Exception:
            pass
    
    leads = scored_df.to_dict(orient='records') if not scored_df.empty else []
    
    current_time_iso = datetime.now().isoformat()
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    summary_data["source_file"] = CENTRAL_CSV_PATH
    
    report_data = {
        "metadata": {
            "processed_at": current_time_iso,
            "processed_timestamp": current_time_iso,
            "source_csv": CENTRAL_CSV_PATH,
            "total_records": len(leads)
        },
        "summary": summary_data,
        "sample_outreach_messages": [l['outreach_message'] for l in leads if l.get('outreach_message')][:5],
        "leads": leads
    }
    
    # Generate timestamped filename for instant VS Code workspace inspection
    timestamped_filename = f"output_{timestamp_str}.json"
    target_paths = [
        timestamped_filename,
        os.path.join(project_root, timestamped_filename),
        os.path.join("/usr/local/airflow", timestamped_filename)
    ]
    
    for path in target_paths:
        try:
            parent_dir = os.path.dirname(path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
                
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2)
            print(f"[Export] Successfully generated timestamped report at: {os.path.abspath(path)}")
        except Exception as e:
            print(f"[Export] Error writing to {path}: {e}")

def ai_explain_pipeline_results(**context):
    _, db_path, _ = get_project_paths()
    
    # Read metrics directly from DuckDB storage
    con = duckdb.connect(db_path)
    summary_df = con.execute("SELECT * FROM pipeline_summary").fetchdf()
    con.close()
    
    summary_data = summary_df.to_dict(orient='records')[0] if not summary_df.empty else {}
    
    total_leads = summary_data.get('total_leads', 0)
    qualified_count = summary_data.get('qualified_count', 0)
    review_count = summary_data.get('review_count', 0)
    rejected_count = summary_data.get('rejected_count', 0)
    timestamp = summary_data.get('processed_timestamp', datetime.now().isoformat())

    slack_message = (
        f"📊 *Sales Pipeline Execution Report (DuckDB Backend)*\n"
        f"⏰ Processed At: *{timestamp}*\n"
        f"✅ Qualified: *{qualified_count}/{total_leads}*\n"
        f"⚠️ Review Needed: *{review_count}*\n"
        f"❌ Rejected: *{rejected_count}*\n"
        f"📁 Source CSV: `{CENTRAL_CSV_PATH}`"
    )

    context['ti'].xcom_push(key='ai_summary_message', value=slack_message)
    return slack_message

def send_sales_slack_alert(**context):
    ti = context['ti']
    message = ti.xcom_pull(key='ai_summary_message', task_ids='ai_explain_task')
    if not message:
        return
    try:
        from airflow.models import Variable
        webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    except Exception:
        webhook_url = None

    if webhook_url:
        requests.post(webhook_url, json={"text": message})
    else:
        print(f"[Slack Simulated Webhook Payload]:\n{message}")

with DAG(
    'lead_qualification_pipeline',
    default_args=default_args,
    description='DuckDB-Backed Lead Qualification Pipeline with Centralized CSV & Timestamped Reports',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'lead-scoring', 'duckdb', 'centralized-csv'],
) as dag:

    health_task = PythonOperator(task_id='health_check_task', python_callable=check_pipeline_health)
    ingest_task = PythonOperator(task_id='ingest_leads_task', python_callable=ingest_leads_to_db)
    score_task = PythonOperator(task_id='score_leads_task', python_callable=evaluate_and_score_leads)
    export_task = PythonOperator(task_id='export_report_task', python_callable=export_report_to_json)
    ai_explain_task = PythonOperator(task_id='ai_explain_task', python_callable=ai_explain_pipeline_results)
    slack_task = PythonOperator(task_id='slack_alert_task', python_callable=send_sales_slack_alert)

    # Sequential execution pipeline utilizing DuckDB backend state
    health_task >> ingest_task >> score_task >> export_task >> ai_explain_task >> slack_task