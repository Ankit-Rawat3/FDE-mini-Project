# Production Lead Intelligence & Qualification System

An automated, enterprise-grade lead intelligence pipeline designed to solve B2B lead qualification bottlenecks. Built with **Python**, **Apache Airflow**, **DuckDB**, and optional **LLM batch processing**, this system automates the intake, scoring, outreach generation, and reporting for inbound sales leads.

---

## 🌟 Architecture & Key Features

1. **Centralized Configuration:** Define input CSV datasets in a single place (`CENTRAL_CSV_PATH` / `CONFIG_INPUT_CSV`) so data flows seamlessly through both local CLI scripts and containerized DAG runs.
2. **DuckDB Analytical Backend:** Replaces heavy Airflow XCom metadata passing by persisting raw leads, scored leads, and pipeline summaries in an embedded **DuckDB** database (`include/lead_qualification.duckdb`).
3. **Multi-Factor Scoring Engine:** Scores leads from **0.0 to 10.0** based on company size, industry vertical, lead source intent, and interaction recency.
4. **Resilient Local & Pipeline Execution:** Handles missing data (e.g., missing company size, sparse interaction dates) gracefully without crashing.
5. **Timestamped Report Generation:** Outputs pristine `output_report_YYYYMMDD_HHMMSS.json` files directly into your workspace for instant VS Code inspection.
6. **Slack Sales Alerts:** Generates executive summaries dispatched via webhooks.

---

## 🎯 Qualification Rubric (0 – 10 Scale)

Leads are evaluated against four key criteria:

| Criterion | Max Points | Weighting & Logic |
| :--- | :---: | :--- |
| **Company Size** | **3.0 pts** | Target SMB/Mid-market (11–5,000 employees) = **+3.0**; Enterprise ($>5,000$) or Startup ($<11$) = **+1.0**; Missing/NA = **0 pts**. |
| **Industry Alignment** | **3.0 pts** | Tier 1 (SaaS, FinTech, Healthcare, Cybersecurity, Analytics) = **+3.0**; Tier 2 (Retail, Logistics, Manufacturing, EdTech) = **+2.0**; Other = **+1.0**. |
| **Source Intent** | **2.0 pts** | High Intent (Inbound Demo Request, Referral, Sales Call) = **+2.0**; Medium Intent (Webinar, Content Download) = **+1.0**; Cold/Other = **+0.5**. |
| **Interaction Recency** | **2.0 pts** | Recent ($\le 60$ days ago) = **+2.0**; Moderately Aged ($\le 180$ days ago) = **+1.0**; Stale ($>180$ days) = **0 pts**. |

### Decision & Routing Rules:
* **`QUALIFIED` (Priority: HIGH):** Score $\ge 7.0$ with zero missing critical fields. Triggers personalized outreach message generation.
* **`REVIEW` (Priority: MEDIUM):** Score $4.0 - 6.9$ or flagged for missing/incomplete data.
* **`REJECTED` (Priority: LOW):** Score $< 4.0$.

---

## 📁 Project Structure

```
lead-qualification-system/
├── dags/
│   └── lead_qualification_pipeline.py  # DuckDB-backed Apache Airflow DAG
├── include/
│   ├── data/
│   │   ├── leads_training.csv          # Training dataset (30 records)
│   │   ├── leads_full_scale_100.csv    # Scale validation dataset (100 records)
│   │   └── leads_testing.csv           # Edge-case testing dataset (20 records)
│   └── lead_qualification.duckdb       # DuckDB analytical state warehouse
├── main.py                             # Standalone local CLI runner
├── docker-compose.yaml                 # Docker Compose setup with volume mounts
├── requirements.txt                    # System dependencies (pandas, duckdb, requests)
└── README.md                           # Documentation
```

---

## 🚀 How to Run

### Option 1: Standalone Local CLI (`main.py`)
Run the standalone CLI tool directly on your host machine to evaluate leads and instantly generate a local report in VS Code:

```bash
# Run using the default training dataset
python main.py

# Or run against any dataset using command-line arguments
python main.py --csv leads_full_scale_100.csv --output output_report.json
```

### Option 2: Apache Airflow Orchestration DAG
1. **Start Airflow locally:**
   ```bash
   astro dev start
   ```
2. Open your browser to `http://localhost:8080`.
3. Locate `lead_qualification_pipeline` and trigger the DAG.
4. Generated timestamped JSON reports (`output_YYYYMMDD_HHMMSS.json`) will automatically sync from the container back to your VS Code project folder.

---

## 📊 Sample Output Format (`output_report_YYYYMMDD_HHMMSS.json`)

```json
{
  "metadata": {
    "processed_at": "2026-10-29T18:50:00.123456",
    "source_csv": "include/data/leads_testing.csv",
    "total_records": 20
  },
  "summary": {
    "total_leads": 20,
    "qualified_count": 8,
    "review_count": 7,
    "rejected_count": 5
  },
  "sample_outreach_messages": [
    "Hi Alice, Following up on your recent demo request, I noticed CloudScale AI is making strong moves in SaaS. We help growing teams automate workflow bottlenecks. Open to a 10-minute intro call?"
  ],
  "leads": [
    {
      "name": "Alice Chen",
      "company": "CloudScale AI",
      "score": 8.0,
      "decision": "QUALIFIED",
      "reasoning": "Company size 50 is in target SMB/Mid-market range (3 pts); Industry 'SaaS' is Tier-1 high fit (3 pts); Source 'Inbound Demo Request' is high intent (2 pts)",
      "outreach_message": "Hi Alice, Following up on your recent demo request..."
    }
  ]
}
```

---

## 🛠️ Configuration Options

* **Central CSV Selection:** Modify `CENTRAL_CSV_PATH` at the top of `dags/lead_qualification_pipeline.py` or `main.py` to target `leads_training.csv`, `leads_full_scale_100.csv`, or `leads_testing.csv`.
* **Slack Webhook Integration:** Set the `SLACK_WEBHOOK_URL` Airflow Variable to enable live channel notifications upon pipeline completion.