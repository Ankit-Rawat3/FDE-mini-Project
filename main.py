"""
Production Lead Intelligence System - Standalone CLI
- Single configuration point for the input CSV file.
- Single output JSON report featuring an automated timestamp in its filename.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime

# ==========================================
CONFIG_INPUT_CSV = "leads_testing.csv"
# ==========================================

def evaluate_lead(row, ref_date):
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
        reasoning.append("Invalid or missing interaction date")

    has_missing = pd.isna(row.get('company_size')) or pd.isna(row.get('industry')) or pd.isna(row.get('source')) or pd.isna(row.get('last_interaction_date'))
    
    if score >= 7.0 and not has_missing:
        decision, priority = "QUALIFIED", "HIGH"
    elif score >= 4.0:
        decision, priority = "REVIEW", "MEDIUM"
    else:
        decision, priority = "REJECTED", "LOW"

    return {
        "name": str(row.get('name', 'Unknown')) if pd.notna(row.get('name')) else "Unknown",
        "company": str(row.get('company', 'Unknown')) if pd.notna(row.get('company')) else "Unknown",
        "company_size": row.get('company_size') if pd.notna(row.get('company_size')) else "N/A",
        "industry": industry, "source": source,
        "score": round(score, 1), "decision": decision, "priority": priority,
        "reasoning": "; ".join(reasoning) if reasoning else "Met target SMB/Enterprise profile criteria"
    }

def generate_outreach_message(lead):
    if lead['decision'] != "QUALIFIED":
        return ""
    name, company, industry = lead['name'], lead['company'], lead['industry']
    hook = "Following up on your recent demo request," if 'demo' in lead['source'].lower() else "I came across your profile and"
    return f"Hi {name}, {hook} I noticed {company} is scaling its footprint in {industry}. We help growing teams automate workflow bottlenecks and cut sales cycle times by 30%. Would you be open to a 10-minute intro call?"

def process_leads(csv_path):
    # Robust path resolution for input CSV
    resolved_csv_path = csv_path
    if not os.path.exists(resolved_csv_path):
        if os.path.exists(os.path.join("include", "data", resolved_csv_path)):
            resolved_csv_path = os.path.join("include", "data", resolved_csv_path)
        else:
            print(f"Error: Input CSV file '{csv_path}' not found.")
            sys.exit(1)
            
    print(f"[Ingest] Reading CSV file: {resolved_csv_path}")
    df = pd.read_csv(resolved_csv_path)
    df['parsed_date'] = pd.to_datetime(df['last_interaction_date'], errors='coerce')
    ref_date = df['parsed_date'].max() if pd.notna(df['parsed_date'].max()) else datetime.now()

    results = []
    for _, row in df.iterrows():
        res = evaluate_lead(row, ref_date)
        res['outreach_message'] = generate_outreach_message(res)
        results.append(res)

    current_iso = datetime.now().isoformat()
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate output filename containing the timestamp (e.g., output_20261029_164530.json)
    output_filename = f"output_{CONFIG_INPUT_CSV}_{timestamp_str}.json"

    report = {
        "metadata": {
            "processed_timestamp": current_iso,
            "source_csv": resolved_csv_path,
            "total_records": len(df)
        },
        "summary": {
            "total_leads": len(df),
            "qualified": sum(1 for r in results if r['decision'] == 'QUALIFIED'),
            "review": sum(1 for r in results if r['decision'] == 'REVIEW'),
            "rejected": sum(1 for r in results if r['decision'] == 'REJECTED'),
            "processed_timestamp": current_iso
        },
        "sample_outreach_messages": [r['outreach_message'] for r in results if r['outreach_message']][:5],
        "leads": results
    }

    # Save to the timestamped JSON report file
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[Export] Processed {len(df)} leads successfully.")
    print(f"[Export] Generated timestamped report: '{output_filename}'")

if __name__ == "__main__":
    process_leads(CONFIG_INPUT_CSV)