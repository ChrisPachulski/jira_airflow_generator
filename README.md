# Airflow DAG Generation from Templated Jira Tickets

This repository demonstrates a streamlined approach to automatically generate Airflow DAG configurations from Jira tickets. It pulls custom data fields from Jira API, parses scheduling information, builds SQL statements dynamically, and outputs:

A JSON config (to be included in an Airflow DAG).

A sample CSV report representing what the DAG would produce.

--------------------------------------------------------------------------------
TABLE OF CONTENTS
1. [Project Structure](#project-structure) 
2. [Installation & Setup](#installation--setup) 
3. [Usage Example](#usage-example)
4. [How It Work](#how-it-works)
   - [Jira Client](#1-jira-client)
   - [Scheduler](#2-scheduler)
   - [SQL Validations](#3-sql-validations)
   - [DAG Generation](#4-dag-generation)

--------------------------------------------------------------------------------
## PROJECT STRUCTURE
```bash
airflow_report_generator/
├─ __init__.py
├─ jira_client.py        
├─ schedule_parser.py    
├─ sql_generator.py      
└─ advertiser_report_generator.py
```

--------------------------------------------------------------------------------
## INSTALLATION & SETUP
1) Install dependencies (example with pip):
   pip install -r requirements.txt
   or 
   pip install .

2) Set environment variables as needed for:
   - Database connection (CLICKHOUSE_HOST in these instances)
   - Jira credentials (can also be managed via a jira_credential.py script or environment vars).

--------------------------------------------------------------------------------
## USAGE EXAMPLE

### Example: main.py
```python
from airflow_report_generator.advertiser_report_generator import AdvertiserReportGenerator

def main():
    generator = AdvertiserReportGenerator()
    dag_json_str, sample_df = generator.generate_json_and_sample(
        ticket_id="AD-378",
        json_path="my_dag_config.json",
        csv_path="sample_report.csv"
    )

    print("Generated DAG JSON:\n", dag_json_str)
    print("Sample Report (first few rows):\n", sample_df.head())

if __name__ == "__main__":
    main()
```
- Running:
  python main.py
- This will:
  1) Pull Jira data for "AD-378".
  2) Parse schedule & timespan from the ticket’s description fields.
  3) Generate a .json file (my_dag_config.json) with an Airflow-ready config.
  4) Create a sample CSV file (sample_report.csv) showing what the DAG's report would look like.


--------------------------------------------------------------------------------
## HOW IT WORKS

1. Jira Client
   - jira_client.py: 
      - Connects to Jira and retrieves ticket fields: time spans, columns, recipients, etc.

2. Scheduler
   - schedule_parser.py:
      - Interprets text like "Every Monday at 8 AM CST" or "Daily at 5 PM" 
       and converts into a cron expression (e.g., "0 5 * * *").
      - Derives appropriate date ranges (e.g. "Last 7 days", "MTD", etc.).

3. SQL Validations
   - sql_generator.py:
      - Maps user-friendly columns (e.g. "clicks", "impressions") to actual SQL expressions.
      - Builds SELECT statements, GROUP BY clauses, aliases, etc.

4. DAG Generation
   - advertiser_report_generator.py:
      - Orchestrates the overall workflow:
          1) Fetch Jira ticket data.
          2) Parse scheduling info.
          3) Build the final JSON for Airflow (including DAG schedule, query, columns).
          4) Optionally generates a sample CSV for stakeholder review.


