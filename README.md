# Airflow DAG Generation from Templated Jira Tickets

This repository demonstrates a streamlined approach to automatically generate Airflow DAG configurations from Jira tickets. It pulls custom data fields from Jira API, parses scheduling information, builds SQL statements dynamically, and outputs:

A JSON config (to be included in an Airflow DAG).
A sample CSV report representing what the DAG would produce.
--------------------------------------------------------------------------------
TABLE OF CONTENTS
1. Project Structure
2. Installation & Setup
3. Usage Example
4. How It Works
5. Testing
6. Contributing
7. License

--------------------------------------------------------------------------------
1. PROJECT STRUCTURE

airflow_report_generator/
├─ __init__.py
├─ jira_client.py        [Jira ticket fetching logic]
├─ schedule_parser.py    [Parsing cron & time range logic]
├─ sql_generator.py      [Building SQL SELECT columns, aliases, etc.]
├─ advertiser_report_generator.py
└─ (other files, e.g. tests/)

--------------------------------------------------------------------------------
2. INSTALLATION & SETUP

1) Clone this repository:
   git clone https://github.com/YourOrg/airflow-advertiser-report-generator.git
   cd airflow-advertiser-report-generator

2) Install dependencies (example with pip):
   pip install -r requirements.txt
   or 
   pip install .

3) Set environment variables as needed for:
   - CLICKHOUSE_HOST (e.g., 'clickhouse://host_or_ip')
   - Jira credentials (can also be managed via a jira_credential.py script or environment vars).

--------------------------------------------------------------------------------
3. USAGE EXAMPLE

# Example: main.py
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

- Running:
  python main.py
- This will:
  1) Pull Jira data for "AD-378".
  2) Parse schedule & timespan from the ticket’s description fields.
  3) Generate a .json file (my_dag_config.json) with an Airflow-ready config.
  4) Create a sample CSV file (sample_report.csv) showing what the DAG's report would look like.

--------------------------------------------------------------------------------
4. HOW IT WORKS

- jira_client.py: 
  - Connects to Jira and retrieves ticket fields: time spans, columns, recipients, etc.

- schedule_parser.py:
  - Interprets text like "Every Monday at 8 AM CST" or "Daily at 5 PM" 
    and converts into a cron expression (e.g., "0 5 * * *").
  - Derives appropriate date ranges (e.g. "Last 7 days", "MTD", etc.).

- sql_generator.py:
  - Maps user-friendly columns (e.g. "clicks", "impressions") to actual SQL expressions.
  - Builds SELECT statements, GROUP BY clauses, aliases, etc.

- advertiser_report_generator.py:
  - Orchestrates the overall workflow:
    1) Fetch Jira ticket data.
    2) Parse scheduling info.
    3) Build the final JSON for Airflow (including DAG schedule, query, columns).
    4) Optionally generates a sample CSV for stakeholder review.

--------------------------------------------------------------------------------
5. TESTING

- Tests (if available) typically reside in a tests/ folder.
- Use pytest or similar:
  python -m pytest tests
