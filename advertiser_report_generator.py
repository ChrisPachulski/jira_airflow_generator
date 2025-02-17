# airflow_report_generator/advertiser_report_generator.py

import os
import re
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from clickhouse_driver import Client

# Local imports
from .jira_client import get_jira_ticket_info
from .sql_generator import sql_statement_generator
from .schedule_parser import (
    discover_timespan,
    discover_cron,
    timedelta_calculator
)


class AdvertiserReportGenerator:
    """
    Main orchestrator class. Ties together Jira fetching, schedule parsing,
    SQL generation, and DAG JSON generation.
    """

    def __init__(self, clickhouse_host: str = None, db_name: str = 'addotnet'):
        """
        Optionally pass clickhouse connection info.
        """
        # e.g., read from environment or passed arguments
        self.clickhouse_host = clickhouse_host or os.environ.get('CLICKHOUSE_HOST', 'localhost')
        self.db_name = db_name
        self.client = Client(self.clickhouse_host, database=self.db_name)
    
    def get_advertiser_name(self, lid_hid: pd.Series) -> str:
        """
        Pull advertiser name from either ad_event_view (for today's date) 
        or fallback to advertiser_dim.

        Args:
            lid_hid (pd.Series): 'lid_hid' column from the Jira DF
        Returns:
            str: Cleaned advertiser name (title-cased, underscores for spaces, etc.)
        """
        lid = lid_hid.str.split('~', expand=True)[0].iloc[0]
        hid = lid_hid.str.split('~', expand=True)[1].iloc[0]
        todays_date = datetime.today().strftime('%Y-%m-%d')

        sql_ad_event = f"""
            SELECT DISTINCT advertiser_name
            FROM ad_event_view
            WHERE event_date = '{todays_date}'
              AND advertiser_lid = {lid}
              AND advertiser_hid = {hid}
        """
        advertiser_name_df = self.client.query_dataframe(sql_ad_event)

        # Fallback if no results
        if advertiser_name_df.empty:
            sql_advertiser_dim = f"""
                SELECT DISTINCT advertiser_name
                FROM advertiser_dim
                WHERE advertiser_hid = {hid}
            """
            advertiser_name_df = self.client.query_dataframe(sql_advertiser_dim)
        
        if not advertiser_name_df.empty:
            name_str = advertiser_name_df['advertiser_name'].iloc[0]
            # Format e.g. "Acme Dot Com" -> "Acme.com" -> "Acme_com"
            name_str = name_str.title().replace('dotcom', '.com').replace(' ', '_')
            return name_str
        else:
            return "Unknown_Advertiser"

    def assemble_int_dag_json(
        self,
        delivery: str,
        recipients: str,
        advertiser_input: str,
        time_span: int,
        cron_job: str,
        sql_elements: list
    ) -> str:
        """
        For integer-based timespans (e.g., last X days).
        """
        timedelta_value = timedelta_calculator(cron_job)

        data = f'''
        {{
          "export_setting": {{
            "export_type": "{delivery}",
            "mail_to": "{recipients}, chris.pachulski@ad.net",
            "attachment_name": "adnet_{advertiser_input}_${{start_date}}_${{end_date}}.csv",
            "export_dest_dir": "",
            "mail_subject": "Ad.net - {advertiser_input.replace('_'," ")} {timedelta_value[0]} Report",
            "export_password": "",
            "export_url": "",
            "mail_from": "NO-REPLY@ad.net",
            "mail_body": "Stats attached.",
            "export_username": ""
          }},
          "report_definition": {{
            "reportType": "SCRIPTED",
            "quotechar": "\\"",
            "report_for_past_n_days": {time_span},
            "escapechar": "\\"",
            "columns": "{sql_elements[0]}",
            "calculateTotals": false,
            "lowerCtr": "",
            "upperCtr": "",
            "separator": ",",
            "lineEnd": "\\n",
            "scriptedTableReport": {{
                "database": "CLICKHOUSE",
                "scriptedSql": "{sql_elements[1]} where event_date BETWEEN :startDate AND :endDate AND (advertiser_lid = {sql_elements[2]} AND advertiser_hid = {sql_elements[3]}) GROUP BY {sql_elements[4]} ORDER BY event_date desc"
            }}
          }},
          "external_dependency": {{
            "timedelta": {{
                "hours": {timedelta_value[1]},
                "minutes": {timedelta_value[2]}
            }},
            "task_id": "process_completed_notification",
            "dag_id": "traffic-server-etl-daily"
          }},
          "schedule_interval": "{cron_job}"
        }}
        '''
        return data

    def assemble_str_dag_json(
        self,
        delivery: str,
        recipients: str,
        advertiser_input: str,
        time_span: str,
        cron_job: str,
        sql_elements: list
    ) -> str:
        """
        For string-based timespans, e.g. '2025-01-15', 'toStartOfMonth(today())', etc.
        """
        timedelta_value = timedelta_calculator(cron_job)

        data = f'''
        {{
          "export_setting": {{
            "export_type": "{delivery}",
            "mail_to": "{recipients}, chris.pachulski@ad.net",
            "attachment_name": "adnet_{advertiser_input}_{time_span}_${{end_date}}.csv",
            "export_dest_dir": "",
            "mail_subject": "Ad.net - {advertiser_input.replace('_'," ")} {timedelta_value[0]} Report",
            "export_password": "",
            "export_url": "",
            "mail_from": "NO-REPLY@ad.net",
            "mail_body": "Stats attached.",
            "export_username": ""
          }},
          "report_definition": {{
            "reportType": "SCRIPTED",
            "quotechar": "\\"",
            "report_for_past_n_days": 1,
            "escapechar": "\\"",
            "columns": "{sql_elements[0]}",
            "calculateTotals": false,
            "lowerCtr": "",
            "upperCtr": "",
            "separator": ",",
            "lineEnd": "\\n",
            "scriptedTableReport": {{
                "database": "CLICKHOUSE",
                "scriptedSql": "{sql_elements[1]} where event_date BETWEEN '{time_span}' AND :endDate AND (advertiser_lid = {sql_elements[2]} AND advertiser_hid = {sql_elements[3]}) GROUP BY {sql_elements[4]} ORDER BY event_date desc"
            }}
          }},
          "external_dependency": {{
            "timedelta": {{
                "hours": {timedelta_value[1]},
                "minutes": {timedelta_value[2]}
            }},
            "task_id": "process_completed_notification",
            "dag_id": "traffic-server-etl-daily"
          }},
          "schedule_interval": "{cron_job}"
        }}
        '''
        return data

    def assemble_default_dag_json(
        self,
        delivery: str,
        recipients: str,
        advertiser_input: str,
        time_span: str,
        cron_job: str,
        sql_elements: list
    ) -> str:
        """
        For special placeholders like MTD or YTD, or fallback to standard startDate/endDate usage.
        """
        timedelta_value = timedelta_calculator(cron_job)

        if time_span == 'toStartOfMonth(today())':
            display_span = 'MTD'
            subject_time = f'{display_span} Report'
            date_filter = "toStartOfMonth(today()) AND :endDate"
        elif time_span == 'toStartOfYear(today())':
            display_span = 'YTD'
            subject_time = f'{display_span} Report'
            date_filter = "toStartOfYear(today()) AND :endDate"
        else:
            display_span = time_span
            subject_time = f'{timedelta_value[0]} Report'
            date_filter = ":startDate AND :endDate"

        data = f'''
        {{
          "export_setting": {{
            "export_type": "{delivery}",
            "mail_to": "{recipients}, chris.pachulski@ad.net",
            "attachment_name": "adnet_{advertiser_input}_{display_span}_${{end_date}}.csv",
            "export_dest_dir": "",
            "mail_subject": "Ad.net - {advertiser_input.replace('_'," ")} {subject_time}",
            "export_password": "",
            "export_url": "",
            "mail_from": "NO-REPLY@ad.net",
            "mail_body": "Stats attached.",
            "export_username": ""
          }},
          "report_definition": {{
            "reportType": "SCRIPTED",
            "quotechar": "\\"",
            "report_for_past_n_days": 1,
            "escapechar": "\\"",
            "columns": "{sql_elements[0]}",
            "calculateTotals": false,
            "lowerCtr": "",
            "upperCtr": "",
            "separator": ",",
            "lineEnd": "\\n",
            "scriptedTableReport": {{
                "database": "CLICKHOUSE",
                "scriptedSql": "{sql_elements[1]} where event_date BETWEEN {date_filter} AND (advertiser_lid = {sql_elements[2]} AND advertiser_hid = {sql_elements[3]}) GROUP BY {sql_elements[4]} ORDER BY event_date desc"
            }}
          }},
          "external_dependency": {{
            "timedelta": {{
                "hours": {timedelta_value[1]},
                "minutes": {timedelta_value[2]}
            }},
            "task_id": "process_completed_notification",
            "dag_id": "traffic-server-etl-daily"
          }},
          "schedule_interval": "{cron_job}"
        }}
        '''
        return data

    def export_json(self, airflow_dag: str, directory: str) -> None:
        """
        Write the assembled JSON to a local file, and also print it in a clean format.
        """
        json_output = airflow_dag.replace('\n', '').replace('    ', '')
        with open(directory, 'w') as json_file:
            json.dump(json_output, json_file, ensure_ascii=False, indent=4)
        print(json_output)

    def sample_report_generator(
        self, 
        sql_elements: list, 
        time_span, 
        directory: str
    ) -> pd.DataFrame:
        """
        Generates a sample CSV report for stakeholder validation by querying ClickHouse.
        """
        sql_base = sql_elements[5]
        lid = sql_elements[2]
        hid = sql_elements[3]
        group_by = sql_elements[4]

        # Determine start/end for queries
        if isinstance(time_span, int):
            start_date = (datetime.today() - timedelta(days=time_span)).strftime('%Y-%m-%d')
        elif isinstance(time_span, str) and re.search(r'\d{4}-\d{2}-\d{2}', time_span):
            start_date = pd.to_datetime(time_span).strftime('%Y-%m-%d')
        else:
            # fallback if not recognized
            start_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')

        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        # Build final query
        new_sql = f"""
            {sql_base}
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
              AND advertiser_lid = {lid} 
              AND advertiser_hid = {hid}
            GROUP BY {group_by}
            ORDER BY event_date desc
        """

        sample_report_df = self.client.query_dataframe(new_sql)

        # Rename columns for readability
        sample_report_df.columns = [
            col.replace('_', ' ') for col in sample_report_df.columns
        ]

        sample_report_df.to_csv(directory, index=False)
        return sample_report_df
    
    def generate_json_and_sample(
        self,
        ticket_id: str,
        json_path: str,
        csv_path: str
    ):
        """
        High-level method: fetch Jira data, parse time span & cron,
        build DAG JSON, export JSON, and generate sample CSV.

        Returns:
            tuple: (airflow_dag_string, sample_report_dataframe)
        """
        # 1) Fetch from Jira
        all_data_df = get_jira_ticket_info(ticket_id)

        # 2) Parse columns
        delivery_method = all_data_df['delivery_method'].str.lower().iloc[0]
        recipients = all_data_df['recipient_list'].str.lower().str.replace(';', ',').iloc[0]
        
        # 3) Discover Advertiser
        advertiser_input = self.get_advertiser_name(all_data_df['lid_hid'])

        # 4) Time Span & Cron
        time_span = discover_timespan(all_data_df['time_span'])
        cron_job = discover_cron(all_data_df['period_and_time'])

        # 5) SQL elements
        sql_elements = sql_statement_generator(all_data_df)

        # 6) Assemble DAG JSON
        if isinstance(time_span, int):
            airflow_dag = self.assemble_int_dag_json(
                delivery=delivery_method,
                recipients=recipients,
                advertiser_input=advertiser_input,
                time_span=time_span,
                cron_job=cron_job,
                sql_elements=sql_elements
            )
        elif isinstance(time_span, str) and '-' in time_span:
            airflow_dag = self.assemble_str_dag_json(
                delivery=delivery_method,
                recipients=recipients,
                advertiser_input=advertiser_input,
                time_span=time_span,
                cron_job=cron_job,
                sql_elements=sql_elements
            )
        else:
            airflow_dag = self.assemble_default_dag_json(
                delivery=delivery_method,
                recipients=recipients,
                advertiser_input=advertiser_input,
                time_span=time_span,
                cron_job=cron_job,
                sql_elements=sql_elements
            )

        # 7) Export JSON
        self.export_json(airflow_dag, directory=json_path)

        # 8) Generate sample CSV
        sample_report = self.sample_report_generator(
            sql_elements=sql_elements,
            time_span=time_span,
            directory=csv_path
        )

        return (airflow_dag.replace('\n', '').replace('    ', ''), sample_report)

