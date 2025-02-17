# airflow_report_generator/jira_client.py
import pandas as pd
import janitor
import re
from atlassian import Jira
import jira_credential  # Ensure your path or import is correct

def get_jira_ticket_info(ticket: str) -> pd.DataFrame:
    """
    Accesses Jira and pulls custom fields designed to make report generation easier.
    
    Args:
        ticket (str): The Jira ticket key (e.g. 'AD-378').

    Returns:
        pd.DataFrame: Columns include:
            - key (ticket id)
            - summary (ticket title)
            - description (ticket body)
            - self (ticket url)
            - time_span (look back period)
            - sql_select_columns (stakeholder desired fields)
            - period_and_time (Frequency and time of day)
            - needs_impression_boolean (TODO: build additional function for impression script generation)
            - delivery_method (Only email accepted as of now)
            - recipient_list 
            - lid_hid
    """
    jira = Jira(
        url="https://ad--dot--net.atlassian.net",
        username=jira_credential.username,
        password=jira_credential.password
    )
    
    JQL = f'key = "{ticket}" ORDER BY created DESC'
    data = jira.jql(JQL, start=0, limit=100)

    # Normalize JSON data
    df_nested_list = pd.json_normalize(data, record_path=["issues"]).clean_names()

    column_mapping = {
        "key": "key",
        "fields_summary": "summary",
        "fields_description": "description",
        "self": "self",
        "fields_customfield_10093": "time_span",
        "fields_customfield_10094": "sql_select_columns",
        "fields_customfield_10095": "period_and_time",
        "fields_customfield_10096": "needs_impression_boolean",
        "fields_customfield_10097": "delivery_method",
        "fields_customfield_10098": "recipient_list",
        "fields_customfield_10099": "lid_hid",
    }

    # Explode attachment (if needed) and select columns
    fields_attachment_exploded_df = df_nested_list.explode("fields_attachment")[
        [
            "key",
            "fields_summary",
            "fields_description",
            "self",
            "fields_customfield_10093",
            "fields_customfield_10094",
            "fields_customfield_10095",
            "fields_customfield_10096",
            "fields_customfield_10097",
            "fields_customfield_10098",
            "fields_customfield_10099",
        ]
    ]

    exploded_df = (
        fields_attachment_exploded_df
        .rename(columns=column_mapping)
        .drop_duplicates()
    )

    # Clean up recipient_list
    exploded_df['recipient_list'] = exploded_df['recipient_list'].str.replace(r'[\n]+', ',', regex=True)
    exploded_df['recipient_list'] = exploded_df['recipient_list'].str.replace(r'\s*,\s*', ',', regex=True)
    exploded_df['recipient_list'] = exploded_df['recipient_list'].str.replace(r',+', ',', regex=True)

    return exploded_df

