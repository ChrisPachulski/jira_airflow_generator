# airflow_report_generator/sql_generator.py

import pandas as pd
import re

# This mapping can be stored in a separate JSON or config if you want.
MAPPING_DICT = {
    'date': 'event_date',
    'partner id': 'partner_id',
    'ad group name': 'adgroup_name',
    'ad group': 'adgroup_name',
    'adgroup name': 'adgroup_name',
    'adgroup': 'adgroup_name',
    'ad group': 'adgroup_name',
    'adgroup lid': 'adgroup_lid',
    'adgroup hid': 'adgroup_hid',
    'campaign': 'campaign_name',
    'campaign name': 'campaign_name',
    'campaign lid': 'campaign_lid',
    'campaign hid': 'campaign_hid',
    'sid': 'sid',
    'said': 'said',
    'sid-said': 'concat(sid,"~",said)',
    'advertiser name': 'advertiser_name',
    'advertiser': 'advertiser_name',
    'keyword': 'viewed_text',
    'pub name': 'affiliate_account_name',
    'publisher name': 'affiliate_account_name',
    'publisher': 'affiliate_account_name',
    'publisher lid': 'affiliate_account_lid',
    'publisher hid': 'affiliate_account_hid',
    'source name': 'traffic_source_name',
    'source': 'traffic_source_name',
    'traffic source lid': 'traffic_source_lid',
    'traffic source hid': 'traffic_source_hid',
    'cpc': 'CASE WHEN isNaN(round(sum(revenue)/sum(paid_clicks),2)) THEN 0 ELSE round(sum(revenue)/sum(paid_clicks),2) END',
    'cpa': 'CASE WHEN isNaN(round(sum(revenue)/sum(actions_worth), 2)) THEN 0 WHEN isInfinite(round(sum(revenue)/sum(actions_worth), 2)) THEN 0 ELSE round(sum(revenue)/sum(actions_worth), 2) END',
    'ctr': 'COALESCE(ROUND(SUM(paid_clicks)/SUM(impressions), 6), 0)',
    'cvr': 'COALESCE(ROUND(SUM(actions_worth) / SUM(paid_clicks), 6), 0)',
    'clicks': 'COALESCE(sum(paid_clicks),0)',
    'conversions': 'COALESCE(sum(event_fires),0)',
    'reservations': 'COALESCE(sum(event_fires),0)',
    'actions': 'COALESCE(sum(actions_worth),0)',
    'conversion rate': 'round(sum(actions_worth)/sum(paid_clicks), 4)',
    'conversion value': 'round(COALESCE(sum(dollars_worth),0),2)',
    'impressions': 'round(COALESCE(sum(impressions),0),0)',
    'imps': 'round(COALESCE(sum(impressions),0),0)',
    'spend': 'round(COALESCE(sum(revenue),0),2)',
    'cost': 'round(COALESCE(sum(revenue),0),2)',
    'revenue': 'round(COALESCE(sum(dollars_worth),0),2)',
    'inquiries': 'round(COALESCE(sum(actions_worth),0),2)',
    'purchases': 'round(COALESCE(sum(event_fires),0),2)'
}

def generate_selects(main_text: str) -> str:
    """
    Generates the single SQL snippet based on the main_text using MAPPING_DICT.

    Args:
        main_text (str): The field name to map (e.g. 'clicks', 'date', 'publisher name').

    Returns:
        str: Corresponding column or expression for the SQL statement.
    """
    key = main_text.strip().lower()
    return MAPPING_DICT.get(key, None)

def add_backticks(value: str) -> str:
    """
    If the column name has spaces, surround it with backticks.
    E.g. 'Publisher Name' -> '`Publisher Name`'.
    
    Args:
        value (str): The column name.

    Returns:
        str: Possibly backtick-surrounded name.
    """
    if ' ' in value:
        return f'`{value}`'
    return value

def sql_statement_generator(dataframe: pd.DataFrame):
    """
    Generates all needed elements for SQL generation in actual DAG and sample report.

    Args:
        dataframe (pd.DataFrame): The exploded dataframe from `get_jira_ticket_info`.

    Returns:
        list: 
            [ 
              0: Column Renaming for DAG (as_column|Real Name),
              1: Select & From for DAG Query (built on top of `generate_selects`),
              2: lid,
              3: hid,
              4: Group By str,
              5: Sample Report base SQL with stakeholder column names
            ]
    """
    # Clean & split the 'sql_select_columns'
    sql_column = (
        dataframe['sql_select_columns']
        .str.lower()
        .str.split(r'\n|\, and |\, |,')
        .explode('text_column')
    )

    select_df = pd.DataFrame()
    # Extract main_text and info using potential patterns
    select_df[['main_text_1', 'info_1']] = sql_column.str.extract(r'^(.*?)(?:\s*-\s*|\s*=\s*(.*))?$')
    select_df[['main_text_2', 'info_2']] = sql_column.str.extract(r'^([^()]+)\s*\(([^()]+)\)$')

    select_df['main_text'] = select_df['main_text_1'].fillna(select_df['main_text_2'])
    select_df['info'] = select_df['info_1'].fillna(select_df['info_2'])

    # Drop temp columns
    select_df.drop(['main_text_1', 'info_1', 'main_text_2', 'info_2'], axis=1, inplace=True)

    # Where main_text is still null, fill with original
    select_df['main_text'] = select_df['main_text'].fillna(sql_column)
    
    # Title-case or capitalize smaller words
    def _transform_text(x: str) -> str:
        return ' '.join([word.capitalize() if len(word) <= 3 else word.title()
                         for word in x.split()])
    select_df['modified_main_text'] = select_df['main_text'].apply(_transform_text)

    # Filter out empty rows
    select_df = select_df[select_df['main_text'] != '']

    # Generate the actual SQL snippet
    select_df['selects_for_sql'] = select_df['main_text'].apply(generate_selects)
    
    # Build aliases
    select_df['aliases_for_sql'] = (
        select_df['main_text'].str.replace(' ', '_') + '_as_column'
    )
    
    select_df['json_col_names'] = (
        select_df['aliases_for_sql'] + '|' + select_df['modified_main_text']
    )

    select_df['final_sql_select_column'] = (
        select_df['selects_for_sql'].fillna('')
        + ' as '
        + select_df['aliases_for_sql']
    )

    # For the sample report, ensure we backtick real column headings
    select_df['sample_main_text'] = select_df['modified_main_text'].apply(add_backticks)

    select_df['sample_report_selects'] = (
        select_df['selects_for_sql'].fillna('')
        + ' as '
        + select_df['sample_main_text']
    )

    # Build strings
    melted_column_names = ', '.join(select_df['json_col_names'].tolist())
    melted_base_sql = (
        f"SELECT {', '.join(select_df['final_sql_select_column'].tolist())} "
        f"FROM ad_event_view "
    )

    # We assume there's only one row in the input df for 'lid_hid'
    lid = dataframe['lid_hid'].str.split('~', expand=True)[0].iloc[0]
    hid = dataframe['lid_hid'].str.split('~', expand=True)[1].iloc[0]

    # Group by columns: exclude aggregate expressions
    aggregate_keywords = ['sum', 'count', 'min', 'max']
    group_by_df = select_df[
        ~select_df['selects_for_sql'].str.contains('|'.join(aggregate_keywords), case=False, na=False)
    ]
    melted_group_by = ', '.join(group_by_df['selects_for_sql'].dropna().tolist())

    sample_melted_base_sql = (
        f"SELECT {', '.join(select_df['sample_report_selects'].tolist())} "
        f"FROM ad_event_view "
    )

    return [
        melted_column_names,      # 0
        melted_base_sql,          # 1
        lid,                      # 2
        hid,                      # 3
        melted_group_by,          # 4
        sample_melted_base_sql    # 5
    ]

