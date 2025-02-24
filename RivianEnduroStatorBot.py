import pandas as pd
import os
import requests
import json
import schedule
import time
import pytz
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pyspark.sql import SparkSession
from databricks import sql

# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME = "rivian-prod-us-west-2.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/b050a7573faba9ab"
# DATABRICKS_ACCESS_TOKEN = "dapi0eb9a92a05ca6d1eb0a444b125490361"
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")

# slack_token = "xoxb-2995242172-7567570817185-SBLzNdVOIHNolyQszfZHMBbu"
slack_token = os.getenv("SLACK_TOKEN")
# url = 'https://hooks.slack.com/services/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'
# url = 'https://hooks.slack.com/services/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'
url = os.getenv("URL")

# Slack setup
client = WebClient(token=slack_token)

def send_message_to_slack(channel, text):
    try:
        response = client.chat_postMessage(
            channel=channel,
            text=text
        )
        print(f"Message sent to {channel} with timestamp {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")

def create_databricks_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN
    )

def execute_query(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0].upper() for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)

def job():
    t0 = time.time()
    conn = create_databricks_connection()

    local_tz = pytz.timezone("America/Chicago")  # Change this to your expected timezone
    utc_now = datetime.now(pytz.utc)  # Get current UTC time
    local_now = utc_now.astimezone(local_tz)  # Convert to local timezone

    one_hour_before = datetime.now() - timedelta(hours=1)
    recorded_at = one_hour_before.strftime('%Y-%m-%d %H:00')

    # Define the queries
    query_20 = f"""
    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Assembly error' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description = '{0} Assembly error {1}'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Paperjam: insulating 1' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description = 'Paperjam Station [ __KeyInsulating1 ]'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Paperjam: insulating 2' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description = 'Paperjam Station [ __KeyInsulating2 ]'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Slot Search Fail' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike '%No Slot at Stator detected%'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Post-Forming' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike '%Cylinder [ __KeyPostForming%Cylinder working position%'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '020' as STATION_NAME,
        'Paper Pusher' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-020%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike ('%Axis not in control [ PaperPusher (Z6)%') 
    group by STATION_NAME
    """

    query_40 = f"""
    SELECT COUNT(distinct product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '040'
    AND PARAMETER_NAME = 'Force process value'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    GROUP BY STATION_NAME, PARAMETER_NAME
    """

    query_50 = f"""
    select 
        COUNT(*) as COUNT,
        '050' as STATION_NAME,
        'Assembly error' as PARAMETER_NAME
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-050%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike '%Assembly error%'
    group by STATION_NAME

    union all

    select 
        COUNT(*) as COUNT,
        '050' as STATION_NAME,
        'Check Plate Fails' as alarm_description
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-050%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike ('%Assembly error%Task[301]%')
    group by STATION_NAME
    """
    
    query_60 = f"""
    select
        COUNT(*) as COUNT,
        '060' as STATION_NAME,
        'Bad Cuts/Welding Fail' as ALARM_DESCRIPTION
    from manufacturing.drive_unit.fct_du02_scada_alarms
    where alarm_source_scada_short_name ilike '%STTR01-060%'
    and activated_at > '{recorded_at}'
    and alarm_priority_desc in ('high', 'critical')
    and alarm_description ilike '%Assembly error :%'
    group by STATION_NAME
    """
    
    query_65 = f"""
    SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '065'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
        (PARAMETER_NAME = 'Value Height Pin X' AND (parameter_value_num < 39 OR parameter_value_num > 44.7)) OR
        (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_num < 5000 OR parameter_value_num > 12000)) OR
        (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_num < 2.6 OR parameter_value_num > 3.8)) OR
        (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_num < 1.2 OR parameter_value_num > 3.0)) OR
        (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_num < 13 OR parameter_value_num > 45)) OR
        (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_num < -45 OR parameter_value_num > -13)) OR
        (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_num < 0 OR parameter_value_num > 0.6)) OR
        (PARAMETER_NAME = 'Value Pin 1 edge to stack edge' AND (parameter_value_num < 33.580 OR parameter_value_num > 42.300)) OR
        (PARAMETER_NAME = 'Value Pin 5 edge to stack edge' AND (parameter_value_num < 4.5 OR parameter_value_num > 12.50))
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    ORDER BY COUNT DESC
    """

    query_100 = f"""
    SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '100'
    AND recorded_at > '{recorded_at}'
    AND (
        (PARAMETER_NAME = 'Value height pin x' AND (parameter_value_num < 47.500 OR parameter_value_num > 50.800)) OR
        (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_num < 1.1 OR parameter_value_num > 2.5)) OR
        (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_num < 1.850 OR parameter_value_num > 4)) OR
        (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_num < 2700 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_num < 15 OR parameter_value_num > 40)) OR
        (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_num < -40 OR parameter_value_num > -15)) OR
        (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_num < 0 OR parameter_value_num > 0.95)) OR
        (PARAMETER_NAME ILIKE '%defect%' AND (parameter_value_num < 0 OR parameter_value_num > 0.4)) OR
        (PARAMETER_NAME ILIKE '%OD Outer Winding Grayscale%' AND parameter_value_num != 0) OR
        (PARAMETER_NAME ILIKE 'Value Label Label OD Winding Grayscale' AND parameter_value_num != 0) OR
        (PARAMETER_NAME ILIKE 'Value Label OD Winding Grayscale' AND parameter_value_num != 0) OR
        (PARAMETER_NAME ILIKE 'Value Label OD Pins Grayscale' AND parameter_value_num != 0) OR
        (PARAMETER_NAME ILIKE 'Value Label ID' AND parameter_value_num != 0)
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    ORDER BY COUNT DESC
    """

    query_110 = f"""
    SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '110'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
        (PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) OR
        (PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) OR
        (PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) OR
        (PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) OR
        (PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) OR
        (PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) OR
        (PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) OR
        (PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) OR
        (PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000))
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    """

    query_210 = f"""
    SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '210'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
        (PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) OR
        (PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -10 OR parameter_value_num > 3)) OR
        (PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) OR
        (PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) OR
        (PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 33.580 OR parameter_value_num > 42.300)) OR
        (PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) OR
        (PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) OR
        (PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 0 OR parameter_value_num > 9.9)) OR
        (PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 0 OR parameter_value_num > 9.9)) OR
        (PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 0 OR parameter_value_num > 9.9)) OR
        (PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) OR
        (PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000))
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    ORDER BY COUNT DESC
    """


    # Execute queries and fetch data into DataFrames
    df_20 = pd.read_sql(query_20, conn)
    df_40 = pd.read_sql(query_40, conn)
    df_50 = pd.read_sql(query_50, conn)
    df_60 = pd.read_sql(query_60, conn)
    df_65 = pd.read_sql(query_65, conn)
    df_100 = pd.read_sql(query_100, conn)
    df_110 = pd.read_sql(query_110, conn)
    df_210 = pd.read_sql(query_210, conn)

    # Combine DataFrames
    df_combined = pd.concat([df_20, df_40, df_50, df_60, df_65, df_100, df_110, df_210], ignore_index=True)

    # Sort combined DataFrame by 'COUNT' column
    if 'COUNT' in df_combined.columns:
        df_combined = df_combined.sort_values(['COUNT'], ascending=False, ignore_index=True)

    # print(df_combined)
    # print(df_combined.columns)


    # Sum the COUNTs per STATION_NAME
    # df_sum = df_combined.groupby('STATION_NAME')['COUNT'].sum().reset_index()
    df_sum = df_combined.groupby(df_combined.columns[df_combined.columns.str.upper() == "STATION_NAME"][0])['COUNT'].sum().reset_index()


    # Sort combined DataFrame by 'COUNT' column
    df_sum = df_sum.sort_values(['COUNT'], ascending=False, ignore_index=True)

    # print(df_sum)

    # Convert DataFrames to a JSON-like format (table-like string)
    def df_to_table(df):
        table_str = df.to_string(index=False)
        return "```" + table_str + "```"

    df_combined_str = df_to_table(df_combined)
    df_sum_str = df_to_table(df_sum)

    # Payload with both DataFrames formatted as tables
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    # "text": "Fail COUNT by Parameter: " + datetime.now().strftime('%Y-%m-%d %H:00')
                    "text": "-------------------------------------/n*Fail count by Parameter:* " + recorded_at + " to " + (one_hour_before + timedelta(hours=1)).strftime('%H:00')

                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": df_combined_str
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Fails by Station Pareto:*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": df_sum_str
                }
            }
        ]
    }

    # Send the payload to Slack using a webhook
    # url = http_post_hash.url
    headers = {'Content-type': 'application/json'}
    print(f"DEBUG: Sending message to Slack. Token: {slack_token}, Webhook URL: {url}")
    print(f"DATABRICKS_ACCESS_TOKEN Loaded: {DATABRICKS_ACCESS_TOKEN is not None}")
    print(f"SLACK_TOKEN Loaded: {slack_token is not None}")
    print(f"SLACK_WEBHOOK_URL Loaded: {url is not None}")


    response = requests.post(url, headers=headers, data=json.dumps(payload))

    # print(response.status_code)
    # print(response.text)

    # t1 = time.time()
    # print(f'Time taken: {t1-t0} seconds')
    # print(f"Task completed. Next run scheduled in 1 hour.")

# Schedule the job to run every hour
# schedule.every(30).seconds.do(job)
# schedule.every().hour.at(":01").do(job)

# Keep the script running to maintain the schedule
# while True:
#     schedule.run_pending()
#     # time.sleep(15)

job()  # Run the function once


