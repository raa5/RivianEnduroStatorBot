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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
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
    AND parameter_id = 2
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    GROUP BY STATION_NAME, PARAMETER_NAME
    """

    query_50 = f"""
    SELECT 
        COUNT(*) as COUNT,
        '050' as STATION_NAME,
        'Twisting Check Plate Fails' as PARAMETER_NAME
    FROM (
        SELECT *,
               LAG(cleared_at) OVER (PARTITION BY alarm_source_scada_short_name ORDER BY activated_at) AS prev_cleared_at
        FROM manufacturing.drive_unit.fct_du02_scada_alarms
        WHERE alarm_source_scada_short_name ILIKE '%STTR01-050%'
        AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
        AND alarm_priority_desc IN ('high', 'critical')
        AND alarm_description ILIKE '%Assembly error%Task[301]%'
    ) subquery
    WHERE activated_at > prev_cleared_at + INTERVAL '60 seconds'
       OR prev_cleared_at IS NULL; -- Keep the first occurrence
    """
    
    query_60 = f"""
    SELECT 
          COUNT(*) as COUNT,
          '060' as STATION_NAME,
          'Bad Cuts/Welding Fail' as ALARM_DESCRIPTION
    FROM manufacturing.drive_unit.fct_du02_scada_alarms
    WHERE alarm_source_scada_short_name ILIKE '%STTR01-060%'
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
    AND alarm_priority_desc IN ('high', 'critical')
    AND alarm_description ILIKE '%Assembly error :%'
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

    query_80 = f"""
    SELECT 
          COUNT(*) as COUNT,
          '080' as STATION_NAME,
          'Bad Cuts/Welding Fail' as ALARM_DESCRIPTION
    FROM manufacturing.drive_unit.fct_du02_scada_alarms
    WHERE alarm_source_scada_short_name ILIKE '%STTR01-080%'
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
    AND alarm_priority_desc IN ('high', 'critical')
    AND alarm_description ILIKE '%Assembly error :%'
    group by STATION_NAME
    
    UNION ALL
    
    SELECT 
          COUNT(*) as COUNT,
          '080' as STATION_NAME,
          'Laser: General Error' as ALARM_DESCRIPTION
    FROM manufacturing.drive_unit.fct_du02_scada_alarms
    WHERE alarm_source_scada_short_name ILIKE '%STTR01-080%'
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
    AND alarm_priority_desc IN ('high', 'critical')
    AND alarm_description ILIKE '%Laser: General error%'
    group by STATION_NAME
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
         ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_id = 01 or work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_id = 02)) 
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    ORDER BY COUNT DESC
    """

    query_210_unique_sn = f"""
    select count(distinct product_serial) as COUNT, station_name
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '210'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
        ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_name= '01' or work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_name= '02'))
    )
    GROUP BY ALL
    """

    # Execute queries and fetch data into DataFrames
    df_20 = pd.read_sql(query_20, conn)
    df_40 = pd.read_sql(query_40, conn)
    df_50 = pd.read_sql(query_50, conn)
    df_60 = pd.read_sql(query_60, conn)
    df_65 = pd.read_sql(query_65, conn)
    df_80 = pd.read_sql(query_80, conn)
    df_100 = pd.read_sql(query_100, conn)
    df_110 = pd.read_sql(query_110, conn)
    df_210 = pd.read_sql(query_210, conn)
    
    df_210_unique_sn = pd.read_sql(query_210_unique_sn, conn)
    


    # Combine DataFrames
    df_combined = pd.concat([df_20, df_40, df_50, df_60, df_65, df_80, df_100, df_110, df_210], ignore_index=True)

    df_combined["PARAMETER_NAME"] = df_combined["ALARM_DESCRIPTION"].fillna(df_combined["PARAMETER_NAME"])
    df_combined.drop(columns=["ALARM_DESCRIPTION"], inplace=True)  # Remove old column
    df_combined = df_combined[df_combined["COUNT"] > 0]


    # Sort combined DataFrame by 'COUNT' column
    if 'COUNT' in df_combined.columns:
        df_combined = df_combined.sort_values(['COUNT'], ascending=False, ignore_index=True)

    # Aggregate total failures per station (without duplicates)
    df_sum = df_combined.groupby("STATION_NAME")["COUNT"].sum().reset_index()

    # Merge unique product serial failures for Station 210
    if not df_210_unique_sn.empty and "STATION_NAME" in df_210_unique_sn.columns and "COUNT" in df_210_unique_sn.columns:
        df_210_unique_sn = df_210_unique_sn.rename(columns={"COUNT": "FAIL_COUNT"})

        # Merge Station 210's unique product serial failures into df_sum
        df_sum = df_sum.merge(df_210_unique_sn, on="STATION_NAME", how="left")

        # Replace total failure count with unique serial count for Station 210
        df_sum["COUNT"] = df_sum["FAIL_COUNT"].fillna(df_sum["COUNT"])
        
        # Drop the temporary column
        df_sum.drop(columns=["FAIL_COUNT"], inplace=True)
    else:
        print("Warning: STATION_NAME or COUNT column missing from df_210_unique_sn. Falling back to regular sum.")

    # Convert NaNs to 0 and ensure integer counts
    df_sum["COUNT"] = df_sum["COUNT"].fillna(0).astype(int)

    # Sort results
    df_sum = df_sum[df_sum["COUNT"] > 0]
    df_sum = df_sum.sort_values(['COUNT'], ascending=False, ignore_index=True)
    



    # # Ensure df_unique_serials has necessary columns before proceeding
    # if 'STATION_NAME' in df_210_unique_sn.columns and 'COUNT' in df_210_unique_sn.columns:
    #     df_sum = df_210_unique_sn.rename(columns={"COUNT": "FAIL_COUNT"})
    # else:
    #     print("Warning: STATION_NAME or COUNT column missing from df_unique_serials. Falling back to regular sum.")
    #     df_sum = df_combined.groupby('STATION_NAME')['COUNT'].sum().reset_index()

    # Convert DataFrames to a JSON-like format (table-like string)
    def df_to_table(df):
        table_str = df.to_string(index=False)
        return table_str 
    

    df_combined_str = df_to_table(df_combined)
    df_sum_str = df_to_table(df_sum)

    # Payload with both DataFrames formatted as tables
    payload = {
        "blocks": [
            { "type": "divider" },  # Add a divider to separate sections clearly
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    # "text": "Fail COUNT by Parameter: " + datetime.now().strftime('%Y-%m-%d %H:00')
                    "text": "*🚨Fail count by Parameter:* " + recorded_at + " to " + (one_hour_before + timedelta(hours=1)).strftime('%H:00')

                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "```" + df_combined_str + "```"
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
                    "text": "```" + df_sum_str + "```"
                }
            
            },
            { "type": "divider" },  # Add a divider to separate sections clearly
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
