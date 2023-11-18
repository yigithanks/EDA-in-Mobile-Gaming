#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT user_id, AVG(break_time) AS using_freq

FROM (SELECT
    user_id,
    date,
    LAG(date) OVER (PARTITION BY user_id ORDER BY date) AS previous_date,
    TIMESTAMP_DIFF(date, (LAG(date) OVER (PARTITION BY user_id ORDER BY date)), DAY) AS break_time,
FROM
(    
    SELECT
user_id, DATE(event_time) AS date,
FROM
casexxx.Analytics.dataset_session
GROUP BY 
user_id, date
ORDER BY 
date DESC
))
GROUP BY user_id

"""
#query results to dataframe
user_freq = c.query(query).to_dataframe()  

#client connection
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT user_id, country  
FROM casexxx.Analytics.dataset_install
"""
#query results to dataframe
daily_ins = c2.query(query).to_dataframe()

country_freq = daily_ins.merge(user_freq, on = 'user_id', how = 'right')
country_mean_freq = country_freq[country_freq['using_freq'].isna() == False].groupby(['country'])['using_freq'].mean()
print(country_mean_freq)
