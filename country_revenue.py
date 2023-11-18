#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT user_id, SUM(CAST(revenue AS INT64)) AS user_revenue
FROM casexxx.Analytics.dataset_revenue
GROUP BY user_id
"""
#query results to dataframe
user_rev = c.query(query).to_dataframe()

#client connection
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT user_id, country
FROM casexxx.Analytics.dataset_install
"""
#query results to dataframe
country = c2.query(query).to_dataframe()

country_rev = country.merge(user_rev, on = 'user_id')
country_avg = country_rev.groupby(['country'])['user_revenue'].mean()
print(country_avg)