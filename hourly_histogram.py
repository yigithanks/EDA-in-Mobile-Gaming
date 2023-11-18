#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import time

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT  event_time AS event_time, CAST(revenue AS INT64) AS revenue
FROM casexxx.Analytics.dataset_revenue
"""

#query results to dataframe
revenue_time = c.query(query).to_dataframe()

revenue_time['event_time'] = pd.to_datetime(revenue_time['event_time'])

revenue_time['hour'] = revenue_time['event_time'].dt.hour

hourly_revenue = pd.DataFrame()
hourly_revenue['total_revenue'] = revenue_time.groupby(['hour'])['revenue'].sum()
print(hourly_revenue)

# Create a histogram
plt.hist(revenue_time['hour'], bins=24, range=(0, 24), alpha=0.7, edgecolor='black')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Purchases')
plt.title('Hourly Histogram')
plt.xticks(range(0, 24))

plt.figure()
plt.bar(hourly_revenue.index, hourly_revenue['total_revenue'])
plt.xlabel('Hour of the Day')
plt.ylabel('Total Purchase')
plt.show()


c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT  event_time AS event_time, CAST(coin_amount AS INT64) AS coin_amount
FROM casexxx.Analytics.dataset_coin_spend
"""

#query results to dataframe
coin_time = c2.query(query).to_dataframe()

coin_time['event_time'] = pd.to_datetime(coin_time['event_time'])

coin_time['hour'] = coin_time['event_time'].dt.hour
print(coin_time)

hourly_coins = pd.DataFrame()
hourly_coins['total_coins'] = coin_time.groupby(['hour'])['coin_amount'].sum()
print(hourly_coins)

#Create a histogram
plt.hist(coin_time['hour'], bins=24, range=(0, 24), alpha=0.7, edgecolor='black')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Coin Spending')
plt.title('Hourly Histogram of Coin Spending')
plt.xticks(range(0, 24))

plt.figure()
plt.bar(hourly_coins.index, hourly_coins['total_coins'])
plt.xlabel('Hour of the Day')
plt.ylabel('Total Coin Spent')


c3 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT  event_time AS event_time, user_id AS session
FROM casexxx.Analytics.dataset_session
"""

#query results to dataframe
hourly_user = c3.query(query).to_dataframe()

hourly_user['event_time'] = pd.to_datetime(hourly_user['event_time'])

hourly_user['hour'] = hourly_user['event_time'].dt.hour
print(hourly_user)

plt.figure()
plt.hist(hourly_user['hour'], bins=24, range=(0, 24), alpha=0.7, edgecolor='black')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Active Users')
plt.title('Hourly Histogram of Active Users')
plt.xticks(range(0, 24))

plt.show()