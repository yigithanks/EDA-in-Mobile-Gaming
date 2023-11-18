#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import numpy as np
from datetime import datetime 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT DATE(event_time) AS date, COUNT(DISTINCT user_id) AS daily_user
FROM casexxx.Analytics.dataset_session
GROUP BY date
ORDER BY date
"""
#query results to dataframe
daily_act = c.query(query).to_dataframe()


fig, ax = plt.subplots()
ax.plot(daily_act['date'], daily_act['daily_user'], color = 'r')
ax.set_xlabel('Date (YYYY-MM)')
ax.set_ylabel('Daily Active Users')
ax.set_title('Daily Active User Graph')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.show()


retention_data = [0]

for day in range(len(daily_act)-1):
    current_user = daily_act['daily_user'][day]
    next_day_user = daily_act['daily_user'][day+1]

    retention = next_day_user/current_user*100
    retention_data.append(retention)

retention_df = daily_act.copy()
retention_df['retention'] = retention_data
retention_df = retention_df[['date', 'retention']][2:]


fig2, ax2 = plt.subplots()
ax2.plot(retention_df['date'], retention_df['retention'], color = 'b')
ax2.set_xlabel('Date (MM-DD)')
ax2.set_ylabel('Retention Rate (%)')
ax2.set_title('Retention Graph')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.show()

# QUERYING THE REVENUE DATA TO CALCULATE THE CONVERSION RATE
# QUERYING THE REVENUE DATA TO CALCULATE THE CONVERSION RATE
# QUERYING THE REVENUE DATA TO CALCULATE THE CONVERSION RATE
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT DATE(event_time) AS date, COUNT(DISTINCT user_id) AS daily_paying_user
FROM casexxx.Analytics.dataset_revenue
GROUP BY date
ORDER BY date
"""
#query results to dataframe
count_purchase = c2.query(query).to_dataframe()

conversion = daily_act.merge(count_purchase, on = 'date', how = 'inner')
conversion['conversion_rate'] = conversion['daily_paying_user']/conversion['daily_user']

fig, ax3 = plt.subplots()
ax3.plot(conversion['date'], conversion['conversion_rate'], color = 'r')
ax3.set_xlabel('Date (MM-DD)')
ax3.set_ylabel('Conversion Rate')
ax3.set_title('Conversion Rate Graph')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.show()

#client connection
c4 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(event_time) AS date, 
COUNT(CASE WHEN platform = 'ios' THEN user_id END) AS ios_users,
COUNT(CASE WHEN platform = 'android' THEN user_id END) AS and_users,
FROM casexxx.Analytics.dataset_session
GROUP BY date
ORDER BY date
"""
#query results to dataframe
platform_user = c4.query(query).to_dataframe()

fig, ax4 = plt.subplots()
ax4.plot(platform_user['date'], platform_user['ios_users'],label = 'iOS', color = 'r')
ax4.plot(platform_user['date'], platform_user['and_users'], label = 'Android', color = 'y')
ax4.set_xlabel('Date (MM-DD)')
ax4.set_ylabel('Daily User Activity')
ax4.set_title('Daily Activity in each Platform (iOS/Android)')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()

print((platform_user['ios_users'].mean()-platform_user['and_users'].mean())/platform_user['and_users'].mean())

