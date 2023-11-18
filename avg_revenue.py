#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(event_time) AS date, SUM(CAST(revenue AS INT64)) AS daily_revenue
FROM xxx.Analytics.dataset_revenue
GROUP BY date
ORDER BY date
"""
#query results to dataframe
daily_rev = c.query(query).to_dataframe()


fig, ax = plt.subplots()
ax.plot(daily_rev['date'], daily_rev['daily_revenue'], color = 'r')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Revenue')
ax.set_title('Daily Revenue Graph')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)


c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT DATE(event_time) AS date, COUNT(DISTINCT user_id) AS daily_user
FROM xxx.Analytics.dataset_session
GROUP BY date
ORDER BY date
"""
#query results to dataframe
daily_act = c2.query(query).to_dataframe()

arpu = daily_rev['daily_revenue']/daily_act['daily_user']
arpu_df = pd.DataFrame({'date':daily_act['date'],'avg_rev_per_user':arpu})
arpu_df['revenue'] = daily_rev['daily_revenue']
arpu_df['users'] = daily_act['daily_user']
print(arpu_df)

c3 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT DATE(event_time) AS date, SUM(CAST(revenue AS INT64)) AS daily_revenue, 
COUNT(DISTINCT user_id) AS daily_paying_user
FROM xxx.Analytics.dataset_revenue
GROUP BY date
ORDER BY date
"""
#query results to dataframe
daily_act_paying = c3.query(query).to_dataframe()

arpu2 = daily_rev['daily_revenue']/daily_act_paying['daily_paying_user']
arpu_df2 = pd.DataFrame({'date':daily_act_paying['date'],'avg_rev_per_paying_user':arpu2})
arpu_df2['revenue'] = daily_rev['daily_revenue']
arpu_df2['paying_users'] = daily_act_paying['daily_paying_user']
print(arpu_df2)

fig2, ax2 = plt.subplots()
ax2.plot(arpu_df['date'], arpu_df['avg_rev_per_user'], color = 'r')
ax3 = ax2.twinx()
ax3.plot(arpu_df2['date'], arpu_df2['avg_rev_per_paying_user'], color = 'b')
ax2.set_xlabel('Date (MM-DD)')
ax2.set_ylabel('ARPU', color = 'r')
ax3.set_ylabel('ARPPU', color = 'b')
ax2.set_title('ARPU and ARPPU Graphs')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)

plt.show()
