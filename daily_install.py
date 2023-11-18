#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ SELECT DATE(event_time) AS date, COUNT(DISTINCT user_id) AS daily_installs 
FROM xxx.Analytics.dataset_install
GROUP BY date
ORDER BY date
"""
#query results to dataframe
daily_ins = c.query(query).to_dataframe()
daily_ins = daily_ins[:(len(daily_ins) - 1)]
print(daily_ins)


fig, ax = plt.subplots()
ax.plot(daily_ins['date'], daily_ins['daily_installs'], color = 'r')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Installs')
ax.set_title('Number of Daily Installs')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)


c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(event_time) AS date,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' AND network = 'Sid' THEN user_id END) AS sid_user_id,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' AND network = 'Buzz' THEN user_id END) AS buzz_user_id,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' AND network = 'Woody' THEN user_id END) AS woody_user_id,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' AND network = 'Jessie' THEN user_id END) AS jessie_user_id,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' AND network = 'Organic' THEN user_id END) AS organic_user_id
FROM xxx.Analytics.dataset_install
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
network_user = c2.query(query).to_dataframe()

fig, ax2 = plt.subplots()
ax2.plot(network_user['date'], network_user['sid_user_id'], label = 'Sid', color = 'r')
ax2.plot(network_user['date'], network_user['buzz_user_id'], label = 'Buzz', color = 'b')
ax2.plot(network_user['date'], network_user['woody_user_id'], label = 'Woody',  color = 'g')
ax2.plot(network_user['date'], network_user['jessie_user_id'], label = 'Jessie',  color = 'm')
ax2.plot(network_user['date'], network_user['organic_user_id'], label = 'Organic',  color = 'y')
ax2.set_xlabel('Date (MM-DD)')
ax2.set_ylabel('Daily Installs')
ax2.set_title('Number of Daily Installs in each Network in Pluton')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()


plt.show()
