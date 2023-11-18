#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(event_time) AS date,
    COUNT(CASE WHEN country = 'Venus' THEN user_id END) AS venus_installs,
    COUNT(CASE WHEN country = 'Pluton' THEN user_id END) AS pluton_installs,
    COUNT(CASE WHEN country = 'Saturn' THEN user_id END) AS saturn_installs,
    COUNT(CASE WHEN country = 'Uranus' THEN user_id END) AS uranus_installs,
    COUNT(CASE WHEN country = 'Mercury' THEN user_id END) AS mercury_installs
FROM casexxx.Analytics.dataset_install
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
daily_ins = c.query(query).to_dataframe()
daily_ins = daily_ins[:(len(daily_ins) - 1)]


fig, ax = plt.subplots()
ax.plot(daily_ins['date'], daily_ins['venus_installs'], label = 'Venus', color = 'r')
ax.plot(daily_ins['date'], daily_ins['pluton_installs'], label = 'Pluton', color = 'b')
ax.plot(daily_ins['date'], daily_ins['saturn_installs'], label = 'Saturn',  color = 'g')
ax.plot(daily_ins['date'], daily_ins['uranus_installs'], label = 'Uranus',  color = 'm')
ax.plot(daily_ins['date'], daily_ins['mercury_installs'], label = 'Mercury',  color = 'y')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Installs')
ax.set_title('Number of Daily Installs in each Country')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()

c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(date) AS date,
    SUM(CASE WHEN country = 'Venus' THEN cost END) AS venus_cost,
    SUM(CASE WHEN country = 'Pluton' THEN cost END) AS pluton_cost,
    SUM(CASE WHEN country = 'Saturn' THEN cost END) AS saturn_cost,
    SUM(CASE WHEN country = 'Uranus' THEN cost END) AS uranus_cost,
    SUM(CASE WHEN country = 'Mercury' THEN cost END) AS mercury_cost
FROM casexxx.Analytics.dataset_cost
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
cost = c2.query(query).to_dataframe()

install_cost = daily_ins.merge(cost, on = 'date', how = 'inner')

print(install_cost[['pluton_installs', 'pluton_cost']])

# DAILY COUNTRY USERS
# DAILY COUNTRY USERS
# DAILY COUNTRY USERS
#client connection
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """
SELECT DATE(t1.event_time) AS date,
    COUNT(DISTINCT CASE WHEN country = 'Venus' THEN t1.user_id END) AS venus_users,
    COUNT(DISTINCT CASE WHEN country = 'Pluton' THEN t1.user_id END) AS pluton_users,
    COUNT(DISTINCT CASE WHEN country = 'Saturn' THEN t1.user_id END) AS saturn_users,
    COUNT(DISTINCT CASE WHEN country = 'Uranus' THEN t1.user_id END) AS uranus_users,
    COUNT(DISTINCT CASE WHEN country = 'Mercury' THEN t1.user_id END) AS mercury_users
FROM casexxx.Analytics.dataset_session AS t1
INNER JOIN
casexxx.Analytics.dataset_install AS t2
ON t1.user_id = t2.user_id
GROUP BY
    date
ORDER BY
    date
    """
#query results to dataframe
daily_country_ins = c2.query(query).to_dataframe()
daily_country_ins = daily_country_ins[:(len(daily_ins) - 1)]

venus_mean = daily_country_ins['venus_users'].mean() 
pluton_mean = daily_country_ins['pluton_users'].mean() 
mercury_mean = daily_country_ins['mercury_users'].mean() 
uranus_mean = daily_country_ins['uranus_users'].mean() 
saturn_mean = daily_country_ins['saturn_users'].mean() 

print(venus_mean, pluton_mean, mercury_mean, uranus_mean, saturn_mean)

fig, ax2 = plt.subplots()
ax2.plot(daily_country_ins['date'], daily_country_ins['venus_users'], label = 'Venus', color = 'r')
ax2.plot(daily_country_ins['date'], daily_country_ins['pluton_users'], label = 'Pluton', color = 'b')
ax2.plot(daily_country_ins['date'], daily_country_ins['saturn_users'], label = 'Saturn',  color = 'g')
ax2.plot(daily_country_ins['date'], daily_country_ins['uranus_users'], label = 'Uranus',  color = 'm')
ax2.plot(daily_country_ins['date'], daily_country_ins['mercury_users'], label = 'Mercury',  color = 'y')
ax2.set_xlabel('Date (MM-DD)')
ax2.set_ylabel('Number of Users')
ax2.set_title('Number of Daily Users in each Country')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.xticks(rotation=45)
plt.show()