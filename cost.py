#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(date) AS date,
    SUM(CASE WHEN country = 'Venus' THEN cost END) AS venus_cost,
    SUM(CASE WHEN country = 'Pluton' THEN cost END) AS pluton_cost,
    SUM(CASE WHEN country = 'Saturn' THEN cost END) AS saturn_cost,
    SUM(CASE WHEN country = 'Uranus' THEN cost END) AS uranus_cost,
    SUM(CASE WHEN country = 'Mercury' THEN cost END) AS mercury_cost
FROM xxx.Analytics.dataset_cost
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
cost = c.query(query).to_dataframe()


#client connection
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(t1.event_time) AS date,
    SUM(CAST(CASE WHEN country = 'Venus' THEN revenue END as INT64)) AS venus_revenue,
    SUM(CAST(CASE WHEN country = 'Pluton' THEN revenue END as INT64)) AS pluton_revenue,
    SUM(CAST(CASE WHEN country = 'Saturn' THEN revenue END as INT64)) AS saturn_revenue,
    SUM(CAST(CASE WHEN country = 'Uranus' THEN revenue END as INT64)) AS uranus_revenue,
    SUM(CAST(CASE WHEN country = 'Mercury' THEN revenue END as INT64)) AS mercury_revenue
FROM xxx.Analytics.dataset_revenue AS t1
INNER JOIN
xxx.Analytics.dataset_install AS t2
ON t1.user_id = t2.user_id
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
revenue = c2.query(query).to_dataframe()
revenue = revenue.fillna(0)



fig, ax = plt.subplots()
ax.plot(cost['date'], cost['venus_cost'], label = 'Cost', color = 'r')
ax.plot(revenue['date'], revenue['venus_revenue'], label = 'Revenue', color = 'b')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Cost & Revenue')
ax.set_title('Daily Cost & Revenue in Venus')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()

fig, ax = plt.subplots()
ax.plot(cost['date'], cost['pluton_cost'], label = 'Cost', color = 'r')
ax.plot(revenue['date'], revenue['pluton_revenue'], label = 'Revenue', color = 'b')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Cost & Revenue')
ax.set_title('Daily Cost & Revenue in Pluton')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()

fig, ax = plt.subplots()
ax.plot(cost['date'], cost['saturn_cost'], label = 'Cost',  color = 'r')
ax.plot(revenue['date'], revenue['saturn_revenue'], label = 'Revenue', color = 'b')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Cost & Revenue')
ax.set_title('Daily Cost & Revenue in Saturn')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()

fig, ax = plt.subplots()
ax.plot(cost['date'], cost['uranus_cost'], label = 'Cost',  color = 'r')
ax.plot(revenue['date'], revenue['uranus_revenue'], label = 'Revenue', color = 'b')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Cost & Revenue')
ax.set_title('Daily Cost & Revenue in Uranus')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()

fig, ax = plt.subplots()
ax.plot(cost['date'], cost['mercury_cost'], label = 'Cost',  color = 'r')
ax.plot(revenue['date'], revenue['mercury_revenue'], label = 'Revenue', color = 'b')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Cost & Revenue')
ax.set_title('Daily Cost & Revenue in Mercury')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()


fig, ax = plt.subplots()
ax.plot(revenue['date'], revenue['venus_revenue'], label = 'Venus', color = 'r')
ax.plot(revenue['date'], revenue['pluton_revenue'], label = 'Pluton', color = 'b')
ax.plot(revenue['date'], revenue['saturn_revenue'], label = 'Saturn',  color = 'g')
ax.plot(revenue['date'], revenue['uranus_revenue'], label = 'Uranus',  color = 'm')
ax.plot(revenue['date'], revenue['mercury_revenue'], label = 'Mercury',  color = 'y')
ax.set_xlabel('Date (MM-DD)')
ax.set_ylabel('Daily Revenue')
ax.set_title('Daily Revenue in each Country')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()

c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT DATE(date) AS date,
    SUM(CASE WHEN country = 'Pluton' AND network = 'Sid' THEN cost END) AS sid_cost,
    SUM(CASE WHEN country = 'Pluton' AND network = 'Buzz' THEN cost END) AS buzz_cost,
    SUM(CASE WHEN country = 'Pluton' AND network = 'Woody' THEN cost END) AS woody_cost,
    SUM(CASE WHEN country = 'Pluton' AND network = 'Jessie' THEN cost END) AS jessie_cost,
    SUM(CASE WHEN country = 'Pluton' AND network = 'Organic' THEN cost END) AS organic_cost
FROM xxx.Analytics.dataset_cost
GROUP BY
    date
ORDER BY
    date
"""
#query results to dataframe
network_cost = c2.query(query).to_dataframe()

fig, ax2 = plt.subplots()
ax2.plot(network_cost['date'], network_cost['sid_cost'], label = 'Sid', color = 'r')
ax2.plot(network_cost['date'], network_cost['buzz_cost'], label = 'Buzz', color = 'b')
ax2.plot(network_cost['date'], network_cost['woody_cost'], label = 'Woody',  color = 'g')
ax2.plot(network_cost['date'], network_cost['jessie_cost'], label = 'Jessie',  color = 'm')
ax2.plot(network_cost['date'], network_cost['organic_cost'], label = 'Organic',  color = 'y')
ax2.set_xlabel('Date (MM-DD)')
ax2.set_ylabel('Daily cost')
ax2.set_title('Number of Daily cost in each Network in Pluton')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.gcf().autofmt_xdate()
plt.grid(True)
plt.legend()
plt.show()
