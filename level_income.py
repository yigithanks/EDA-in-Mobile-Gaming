#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT
    t1.user_id,
    t1.event_time AS spend_time,
    CAST(revenue AS INT64) AS revenue,
    previous_date AS previous_end_time,
    t2.event_time AS end_time,
    t2.level,
    t2.time_spent,
    t2.moves_made,
    t2.moves_left
FROM
    casexxx.Analytics.dataset_revenue AS t1
LEFT JOIN (
    SELECT
        t2.*,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_date
    FROM
        casexxx.Analytics.dataset_level_end AS t2
) t2
ON
    t1.user_id = t2.user_id
    AND t1.event_time < t2.event_time
    AND (t1.event_time > t2.previous_date OR t2.previous_date IS NULL)
WHERE status = 'win'
"""
#query results to dataframe
level_revenue_detailed = c.query(query).to_dataframe()


#client connection
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """ 
SELECT
    AVG(CAST(revenue AS INT64)) AS avg_revenue,
    t2.level,
    AVG(time_spent*(moves_made/(moves_made + moves_left))) AS difficulty
FROM
    casexxx.Analytics.dataset_revenue AS t1
LEFT JOIN (
    SELECT
        t2.*,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_date
    FROM
        casexxx.Analytics.dataset_level_end AS t2
) t2
ON
    t1.user_id = t2.user_id
    AND t1.event_time < t2.event_time
    AND (t1.event_time > t2.previous_date OR t2.previous_date IS NULL)
WHERE status = 'win'

GROUP BY level
ORDER BY level
"""
#query results to dataframe
diff_rev = c2.query(query).to_dataframe()

fig, ax = plt.subplots()
ax.plot(diff_rev['level'], diff_rev['difficulty'], label = 'Difficulty', color = 'r')
ax2 = ax.twinx()
ax2.plot(diff_rev['level'], diff_rev['avg_revenue'], label = 'Average Revenue', color = 'y')
ax.set_xlabel('Level')
ax.set_ylabel('Difficulty', color = 'r')
ax2.set_ylabel('Average Level Revenue', color = 'y')
ax.set_title('Difficulty & Average Revenue by Level')
plt.grid(True)
plt.show()