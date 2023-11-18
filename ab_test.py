#import necessary libraries
from google.cloud import bigquery as bq
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#client connection
c = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """
SELECT user_id, group_id, kpi_1, kpi_2
FROM xxx.Analytics.dataset_ab_test
"""
#query results to dataframe
ab_test = c.query(query).to_dataframe()
#print(ab_test)

group1_kpi1 = ab_test[ab_test['group_id']==1]['kpi_1'].mean()
print(group1_kpi1)

# QUERYING THE COIN SPEND DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
# QUERYING THE COIN SPEND DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
# QUERYING THE COIN SPEND DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
c1 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """
SELECT user_id, SUM(coin_amount) AS coins_spent
FROM xxx.Analytics.dataset_coin_spend
GROUP BY user_id
"""

#query results to dataframe
coin_spend = c1.query(query).to_dataframe()

ab_test_coins = ab_test.merge(coin_spend, on='user_id', how = 'left')
ab_test_coins = ab_test_coins[(ab_test_coins['coins_spent'].isna() == False)]
#print(ab_test_coins.head())



plt.figure(figsize=(12, 10))
plt.subplot(2, 2, 1)
plt.scatter(ab_test_coins[ab_test_coins['group_id']==1]['kpi_2'], \
            ab_test_coins[ab_test_coins['group_id']==1]['coins_spent'], \
            label = 'Group 1', color = 'r')
plt.title('Group 1 KPI 1 Coin Spending')

plt.subplot(2, 2, 2)
plt.scatter(ab_test_coins[ab_test_coins['group_id']==2]['kpi_2'], \
            ab_test_coins[ab_test_coins['group_id']==2]['coins_spent'], \
            label = 'Group 2', color = 'b')
plt.title('Group 2 KPI 1 Coin Spending')

plt.subplot(2, 2, 3)
plt.scatter(ab_test_coins[ab_test_coins['group_id']==3]['kpi_2'], \
            ab_test_coins[ab_test_coins['group_id']==3]['coins_spent'], \
            label = 'Group 3', color = 'g')
plt.title('Group 3 KPI 1 Coin Spending')

plt.subplot(2, 2, 4)
plt.scatter(ab_test_coins[ab_test_coins['group_id']==4]['kpi_2'], \
            ab_test_coins[ab_test_coins['group_id']==4]['coins_spent'], \
            label = 'Group 4', color = 'y')
plt.title('Group 4 KPI 1 Coin Spending')
plt.show()


# QUERYING THE REVENUE DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
# QUERYING THE REVENUE DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
# QUERYING THE REVENUE DATA TO OBSERVE THE RELATION WITH GROUPS AND KPI's
c2 = bq.Client.from_service_account_json("/Users/yigithanks/Desktop/xxx/yigithanks.json")
query = """
SELECT user_id, SUM(CAST(revenue AS INT64)) AS revenue
FROM xxx.Analytics.dataset_revenue
GROUP BY user_id
"""

# #query results to dataframe
revenue = c2.query(query).to_dataframe()

ab_test_rev = ab_test.merge(revenue, on='user_id', how = 'left')
ab_test_rev = ab_test_rev[(ab_test_rev['revenue'].isna() == False)]
print(ab_test_rev)

plt.figure(figsize=(12, 10))

plt.subplot(2, 2, 1)
plt.scatter(ab_test_rev[ab_test_rev['group_id']==1]['kpi_2'], \
            ab_test_rev[ab_test_rev['group_id']==1]['revenue'], \
            label = 'Group 1')
plt.title('Group 1 KPI 1 Revenue')

plt.subplot(2, 2, 2)
plt.scatter(ab_test_rev[ab_test_rev['group_id']==2]['kpi_2'], \
            ab_test_rev[ab_test_rev['group_id']==2]['revenue'], \
            label = 'Group 2')
plt.title('Group 2 KPI 1 Revenue')

plt.subplot(2, 2, 3)
plt.scatter(ab_test_rev[ab_test_rev['group_id']==3]['kpi_2'], \
            ab_test_rev[ab_test_rev['group_id']==3]['revenue'], \
            label = 'Group 3')
plt.title('Group 3 KPI 1 Revenue')

plt.subplot(2, 2, 4)
plt.scatter(ab_test_rev[ab_test_rev['group_id']==4]['kpi_2'], \
            ab_test_rev[ab_test_rev['group_id']==4]['revenue'], \
            label = 'Group 4')
plt.title('Group 4 KPI 1 Revenue')
plt.tight_layout()
plt.show()


# GROUP BY GROUP KPI SCATTER
# GROUP BY GROUP KPI SCATTER
# GROUP BY GROUP KPI SCATTER
plt.scatter(ab_test[ab_test['group_id']==1]['kpi_1'], ab_test[ab_test['group_id']==1]['kpi_2'], label = 'Group 1', c = 'r')
plt.scatter(ab_test[ab_test['group_id']==2]['kpi_1'], ab_test[ab_test['group_id']==2]['kpi_2'], label = 'Group 2', c = 'b')
plt.scatter(ab_test[ab_test['group_id']==3]['kpi_1'], ab_test[ab_test['group_id']==3]['kpi_2'], label = 'Group 3', c = 'g')
plt.scatter(ab_test[ab_test['group_id']==4]['kpi_1'], ab_test[ab_test['group_id']==4]['kpi_2'], label = 'Group 4', c = 'y')
plt.xlabel('KPI 1')
plt.ylabel('KPI 2')
plt.title('Results of A/B Test')
plt.legend()
plt.show()

#histogram to see if any interpretation can be made
plt.figure()
plt.hist(ab_test[(ab_test['group_id']==3) & (ab_test['kpi_2']<10)]['kpi_2'], bins=45, alpha=0.7, edgecolor='black')
plt.xlabel('KPI 2 Value Ranges')
plt.ylabel('Weight of the KPI Score')
plt.title('Distribution of KPI 2 Values for Group 3')
plt.show()

