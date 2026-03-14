# -------------------------------------------
# TASK 3 - DATA WRANGLING & STATISTICAL ANALYSIS
# -------------------------------------------

import pandas as pd
import psycopg2
from pymongo import MongoClient
from scipy.stats import chi2_contingency

# -------------------------------------------
# 1. CONNECT TO POSTGRESQL
# -------------------------------------------

print("Connecting to PostgreSQL...")

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Ayush@#7662",
    port="5432"
)

print("Connected to PostgreSQL")

# -------------------------------------------
# 2. LOAD SQL DATA
# -------------------------------------------

sql_query = """
SELECT
    c.customer_id,
    c.company_name,
    c.contact_email,
    s.plan_id,
    s.status,
    s.start_date,
    s.end_date
FROM nimbus.customers c
JOIN nimbus.subscriptions s
ON c.customer_id = s.customer_id
"""

sql_data = pd.read_sql(sql_query, conn)

print("SQL Rows:", len(sql_data))

# -------------------------------------------
# 3. CONNECT TO MONGODB
# -------------------------------------------

print("Connecting to MongoDB...")

client = MongoClient("mongodb://localhost:27017/")
db = client["nimbus_events"]

print("Connected to MongoDB")

# -------------------------------------------
# 4. LOAD MONGODB EVENTS
# -------------------------------------------

events_collection = db["events"]

mongo_cursor = events_collection.find()

mongo_data = pd.DataFrame(list(mongo_cursor))

print("MongoDB Rows:", len(mongo_data))

# -------------------------------------------
# 5. ENSURE REQUIRED COLUMNS
# -------------------------------------------

required_cols = ['user_id', 'customer_id', 'event_type', 'timestamp']

for col in required_cols:
    if col not in mongo_data.columns:
        mongo_data[col] = None

if 'session_duration' not in mongo_data.columns:
    mongo_data['session_duration'] = 0

mongo_data = mongo_data[
    ['user_id', 'customer_id', 'event_type', 'timestamp', 'session_duration']
]

# -------------------------------------------
# 6. DATA CLEANING
# -------------------------------------------

print("Cleaning Data...")

mongo_data['session_duration'] = mongo_data['session_duration'].fillna(0)

mongo_data['timestamp'] = pd.to_datetime(
    mongo_data['timestamp'],
    errors='coerce'
)

mongo_data = mongo_data.drop_duplicates()

mongo_data = mongo_data[mongo_data['session_duration'] < 86400]

sql_data['company_name'] = sql_data['company_name'].astype(str)

print("Mongo Rows After Cleaning:", len(mongo_data))

# -------------------------------------------
# 7. FIX DATA TYPES BEFORE MERGE
# -------------------------------------------

sql_data['customer_id'] = sql_data['customer_id'].astype(str)
mongo_data['customer_id'] = mongo_data['customer_id'].astype(str)

# -------------------------------------------
# 8. MERGE SQL + MONGODB DATA
# -------------------------------------------

merged_data = pd.merge(
    sql_data,
    mongo_data,
    on="customer_id",
    how="left"
)

print("Merged Rows:", len(merged_data))

# -------------------------------------------
# 9. CREATE CHURN VARIABLE
# -------------------------------------------

merged_data['churned'] = merged_data['end_date'].notnull()

# -------------------------------------------
# 10. FEATURE USAGE
# -------------------------------------------

feature_users = merged_data[
    merged_data['event_type'] == 'feature_x_used'
]['customer_id'].unique()

merged_data['used_feature_x'] = merged_data['customer_id'].isin(feature_users)

# -------------------------------------------
# 11. HYPOTHESIS TEST
# -------------------------------------------

print("Running Hypothesis Test...")

contingency = pd.crosstab(
    merged_data['used_feature_x'],
    merged_data['churned']
)

if contingency.shape == (2, 2):

    chi2, p, dof, expected = chi2_contingency(contingency)

    print("Chi-Square Test")
    print("P-Value:", p)

    if p < 0.05:
        print("Reject H0 → Feature usage impacts churn")
    else:
        print("Fail to reject H0")

else:
    print("Not enough data for hypothesis test")

# -------------------------------------------
# 12. CUSTOMER ENGAGEMENT METRICS
# -------------------------------------------

user_metrics = merged_data.groupby('customer_id').agg(
    session_count=('event_type', 'count'),
    avg_session_duration=('session_duration', 'mean')
).reset_index()

# -------------------------------------------
# 13. CREATE CUSTOMER SEGMENTS
# -------------------------------------------

user_metrics['segment'] = pd.qcut(
    user_metrics['session_count'].rank(method='first'),
    q=3,
    labels=['Low Engagement', 'Medium Engagement', 'High Engagement']
)

print("\nCustomer Segments Distribution:")

print(user_metrics['segment'].value_counts())

# -------------------------------------------
# 14. SAVE FILES
# -------------------------------------------

merged_data.to_csv("merged_customer_data.csv", index=False)

user_metrics.to_csv("customer_segments.csv", index=False)

print("\nFiles Saved Successfully:")
print("merged_customer_data.csv")
print("customer_segments.csv")

# -------------------------------------------
# END
# -------------------------------------------