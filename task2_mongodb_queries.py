from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# --- MongoDB connection ---
uri = "mongodb://localhost:27017/nimbus_events"  # Replace with your MongoDB URI
client = MongoClient(uri)
db = client["nimbus_events"]
collection = db["user_activity_logs"]

# ---------------------------
# Q1: Avg sessions per user per week
# ---------------------------
print("Q1: Avg sessions per user per week")

pipeline_q1 = [
    {"$addFields": {
        "customer_id_int": {"$toInt": {"$ifNull": ["$customer_id", "$customerId"]}},
        "ts": {"$toDate": "$timestamp"}
    }},
    {"$match": {"customer_id_int": {"$ne": None}, "ts": {"$type": "date"}}},
    {"$addFields": {"week": {"$isoWeek": "$ts"}, "year": {"$isoWeekYear": "$ts"}}},
    {"$group": {
        "_id": {"customer_id": "$customer_id_int", "year": "$year", "week": "$week"},
        "sessions": {"$sum": 1},
        "durations": {"$push": "$session_duration_sec"}
    }}
]

results_q1 = list(collection.aggregate(pipeline_q1))

print("\nSample Output Q1:")
for r in results_q1[:5]:  # Show first 5 rows
    durations = [d for d in r.get("durations", []) if d is not None]
    p25 = np.percentile(durations, 25) if durations else 0
    p50 = np.percentile(durations, 50) if durations else 0
    p75 = np.percentile(durations, 75) if durations else 0
    print(f"Customer {r['_id']['customer_id']}, Week {r['_id']['week']}: sessions={r['sessions']}, 25th={p25}, 50th={p50}, 75th={p75}")

# ---------------------------
# Q2: DAU and 7-day retention per feature
# ---------------------------
print("\nQ2: DAU and 7-day retention per feature")

pipeline_dau = [
    {"$addFields": {
        "ts": {"$toDate": "$timestamp"},
        "user_id": {"$ifNull": ["$member_id", "$userId"]}
    }},
    {"$match": {"ts": {"$type": "date"}, "user_id": {"$ne": None}, "event_type": {"$exists": True}}},
    {"$group": {
        "_id": {
            "feature": "$event_type",
            "user": "$user_id",
            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}}
        }
    }}
]

user_days = list(collection.aggregate(pipeline_dau))

# Organize users per feature per day
feature_users_by_date = defaultdict(lambda: defaultdict(set))
for doc in user_days:
    _id = doc["_id"]
    feature = _id.get("feature")
    date = _id.get("date")
    user = _id.get("user")
    if feature and date and user:
        feature_users_by_date[feature][date].add(user)

# Calculate DAU + 7-day retention
retention_results = {}
for feature, dates_dict in feature_users_by_date.items():
    retention_results[feature] = []
    sorted_dates = sorted(dates_dict.keys())
    for i, date_str in enumerate(sorted_dates):
        dau = len(dates_dict[date_str])
        date_dt = datetime.strptime(date_str, "%Y-%m-%d")
        retained = set()
        for j in range(i+1, len(sorted_dates)):
            next_date_dt = datetime.strptime(sorted_dates[j], "%Y-%m-%d")
            if (next_date_dt - date_dt).days <= 7:
                retained.update(dates_dict[sorted_dates[j]].intersection(dates_dict[date_str]))
        retention_results[feature].append({"date": date_str, "DAU": dau, "7d_retention": len(retained)})

print("\nSample Output Q2:")
for feature, values in list(retention_results.items())[:3]:
    print(f"Feature: {feature}, Sample: {values[:3]}")

# ---------------------------
# Q3: Onboarding Funnel
# ---------------------------
print("\nQ3: Onboarding Funnel")

funnel_events = ["signup", "first_login", "workspace_created", "first_project", "invited_teammate"]

funnel_counts = {}
for event in funnel_events:
    funnel_counts[event] = collection.count_documents({"event_type": event})

drop_off = {}
prev_count = None
for stage in funnel_events:
    if prev_count is None:
        drop_off[stage] = 0
    else:
        drop_off[stage] = (prev_count - funnel_counts[stage]) / prev_count * 100 if prev_count != 0 else 0
    prev_count = funnel_counts[stage]

print("Funnel Counts:", funnel_counts)
print("Drop-off % per stage:", drop_off)

# ---------------------------
# Q4: Top 20 engaged free-tier users
# ---------------------------
print("\nQ4: Top 20 engaged free-tier users")

# If plan_tier exists in MongoDB
pipeline_q4 = [
    {"$match": {"plan_tier": "free"}},  # ensure only free-tier users
    {"$group": {
        "_id": "$customer_id",
        "engagement_score": {"$sum": 1}  # simple metric: number of events
    }},
    {"$sort": {"engagement_score": -1}},
    {"$limit": 20}
]

results_q4 = list(collection.aggregate(pipeline_q4))
print("Top 20 free-tier users with engagement score:")
for r in results_q4:
    print(r)

client.close()