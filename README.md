# NimbusAI Data Analyst Internship Challenge

## Overview

This project is part of the NimbusAI Data Analyst Intern take-home challenge.  
The goal of this analysis is to investigate customer engagement, product usage, and churn patterns using data from both SQL and MongoDB databases.

The analysis combines structured customer data with product event logs to generate insights that help leadership make data-driven product and retention decisions.

---

## Business Problem

NimbusAI leadership observed:

- Increasing customer churn
- Rising support tickets
- Uncertainty around which product features drive engagement

This project analyzes customer behavior and engagement patterns to identify potential retention strategies and product improvement opportunities.

---

## Data Sources

Two databases were used in this analysis.

### PostgreSQL Database (nimbus_core)

Contains structured business data:

- customers
- subscriptions
- plans
- support_tickets
- team_members
- billing_invoices
- feature_flags

### MongoDB Database (nimbus_events)

Contains product activity logs:

- user sessions
- feature usage
- onboarding events
- clickstream data
- user engagement metrics

---

## Tasks Completed

### Task 1 — SQL Analysis

Advanced SQL queries were written using:

- Joins
- Aggregations
- Window functions
- Common Table Expressions (CTEs)
- Time-series calculations

These queries analyzed:

- subscription trends
- lifetime value
- churn patterns
- support ticket rates
- potential duplicate accounts

File: queries.sql


---

### Task 2 — MongoDB Event Analysis

MongoDB aggregation pipelines were used to analyze product usage and engagement patterns.

Key analyses include:

- session activity metrics
- feature adoption
- onboarding funnel analysis
- user engagement scoring

File:task2_mongodb_queries.py


---

### Task 3 — Data Wrangling & Statistical Analysis

Python was used to merge and clean data from both databases.

Key steps:

- Handling missing values
- Removing duplicate records
- Converting timestamps
- Filtering outliers
- Merging SQL and MongoDB datasets

A statistical hypothesis test was performed to evaluate whether feature usage impacts customer churn.

Customer engagement segments were also created using session activity.

File:task3_analysis.py


---

### Task 4 — Interactive Dashboard

An interactive dashboard was built using **Power BI** to present insights to business stakeholders.

Dashboard highlights:

- Customer engagement segmentation
- Feature usage vs churn
- Customer activity patterns
- Session duration analysis
- Interactive filters for exploration

File:  NimbusAI_Dashboard.pbix


---

## Key Insights

**1. High Engagement Customers Show Lower Churn**

Users with higher session activity demonstrate stronger retention and long-term product adoption.

**2. Feature Usage Correlates With Retention**

Customers actively using key product features show lower churn rates, indicating the importance of feature adoption.

**3. Low Engagement Users Represent the Highest Churn Risk**

Users with limited interaction with the platform are significantly more likely to churn.

---

## Business Recommendations

Based on the analysis:

1. Promote adoption of key product features to improve retention.
2. Improve onboarding to increase early user engagement.
3. Develop targeted strategies for low-engagement customers.

---

## Tools & Technologies

- Python
- PostgreSQL
- MongoDB
- Power BI
- Pandas
- SciPy
- Git / GitHub

---

## Project Structure


---

## Key Insights

**1. High Engagement Customers Show Lower Churn**

Users with higher session activity demonstrate stronger retention and long-term product adoption.

**2. Feature Usage Correlates With Retention**

Customers actively using key product features show lower churn rates, indicating the importance of feature adoption.

**3. Low Engagement Users Represent the Highest Churn Risk**

Users with limited interaction with the platform are significantly more likely to churn.

---

## Business Recommendations

Based on the analysis:

1. Promote adoption of key product features to improve retention.
2. Improve onboarding to increase early user engagement.
3. Develop targeted strategies for low-engagement customers.

---

## Tools & Technologies

- Python
- PostgreSQL
- MongoDB
- Power BI
- Pandas
- SciPy
- Git / GitHub

---

## Project Structure

.
├── queries.sql
├── task2_mongodb_queries.py
├── task3_analysis.py
├── NimbusAI_Dashboard.pbix
└── README.md


---

## Author

**Ayush Raj**

Data Analyst Intern Candidate

---

## Future Improvements

If given more time, the following analyses could be added:

- churn prediction model
- cohort retention analysis
- feature adoption trend analysis
- customer lifetime value modeling

---
