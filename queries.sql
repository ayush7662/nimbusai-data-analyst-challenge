--NimbusAI Data Analyst Intern Assignment

-- Task 1: SQL Queries
-- Candidate: Ayush Raj
-- gmail: kamalayush6541@gmail.com


---------------------------------------------------------
-- Q1: For each subscription plan, calculate:
-- number of active customers,
-- average monthly revenue,
-- support ticket rate over the last 6 months
---------------------------------------------------------



SELECT
    p.plan_name,
    COUNT(DISTINCT s.customer_id) AS active_customers,
    AVG(p.monthly_price_usd) AS avg_monthly_revenue,
    COUNT(t.ticket_id)::float /
    NULLIF(COUNT(DISTINCT s.customer_id),0) AS tickets_per_customer

FROM subscriptions s
JOIN plans p
ON s.plan_id = p.plan_id

LEFT JOIN support_tickets t
ON s.customer_id = t.customer_id
AND t.created_at >= CURRENT_DATE - INTERVAL '6 months'

WHERE s.status = 'active'

GROUP BY p.plan_name
ORDER BY p.plan_name;






-------------------------------------------------
-- Q2: Rank customers within each plan tier by
-- total lifetime value (LTV) and show percentage
-- difference from the tier average
-------------------------------------------------



SELECT
    s.customer_id,
    p.plan_tier,

    SUM(b.total_usd) AS lifetime_value,

    RANK() OVER (
        PARTITION BY p.plan_tier
        ORDER BY SUM(b.total_usd) DESC
    ) AS rank_in_tier,

    ROUND(
        (
            SUM(b.total_usd) -
            AVG(SUM(b.total_usd)) OVER (PARTITION BY p.plan_tier)
        )
        /
        AVG(SUM(b.total_usd)) OVER (PARTITION BY p.plan_tier)
        * 100,
        2
    ) AS pct_diff_from_tier_avg

FROM subscriptions s
JOIN plans p
ON s.plan_id = p.plan_id

JOIN billing_invoices b
ON s.subscription_id = b.subscription_id

GROUP BY
    s.customer_id,
    p.plan_tier;




-------------------------------------------------
-- Q3: Customers who downgraded their plan in the
-- last 90 days and had >3 support tickets in the
-- 30 days before downgrading. Show current & previous plans.
-------------------------------------------------


WITH recent_subscriptions AS (
    SELECT
        s.customer_id,
        s.subscription_id,
        s.plan_id AS current_plan_id,
        s.created_at AS change_date
    FROM subscriptions s
    WHERE s.created_at >= CURRENT_DATE - INTERVAL '90 days'
),

previous_plans AS (
    SELECT
        rs.customer_id,
        rs.subscription_id,
        rs.current_plan_id,
        rs.change_date,
        p_prev.plan_id AS previous_plan_id,
        p_prev.plan_name AS previous_plan_name,
        p_prev.monthly_price_usd AS previous_price
    FROM recent_subscriptions rs
    JOIN subscriptions s_prev
        ON s_prev.customer_id = rs.customer_id
        AND s_prev.created_at < rs.change_date
    JOIN plans p_prev
        ON s_prev.plan_id = p_prev.plan_id
    
    WHERE s_prev.created_at = (
        SELECT MAX(created_at)
        FROM subscriptions
        WHERE customer_id = rs.customer_id
        AND created_at < rs.change_date
    )
),

downgrades AS (
    SELECT
        pp.customer_id,
        pp.previous_plan_id,
        pp.previous_plan_name,
        pp.previous_price,
        p_curr.plan_id AS current_plan_id,
        p_curr.plan_name AS current_plan_name,
        p_curr.monthly_price_usd AS current_price,
        pp.change_date
    FROM previous_plans pp
    JOIN plans p_curr
        ON pp.current_plan_id = p_curr.plan_id
    WHERE p_curr.monthly_price_usd < pp.previous_price  
)

SELECT
    d.customer_id,
    d.previous_plan_name AS previous_plan,
    d.current_plan_name AS current_plan,
    COUNT(t.ticket_id) AS tickets_before_downgrade
FROM downgrades d
LEFT JOIN support_tickets t
    ON t.customer_id = d.customer_id
    AND t.created_at BETWEEN d.change_date - INTERVAL '30 days'
                       AND d.change_date
GROUP BY
    d.customer_id,
    d.previous_plan_name,
    d.current_plan_name
HAVING COUNT(t.ticket_id) > 3
ORDER BY d.customer_id;





-------------------------------------------------
-- Q4: Month-over-Month growth of new subscriptions
-- and rolling 3-month churn rate by plan tier.
-- Flag months where churn exceeds 2x rolling average.
-------------------------------------------------


WITH monthly_subscriptions AS (

    SELECT
        DATE_TRUNC('month', s.created_at) AS month,
        p.plan_tier,
        COUNT(*) AS new_subscriptions
    FROM subscriptions s
    JOIN plans p
        ON s.plan_id = p.plan_id
    GROUP BY month, p.plan_tier
),

monthly_churn AS (

    SELECT
        DATE_TRUNC('month', churned_at) AS month,
        COUNT(*) AS churned_customers
    FROM customers
    WHERE churned_at IS NOT NULL
    GROUP BY month
),

combined_data AS (

    SELECT
        ms.month,
        ms.plan_tier,
        ms.new_subscriptions,
        COALESCE(mc.churned_customers,0) AS churned_customers
    FROM monthly_subscriptions ms
    LEFT JOIN monthly_churn mc
        ON ms.month = mc.month
)

SELECT
    month,
    plan_tier,
    new_subscriptions,

    
    LAG(new_subscriptions) OVER (
        PARTITION BY plan_tier
        ORDER BY month
    ) AS previous_month_subscriptions,

    ROUND(
        (
            new_subscriptions -
            LAG(new_subscriptions) OVER (
                PARTITION BY plan_tier
                ORDER BY month
            )
        )::numeric /
        NULLIF(
            LAG(new_subscriptions) OVER (
                PARTITION BY plan_tier
                ORDER BY month
            ),0
        ) * 100,
        2
    ) AS mom_growth_percent,

    churned_customers,

    
    AVG(churned_customers) OVER (
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_month_churn,

    
    CASE
        WHEN churned_customers >
        2 * AVG(churned_customers) OVER (
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
        THEN 'High Churn Alert'
        ELSE 'Normal'
    END AS churn_flag

FROM combined_data

ORDER BY month, plan_tier;





-------------------------------------------------
-- Q5: Detect potential duplicate customer accounts
--
-- Matching Logic:
-- 1. Compare company names ignoring case
-- 2. Compare email domains from contact_email
-- 3. Detect overlapping team members assigned
--    to more than one customer account
-------------------------------------------------


SELECT
    c1.customer_id AS customer_1,
    c2.customer_id AS customer_2,
    c1.company_name AS company_1,
    c2.company_name AS company_2,

    SPLIT_PART(c1.contact_email,'@',2) AS domain_1,
    SPLIT_PART(c2.contact_email,'@',2) AS domain_2,

    tm1.email AS overlapping_member_email

FROM customers c1
JOIN customers c2
    ON c1.customer_id < c2.customer_id

JOIN team_members tm1
    ON tm1.customer_id = c1.customer_id

JOIN team_members tm2
    ON tm2.customer_id = c2.customer_id
    AND tm1.email = tm2.email

WHERE
    LOWER(c1.company_name) = LOWER(c2.company_name)

AND SPLIT_PART(c1.contact_email,'@',2) =
    SPLIT_PART(c2.contact_email,'@',2);