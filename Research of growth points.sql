--Distribution of the number of user sessions
WITH profiles AS (
    SELECT DISTINCT 
        visitor_uuid,
        FIRST_VALUE(c.city_name) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS city_name,
        FIRST_VALUE(datetime) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS first_date,
        FIRST_VALUE(device_type) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS device_type,
        FIRST_VALUE(source) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS source
    FROM module3_analytics_events AS a
    LEFT JOIN module3_cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1
),
sessions AS (
    SELECT 
        visitor_uuid,
        MAX(visit_num) AS sessions_num
    FROM module3_analytics_events
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND log_date BETWEEN '2021-06-15' AND '2021-07-01'
    GROUP BY visitor_uuid
)
SELECT 
    p.device_type,
    s.sessions_num,
    COUNT(DISTINCT s.visitor_uuid) AS uniques
FROM sessions s
LEFT JOIN profiles p ON s.visitor_uuid = p.visitor_uuid
GROUP BY 
    p.device_type,
    s.sessions_num
ORDER BY 
    p.device_type,
    s.sessions_num;



--Distribution of first purchases by session numbers

WITH profiles as (

    /* User profiles */

    SELECT DISTINCT visitor_uuid,
           FIRST_VALUE(c.city_name) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS city_name,
           FIRST_VALUE(a.first_date) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS first_date,
           FIRST_VALUE(a.device_type) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS device_type,
           FIRST_VALUE(a.source) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS source
    FROM module3_analytics_events AS a
    LEFT JOIN module3_cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1

),
orders AS (

    /* Purchases and their numbers */

    SELECT visitor_uuid,
           visit_num,
           DENSE_RANK() OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS event_num
    FROM module3_analytics_events
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND log_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND event = 'order'

)

/* Calculate medians */

SELECT p.device_type,
       COUNT (DISTINCT o.visitor_uuid) AS buyers,
       PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY visit_num)/* §©§Ñ§Õ§Ñ§Û§ä§Ö §Ó§í§â§Ñ§Ø§Ö§ß§Ú§Ö §Õ§Ý§ñ §â§Ñ§ã§é§×§ä§Ñ §Þ§Ö§Õ§Ú§Ñ§ß§ß§à§Ô§à §ß§à§Þ§Ö§â§Ñ §ã§Ö§ã§ã§Ú§Ú §Ù§Õ§Ö§ã§î */ AS median_visit_num
FROM orders o
LEFT JOIN profiles p ON o.visitor_uuid = p.visitor_uuid
WHERE o.event_num = 1
GROUP BY p.device_type
ORDER BY buyers DESC


--Studying the sequence of user actions

WITH profiles as (
    SELECT DISTINCT 
        visitor_uuid,
        FIRST_VALUE(c.city_name) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS city_name,
        FIRST_VALUE(a.first_date) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS first_date,
        FIRST_VALUE(a.device_type) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS device_type,
        FIRST_VALUE(a.source) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS source
    FROM module3_analytics_events AS a
    LEFT JOIN module3_cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1
),
events AS (
    SELECT 
        visitor_uuid,
        visit_num,
        event,
        DENSE_RANK() OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS event_num
    FROM module3_analytics_events
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND log_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1
),
events_profiles AS (
    SELECT 
        e.visitor_uuid,
        p.city_name,
        p.first_date,
        p.device_type,
        p.source,
        e.visit_num,
        e.event,
        e.event_num
    FROM events e
    LEFT JOIN profiles p ON e.visitor_uuid = p.visitor_uuid
)

SELECT 
    city_name,
    first_date,
    device_type,
    source,
    visit_num,
    event_num,
    event,
    COUNT(DISTINCT visitor_uuid) AS events
FROM events_profiles
WHERE event_num <= 10  
GROUP BY 
    city_name,
    first_date,
    device_type,
    source,
    visit_num,
    event_num,
    event
ORDER BY 
    city_name,
    first_date,
    device_type,
    source,
    visit_num,
    event_num;

--User journey

WITH profiles as (

    /* User profiles */

    SELECT DISTINCT visitor_uuid,
           FIRST_VALUE(c.city_name) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS city_name,
           FIRST_VALUE(a.datetime) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS first_datetime,
           FIRST_VALUE(a.first_date) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS first_date,
           FIRST_VALUE(a.device_type) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS device_type,
           FIRST_VALUE(a.source) OVER (PARTITION BY visitor_uuid ORDER BY datetime) AS source
    FROM module3_analytics_events AS a
    LEFT JOIN module3_cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1

),
events AS (

    /* Events committed by users during the first session */

    SELECT visitor_uuid,
           event,
           datetime
    FROM module3_analytics_events
    WHERE first_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND log_date BETWEEN '2021-06-15' AND '2021-07-01'
          AND visit_num = 1

)

/* Profiles, events and times of their occurrence */

SELECT e.visitor_uuid,
       p.city_name,
       p.first_date,
       p.device_type,
       p.source,
       e.event,
       MIN(EXTRACT(epoch FROM e.datetime - p.first_datetime)/60) AS time_to_event
FROM events e
LEFT JOIN profiles p ON e.visitor_uuid = p.visitor_uuid
GROUP BY e.visitor_uuid,
         p.city_name,
         p.first_date,
         p.device_type,
         p.source,
         e.event



--RFM-analysis

WITH profiles as (

    /* User profiles */

    SELECT DISTINCT user_id,
           FIRST_VALUE(c.city_name) OVER (PARTITION BY user_id ORDER BY datetime) AS city_name,
           FIRST_VALUE(a.first_date) OVER (PARTITION BY user_id ORDER BY datetime) AS first_date,
           FIRST_VALUE(a.device_type) OVER (PARTITION BY user_id ORDER BY datetime) AS device_type,
           FIRST_VALUE(a.source) OVER (PARTITION BY user_id ORDER BY datetime) AS source
    FROM module3_analytics_events AS a
    LEFT JOIN module3_cities c ON a.city_id = c.city_id
    WHERE first_date BETWEEN '2021-05-01' AND '2021-06-01'
          AND user_id IS NOT NULL

),
orders AS (

    /* User purchases */

    SELECT 'dummy' AS dummy_key,
           user_id,
           MIN(DATE('2021-07-01') - log_date) AS recency,
           COUNT(log_date) AS frequency,
           SUM(revenue) AS monetary_value
    FROM module3_analytics_events
    WHERE first_date BETWEEN '2021-05-01' AND '2021-06-01'
          and event = 'order'
    GROUP BY user_id

),
boundaries AS (

    /* Boundaries of RFM groups */

    SELECT 'dummy' AS dummy_key,
       PERCENTILE_CONT(0.33) WITHIN GROUP(ORDER BY recency) AS p33_recency,
       PERCENTILE_CONT(0.66) WITHIN GROUP(ORDER BY recency) AS p66_recency,

       PERCENTILE_CONT(0.33) WITHIN GROUP(ORDER BY frequency) AS p33_frequency,
       PERCENTILE_CONT(0.66) WITHIN GROUP(ORDER BY frequency) AS p66_frequency,

       PERCENTILE_CONT(0.33) WITHIN GROUP(ORDER BY monetary_value) AS p33_monetary_value,
       PERCENTILE_CONT(0.66) WITHIN GROUP(ORDER BY monetary_value) AS p66_monetary_value
    FROM orders


),
rfm AS (

    /* Define RFM groups for users */

    SELECT user_id,
           recency,
           frequency,
           monetary_value,
           p33_recency,
           p66_recency,
           p33_frequency,
           p66_frequency,
           p33_monetary_value,
           p66_monetary_value,
           CASE 
           WHEN recency <= p33_recency THEN 3 
           WHEN recency > p33_recency AND recency <= p66_recency THEN 2
           ELSE 1
       END AS r,
       CASE 
           WHEN frequency <= p33_frequency THEN 1 
           WHEN frequency > p33_frequency AND frequency <= p66_frequency THEN 2
           ELSE 3
       END AS f,  
       CASE 
           WHEN monetary_value <= p33_monetary_value THEN 1 
           WHEN monetary_value > p33_monetary_value AND monetary_value <= p66_monetary_value THEN 2
           ELSE 3
       END AS m
    FROM orders o
    LEFT JOIN boundaries b ON b.dummy_key = o.dummy_key

)

/* Merge RFM groups with profiles */

SELECT r.user_id,
       city_name,
       first_date,
       device_type,
       source,
       r, f, m
FROM rfm r
LEFT JOIN profiles p ON p.user_id = r.user_id



--ABC-XYZ-§Ñ§ß§Ñ§Ý§Ú§Ù

WITH daily_revenue AS (

    /* Calculate the weekly revenue of networks */

    SELECT p.chain,
           date_trunc('week', log_date) as log_week,
           SUM(revenue) AS revenue
    FROM module3_analytics_events e
    LEFT JOIN module3_partners p on p.rest_id = e.rest_id
    WHERE event = 'order'
    GROUP BY p.chain,
             date_trunc('week', log_date)

),
partners AS (

    /* Calculate the coefficients of variability */

    SELECT chain,
           SUM(revenue) AS revenue,
           STDDEV(revenue) AS std,
           STDDEV(revenue)/AVG(revenue) AS var_coeff
    FROM daily_revenue
    GROUP BY chain

),
abc_xyz AS (

    /* Calculate shares of total revenue */

    SELECT chain,
           revenue,
           SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
           SUM(revenue) OVER () tot_rev,
           SUM(revenue) OVER (ORDER BY revenue DESC)/SUM(revenue) OVER () AS perc,
           std,
           var_coeff
    FROM partners
    ORDER BY revenue DESC

)

/* We distribute partner restaurants into groups */

SELECT chain,
       CASE 
           WHEN perc <= 0.8 THEN 'A'
           WHEN perc <= 0.95 THEN 'B'
           ELSE 'C'
       END AS abc,
       CASE 
           WHEN var_coeff <= 0.1 THEN 'X'
           WHEN var_coeff <= 0.3 THEN 'Y'
           ELSE 'Z'
       END AS xyz
FROM abc_xyz