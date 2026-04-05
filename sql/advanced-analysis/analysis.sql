-- Q1 — Total trips and revenue by trip status
SELECT
    trip_status,
    COUNT(*)                    AS total_trips,
    SUM(fare_amount)            AS total_revenue,
    ROUND(AVG(fare_amount), 2)  AS average_fare
FROM trips
GROUP BY trip_status
ORDER BY total_trips DESC;

-- Q2 — Drivers with more than 4 completed trips
select
    driver_id,
    driver_name,
    count(trip_id) as total_trips,
    sum(fare_amount) as total_revenue
from trips
where trip_status = 'completed'
group by driver_id, driver_name
having count(trip_id) > 4;

-- Q3 — how many trips happened on each date. show date and trip count ordered by date
SELECT
    DATE(request_time) AS trip_date,
    COUNT(*) AS trip_count
FROM trips
GROUP BY trip_date
ORDER BY trip_date;

-- Q4 — Q4 — String functions Some pickup locations have inconsistent spacing.
--Return all distinct pickup locations with leading/trailing whitespace stripped and converted to uppercase.
SELECT DISTINCT
    UPPER(TRIM(pickup_location)) AS cleaned_pickup_location
FROM trips
WHERE pickup_location IS NOT NULL
ORDER BY cleaned_pickup_location;

-- Q5 -- NULL handling
--List all trips where rating is NULL. Show trip ID, driver name, and trip status.
SELECT
    trip_id,
    driver_name,
    trip_status
FROM trips
WHERE rating IS NULL
ORDER BY trip_id;

-- Q6 -- CASE WHEN
-- Categorise each trip by fare size: 'budget' for fares under 1000, 'standard' for 1000–2000, 'premium' for above 2000.
-- Show trip ID, fare amount, and the category.
SELECT
    trip_id,
    fare_amount,
    CASE
        WHEN fare_amount < 1000 THEN 'budget'
        WHEN fare_amount BETWEEN 1000 AND 2000 THEN 'standard'
        ELSE 'premium'
    END AS fare_category
FROM trips
ORDER BY trip_id;

-- Q7 -- Window function: ROW_NUMBER: Rank each driver's trips by fare amount (highest first) using ROW_NUMBER.
--  Show driver name, trip ID, fare amount, and the rank. Only show each driver's top 3 trips.
WITH ranked_trips AS (
    SELECT
        driver_name,
        trip_id,
        fare_amount,
        ROW_NUMBER() OVER (PARTITION BY driver_name ORDER BY fare_amount DESC) AS fare_rank
    FROM trips
    WHERE trip_status = 'completed'
)
SELECT
    driver_name,
    trip_id,
    fare_amount,
    fare_rank
FROM ranked_trips
WHERE fare_rank <= 3
ORDER BY driver_name, fare_rank;

-- Q8 — Window function: running total: Calculate a running total of revenue ordered by request_time.
-- Show trip ID, request time, fare amount, and the cumulative revenue so far.
SELECT
    trip_id,
    request_time,
    fare_amount,
    round(SUM(fare_amount) OVER (ORDER BY request_time), 2) AS cumulative_revenue
FROM trips
ORDER BY request_time;

-- Q9 — CTE Using a CTE, first calculate each driver's average rating and total trips,
-- then in the main query return only drivers with an average rating above 4.3.
WITH driver_stats AS (
    SELECT
        driver_id,
        driver_name,
        AVG(rating) AS average_rating,
        COUNT(*) AS total_trips
    FROM trips
    WHERE rating IS NOT NULL
    GROUP BY driver_id, driver_name
)
SELECT
    driver_name,
    average_rating,
    total_trips
FROM driver_stats
WHERE average_rating > 4.3
ORDER BY average_rating DESC;

-- Q10 — CASE WHEN + aggregation combined For each payment type, how many trips were 'budget', 'standard', and 'premium'?
-- This is a pivot-style query using CASE WHEN inside COUNT.
SELECT
    payment_type,
    COUNT(CASE WHEN fare_amount < 1000 THEN 1 END) AS budget_trips,
    COUNT(CASE WHEN fare_amount BETWEEN 1000 AND 2000 THEN 1 END) AS standard_trips,
    COUNT(CASE WHEN fare_amount > 2000 THEN 1 END) AS premium_trips
FROM trips
WHERE trip_status = 'completed'
GROUP BY payment_type
ORDER BY payment_type;


