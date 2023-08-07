-- How quickly is the company making sales?
WITH cte_timestamp_column AS (
    SELECT
        year,
        CONCAT(year, '-', month, '-', day, ' ', timestamp) AS ymd_timestamp
    FROM dim_date_times
),
cte_sales AS (
    SELECT
        year,
        ymd_timestamp,
        LEAD(ymd_timestamp) OVER (PARTITION BY year ORDER BY ymd_timestamp) AS next_timestamp
    FROM
        cte_timestamp_column
),
cte_time_difference AS (
    SELECT
        year,
        ymd_timestamp,
        next_timestamp,
        next_timestamp::timestamp - ymd_timestamp::timestamp AS time_difference
    FROM
        cte_sales
)
SELECT
    year,
    AVG(time_difference) AS average_time_difference
FROM
    cte_time_difference
WHERE
    next_timestamp IS NOT NULL -- Exclude the last sale of each year
GROUP BY
    year
ORDER BY
    year;
