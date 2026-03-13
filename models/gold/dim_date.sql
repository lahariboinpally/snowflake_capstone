WITH dates AS (
 
SELECT DISTINCT
DATE(order_date) AS full_date
FROM {{ ref('silver_orders_data') }}
 
)
 
SELECT
 
/* DateKey */
TO_NUMBER(TO_CHAR(full_date,'YYYYMMDD')) AS datekey,
 
/* Full Date */
full_date,
 
/* Year */
YEAR(full_date) AS year,
 
/* Quarter */
QUARTER(full_date) AS quarter,
 
/* Month */
MONTH(full_date) AS month,
 
/* Week */
WEEK(full_date) AS week,
 
/* Day of Week */
DAYNAME(full_date) AS day_of_week,
 
/* Holiday Flag (Basic US holidays) */
CASE
WHEN TO_CHAR(full_date,'MM-DD') IN ('01-01','07-04','12-25')
THEN TRUE
ELSE FALSE
END AS holiday_flag,
 
/* Season */
CASE
WHEN MONTH(full_date) IN (12,1,2) THEN 'Winter'
WHEN MONTH(full_date) IN (3,4,5) THEN 'Spring'
WHEN MONTH(full_date) IN (6,7,8) THEN 'Summer'
ELSE 'Fall'
END AS season
 
FROM dates
 