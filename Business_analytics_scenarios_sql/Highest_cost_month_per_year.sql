--Which month in each year produced the highest cost of sales?
WITH total_sales_all_months AS (
    SELECT SUM(product_price * orders_table.product_quantity) AS grand_total_sales
    FROM dim_products
    INNER JOIN orders_table ON dim_products.product_code = orders_table.product_code
)
SELECT 
 	SUM(product_price * orders_table.product_quantity) AS total_sales,
    dim_date_times.year,
	dim_date_times.month
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
CROSS JOIN
    total_sales_all_months
GROUP BY
    dim_date_times.month, dim_date_times.year, total_sales_all_months.grand_total_sales
ORDER BY
    total_sales DESC;