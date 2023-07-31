--Which months produce the average highest cost of sales typically?
SELECT 
    dim_date_times.month,
    SUM(product_price * product_quantity) AS total_sales
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    dim_date_times.month
ORDER BY
    total_sales DESC;