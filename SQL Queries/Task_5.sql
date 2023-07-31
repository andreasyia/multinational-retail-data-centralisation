--What percentage of sales come through each type of store?
WITH total_sales_all_stores AS (
    SELECT SUM(product_price * orders_table.product_quantity) AS grand_total_sales
    FROM dim_products
    INNER JOIN orders_table ON dim_products.product_code = orders_table.product_code
)
SELECT 
    dim_store_details.store_type,
    SUM(product_price * orders_table.product_quantity) AS total_sales,
    (SUM(product_price * orders_table.product_quantity) / total_sales_all_stores.grand_total_sales) * 100.0 AS percentage_total
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
CROSS JOIN
    total_sales_all_stores
GROUP BY
    dim_store_details.store_type, total_sales_all_stores.grand_total_sales
ORDER BY
    total_sales DESC;