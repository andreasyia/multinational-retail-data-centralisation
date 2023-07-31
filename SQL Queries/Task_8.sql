--Which German store type is selling the most?
WITH total_sales_Germany_stores AS (
    SELECT SUM(product_price * orders_table.product_quantity) AS Germany_total_sales
    FROM dim_products
    INNER JOIN orders_table ON dim_products.product_code = orders_table.product_code
    INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    WHERE dim_store_details.country_code = 'DE'
)
SELECT 
    SUM(product_price * orders_table.product_quantity) AS total_sales,
    dim_store_details.store_type,
	dim_store_details.country_code
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
CROSS JOIN
    total_sales_Germany_stores
WHERE
    dim_store_details.country_code = 'DE' 
GROUP BY
    dim_store_details.store_type, 
	total_sales_Germany_stores.Germany_total_sales, 
	dim_store_details.country_code
ORDER BY
    total_sales ASC;WITH total_sales_Germany_stores AS (
    SELECT SUM(product_price * orders_table.product_quantity) AS Germany_total_sales
    FROM dim_products
    INNER JOIN orders_table ON dim_products.product_code = orders_table.product_code
    INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    WHERE dim_store_details.country_code = 'DE'
)
SELECT 
    SUM(product_price * orders_table.product_quantity) AS total_sales,
    dim_store_details.store_type,
	dim_store_details.country_code
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
CROSS JOIN
    total_sales_Germany_stores
WHERE
    dim_store_details.country_code = 'DE' 
GROUP BY
    dim_store_details.store_type, 
	total_sales_Germany_stores.Germany_total_sales, 
	dim_store_details.country_code
ORDER BY
    total_sales ASC;