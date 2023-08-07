--How many sales are coming from online?
SELECT 
    CASE 
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location,
    SUM(product_quantity) AS product_quantity_count,
    COUNT(product_price * product_quantity) AS number_of_sales
FROM 
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY
    location
ORDER BY
    product_quantity_count DESC;