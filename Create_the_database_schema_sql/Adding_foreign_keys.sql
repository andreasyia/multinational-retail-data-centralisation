ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_details
FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_times
FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_products
FOREIGN KEY (product_code) REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_details
FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_users
FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);
