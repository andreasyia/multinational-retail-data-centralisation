ALTER TABLE dim_store_details
ALTER COLUMN longitude FLOAT,
ALTER COLUMN locality VARCHAR(255),
ALTER COLUMN store_code VARCHAR(12),
ALTER COLUMN staff_numbers SMALLINT,
ALTER COLUMN opening_date DATE,
ALTER COLUMN store_type VARCHAR(255) NULLABLE,
ALTER COLUMN latitude FLOAT,
ALTER COLUMN country_code VARCHAR(2),
ALTER COLUMN continent VARCHAR(255);

UPDATE dim_store_details
SET locality = COALESCE(locality, 'N/A')
WHERE locality IS NULL;


