SELECT * FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(255);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(255);

ALTER TABLE dim_date_times
ALTER COLUMN DAY TYPE VARCHAR(255);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(255);

-- generate and update new uuids then put that in a new column

ALTER TABLE dim_date_times
ADD COLUMN new_date_uuid UUID;

UPDATE dim_date_times
SET new_date_uuid = gen_random_uuid(); -- This function generates a random UUID

ALTER TABLE dim_date_times
DROP COLUMN date_uuid;

ALTER TABLE dim_date_times
RENAME COLUMN new_date_uuid TO date_uuid;


-- Update dim_card_details table

SELECT * FROM dim_card_details;

SELECT MAX(LENGTH(CAST(date_payment_confirmed AS TEXT))) AS dp_lenght
FROM dim_card_details;

-- Some rows still contain null values, i need to use dynamic sql to loop through and drop all rows with null values

DO $$
DECLARE
    sql_query TEXT;
BEGIN
    SELECT 'DELETE FROM ' || table_name || ' WHERE ' || string_agg(column_name || ' IS NULL', ' OR ')
    INTO sql_query
    FROM information_schema.columns
    WHERE table_schema = 'public'  -- Adjust schema if needed
      AND table_name = 'dim_card_details' -- Adjust table name
    GROUP BY table_name;
    
    EXECUTE sql_query;
END $$;
-- checking if there are still null values
SELECT *
FROM dim_card_details
WHERE date_payment_confirmed IS NULL;

DELETE FROM dim_card_details
WHERE date_payment_confirmed IS NULL;
-- now casting the date_payment_method column to date format
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

-- Find duplicate card numbers
SELECT card_number, COUNT(*)
FROM dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;

DELETE FROM dim_card_details
WHERE card_number IN (
    SELECT card_number
    FROM (
        SELECT card_number, ROW_NUMBER() OVER (PARTITION BY card_number ORDER BY card_number) AS rn
        FROM dim_card_details
    ) AS t
    WHERE rn > 1
);


-- Delete null card numbers
DELETE FROM dim_card_details
WHERE card_number IS NULL;

-- Find NULL card numbers
SELECT *
FROM dim_card_details
WHERE card_number IS NULL;

UPDATE dim_card_details
SET date_payment_confirmed = 2015-11-25
WHERE date_payment_confirmed IS NULL;

DELETE FROM dim_card_details
WHERE date_payment_confirmed IS NOT NULL
AND NOT date_payment_confirmed ~ '^\d{4}-\d{2}-\d{2}$';




-- Updating the primary keys in each table to match the orders_table
SELECT * FROM dim_card_details;
-- Update dim_card_details
ALTER TABLE dim_card_details
ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);

-- Update dim_date_times table with it primary key
SELECT * FROM dim_date_times;
-- Update dim_card_details
ALTER TABLE dim_date_times
ADD CONSTRAINT dim_date_times_pk PRIMARY KEY (date_uuid);

-- Delete null dim_date_times
DELETE FROM dim_date_times
WHERE timestamp = 'NULL';

-- Update dim_product_table with it primary key
SELECT * FROM dim_products;
-- Update dim_card_details
ALTER TABLE dim_products
ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);

-- Update dim_store_details with it primary key
SELECT * FROM dim_store_details;
-- Update dim_card_details
DELETE FROM dim_store_details
WHERE index = '437';
ALTER TABLE dim_store_details
ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);

-- Update dim_users table with it primary key
SELECT * FROM dim_users;
-- Update dim_card_details
ALTER TABLE dim_users
ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);
-- Casting user_uuid into a uuid format for consistency
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

-- creating foreign keys in the orders_table
SELECT * FROM orders_table;
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
DELETE FROM orders_table
WHERE date_uuid = '9476f17e-5d6a-4117-874d-9cdb38ca1fa6';


ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE varchar(22);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
