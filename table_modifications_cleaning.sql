SELECT * FROM orders_table;

SELECT MAX(LENGTH(CAST(card_number AS TEXT))) AS card_number_length
FROM orders_table;

SELECT MAX(LENGTH(CAST(user_uuid AS TEXT))) AS user_uuid_lenght
FROM orders_table;

SELECT MAX(LENGTH(CAST(store_code  AS TEXT))) AS store_code_lenght
FROM orders_table;

SELECT MAX(LENGTH(CAST(product_code  AS TEXT))) AS product_code_lenght
FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE varchar(11);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE varchar(12);

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE varchar(19);

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE smallint;



SELECT * FROM dim_users;

SELECT MAX(LENGTH(CAST(first_name AS TEXT))) AS first_name_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE varchar(255);

SELECT MAX(LENGTH(CAST(last_name AS TEXT))) AS last_name_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE varchar(255);

SELECT MAX(LENGTH(CAST(date_of_birth AS TEXT))) AS date_of_birth_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE date;

SELECT MAX(LENGTH(CAST(country_code AS TEXT))) AS country_code_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE varchar(3);

SELECT MAX(LENGTH(CAST(user_uuid AS TEXT))) AS user_uuid_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

SELECT MAX(LENGTH(CAST(join_date AS TEXT))) AS join_date_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE date;



CREATE OR REPLACE FUNCTION alter_table_columns()
RETURNS void AS $$
DECLARE
    col_name text;
    max_length integer;
    rec record;
BEGIN
    -- Loop through each column you want to alter
    FOR rec IN 
        SELECT 'first_name' AS col_name, 'varchar(255)' AS col_type
        UNION ALL
        SELECT 'last_name' AS col_name, 'varchar(255)' AS col_type
        UNION ALL
        SELECT 'date_of_birth' AS col_name, 'date' AS col_type
    LOOP
        -- Check the type and determine the maximum length
        IF rec.col_type LIKE 'varchar%' THEN
            EXECUTE format('SELECT MAX(LENGTH(CAST(%I AS TEXT))) FROM dim_users', rec.col_name)
            INTO max_length;
            
            -- Alter the column if necessary
            IF max_length IS NOT NULL THEN
                EXECUTE format('ALTER TABLE dim_users ALTER COLUMN %I TYPE varchar(%s)', rec.col_name, max_length);
            END IF;
        ELSIF rec.col_type = 'date' THEN
            EXECUTE format('ALTER TABLE dim_users ALTER COLUMN %I TYPE date USING %I::date', rec.col_name, rec.col_name);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the function to alter the table columns
SELECT alter_table_columns();


SELECT * FROM dim_store_details;

UPDATE dim_store_details
SET latitude = lat
WHERE latitude IS NULL OR latitude = '';

ALTER TABLE dim_store_details
DROP COLUMN lat;

-- Add the lat column back to the original table
ALTER TABLE dim_store_details
ADD COLUMN lat TEXT
WHERE dim_store_details.index = dim_store_details_backup.index;  -- Use the appropriate data type

-- Update the lat column with values from the backup table
UPDATE dim_store_details
SET lat = dim_store_details_backup.lat
FROM dim_store_details_backup
WHERE dim_store_details.index = dim_store_details_backup.index;  -- Assuming there's an 'id' column to match rows

-- Drop the backup table if no longer needed
DROP TABLE store_details_backup;




SELECT MAX(LENGTH(CAST(longitude AS TEXT))) AS longitude_lenght
FROM dim_store_details;
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE double precision USING longitude::double precision;

-- Step 1: Identify non-numeric values (for verification, optional)
SELECT longitude
FROM dim_store_details
WHERE NOT longitude ~ '^-?[0-9]+(\.[0-9]*)?$';

-- Step 2: Handle non-numeric values by setting them to NULL
UPDATE dim_store_details
SET longitude = NULL
WHERE NOT longitude ~ '^-?[0-9]+(\.[0-9]*)?$';

-- Step 3: Convert column type
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE double precision USING longitude::double precision;


SELECT MAX(LENGTH(CAST(last_name AS TEXT))) AS last_name_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE varchar(255);

SELECT MAX(LENGTH(CAST(longitude AS TEXT))) AS longitude_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE date;

SELECT MAX(LENGTH(CAST(country_code AS TEXT))) AS country_code_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE varchar(3);

SELECT MAX(LENGTH(CAST(user_uuid AS TEXT))) AS user_uuid_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

SELECT MAX(LENGTH(CAST(join_date AS TEXT))) AS join_date_lenght
FROM dim_users;
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE date;