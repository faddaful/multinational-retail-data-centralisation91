-- Database: sales_data

-- DROP DATABASE IF EXISTS sales_data;

CREATE DATABASE sales_data
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
	
	
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