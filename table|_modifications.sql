SELECT * FROM dim_products;

ALTER TABLE dim_products
ADD COLUMN weight_class varchar(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;


-- Change the data type of product_price to FLOAT
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;

-- Change the data type of weight to FLOAT
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

-- Change the data type of EAN to VARCHAR(20) (assuming a length of 20, adjust as necessary)
ALTER TABLE dim_products
RENAME COLUMN EAN TO ean;
ALTER TABLE dim_products
ALTER COLUMN EAN TYPE VARCHAR(20);

-- Change the data type of product_code to VARCHAR(50) (assuming a length of 50, adjust as necessary)
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(255);

-- Change the data type of date_added to DATE
ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::date;

-- Change the data type of uuid to UUID
ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

-- Change the data type of still_available to BOOL
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING (still_available::BOOLEAN);

-- Ensure weight_class is VARCHAR(20) (assuming a length of 20, adjust as necessary)
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(20);
