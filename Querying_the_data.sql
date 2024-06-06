SELECT * FROM orders_table;
SELECT * FROM dim_users;
SELECT * FROM dim_store_details;
SELECT * FROM dim_card_details;
SELECT * FROM dim_products;
SELECT * FROM dim_date_times;

SELECT s.country_code, c.country, COUNT(o.store_code) AS total_no_stores
FROM dim_users s
JOIN orders_table o ON s.country_code = c.country_code
GROUP BY c.country_code, c.country_name
ORDER BY total_no_stores DESC;


SELECT EXTRACT(YEAR FROM order_date) AS year,
       AVG(actual_time_taken) AS actual_time_taken
FROM (
    SELECT order_date,
           EXTRACT(EPOCH FROM (LEAD(order_date) OVER (ORDER BY order_date) - order_date)) AS actual_time_taken
    FROM orders_table
) AS subquery
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY year;

