-- orders_detail-------------------------------
CREATE TABLE orders_table_new_type(
    date_uuid UUID,
    user_uuid UUID,
    card_number VARCHAR,
    store_code VARCHAR,
    product_code VARCHAR,
    product_quantity SMALLINT
)

-- INSERT 
INSERT INTO orders_table_new_type 
(
    date_uuid,
    user_uuid,
    card_number,
    store_code,
    product_code,
    product_quantity) 
SELECT 
CAST(date_uuid AS UUID),
CAST(user_uuid AS UUID),
CAST(card_number AS VARCHAR(19)),
CAST(store_code AS VARCHAR(12)),
CAST(product_code AS VARCHAR(11)),
CAST(product_quantity AS SMALLINT)
FROM orders_table


-- dim_users_table---------------------------------------
CREATE TABLE dim_users_table_new_type(
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    date_of_birth DATE,
    country_code VARCHAR,
    user_uuid UUID,
    join_date DATE

)
-- INSERT
INSERT INTO dim_users_table_new_type (
    first_name,
    last_name,
    date_of_birth,
    country_code,
    user_uuid,
    join_date
)
SELECT 
CAST(first_name AS VARCHAR(255)),
CAST(last_name AS VARCHAR(255)),
CAST(date_of_birth AS DATE),
CAST(country_code AS VARCHAR),
CAST(user_uuid AS UUID),
CAST(join_date AS DATE)
FROM dim_user

-- dim_stroe_details-----------------------------------

CREATE TABLE dim_store_details_new_type (
    longitude FLOAT,
    locality VARCHAR(255),
    store_code VARCHAR,
    staff_numbers SMALLINT,
    opening_date DATE,
    store_type VARCHAR(255),
    latitude FLOAT,
    country_code VARCHAR,
    continent VARCHAR(255)
)

--INSERT 
INSERT INTO dim_store_details_new_type(
    longitude,
    locality,
    store_code,
    staff_numbers,
    opening_date,
    store_type,
    latitude,
    country_code,
    continent
)
SELECT 
CAST(longitude AS FLOAT),
CAST(locality AS VARCHAR(255)),
CAST(store_code AS varchar(12)),
CAST(staff_numbers AS SMALLINT),
CAST(opening_date AS DATE),
CAST(store_type AS VARCHAR(255)),
CAST(latitude AS FLOAT),
CAST(country_code AS VARCHAR(2)),
CAST(continent AS VARCHAR(255))
FROM dim_store_details


-- dim_products-------------------------------------------
ALTER TABLE dim_products ADD weight_class VARCHAR;

UPDATE dim_products
SET 
        weight_class = CASE
		WHEN weight_kg < 3 THEN 'Light'
		WHEN weight_kg BETWEEN 3 AND 40 THEN 'Min_Sized'
		WHEN weight_kg BETWEEN 41 AND 140 THEN 'Heavy'
		WHEN weight_kg >= 141 THEN 'Truck_Required' END;

ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

CREATE TABLE dim_products_new_type (
    product_price FLOAT,
    weight_kg FLOAT,
    EAN VARCHAR,
    product_code VARCHAR,
    date_added DATE,
    uuid UUID,
    still_available BOOL,
    weight_class VARCHAR);

-- INSERT
INSERT INTO dim_products_new_type (
    product_price,
    weight_kg,
    EAN,
    product_code,
    date_added,
    uuid,
    still_available,
    weight_class 
)
SELECT 
	CAST(product_price AS FLOAT),
	CAST(weight_kg AS FLOAT),
	CAST("EAN" AS VARCHAR(17)),
	CAST(product_code AS VARCHAR(12)),
	CAST(date_added AS DATE),
	CAST(uuid AS UUID),
	CAST(still_available AS BOOL),
	CAST(weight_class AS VARCHAR(12))
FROM dim_products;

-- dim_date_time---------------------------------------------
CREATE TABLE dim_date_times_new_type (
    timestamp TEXT,
    month CHAR(2),
    year CHAR(4),
    day CHAR(2),
    time_period VARCHAR(10),
    date_uuid UUID,
    date TIMESTAMP
);
--INSERT
INSERT INTO dim_date_times_new_type (
    timestamp,
    month,
    year,
    day,
    time_period,
    date_uuid,
    date
)
SELECT
    CAST(timestamp AS TEXT),
    CAST(month AS CHAR(2)),
    CAST(year AS CHAR(4)),
    CAST(day AS CHAR(2)),
	CAST(time_period AS VARCHAR(10)),
	CAST(date_uuid AS UUID)
    CAST(date AS TIMESTAMP)
FROM dim_date_times



-- dim_card_details-------------------------------------------
CREATE TABLE dim_card_details_new(
    card_number VARCHAR,
    expiry_date VARCHAR,
    date_payment_confirmed DATE
);

--INSERT
INSERT INTO dim_card_details_new(
    card_number,
    expiry_date,
    date_payment_confirmed
)
SELECT
	CAST(card_number AS VARCHAR(19)),
	CAST(expiry_date AS VARCHAR(10)),
	CAST(date_payment_confirmed AS DATE)
FROM dim_card_details;


SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE
     TABLE_NAME = 'orders_table_new_type' AND
     COLUMN_NAME = 'card_number';

-- PRIMARY KEY ADD---------------------------

-- USER_TABLE ------------------
ALTER TABLE dim_users_table_new_type
ADD PRIMARY KEY (user_uuid);

-- STORE_DETAILS
ALTER TABLE dim_stroe_details_new_type
ADD PRIMARY KEY(store_code);

ALTER TABLE dim_store_details_new_type
DROP CONSTRAINT dim_store_details_new_type_PKEY;

ALTER TABLE dim_store_details_new_type
ADD PRIMARY KEY(store_code);

--PRODUCTS
ALTER TABLE dim_products_new_type
ADD PRIMARY KEY(product_code);

--DATE_TIMES
ALTER TABLE dim_date_times_new_type
ADD PRIMARY KEY(date_uuid);

--CARD_DETAILS
ALTER TABLE dim_card_details_new
ADD PRIMARY KEY(card_number);

-----------FOREIGN KEY

-- PRODUCT ------------------
ALTER TABLE IF EXISTS public.orders_table_new_type
    ADD CONSTRAINT FK_PRODUCT FOREIGN KEY (product_code)
    REFERENCES public.dim_products_new_type (product_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE orders_table_new_type
DROP CONSTRAINT FK_PRODUCT;



--USER--------------
ALTER TABLE IF EXISTS public.orders_table_new_type
    ADD CONSTRAINT FK_USER FOREIGN KEY (user_uuid)
    REFERENCES public.dim_users_table_new_type (user_uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

--DATE
ALTER TABLE orders_table_new_type
    ADD CONSTRAINT FK_DATE FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times_new_type (date_uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

--CARD_NUMBER
ALTER TABLE orders_table_new_type
    ADD CONSTRAINT FK_CARD FOREIGN KEY (card_number)
    REFERENCES dim_card_details_new (card_number)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- STORE
ALTER TABLE orders_table_new_type
    ADD CONSTRAINT FK_STORE FOREIGN KEY (store_code)
    REFERENCES dim_stroe_details_new_type (store_code)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE orders_table_new_type
DROP CONSTRAINT FK_STORE;

DROP TABLE dim_stroe_details_new_type;


ALTER TABLE orders_table_new_type
    ADD CONSTRAINT FK_STORE FOREIGN KEY (store_code)
    REFERENCES dim_store_details_new_type (store_code)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- HOW MANY STORES -------------------
SELECT DISTINCT(country_code) AS country, COUNT(store_code) AS TOTAL_NO_STORES
FROM dim_stroe_details_new_type
GROUP BY country 
ORDER BY TOTAL_NO_STORES DESC;

--LOCAL
SELECT DISTINCT(locality) AS locality, COUNT(store_code) AS total_no_stores
FROM dim_stroe_details_new_type
GROUP BY locality 
ORDER BY total_no_stores DESC, locality DESC
LIMIT 7;

--SALES
SELECT SUM(od.product_quantity*pro.product_price) AS TOTAL_SALES,
dt.month
FROM  orders_table_new_type AS od
LEFT JOIN dim_products_new_type AS pro
ON od.product_code = pro.product_code
LEFT JOIN dim_date_times_new_type AS dt
ON od.date_uuid = dt.date_uuid
GROUP BY dt.month
ORDER BY TOTAL_SALES DESC;

--FROM ONLINE
SELECT 
    COUNT(CASE WHEN pro.still_available THEN 1 END) AS numbers_of_sales, -- count true value
    SUM(CASE WHEN pro.still_available THEN od.product_quantity END) AS product_quantity_count,
    CASE 
        WHEN st.store_type IN ('Local','Mall Kiosk', 'Outlet','Super Store') THEN 'Offline'
        ELSE 'Web'
    END AS location
FROM orders_table_new_type AS od
LEFT JOIN dim_store_details_new_type AS st
ON od.store_code = st.store_code
LEFT JOIN dim_products_new_type AS pro
ON  pro.product_code = od.product_code
GROUP BY 
    CASE 
        WHEN st.store_type IN ('Local','Mall Kiosk','Outlet','Super Store') THEN 'Offline'
        ELSE 'Web'
    END
ORDER BY location DESC;

SELECT 
    COUNT(NULLIF(pro.still_available,FALSE)) AS numbers_of_sales, -- count ture values
    SUM(CASE WHEN pro.still_available THEN od.product_quantity END) AS product_quantity_count,
    CASE 
        WHEN st.store_type IN ('Local','Mall Kiosk', 'Outlet','Super Store') THEN 'Offline'
        ELSE 'Web'
    END AS location
FROM orders_table_new_type AS od
LEFT JOIN dim_store_details_new_type AS st
ON od.store_code = st.store_code
LEFT JOIN dim_products_new_type AS pro
ON  pro.product_code = od.product_code
GROUP BY 
    CASE 
        WHEN st.store_type IN ('Local','Mall Kiosk','Outlet','Super Store') THEN 'Offline'
        ELSE 'Web'
    END
ORDER BY location DESC;

-- EACH STORE REVENUE
SELECT 
store_type,
numbers_of_sales,
ROUND(CAST((numbers_of_sales/SUM(numbers_of_sales) OVER()) * 100 AS numeric) ,2) AS "percentage_total(%)"
FROM (
    SELECT
    st.store_type,
    ROUND(CAST(SUM(od.product_quantity*pro.product_price) AS numeric), 2) AS numbers_of_sales
    FROM orders_table_new_type AS od
    LEFT JOIN dim_store_details_new_type AS st
    ON od.store_code = st.store_code
    LEFT JOIN dim_products_new_type AS pro
    ON od.product_code = pro.product_code 
    GROUP BY st.store_type
    ORDER BY numbers_of_sales DESC) AS T;


-- WHICH MONTH 
SELECT 
SUM(od.product_quantity*pro.product_price) AS total_sales,
d.year AS year,
d.month AS month
FROM orders_table_new_type AS od
LEFT JOIN dim_products_new_type AS pro
ON od.product_code = pro.product_code
LEFT JOIN dim_date_times_new_type AS d
ON od.date_uuid = d.date_uuid
GROUP by d.month,d.year 
ORDER BY TOTAL_SALES DESC;


--SALES
WITH sales_by_month AS (
    SELECT 
        TRUNC(SUM(od.product_quantity*pro.product_price)) AS total_sales,
        dt.year,
        dt.month
    FROM dim_products_new_type AS pro
    LEFT JOIN orders_table_new_type AS od
        ON od.product_code = pro.product_code
    LEFT JOIN dim_date_times_new_type AS dt
        ON od.date_uuid = dt.date_uuid
    GROUP BY 
        dt.year, 
        dt.month
)
SELECT 
    sub2.total_sales,
    sub2.year,
    sub2.month
FROM (
    SELECT 
        ROW_NUMBER() OVER(PARTITION BY year ORDER BY total_sales DESC) AS rank,
        total_sales,
        year,
        month
    FROM sales_by_month
    ORDER BY 
    total_sales DESC
) AS sub2
WHERE sub2.rank = 1 LIMIT 5;


---------Task 7
SELECT 
SUM(st.staff_numbers) AS total_staff_numbers,
st.country_code AS country_code
FROM 
dim_store_details_new_type AS st
GROUP BY
CASE
WHEN st.store_type = 'Web Portal'THEN st.country_code = 'Web' END,
country_code
ORDER BY 
total_staff_numbers DESC;

------Task 8
SELECT 
COUNT(od.product_quantity*pro.product_price) AS total_sales,
st.store_type AS store_type,
st.country_code AS country_code
FROM
orders_table_new_type AS od
LEFT JOIN
dim_products_new_type AS pro
ON 
od.product_code = pro.product_code
LEFT JOIN
dim_store_details_new_type AS st
ON
od.store_code = st.store_code
WHERE st.country_code = 'DE'
GROUP BY
country_code,
store_type
ORDER BY
total_sales;


---Task 9
WITH making_sale AS (
    SELECT 
    DATE_TRUNC('year', date) AS year,
    TO_TIMESTAMP(TO_CHAR(date, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')AS sale_timestamp,
    LEAD(TO_TIMESTAMP(TO_CHAR(date, 'YYYY-MM-DD HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS')) OVER(PARTITION BY DATE_TRUNC('year', date) ORDER BY date) AS sale_timestamp_next
    FROM dim_date_times_new_type
)
SELECT 
year,
AVG(sale_timestamp_next - sale_timestamp) AS actual_time_taken
FROM making_sale
GROUP BY year
ORDER BY actual_time_taken DESC LIMIT 5;

