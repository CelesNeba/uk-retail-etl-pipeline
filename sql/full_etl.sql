-- Staging layer

-- Create the Staging Database

CREATE DATABASE IF NOT EXISTS staging;
-- This creates a database called staging to hold raw CSV data.
-----------------------------------------------------------------------------------------
--  Create the staging table
USE staging;
CREATE TABLE IF NOT EXISTS stg_retail_prices (
    fact_id VARCHAR(32) PRIMARY KEY,
    raw_date DATE,
    supermarket VARCHAR(50),                               
    product_name VARCHAR(150),
    price_gbp DECIMAL(10,2)
);
-- This table mirrors the CSV structure:
-- fact_id → unique identifier
-- raw_date → date of price
-- supermarket → supermarket name
-- product_name → product name
-- price_gbp → price in GBP
---------------------------------------------------------------------------------------

-- Validation

SELECT COUNT(*) FROM stg_retail_prices;

SELECT * FROM stg_retail_prices
LIMIT 5;


-------------------------------------------------------------------------------------
-- PHASE 2: Analytics Database + Dimension Tables
-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- Data quality check
-- Before building dimensions, we inspect staging data.
-------------------------------------------------------------------------------
-- Check for NULLs
SELECT
    COUNT(*) AS total_rows,
    SUM(raw_date IS NULL) AS null_dates,
    SUM(supermarket IS NULL) AS null_supermarket,
    SUM(product_name IS NULL) AS null_product,
    SUM(price_gbp IS NULL) AS null_price
FROM stg_retail_prices;
-- This checks if any important fields are missing.
--------------------------------------------------------------------------------------------

-- Check for invalid Prices
SELECT COUNT(*) 
FROM stg_retail_prices
WHERE price_gbp <= 0;
-- Prices should never be ≤ 0.
---------------------------------------------------------------------------------------------------------

-- Check for duplicates
SELECT fact_id, COUNT(*)
FROM stg_retail_prices
GROUP BY fact_id
HAVING COUNT(*) > 1;
-- fact_id must be unique.
-----------------------------------------------------------------------------------------------------------

-- Check price distribution
USE staging;

SELECT
    MIN(price_gbp)   AS min_price,
    MAX(price_gbp)   AS max_price,
    AVG(price_gbp)   AS avg_price,
    STDDEV(price_gbp) AS std_price
FROM stg_retail_prices;

-- This shows:
-- Lowest price
-- Highest price
-- Average
-- Standard deviation
---------------------------------------------------------------------------------------------------
-- Find outliers (IQR Method
WITH ranked AS (
    SELECT
        price_gbp,
        NTILE(4) OVER (ORDER BY price_gbp) AS quartile
    FROM stg_retail_prices
)

SELECT
    MIN(CASE WHEN quartile = 1 THEN price_gbp END) AS Q1,
    MIN(CASE WHEN quartile = 3 THEN price_gbp END) AS Q3
FROM ranked;



-- Calculate bounds
SELECT
    Q1,
    Q3,
    (Q3 - Q1) AS IQR,
    Q1 - 1.5*(Q3 - Q1) AS lower_bound,
    Q3 + 1.5*(Q3 - Q1) AS upper_bound
FROM (
    SELECT
        1.25 AS Q1,   -- replace
        8.60 AS Q3    -- replace
) t;


-- Count outliers

SELECT COUNT(*) AS outliers
FROM stg_retail_prices
WHERE price_gbp < -36.64
   OR price_gbp > 62.40;
--------------------------------------------------------------------------------------------

-- Create clean View
-- Instead of touching raw staging data, I'll create a clean view.

CREATE OR REPLACE VIEW staging.vw_clean_retail AS
SELECT
    fact_id,
    raw_date,
    TRIM(supermarket) AS supermarket,
    TRIM(product_name) AS product_name,
    price_gbp
FROM stg_retail_prices
WHERE
    raw_date IS NOT NULL
    AND supermarket IS NOT NULL
    AND product_name IS NOT NULL
    AND price_gbp > 0;
-- What this does:
-- Removes NULLs
-- Removes bad prices
-- Cleans spaces
-- Preserves raw data

-- This is enterprise standard.
---------------------------------------------------------------------------------------------
-- Create supermarket dimension
USE analytics;
-- Create table:
CREATE TABLE dim_supermarket (
    supermarket_id VARCHAR(10) PRIMARY KEY,
    supermarket_name VARCHAR(100),
    location VARCHAR(100),
    store_type VARCHAR(50)
);
-- Insert cleaned data

INSERT INTO dim_supermarket (supermarket_id, supermarket_name, location, store_type)
VALUES
('T01','Tesco','London','Superstore'),
('A01','ASDA','Birmingham','Superstore'),
('AL01','Aldi','Manchester','Discount'),
('M01','Morrisons','Leeds','Superstore'),
('S01','Sainsbury','Bristol','Superstore');

-- check
SELECT * FROM dim_supermarket;
--------------------------------------------------------------------------------------------
-- Create product dimension
-- Create table:
CREATE TABLE dim_product (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(150)
);
-- Insert from staging:
use analytics;

INSERT INTO dim_product (product_id, product_name)
SELECT
    CONCAT('P', LPAD(ROW_NUMBER() OVER (ORDER BY product_name),3,'0')) AS product_id,
    product_name
FROM (
    SELECT DISTINCT product_name
    FROM staging.vw_clean_retail
) t;

SELECT COUNT(*) FROM dim_product;
SELECT * FROM dim_product LIMIT 10;
-----------------------------------------------------------------------------------------
-- Create date dimension
-- Create table:
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(15)
);

-- Insert:

INSERT INTO dim_date
SELECT DISTINCT
    raw_date,
    DAY(raw_date),
    MONTH(raw_date),
    YEAR(raw_date),
    DAYNAME(raw_date)
FROM staging.vw_clean_retail;

-- check 
SELECT COUNT(*) FROM dim_date;
-----------------------------------------------------------------------------------------

-- Next step: Build fact table
-- Goal

-- The fact table will:
-- Connect all dimension tables via foreign keys
-- Store metrics like price_gbp
-- Be analytics-ready for Power BI dashboards

-- Step 1: Create fact table

USE analytics;
CREATE TABLE IF NOT EXISTS fact_prices (
    fact_id VARCHAR(32) PRIMARY KEY,
    date_id DATE,
    product_id VARCHAR(10),
    supermarket_id VARCHAR(10),
    price_gbp DECIMAL(10,2)
);
-- This sets up the empty fact table.
-----------------------------------------------------------------------------------------------------
-- Step 2: Populate fact table with joins

-- I will join cleaned staging view to all dimensions.

INSERT INTO analytics.fact_prices 
(fact_id, date_id, product_id, supermarket_id, price_gbp)

SELECT 
    s.fact_id,
    s.raw_date,
    p.product_id,
    d.supermarket_id,
    s.price_gbp

FROM staging.vw_clean_retail s

JOIN analytics.dim_supermarket d
    ON UPPER(TRIM(s.supermarket)) = UPPER(d.supermarket_name)

JOIN analytics.dim_product p
    ON TRIM(s.product_name) = TRIM(p.product_name)

JOIN analytics.dim_date dt
    ON s.raw_date = dt.date_id;

-- What this does:

-- Joins staging → dimensions using business keys
-- Replaces supermarket and product_name with foreign keys
-- Keeps fact_id and price
---------------------------------------------------------------------------------------------------

-- Step 3: Validate fact table

SELECT COUNT(*) FROM analytics.fact_prices;
---------------------------------------------------------------------------------------------------

-- Aggregation layer

-- Build aggregate table

CREATE TABLE analytics.agg_daily_prices AS

SELECT
    f.date_id,
    s.supermarket_name,
    p.product_name,

    AVG(f.price_gbp) AS avg_price,
    COUNT(*) AS obs_count

FROM analytics.fact_prices f

JOIN analytics.dim_supermarket s
    ON f.supermarket_id = s.supermarket_id

JOIN analytics.dim_product p
    ON f.product_id = p.product_id

GROUP BY
    f.date_id,
    s.supermarket_name,
    p.product_name;
-- I built an aggregated analytics layer optimized for BI consumption

-- Verify
SELECT COUNT(*) FROM analytics.agg_daily_prices;













