-- Databricks notebook source
-- Creates the top-level catalog for the whole project
-- Only needs to run once - IF NOT EXISTS prevents errors on re-run
 
CREATE CATALOG IF NOT EXISTS retailpulse;
 
-- Verify it was created
SHOW CATALOGS;


-- COMMAND ----------

-- Create all three layers of the Medallion Architecture
-- Bronze = raw data, never modified
-- Silver = cleaned + business metrics calculated
-- Gold  = aggregated KPI views for Power BI
 
CREATE SCHEMA IF NOT EXISTS retailpulse.bronze;
CREATE SCHEMA IF NOT EXISTS retailpulse.silver;
CREATE SCHEMA IF NOT EXISTS retailpulse.gold;
 
-- Verify all three schemas exist
SHOW SCHEMAS IN retailpulse;


-- COMMAND ----------

-- Set working catalog so all subsequent cells use retailpulse by default
USE CATALOG retailpulse;
 
-- Confirm current catalog
SELECT current_catalog(), current_database();


-- COMMAND ----------

-- Load raw CSV from FileStore into Bronze Delta table
-- read_files() is Databricks SQL native function
-- inferSchema = true auto-detects column data types
 
CREATE OR REPLACE TABLE retailpulse.bronze.raw_inventory
USING DELTA AS
SELECT 
  Date,
  `Store ID` AS Store_ID,
  `Product ID` AS Product_ID,
  Category,
  Region,
  `Inventory Level` AS Inventory_Level,
  `Units Sold` AS Units_Sold,
  `Units Ordered` AS Units_Ordered,
  `Demand Forecast` AS Demand_Forecast,
  Price,
  Discount,
  `Weather Condition` AS Weather_Condition,
  `Holiday/Promotion` AS Holiday_Promotion,
  `Competitor Pricing` AS Competitor_Pricing,
  Seasonality
FROM read_files(
  '/Volumes/retailpulse/default/retail-pulse-data/retail_store_inventory.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);
 
-- Quick check - how many rows loaded?
SELECT COUNT(*) AS total_rows_loaded
FROM retailpulse.bronze.raw_inventory;

-- COMMAND ----------

-- Full data profiling of bronze table
-- Run this to understand data quality
 
SELECT
  -- Row counts
  COUNT(*)                                    AS total_records,
  COUNT(DISTINCT Store_ID)                    AS unique_stores,
  COUNT(DISTINCT Product_ID)                  AS unique_products,
  COUNT(DISTINCT Category)                    AS unique_categories,
  COUNT(DISTINCT Region)                      AS unique_regions,
 
  -- Date range
  MIN(Date)                                   AS earliest_date,
  MAX(Date)                                   AS latest_date,
 
  -- Null checks - important before transforming
  SUM(CASE WHEN Units_Sold IS NULL
      THEN 1 ELSE 0 END)                      AS null_units_sold,
  SUM(CASE WHEN Inventory_Level IS NULL
      THEN 1 ELSE 0 END)                      AS null_inventory,
  SUM(CASE WHEN Price IS NULL
      THEN 1 ELSE 0 END)                      AS null_price,
  SUM(CASE WHEN Demand_Forecast IS NULL
      THEN 1 ELSE 0 END)                      AS null_forecast,
 
  -- Value ranges - check for obvious data issues
  MIN(CAST(Units_Sold AS INT))                AS min_units_sold,
  MAX(CAST(Units_Sold AS INT))                AS max_units_sold,
  AVG(CAST(Units_Sold AS DOUBLE))             AS avg_units_sold,
  MIN(CAST(Inventory_Level AS INT))           AS min_inventory,
  MAX(CAST(Inventory_Level AS INT))           AS max_inventory,
  MIN(CAST(Price AS DOUBLE))                  AS min_price,
  MAX(CAST(Price AS DOUBLE))                  AS max_price
 
FROM retailpulse.bronze.raw_inventory;

-- COMMAND ----------

-- See all distinct regions and categories in the data
-- To understand geographic and product breakdown
 
SELECT
  Region,
  COUNT(DISTINCT `Store_ID`)      AS stores_in_region,
  COUNT(DISTINCT Category)        AS categories,
  COUNT(*)                        AS total_records
FROM retailpulse.bronze.raw_inventory
GROUP BY Region
ORDER BY total_records DESC;


-- COMMAND ----------

CREATE OR REPLACE TABLE retailpulse.silver.inventory_cleaned
USING DELTA AS
 
SELECT
 
  -- SECTION A: CLEAN AND STANDARDIZE ALL COLUMNS

  -- Date: cast string to proper DATE type
  CAST(Date AS DATE)                              AS sale_date,
 
  -- Store ID and Product ID: remove extra spaces
  TRIM(`Store_ID`)                                AS store_id,
  TRIM(`Product_ID`)                              AS product_id,
 
  -- Category and Region: capitalize first letter of each word

  INITCAP(TRIM(Category))                         AS category,
  INITCAP(TRIM(Region))                           AS region,
 
  -- Numeric columns: cast from string to proper number types
  CAST(`Inventory_Level` AS INT)                  AS inventory_level,
  CAST(`Units_Sold` AS INT)                       AS units_sold,
  CAST(`Units_Ordered` AS INT)                    AS units_ordered,
  CAST(`Demand_Forecast` AS DECIMAL(10,2))        AS demand_forecast,
  CAST(Price AS DECIMAL(10,2))                    AS price,
 
  -- Discount: stored as integer percentage
  CAST(Discount AS INT)                           AS discount,
 
  -- Text columns: trim whitespace
  TRIM(`Weather_Condition`)                       AS weather_condition,
 
  -- Holiday/Promotion: 1 = promo active, 0 = no promo
  CAST(`Holiday_Promotion` AS INT)                AS is_promotion,
 
  -- Competitor price for pricing analysis
  CAST(`Competitor_Pricing` AS DECIMAL(10,2))     AS competitor_price,
 
  -- Seasonality flag
  TRIM(Seasonality)                               AS seasonality,
 

  -- SECTION B: CALCULATED BUSINESS METRICS

 
  -- METRIC 1: REVENUE
  -- Formula: Units Sold x Price x (1 - Discount/100)
  -- e.g. 50 units x $20 x (1 - 10/100) = $900
  ROUND(
    CAST(`Units_Sold` AS DECIMAL) *
    CAST(Price AS DECIMAL) *
    (1 - CAST(Discount AS DECIMAL) / 100),
  2)                                              AS revenue,
 
  -- METRIC 2: SELL-THROUGH RATE (core merchandise metric)
  -- Formula: Units Sold / (Units Sold + Inventory Level) x 100
  -- e.g. 80 sold / (80 + 20 inventory) = 80%
  -- NULLIF prevents divide-by-zero errors
  ROUND(
    CAST(`Units_Sold` AS DECIMAL) /
    NULLIF(
      CAST(`Units_Sold` AS DECIMAL) +
      CAST(`Inventory_Level` AS DECIMAL),
    0) * 100,
  2)                                              AS sell_through_rate,
 
  -- METRIC 3: DAYS OF SUPPLY
  -- Formula: (Inventory Level / Units Sold) x 7
  -- e.g. 200 inventory / 50 sold x 7 = 28 days of supply
  -- CASE handles stores with zero sales (avoids divide-by-zero)
  CASE
    WHEN CAST(`Units_Sold` AS INT) = 0 THEN NULL
    ELSE ROUND(
      CAST(`Inventory_Level` AS DECIMAL) /
      CAST(`Units_Sold` AS DECIMAL) * 7,
    1)
  END                                             AS days_of_supply,
 
  -- METRIC 4: ACTUAL VS FORECAST VARIANCE %
  -- Formula: (Actual - Forecast) / Forecast x 100
  -- Negative = sold less than forecast, Positive = sold more
  ROUND(
    (CAST(`Units_Sold` AS DECIMAL) -
     CAST(`Demand_Forecast` AS DECIMAL)) /
    NULLIF(CAST(`Demand_Forecast` AS DECIMAL), 0) * 100,
  2)                                              AS actual_vs_forecast_pct,
 
  -- BONUS: Price vs Competitor (positive = we are more expensive)
  ROUND(
    CAST(Price AS DECIMAL) -
    CAST(`Competitor_Pricing` AS DECIMAL),
  2)                                              AS price_vs_competitor,
 

  -- SECTION C: INVENTORY STATUS FLAG

 
  -- Flags each record with its inventory health status
  -- Used in exception tracking and Power BI conditional formatting
  CASE
    WHEN CAST(`Inventory_Level` AS INT) = 0
      THEN 'Stockout'
    WHEN CAST(`Units_Sold` AS INT) = 0
     AND CAST(`Inventory_Level` AS INT) > 50
      THEN 'Dead Stock'
    WHEN CAST(`Inventory_Level` AS INT) < 10
      THEN 'Low Stock'
    WHEN ROUND(
      CAST(`Units_Sold` AS DECIMAL) /
      NULLIF(CAST(`Units_Sold` AS DECIMAL) +
      CAST(`Inventory_Level` AS DECIMAL), 0) * 100, 2) < 25
      THEN 'Overstock Risk'
    ELSE 'Healthy'
  END                                             AS inventory_status
 
FROM retailpulse.bronze.raw_inventory
 
-- Data quality filters: remove records that would break metrics
WHERE `Units_Sold` IS NOT NULL
  AND `Inventory_Level` IS NOT NULL
  AND Date IS NOT NULL
  AND CAST(Price AS DECIMAL) > 0;

-- COMMAND ----------

-- Verify 1: Check row counts and column count vs bronze
SELECT
  'bronze' AS layer,
  COUNT(*) AS row_count
FROM retailpulse.bronze.raw_inventory
UNION ALL
SELECT
  'silver' AS layer,
  COUNT(*) AS row_count
FROM retailpulse.silver.inventory_cleaned;


-- COMMAND ----------

-- Verify 2: Check all 4 metrics are calculating correctly
-- Sample 5 rows and inspect every calculated column
SELECT
  store_id,
  product_id,
  units_sold,
  inventory_level,
  price,
  discount,
  revenue,               -- should = units_sold x price x (1-discount)
  sell_through_rate,     -- should = units_sold / (units_sold + inventory) x 100
  days_of_supply,        -- should = inventory / units_sold x 7
  actual_vs_forecast_pct,-- should = (units_sold - demand_forecast) / demand_forecast x 100
  inventory_status       -- should be one of: Healthy/Stockout/Overstock Risk/Low Stock/Dead Stock
FROM retailpulse.silver.inventory_cleaned
LIMIT 5;


-- COMMAND ----------

-- Verify 3: Check distribution of inventory status flags
-- How many records fall into each category
SELECT
  inventory_status,
  COUNT(*)                    AS record_count,
  ROUND(COUNT(*) * 100.0 /
    SUM(COUNT(*)) OVER(), 1)  AS pct_of_total
FROM retailpulse.silver.inventory_cleaned
GROUP BY inventory_status
ORDER BY record_count DESC;


-- COMMAND ----------

-- Verify 4: Check sell-through rate distribution
-- Understand the range of performance across your dataset
SELECT
  CASE
    WHEN sell_through_rate >= 65 THEN 'High Performer (65%+)'
    WHEN sell_through_rate >= 45 THEN 'On Track (45-64%)'
    WHEN sell_through_rate >= 25 THEN 'Monitor (25-44%)'
    ELSE 'Underperforming (<25%)'
  END                         AS performance_tier,
  COUNT(*)                    AS record_count,
  ROUND(AVG(sell_through_rate), 1) AS avg_sell_through
FROM retailpulse.silver.inventory_cleaned
GROUP BY 1
ORDER BY avg_sell_through DESC;


-- COMMAND ----------

-- Apply Liquid Clustering on the 3 most common filter columns
-- store_id   - most queries filter by store
-- sale_date  - all trend analysis filters by date
-- category   - category analysis and slicers filter by this
 
ALTER TABLE retailpulse.silver.inventory_cleaned
CLUSTER BY (store_id, sale_date, category);
 
-- Verify clustering was applied
DESCRIBE TABLE EXTENDED retailpulse.silver.inventory_cleaned;


-- COMMAND ----------

CREATE OR REPLACE VIEW retailpulse.gold.vw_store_kpi AS
 
SELECT
  store_id,
  region,
 
  -- Truncate date to month for monthly aggregation
  DATE_TRUNC('month', sale_date)              AS sales_month,
 
  -- Volume metrics
  COUNT(DISTINCT product_id)                  AS products_carried,
  SUM(revenue)                                AS total_revenue,
  SUM(units_sold)                             AS total_units_sold,
  SUM(units_ordered)                          AS total_units_ordered,
 
  -- Inventory health metrics
  ROUND(AVG(inventory_level), 0)              AS avg_inventory,
  ROUND(AVG(sell_through_rate), 2)            AS avg_sell_through_rate,
  ROUND(AVG(days_of_supply), 1)               AS avg_days_of_supply,
 
  -- Forecast accuracy
  ROUND(AVG(actual_vs_forecast_pct), 2)       AS avg_forecast_variance_pct,
 
  -- Exception counts for KPI cards in Power BI
  SUM(CASE WHEN inventory_status = 'Stockout'
      THEN 1 ELSE 0 END)                      AS stockout_count,
  SUM(CASE WHEN inventory_status = 'Overstock Risk'
      THEN 1 ELSE 0 END)                      AS overstock_count,
  SUM(CASE WHEN inventory_status = 'Dead Stock'
      THEN 1 ELSE 0 END)                      AS dead_stock_count,
 
  -- Promotional revenue
  SUM(CASE WHEN is_promotion = 1
      THEN revenue ELSE 0 END)                AS promo_revenue,
  SUM(CASE WHEN is_promotion = 0
      THEN revenue ELSE 0 END)                AS non_promo_revenue,
 
  -- WINDOW FUNCTION: Month over Month Revenue Growth
  -- LAG() looks at the previous month's revenue for the same store
  -- PARTITION BY store_id = calculate separately for each store
  -- ORDER BY sales_month = look back in time order
  ROUND(
    (SUM(revenue) -
     LAG(SUM(revenue)) OVER (
       PARTITION BY store_id
       ORDER BY DATE_TRUNC('month', sale_date)
     )
    ) /
    NULLIF(
      LAG(SUM(revenue)) OVER (
        PARTITION BY store_id
        ORDER BY DATE_TRUNC('month', sale_date)
      ), 0
    ) * 100,
  2)                                          AS mom_revenue_growth_pct,
 
  -- Performance tier based on sell-through rate
  CASE
    WHEN AVG(sell_through_rate) >= 65         THEN 'High Performer'
    WHEN AVG(sell_through_rate) >= 45         THEN 'On Track'
    WHEN AVG(sell_through_rate) >= 25         THEN 'Monitor'
    ELSE 'Underperforming'
  END                                         AS performance_tier
 
FROM retailpulse.silver.inventory_cleaned
GROUP BY
  store_id,
  region,
  DATE_TRUNC('month', sale_date);


-- COMMAND ----------

-- Test the view immediately after creating it
SELECT *
FROM retailpulse.gold.vw_store_kpi
ORDER BY total_revenue DESC
LIMIT 10;


-- COMMAND ----------

CREATE OR REPLACE VIEW retailpulse.gold.vw_inventory_exceptions AS
 
SELECT
  store_id,
  region,
  product_id,
  category,
  seasonality,
 
  -- Average metrics across all dates for this store-product combo
  ROUND(AVG(inventory_level), 0)              AS avg_inventory,
  ROUND(AVG(units_sold), 1)                   AS avg_units_sold,
  ROUND(AVG(units_ordered), 1)                AS avg_units_ordered,
  ROUND(AVG(demand_forecast), 1)              AS avg_demand_forecast,
  ROUND(AVG(sell_through_rate), 2)            AS avg_sell_through,
  ROUND(AVG(days_of_supply), 1)               AS avg_days_supply,
  ROUND(AVG(actual_vs_forecast_pct), 2)       AS avg_forecast_variance,
  ROUND(AVG(price), 2)                        AS avg_price,
  ROUND(AVG(competitor_price), 2)             AS avg_competitor_price,
  SUM(revenue)                                AS total_revenue,
 
  -- EXCEPTION FLAG
  -- Priority order: Stockout is most urgent, then Overstock Risk, etc.
  CASE
    WHEN AVG(inventory_level) = 0
      THEN 'Stockout'                 -- no stock at all
    WHEN AVG(sell_through_rate) < 20
     AND AVG(days_of_supply) > 60
      THEN 'Overstock Risk'           -- very slow moving, too much stock
    WHEN AVG(sell_through_rate) < 35
      THEN 'Slow Moving'              -- below acceptable sell-through
    WHEN AVG(actual_vs_forecast_pct) < -20
      THEN 'Under Forecast'           -- significantly below plan
    WHEN AVG(actual_vs_forecast_pct) > 20
      THEN 'Outperforming Forecast'   -- doing better than plan
    ELSE 'Healthy'
  END                                         AS exception_flag,
 
  -- ALLOCATION PRIORITY SCORE (0 to 100)
  -- Weighted formula:
  -- 40% weight on sell-through rate (demand signal)
  -- 35% weight on demand forecast (expected future demand)
  -- 25% weight on revenue contribution (business value)
  -- LEAST() caps each component so no single factor dominates
  ROUND(
    LEAST(AVG(sell_through_rate) * 0.4, 40) +
    LEAST((AVG(demand_forecast) / 100) * 0.35, 35) +
    LEAST((SUM(revenue) / 10000) * 0.25, 25),
  1)                                          AS allocation_priority_score
 
FROM retailpulse.silver.inventory_cleaned
GROUP BY
  store_id,
  region,
  product_id,
  category,
  seasonality;


-- COMMAND ----------

-- Test: Show top exceptions sorted by priority
SELECT
  store_id,
  category,
  exception_flag,
  avg_sell_through,
  avg_days_supply,
  allocation_priority_score
FROM retailpulse.gold.vw_inventory_exceptions
WHERE exception_flag != 'Healthy'
ORDER BY allocation_priority_score DESC
LIMIT 15;


-- COMMAND ----------

CREATE OR REPLACE VIEW retailpulse.gold.vw_category_trends AS
 
SELECT
  category,
  region,
  seasonality,
 
  -- Truncate to week for weekly trend analysis
  DATE_TRUNC('week', sale_date)               AS sales_week,
 
  -- Weekly revenue and volume
  SUM(revenue)                                AS weekly_revenue,
  SUM(units_sold)                             AS weekly_units_sold,
  SUM(demand_forecast)                        AS weekly_demand_forecast,
  ROUND(AVG(sell_through_rate), 2)            AS avg_sell_through,
 
  -- Pricing data for competitive analysis
  ROUND(AVG(price), 2)                        AS avg_our_price,
  ROUND(AVG(competitor_price), 2)             AS avg_competitor_price,
  ROUND(AVG(price - competitor_price), 2)     AS avg_price_gap,
 
  -- Promotional performance
  SUM(CASE WHEN is_promotion = 1
      THEN units_sold ELSE 0 END)             AS promo_units,
  SUM(CASE WHEN is_promotion = 0
      THEN units_sold ELSE 0 END)             AS non_promo_units,
 
  -- WINDOW FUNCTION 1: Running cumulative revenue
  -- Shows total revenue accumulated from start to each week
  SUM(SUM(revenue)) OVER (
    PARTITION BY category
    ORDER BY DATE_TRUNC('week', sale_date)
    ROWS UNBOUNDED PRECEDING
  )                                           AS cumulative_revenue,
 
  -- WINDOW FUNCTION 2: Week over Week revenue growth %
  ROUND(
    (SUM(revenue) -
     LAG(SUM(revenue)) OVER (
       PARTITION BY category
       ORDER BY DATE_TRUNC('week', sale_date)
     )
    ) /
    NULLIF(
      LAG(SUM(revenue)) OVER (
        PARTITION BY category
        ORDER BY DATE_TRUNC('week', sale_date)
      ), 0
    ) * 100,
  2)                                          AS wow_revenue_growth_pct,
 
  -- Actual vs Forecast variance at category level
  ROUND(
    (SUM(units_sold) - SUM(demand_forecast)) /
    NULLIF(SUM(demand_forecast), 0) * 100,
  2)                                          AS actual_vs_forecast_pct
 
FROM retailpulse.silver.inventory_cleaned
GROUP BY
  category,
  region,
  seasonality,
  DATE_TRUNC('week', sale_date);


-- COMMAND ----------

-- Test: Show top growing and declining categories
SELECT
  category,
  sales_week,
  weekly_revenue,
  wow_revenue_growth_pct,
  actual_vs_forecast_pct
FROM retailpulse.gold.vw_category_trends
WHERE wow_revenue_growth_pct IS NOT NULL
ORDER BY wow_revenue_growth_pct DESC
LIMIT 10;


-- COMMAND ----------

-- Test 1: Store KPI view - check row counts and top stores
SELECT
  COUNT(*)                    AS total_rows,
  COUNT(DISTINCT store_id)    AS unique_stores,
  COUNT(DISTINCT sales_month) AS months_covered,
  MIN(total_revenue)          AS min_store_revenue,
  MAX(total_revenue)          AS max_store_revenue,
  AVG(avg_sell_through_rate)  AS overall_avg_sell_through
FROM retailpulse.gold.vw_store_kpi;



-- COMMAND ----------

-- Test 2: Inventory exceptions - check exception distribution
SELECT
  exception_flag,
  COUNT(*)                    AS count,
  ROUND(AVG(allocation_priority_score), 1) AS avg_priority_score
FROM retailpulse.gold.vw_inventory_exceptions
GROUP BY exception_flag
ORDER BY count DESC;

-- COMMAND ----------

-- Test 3: Category trends - check WoW growth is calculating
SELECT
  category,
  COUNT(*)                    AS weeks_of_data,
  SUM(weekly_revenue)         AS total_revenue,
  AVG(wow_revenue_growth_pct) AS avg_wow_growth
FROM retailpulse.gold.vw_category_trends
GROUP BY category
ORDER BY total_revenue DESC;

-- COMMAND ----------

-- Pick one store and manually verify its metrics
-- Check that revenue = units_sold x price x (1 - discount)
SELECT
  store_id,
  product_id,
  units_sold,
  price,
  discount,
  inventory_level,
  demand_forecast,
 
  -- Our calculated metrics
  revenue,
  sell_through_rate,
  days_of_supply,
  actual_vs_forecast_pct,
 
  -- Manual verification formulas (should match calculated columns above)
  ROUND(units_sold * price * (1 - discount), 2)          AS manual_revenue_check,
  ROUND(units_sold / NULLIF(units_sold +
    inventory_level, 0) * 100, 2)                        AS manual_str_check,
  ROUND(CAST(inventory_level AS DECIMAL) /
    NULLIF(units_sold, 0) * 7, 1)                        AS manual_dos_check
 
FROM retailpulse.silver.inventory_cleaned
WHERE units_sold > 0
LIMIT 5;


-- COMMAND ----------

SELECT COUNT(*) FROM retailpulse.gold.vw_store_kpi