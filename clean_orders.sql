-- Cleaning status and keep all important columns for later steps
--Standardize: order_status to lowercase with no spaces.
--Keeps all timestamps needed for later cleaning and validation.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.orders_step1_clean_status` AS
SELECT
  order_id,
  customer_id,
  LOWER(TRIM(order_status)) AS order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date
FROM `olist-analysis-465402.Olist_dataset.orders`
WHERE LOWER(TRIM(order_status)) IN (
  'delivered', 'shipped', 'canceled', 'invoiced',
  'processing', 'unavailable', 'created', 'approved'
);

--Removing rows with missing order_id, customer_id, or order_purchase_timestamp
CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.orders_step2_no_nulls` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.orders_step1_clean_status`
WHERE 
  order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_purchase_timestamp IS NOT NULL;

--making sure to follow logical timestamp
--Delivered ≥ Purchased
--Approved ≥ Purchased
--Carrier Pickup ≥ Approved

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.orders_step3_valid_dates` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.orders_step2_no_nulls`
WHERE 
  -- Approved date must be on or after purchase date (if both exist)
  (order_approved_at IS NULL OR order_approved_at >= order_purchase_timestamp)
  
  -- Carrier pickup must be on or after approval date (if both exist)
  AND (order_delivered_carrier_date IS NULL OR order_approved_at IS NULL OR order_delivered_carrier_date >= order_approved_at)

  -- Delivery to customer must be on or after purchase date (if both exist)
  AND (order_delivered_customer_date IS NULL OR order_delivered_customer_date >= order_purchase_timestamp)

  -- Delivery must not happen after estimated date (optional business rule)
  AND (order_delivered_customer_date IS NULL OR order_estimated_delivery_date IS NULL OR order_delivered_customer_date <= order_estimated_delivery_date);

--Instead of deleting rows that have missing (but non-critical) dates, we keep those rows and flag the missing values.
-- Adding boolean flags for missing optional timestamps
-- Keeps important rows but makes missing data easier to analyze

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.orders_step4_null_flags` AS
SELECT
  *,
  -- Flag whether optional timestamps are missing
  order_approved_at IS NULL AS is_missing_approved,
  order_delivered_carrier_date IS NULL AS is_missing_carrier_pickup,
  order_delivered_customer_date IS NULL AS is_missing_customer_delivery,
  order_estimated_delivery_date IS NULL AS is_missing_estimated_delivery
FROM `olist-analysis-465402.Olist_dataset.orders_step3_valid_dates`;

--Final Cleaned Orders Table
CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.orders_clean_final` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.orders_step4_null_flags`;



--Dropping other saved tables
DROP TABLE `olist-analysis-465402.Olist_dataset.orders_step1_clean_status`;
DROP TABLE `olist-analysis-465402.Olist_dataset.orders_step2_no_nulls`;
DROP TABLE `olist-analysis-465402.Olist_dataset.orders_step3_valid_dates`;
DROP TABLE `olist-analysis-465402.Olist_dataset.orders_step4_null_flags`;

