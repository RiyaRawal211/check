-- STEP 1: Removing rows with NULLs in key columns
-- We're ensuring that every item has an order, a product, a seller, and shipping info.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.order_items_step1_no_nulls` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.order_items`
WHERE 
  order_id IS NOT NULL
  AND order_item_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND shipping_limit_date IS NOT NULL
  AND price IS NOT NULL
  AND freight_value IS NOT NULL;

-- STEP 2: Removing or flag price and freight anomalies
-- We're filtering out rows with negative or zero prices or freight values.
-- These are likely data errors or invalid entries.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.order_items_step2_valid_prices` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.order_items_step1_no_nulls`
WHERE 
  price > 0
  AND freight_value >= 0;

-- STEP 3: Validating shipping_limit_date 
-- Ensuring shipping deadline is not before the order was placed.


CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.order_items_step3_valid_shipping` AS
SELECT i.*
FROM `olist-analysis-465402.Olist_dataset.order_items_step2_valid_prices` i
LEFT JOIN `olist-analysis-465402.Olist_dataset.orders_clean_final` o
  ON i.order_id = o.order_id
WHERE 
  o.order_purchase_timestamp IS NULL -- In case there's no match, keep the row
  OR i.shipping_limit_date >= o.order_purchase_timestamp;

-- STEP 4: Removing duplicate line items
-- We're keeping only the first instance of each (order_id, order_item_id) combo.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.order_items_step4_no_duplicates` AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY order_id, order_item_id ORDER BY product_id) AS row_num
  FROM `olist-analysis-465402.Olist_dataset.order_items_step3_valid_shipping`
)
WHERE row_num = 1;

-- STEP 5: Saving the final cleaned table


CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.order_items_clean_final` AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  shipping_limit_date,
  price,
  freight_value
FROM `olist-analysis-465402.Olist_dataset.order_items_step4_no_duplicates`;



-- Deleting step-by-step cleaning tables to keep things clean and organized

DROP TABLE `olist-analysis-465402.Olist_dataset.order_items_step1_no_nulls`;
DROP TABLE `olist-analysis-465402.Olist_dataset.order_items_step2_valid_prices`;
DROP TABLE `olist-analysis-465402.Olist_dataset.order_items_step3_valid_shipping`;
DROP TABLE `olist-analysis-465402.Olist_dataset.order_items_step4_no_duplicates`;

