-- STEP 1: Remove rows with NULLs in key columns
-- We're removing any customer rows where critical fields are missing.
-- critical fields: customer_id, customer_unique_id, city, state, or zip code prefix.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.customers_step1_no_nulls` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.customers`
WHERE
  customer_id IS NOT NULL
  AND customer_unique_id IS NOT NULL
  AND customer_zip_code_prefix IS NOT NULL
  AND customer_city IS NOT NULL
  AND customer_state IS NOT NULL;


-- STEP 2: Normalize text fields
-- We're cleaning up city and state values to make them consistent.
-- Converting them to lowercase and trimming spaces.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.customers_step2_normalized_text` AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  LOWER(TRIM(customer_city)) AS customer_city,
  LOWER(TRIM(customer_state)) AS customer_state
FROM `olist-analysis-465402.Olist_dataset.customers_step1_no_nulls`;


-- STEP 3: Validate ZIP code prefix
-- In this step, we’re:
--   - Ensuring the ZIP prefix is numeric
--   - Making sure it has exactly 5 digits
--   - Keeping only valid rows (you could flag them instead if you want to review later)

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.customers_step3_valid_zip` AS
SELECT *
FROM `olist-analysis-465402.Olist_dataset.customers_step2_normalized_text`
WHERE 
  REGEXP_CONTAINS(CAST(customer_zip_code_prefix AS STRING), r'^\d{5}$');


-- STEP 4: Check for duplicates
-- We'll remove duplicate customer_id entries (if any), keeping the first one.
-- We're using ROW_NUMBER() to identify duplicates and retain only the first per customer_id.

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.customers_step4_no_duplicates` AS
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_unique_id) AS row_num
  FROM `olist-analysis-465402.Olist_dataset.customers_step3_valid_zip`
)
WHERE row_num = 1;


-- STEP 5: Saving the final cleaned table

CREATE OR REPLACE TABLE `olist-analysis-465402.Olist_dataset.customers_clean_final` AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM `olist-analysis-465402.Olist_dataset.customers_step4_no_duplicates`;



-- Deleting intermediate customers cleaning tables
DROP TABLE `olist-analysis-465402.Olist_dataset.customers_step1_no_nulls`;
DROP TABLE `olist-analysis-465402.Olist_dataset.customers_step2_normalized_text`;
DROP TABLE `olist-analysis-465402.Olist_dataset.customers_step3_valid_zip`;
DROP TABLE `olist-analysis-465402.Olist_dataset.customers_step4_no_duplicates`;
