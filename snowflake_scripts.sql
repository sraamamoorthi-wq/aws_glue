CREATE OR REPLACE EXTERNAL VOLUME starburst_glue_data_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'starburst_glue_data_volume',
            STORAGE_PROVIDER = 'S3',
            STORAGE_BASE_URL = '<s3_path>',
            STORAGE_AWS_ROLE_ARN = '<aws_role_arn>'
         )
      );



  CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'consumption_gold'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = '<aws_role_arn>'
  GLUE_CATALOG_ID = '<aws_account_no>'
  GLUE_REGION = 'us-east-2'
  ENABLED = TRUE;

  DESC CATALOG INTEGRATION glue_catalog_int;

create database sfl_iceberg;
create schema gold_consumption;

DESC EXTERNAL VOLUME STARBURST_GLUE_DATA_VOLUME;

ALTER EXTERNAL VOLUME STARBURST_GLUE_DATA_VOLUME
SET ALLOW_WRITES = FALSE;

  -- 1. Mount Customers
CREATE OR REPLACE ICEBERG TABLE glue_monthly_customers
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'monthly_customers'  -- The actual table name in AWS Glue
  EXTERNAL_VOLUME = 'starburst_glue_data_volume';

-- 2. Mount Customer-Account Bridge
CREATE OR REPLACE ICEBERG TABLE glue_monthly_cust_acct
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'monthly_cust_acct'
  EXTERNAL_VOLUME = 'starburst_glue_data_volume';

-- 3. Mount Accounts
CREATE OR REPLACE ICEBERG TABLE glue_monthly_accounts
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'monthly_accounts'
  EXTERNAL_VOLUME = 'starburst_glue_data_volume';

-- 4. Mount Transactions
CREATE OR REPLACE ICEBERG TABLE glue_monthly_transactions
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'monthly_transactions'
  EXTERNAL_VOLUME = 'starburst_glue_data_volume';


CREATE OR REPLACE VIEW semantic_customer_monthly_summary AS -- not required for streamlit

SELECT
    -- 1. Partition/Grain
    c.snp_dt_mth_prtn,
    c.cust_id,
    c.cust_name,
    c.cust_no,

    -- 2. Account Metrics
    -- Counts unique active accounts associated with the customer
    COUNT(DISTINCT a.acct_id) AS total_active_accounts,

    -- 3. DEBIT Metrics
    -- Using ZEROIFNULL handles nulls cleanly for reporting
    ZEROIFNULL(SUM(CASE WHEN t.tran_type = 'Debit' THEN t.total_trans_count ELSE 0 END)) AS debit_trans_count,
    ZEROIFNULL(SUM(CASE WHEN t.tran_type = 'Debit' THEN t.total_amount ELSE 0 END))      AS debit_amount,

    -- 4. CREDIT Metrics
    ZEROIFNULL(SUM(CASE WHEN t.tran_type = 'Credit' THEN t.total_trans_count ELSE 0 END)) AS credit_trans_count,
    ZEROIFNULL(SUM(CASE WHEN t.tran_type = 'Credit' THEN t.total_amount ELSE 0 END))      AS credit_amount

FROM glue_monthly_customers c

-- Link Customer to Account Bridge
-- Ensures we only match records from the same snapshot month
INNER JOIN glue_monthly_cust_acct b
    ON c.cust_id = b.cust_id
   AND c.snp_dt_mth_prtn = b.snp_dt_mth_prtn

-- Link to Account Details
-- Inner join filters out accounts that don't exist in the master list
INNER JOIN glue_monthly_accounts a
    ON b.acct_id = a.acct_id
   AND b.snp_dt_mth_prtn = a.snp_dt_mth_prtn

-- Link to Transactions
-- Left join ensures we keep customers/accounts even if they had 0 transactions this month
LEFT JOIN glue_monthly_transactions t
    ON a.acct_id = t.acct_id
   AND a.snp_dt_mth_prtn = t.snp_dt_mth_prtn

WHERE 
 a.retail_acct = 'Y'
    AND a.is_actv = 'Y'

GROUP BY 
    1, 2, 3, 4;

select * from semantic_customer_monthly_summary;
