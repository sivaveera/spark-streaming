-- Databricks notebook source
CREATE TABLE IF NOT EXISTS TRANSIENT_DL.LKP_SRC_TO_REF_HUB_STORE 
(
  SOURCE_SYSTEM_ID INTEGER,
  SOURCE_SYSTEM_NAME STRING,
  SOURCE_SYSTEM_NATURAL_KEY STRING,
  STORE_RHID INTEGER,
  STORE_GRPID STRING,
  PRIMARY_STORE_CURRENCY_RHID INTEGER,
  PRIMARY_STORE_CURRENCY_GRPID STRING,
  EFFECTIVE_START_TIMESTAMP TIMESTAMP,
  EFFECTIVE_END_TIMESTAMP TIMESTAMP,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/lkp_src_to_ref_hub_store' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

-- LKP HUB table for MST_STORE is loaded here for APROPOS Stores.
INSERT OVERWRITE TRANSIENT_DL.LKP_SRC_TO_REF_HUB_STORE
SELECT
  HUB_STORE.SOURCE_SYSTEM_ID,
  LKP_SRC_SYS_STORE.SOURCE_SYSTEM_NAME,
  HUB_STORE.SOURCE_SYSTEM_NATURAL_KEY,
  STORE.STORE_RHID,
  STORE.STORE_GRPID,
  STORE.PRIMARY_STORE_CURRENCY_RHID,
  STORE.PRIMARY_STORE_CURRENCY_GRPID,
  STORE.EFFECTIVE_START_TIMESTAMP,
  STORE.EFFECTIVE_END_TIMESTAMP,
  CURRENT_TIMESTAMP as REC_CREATE_TIMESTAMP
FROM
  REF_HUB.LKP_SRC_TO_REF_HUB HUB_STORE
  INNER JOIN REF_HUB.LKP_SOURCE_SYSTEM LKP_SRC_SYS_STORE 
  ON LKP_SRC_SYS_STORE.SOURCE_SYSTEM_ID = HUB_STORE.SOURCE_SYSTEM_ID
  INNER JOIN REF_HUB.MST_STORE STORE 
  ON HUB_STORE.GRPID = STORE.STORE_GRPID
WHERE
  HUB_STORE.REF_HUB_TABLE_NAME = 'MST_STORE'
  AND LKP_SRC_SYS_STORE.SOURCE_SYSTEM_NAME = 'APROPOS';

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.LKP_SRC_TO_REF_HUB_STORE; 
-- 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.LKP_SRC_TO_REF_HUB_CURRENCY 
(
  SOURCE_SYSTEM_ID INTEGER,
  SOURCE_SYSTEM_NAME STRING,
  SOURCE_SYSTEM_NATURAL_KEY STRING,
  CURRENCY_RHID INTEGER,
  CURRENCY_GRPID STRING,
  EFFECTIVE_START_TIMESTAMP TIMESTAMP,
  EFFECTIVE_END_TIMESTAMP TIMESTAMP,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/lkp_src_to_ref_hub_currency' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

-- LKP HUB TABLE FOR MST_CURRENCY is built here for APROPOS Currencies.

INSERT OVERWRITE  TRANSIENT_DL.LKP_SRC_TO_REF_HUB_CURRENCY 
SELECT
  HUB_CURRENCY.SOURCE_SYSTEM_ID,
  LKP_SRC_SYS_CCY.SOURCE_SYSTEM_NAME,
  HUB_CURRENCY.SOURCE_SYSTEM_NATURAL_KEY,
  CURRENCY.CURRENCY_RHID,
  CURRENCY.CURRENCY_GRPID,
  CURRENCY.EFFECTIVE_START_TIMESTAMP,
  CURRENCY.EFFECTIVE_END_TIMESTAMP,
  CURRENT_TIMESTAMP as REC_CREATE_TIMESTAMP
FROM
  REF_HUB.LKP_SRC_TO_REF_HUB HUB_CURRENCY
  INNER JOIN REF_HUB.LKP_SOURCE_SYSTEM LKP_SRC_SYS_CCY 
  ON LKP_SRC_SYS_CCY.SOURCE_SYSTEM_ID = HUB_CURRENCY.SOURCE_SYSTEM_ID
  INNER JOIN REF_HUB.MST_CURRENCY CURRENCY 
  ON HUB_CURRENCY.GRPID = CURRENCY.CURRENCY_GRPID
WHERE
  HUB_CURRENCY.REF_HUB_TABLE_NAME = 'MST_CURRENCY'
  AND LKP_SRC_SYS_CCY.SOURCE_SYSTEM_NAME = 'APROPOS';

-- COMMAND ----------

select count(*) row_count from  TRANSIENT_DL.LKP_SRC_TO_REF_HUB_CURRENCY;
--0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.LKP_SRC_TO_REF_HUB_PRODUCT 
(
  SOURCE_SYSTEM_ID INTEGER,
  SOURCE_SYSTEM_NAME STRING,
  SOURCE_SYSTEM_NATURAL_KEY STRING,
  COLOR_RHID INTEGER,
  COLOR_GRPID STRING,
  PRODUCT_RHID INTEGER,
  PRODUCT_GRPID STRING,
  PRODUCT_LINE_RHID INTEGER,
  SIZE_RHID INTEGER,
  SIZE_GRPID STRING,
  STYLE_RHID INTEGER,
  STYLE_GRPID STRING,
  DIVISION_RHID INTEGER,
  DIVISION_GRPID STRING,
  EFFECTIVE_START_TIMESTAMP TIMESTAMP,
  EFFECTIVE_END_TIMESTAMP TIMESTAMP,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/lkp_src_to_ref_hub_product ' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

-- INSERT INTO LKP HUB TABLE FOR MST_PRODUCT for all Products.

INSERT OVERWRITE  TRANSIENT_DL.LKP_SRC_TO_REF_HUB_PRODUCT 
SELECT
  HUB_PRODUCT.SOURCE_SYSTEM_ID,
  LKP_SRC_SYS_PRODUCT.SOURCE_SYSTEM_NAME,
  HUB_PRODUCT.SOURCE_SYSTEM_NATURAL_KEY,
  PRODUCT.COLOR_RHID,
  PRODUCT.COLOR_GRPID,
  PRODUCT.PRODUCT_RHID,
  PRODUCT.PRODUCT_GRPID,
  PRODUCT.PRODUCT_LINE_RHID,  
  PRODUCT.SIZE_RHID,
  PRODUCT.SIZE_GRPID,
  PRODUCT.STYLE_RHID,
  PRODUCT.STYLE_GRPID,
  PRODUCT.DIVISION_RHID,
  PRODUCT.DIVISION_GRPID,
  PRODUCT.EFFECTIVE_START_TIMESTAMP,
  PRODUCT.EFFECTIVE_END_TIMESTAMP,
  CURRENT_TIMESTAMP as REC_CREATE_TIMESTAMP
FROM
  REF_HUB.LKP_SRC_TO_REF_HUB HUB_PRODUCT
  INNER JOIN REF_HUB.LKP_SOURCE_SYSTEM LKP_SRC_SYS_PRODUCT 
  ON LKP_SRC_SYS_PRODUCT.SOURCE_SYSTEM_ID = HUB_PRODUCT.SOURCE_SYSTEM_ID
  INNER JOIN REF_HUB.MST_PRODUCT PRODUCT 
  ON HUB_PRODUCT.GRPID = PRODUCT.PRODUCT_GRPID
WHERE
  HUB_PRODUCT.REF_HUB_TABLE_NAME = 'MST_PRODUCT';

-- COMMAND ----------

select count(*) row_count from  TRANSIENT_DL.LKP_SRC_TO_REF_HUB_PRODUCT;
--0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/order_items_base_daily' 
TBLPROPERTIES ('LAYER' = 'SILVER')
AS 
select * from 
BRZ_CURR_ECOM_SKECHERS.ORDER_ITEMS ORDER_ITEMS 
WHERE ORDER_ITEMS.REC_UPDATE_TIMESTAMP  > NVL(( SELECT MAX(SRC_LAST_UPD_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                                           WHERE JOBNAME='SJ3-SLV dtl_retail_sales_ecom' AND LOAD_STATUS = 'COMPLETED'),
                                        TO_DATE('1900-01-01','yyyy-MM-dd'));

-- COMMAND ----------

INSERT OVERWRITE TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY
select * from 
BRZ_CURR_ECOM_SKECHERS.ORDER_ITEMS ORDER_ITEMS 
WHERE ORDER_ITEMS.REC_UPDATE_TIMESTAMP  > NVL(( SELECT MAX(SRC_LAST_UPD_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                                          WHERE JOBNAME='SJ3-SLV dtl_retail_sales_ecom' AND LOAD_STATUS = 'COMPLETED'),
                                        TO_DATE('1900-01-01','yyyy-MM-dd'));

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY;
--97492

-- COMMAND ----------

INSERT INTO DELTA_LOGS.USECASE_AUDIT_LOGS 
SELECT 
'SJ3-SLV dtl_retail_sales_ecom',
MAX(REC_UPDATE_TIMESTAMP),
NULL, 
CURRENT_TIMESTAMP ,
'SJ3-SLV dtl_retail_sales_ecom', 
NULL,
NULL
FROM TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DLY_ORDERITEMS_STORE 
(
  ORDER_ID INTEGER,
  ORDER_ITEM_ID INTEGER,
  ORDER_TIMESTAMP TIMESTAMP,
  STORE_RHID INTEGER,
  STORE_GRPID STRING,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dly_orderitems_store' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

----This table will hold only STORE related fields for ORDER_ITEMS with incremental data from the last run.

INSERT OVERWRITE TRANSIENT_DL.DLY_ORDERITEMS_STORE 
SELECT
  DISTINCT 
  ORDER_ITEMS.ORDER_ID AS ORDER_ID,
  ORDER_ITEMS.ID AS ORDER_ITEM_ID,
  ORDER_LOG.ORDER_TIMESTAMP AS ORDER_TIMESTAMP,
  HUB_ORDER_STORE.STORE_RHID,
  HUB_ORDER_STORE.STORE_GRPID,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP
FROM
  TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ORDER_ITEMS
  INNER JOIN BRZ_CURR_ECOM_SKECHERS.ORDER_LOG 
  ON ORDER_ITEMS.ORDER_ID = ORDER_LOG.ORDER_ID
  LEFT OUTER JOIN TRANSIENT_DL.LKP_SRC_TO_REF_HUB_STORE HUB_ORDER_STORE 
  ON ORDER_LOG.STORE_NUM || '_' || HUB_ORDER_STORE.SOURCE_SYSTEM_ID = HUB_ORDER_STORE.SOURCE_SYSTEM_NATURAL_KEY
  AND ORDER_LOG.ORDER_TIMESTAMP BETWEEN HUB_ORDER_STORE.EFFECTIVE_START_TIMESTAMP AND HUB_ORDER_STORE.EFFECTIVE_END_TIMESTAMP;

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DLY_ORDERITEMS_STORE;
--97492

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DLY_ORDERITEMS_SHIPSTORE 
(
  ORDER_ID INTEGER,
  ORDER_ITEM_ID INTEGER,
  ORDER_TIMESTAMP TIMESTAMP,
  SHIP_STORE_RHID INTEGER,
  SHIP_STORE_GRPID STRING,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dly_orderitems_shipstore' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

---This table will hold only SHIP STORE related fields for ORDER_ITEMS with incremental data from the last run.

INSERT OVERWRITE TRANSIENT_DL.DLY_ORDERITEMS_SHIPSTORE 
SELECT
  DISTINCT 
  ORDER_ITEMS.ORDER_ID AS ORDER_ID,
  ORDER_ITEMS.ID AS ORDER_ITEM_ID,
  ORDER_LOG.ORDER_TIMESTAMP AS ORDER_TIMESTAMP,
  HUB_ORDER_SHIPSTORE.STORE_RHID AS SHIP_STORE_RHID,
  HUB_ORDER_SHIPSTORE.STORE_GRPID AS SHIP_STORE_GRPID,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP
FROM
  TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ORDER_ITEMS
  INNER JOIN BRZ_CURR_ECOM_SKECHERS.ORDER_LOG 
  ON ORDER_ITEMS.ORDER_ID = ORDER_LOG.ORDER_ID
  LEFT OUTER JOIN TRANSIENT_DL.LKP_SRC_TO_REF_HUB_STORE HUB_ORDER_SHIPSTORE 
  ON ORDER_ITEMS.SHIP_STORE_NUM || '_' || HUB_ORDER_SHIPSTORE.SOURCE_SYSTEM_ID = HUB_ORDER_SHIPSTORE.SOURCE_SYSTEM_NATURAL_KEY
  AND ORDER_LOG.ORDER_TIMESTAMP BETWEEN HUB_ORDER_SHIPSTORE.EFFECTIVE_START_TIMESTAMP AND HUB_ORDER_SHIPSTORE.EFFECTIVE_END_TIMESTAMP ;

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DLY_ORDERITEMS_SHIPSTORE;
--97492

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DLY_ORDERITEMS_CURRENCY 
(
  ORDER_ID INTEGER,
  ORDER_ITEM_ID INTEGER,
  ORDER_TIMESTAMP TIMESTAMP,
  CURRENCY_RHID INTEGER,
  CURRENCY_GRPID STRING,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dly_orderitems_currency' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

----- This table will hold only CURRENCY related fields for ORDER_ITEMS with incremental data from the last run.

INSERT OVERWRITE TRANSIENT_DL.DLY_ORDERITEMS_CURRENCY 
SELECT
  DISTINCT 
  ORDER_ITEMS.ORDER_ID AS ORDER_ID,
  ORDER_ITEMS.ID AS ORDER_ITEM_ID,
  ORDER_LOG.ORDER_TIMESTAMP AS ORDER_TIMESTAMP,
  HUB_ORDER_CURRENCY.CURRENCY_RHID AS CURRENCY_RHID,
  HUB_ORDER_CURRENCY.CURRENCY_GRPID AS CURRENCY_GRPID,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP
FROM
  TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ORDER_ITEMS
  INNER JOIN BRZ_CURR_ECOM_SKECHERS.ORDER_LOG 
  ON ORDER_ITEMS.ORDER_ID = ORDER_LOG.ORDER_ID
  LEFT OUTER JOIN TRANSIENT_DL.LKP_SRC_TO_REF_HUB_CURRENCY HUB_ORDER_CURRENCY 
  ON ORDER_LOG.CURRENCY_CODE || '_' || HUB_ORDER_CURRENCY.SOURCE_SYSTEM_ID = HUB_ORDER_CURRENCY.SOURCE_SYSTEM_NATURAL_KEY
  AND ORDER_LOG.ORDER_TIMESTAMP BETWEEN HUB_ORDER_CURRENCY.EFFECTIVE_START_TIMESTAMP AND HUB_ORDER_CURRENCY.EFFECTIVE_END_TIMESTAMP;

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DLY_ORDERITEMS_CURRENCY;
--97492

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DLY_ORDERITEMS_PRODUCT 
(
  ORDER_ID INTEGER,
  ORDER_ITEM_ID INTEGER,
  ORDER_CHANNEL_CODE STRING,
  ORDER_STATUS_CODE STRING,
  TRANSACTION_TYPE_CODE STRING,
  ORDER_TIMESTAMP TIMESTAMP,
  ORDER_DATE_KEY INTEGER,
  COLOR_RHID INTEGER,
  COLOR_GRPID STRING,
  PRODUCT_RHID INTEGER,
  PRODUCT_GRPID STRING,
  SIZE_RHID INTEGER,
  SIZE_GRPID STRING,
  STYLE_RHID INTEGER,
  STYLE_GRPID STRING,
  DIVISION_RHID INTEGER,
  DIVISION_GRPID STRING,
  CATALOG_ITEM_CODE STRING,
  ITEM_TYPE_DESC STRING,
  UNIT_PRICE_AMT DECIMAL(20, 6),
  SALES_QTY INTEGER,
  TAX_TYPE1_CODE STRING,
  TAX_TYPE1_AMT DECIMAL(20, 6),
  TAX_TYPE2_CODE STRING,
  TAX_TYPE2_AMT DECIMAL(20, 6),
  TAX_TYPE3_CODE STRING,
  TAX_TYPE3_AMT DECIMAL(20, 6),
  TAX_TYPE4_CODE STRING,
  TAX_TYPE4_AMT DECIMAL(20, 6),
  DISCOUNT1_REASON_CODE STRING,
  CANCEL_DATE DATE,
  RETURN_DATE DATE,
  VERSION_NBR INTEGER,
  CARRIER_NAME STRING,
  SHIPPING_VENDOR_NAME STRING,
  TRACKING_CODE STRING,
  CHARGE_IND INTEGER,
  PICK_CODE STRING,
  AVAILABLE_DATE DATE,
  PICK_DATE DATE,
  EMAIL_NOTIFIED_IND INTEGER,
  ORDER_FAILED_NOTIFIED_IND INTEGER,
  ORDER_FAILED_NOTIFIED_DATE DATE,
  ORDER_NOTES_TEXT STRING,
  REVIEW_NOTIFIED_IND INTEGER,
  CREDITED_IND INTEGER,
  COMMENTS_TEXT STRING,
  SHIPPING_METHOD_CODE STRING,
  SHIP_DATE DATE,
  CHARGE_TIMESTAMP TIMESTAMP,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dly_orderitems_product' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

----This table will hold only PRODUCT related fields for ORDER_ITEMS with incremental data from the last run.

INSERT OVERWRITE TRANSIENT_DL.DLY_ORDERITEMS_PRODUCT 
SELECT
  DISTINCT 
  ORDER_ITEMS.ORDER_ID AS ORDER_ID,
  ORDER_ITEMS.ID AS ORDER_ITEM_ID,
  'ECOM' AS ORDER_CHANNEL_CODE,
  ORDER_ITEMS.ORDER_STATUS AS ORDER_STATUS_CODE,
  CAST(NULL AS STRING) AS TRANSACTION_TYPE_CODE,
  ORDER_LOG.ORDER_TIMESTAMP AS ORDER_TIMESTAMP,
  CAST(DATE_FORMAT(ORDER_LOG.ORDER_TIMESTAMP, 'yyyyMMdd') AS INT) AS ORDER_DATE_KEY,
  HUB_ORDER_PRODUCT.COLOR_RHID,
  HUB_ORDER_PRODUCT.COLOR_GRPID,
  HUB_ORDER_PRODUCT.PRODUCT_RHID,
  HUB_ORDER_PRODUCT.PRODUCT_GRPID,
  HUB_ORDER_PRODUCT.SIZE_RHID,
  HUB_ORDER_PRODUCT.SIZE_GRPID,
  HUB_ORDER_PRODUCT.STYLE_RHID,
  HUB_ORDER_PRODUCT.STYLE_GRPID,
  HUB_ORDER_PRODUCT.DIVISION_RHID,
  HUB_ORDER_PRODUCT.DIVISION_GRPID,
  CAST(NULL AS STRING) AS CATALOG_ITEM_CODE,
  ORDER_ITEMS.ITEM_TYPE AS ITEM_TYPE_DESC,
  ORDER_ITEMS.UNIT_PRICE AS UNIT_PRICE_AMT,
  ORDER_ITEMS.QUANTITY AS SALES_QTY,
  ORDER_ITEMS.TAX_TYPE AS TAX_TYPE1_CODE,
  ORDER_ITEMS.TAX AS TAX_TYPE1_AMT,
  ORDER_ITEMS.TAX_TYPE2 AS TAX_TYPE2_CODE,
  ORDER_ITEMS.TAX2 AS TAX_TYPE2_AMT,
  'VAT' AS TAX_TYPE3_CODE,
  ORDER_ITEMS.VAT AS TAX_TYPE3_AMT,
  CAST(NULL AS STRING) AS TAX_TYPE4_CODE,
  CAST(NULL AS DECIMAL(20,6)) AS TAX_TYPE4_AMT,
  CAST(NULL AS STRING) AS DISCOUNT1_REASON_CODE,
  ORDER_ITEMS.CANCEL_DATE AS CANCEL_DATE,
  ORDER_ITEMS.RETURN_DATE AS RETURN_DATE,
  ORDER_ITEMS.VERSION AS VERSION_NBR,
  ORDER_ITEMS.CARRIER AS CARRIER_NAME,
  ORDER_ITEMS.SHIPPING_VENDOR AS SHIPPING_VENDOR_NAME,
  ORDER_ITEMS.TRACKING_NUMBER AS TRACKING_CODE,
  ORDER_ITEMS.CHARGE_OK AS CHARGE_IND,
  ORDER_ITEMS.PICK_OK AS PICK_CODE,
  ORDER_ITEMS.AVAILABLE_DATE AS AVAILABLE_DATE,
  ORDER_ITEMS.PICK_DATE AS PICK_DATE,
  ORDER_ITEMS.EMAIL_NOTIFIED AS EMAIL_NOTIFIED_IND,
  ORDER_ITEMS.ORDER_FAILED_NOTIFIED AS ORDER_FAILED_NOTIFIED_IND,
  ORDER_ITEMS.ORDER_FAILED_LAST_NOTIFIED AS ORDER_FAILED_NOTIFIED_DATE,
  ORDER_ITEMS.ORDER_NOTES AS ORDER_NOTES_TEXT,
  ORDER_ITEMS.REVIEW_NOTIFIED AS REVIEW_NOTIFIED_IND,
  ORDER_ITEMS.CREDITED AS CREDITED_IND,
  ORDER_ITEMS.COMMENTS AS COMMENTS_TEXT,
  ORDER_ITEMS.SHIPPING_METHOD AS SHIPPING_METHOD_CODE,
  ORDER_ITEMS.SHIP_DATE AS SHIP_DATE,
  TO_UTC_TIMESTAMP(ORDER_ITEMS.CHARGE_TIMESTAMP, "PST") AS CHARGE_TIMESTAMP,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP
FROM
  TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ORDER_ITEMS
  INNER JOIN BRZ_CURR_ECOM_SKECHERS.ORDER_LOG 
  ON ORDER_ITEMS.ORDER_ID = ORDER_LOG.ORDER_ID
  LEFT OUTER JOIN TRANSIENT_DL.LKP_SRC_TO_REF_HUB_PRODUCT HUB_ORDER_PRODUCT 
  ON ORDER_ITEMS.SKU_NUMBER || '_' || HUB_ORDER_PRODUCT.SOURCE_SYSTEM_ID = HUB_ORDER_PRODUCT.SOURCE_SYSTEM_NATURAL_KEY
  AND ORDER_LOG.ORDER_TIMESTAMP BETWEEN HUB_ORDER_PRODUCT.EFFECTIVE_START_TIMESTAMP AND HUB_ORDER_PRODUCT.EFFECTIVE_END_TIMESTAMP;

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DLY_ORDERITEMS_PRODUCT ;
--97492

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DLY_ORDERITEMS 
(
  ORDER_ID INTEGER,
  ORDER_ITEM_ID INTEGER,
  ORDER_STATUS_CODE STRING,
  ORDER_CHANNEL_CODE STRING,
  TRANSACTION_TYPE_CODE STRING,
  ORDER_TIMESTAMP TIMESTAMP,
  ORDER_DATE_KEY INTEGER,
  COLOR_RHID INTEGER,
  COLOR_GRPID STRING,
  PRODUCT_RHID INTEGER,
  PRODUCT_GRPID STRING,
  SIZE_RHID INTEGER,
  SIZE_GRPID STRING,
  STYLE_RHID INTEGER,
  STYLE_GRPID STRING,
  DIVISION_RHID INTEGER,
  DIVISION_GRPID STRING,
  CATALOG_ITEM_CODE STRING,
  STORE_RHID INTEGER,
  STORE_GRPID STRING,
  SHIP_STORE_RHID INTEGER,
  SHIP_STORE_GRPID STRING,
  ITEM_TYPE_DESC STRING,
  UNIT_PRICE_AMT DECIMAL(20, 6),
  SALES_QTY INTEGER,
  TAX_TYPE1_CODE STRING,
  TAX_TYPE1_AMT DECIMAL(20, 6),
  TAX_TYPE2_CODE STRING,
  TAX_TYPE2_AMT DECIMAL(20, 6),
  TAX_TYPE3_CODE STRING,
  TAX_TYPE3_AMT DECIMAL(20, 6),
  TAX_TYPE4_CODE STRING,
  TAX_TYPE4_AMT DECIMAL(20, 6),
  DISCOUNT1_REASON_CODE STRING,
  CANCEL_DATE DATE,
  RETURN_DATE DATE,
  VERSION_NBR INTEGER,
  CARRIER_NAME STRING,
  SHIPPING_VENDOR_NAME STRING,
  TRACKING_CODE STRING,
  CHARGE_IND INTEGER,
  PICK_CODE STRING,
  AVAILABLE_DATE DATE,
  PICK_DATE DATE,
  EMAIL_NOTIFIED_IND INTEGER,
  ORDER_FAILED_NOTIFIED_IND INTEGER,
  ORDER_FAILED_NOTIFIED_DATE DATE,
  ORDER_NOTES_TEXT STRING,
  REVIEW_NOTIFIED_IND INTEGER,
  CREDITED_IND INTEGER,
  COMMENTS_TEXT STRING,
  SHIPPING_METHOD_CODE STRING,
  SHIP_DATE DATE,
  CHARGE_TIMESTAMP TIMESTAMP,
  CURRENCY_RHID INTEGER,
  CURRENCY_GRPID STRING,
  REC_CREATE_TIMESTAMP TIMESTAMP
) 
USING DELTA 
LOCATION 's3://test-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dly_orderitems' 
TBLPROPERTIES ('layer' = 'SILVER');

-- COMMAND ----------

--- This table will bring together all attributes from STORE and PRODUCT universe for ORDER_ITEMS to match final table structure: DTL_RETAIL_SALES 

INSERT OVERWRITE TRANSIENT_DL.DLY_ORDERITEMS
SELECT
  DISTINCT 
  DLY_ORDERITEMS_PRODUCT.ORDER_ID,
  DLY_ORDERITEMS_PRODUCT.ORDER_ITEM_ID,
  DLY_ORDERITEMS_PRODUCT.ORDER_STATUS_CODE,
  DLY_ORDERITEMS_PRODUCT.ORDER_CHANNEL_CODE,
  DLY_ORDERITEMS_PRODUCT.TRANSACTION_TYPE_CODE,
  DLY_ORDERITEMS_PRODUCT.ORDER_TIMESTAMP,
  DLY_ORDERITEMS_PRODUCT.ORDER_DATE_KEY,  
  DLY_ORDERITEMS_PRODUCT.COLOR_RHID,
  DLY_ORDERITEMS_PRODUCT.COLOR_GRPID,
  DLY_ORDERITEMS_PRODUCT.PRODUCT_RHID,
  DLY_ORDERITEMS_PRODUCT.PRODUCT_GRPID,
  DLY_ORDERITEMS_PRODUCT.SIZE_RHID,
  DLY_ORDERITEMS_PRODUCT.SIZE_GRPID,
  DLY_ORDERITEMS_PRODUCT.STYLE_RHID,
  DLY_ORDERITEMS_PRODUCT.STYLE_GRPID,
  DLY_ORDERITEMS_PRODUCT.DIVISION_RHID,
  DLY_ORDERITEMS_PRODUCT.DIVISION_GRPID,
  DLY_ORDERITEMS_PRODUCT.CATALOG_ITEM_CODE,
  DLY_ORDERITEMS_STORE.STORE_RHID,
  DLY_ORDERITEMS_STORE.STORE_GRPID,
  DLY_ORDERITEMS_SHIPSTORE.SHIP_STORE_RHID,
  DLY_ORDERITEMS_SHIPSTORE.SHIP_STORE_GRPID,
  DLY_ORDERITEMS_PRODUCT.ITEM_TYPE_DESC,
  DLY_ORDERITEMS_PRODUCT.UNIT_PRICE_AMT,
  DLY_ORDERITEMS_PRODUCT.SALES_QTY, 
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE1_CODE,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE1_AMT,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE2_CODE,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE2_AMT,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE3_CODE,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE3_AMT,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE4_CODE,
  DLY_ORDERITEMS_PRODUCT.TAX_TYPE4_AMT,
  DLY_ORDERITEMS_PRODUCT.DISCOUNT1_REASON_CODE,
  DLY_ORDERITEMS_PRODUCT.CANCEL_DATE,
  DLY_ORDERITEMS_PRODUCT.RETURN_DATE,
  DLY_ORDERITEMS_PRODUCT.VERSION_NBR,
  DLY_ORDERITEMS_PRODUCT.CARRIER_NAME,
  DLY_ORDERITEMS_PRODUCT.SHIPPING_VENDOR_NAME,
  DLY_ORDERITEMS_PRODUCT.TRACKING_CODE,
  DLY_ORDERITEMS_PRODUCT.CHARGE_IND,
  DLY_ORDERITEMS_PRODUCT.PICK_CODE,
  DLY_ORDERITEMS_PRODUCT.AVAILABLE_DATE,
  DLY_ORDERITEMS_PRODUCT.PICK_DATE,
  DLY_ORDERITEMS_PRODUCT.EMAIL_NOTIFIED_IND,
  DLY_ORDERITEMS_PRODUCT.ORDER_FAILED_NOTIFIED_IND,
  DLY_ORDERITEMS_PRODUCT.ORDER_FAILED_NOTIFIED_DATE,
  DLY_ORDERITEMS_PRODUCT.ORDER_NOTES_TEXT,
  DLY_ORDERITEMS_PRODUCT.REVIEW_NOTIFIED_IND,
  DLY_ORDERITEMS_PRODUCT.CREDITED_IND,
  DLY_ORDERITEMS_PRODUCT.COMMENTS_TEXT,
  DLY_ORDERITEMS_PRODUCT.SHIPPING_METHOD_CODE,
  DLY_ORDERITEMS_PRODUCT.SHIP_DATE,
  DLY_ORDERITEMS_PRODUCT.CHARGE_TIMESTAMP,
  DLY_ORDERITEMS_CURRENCY.CURRENCY_RHID,
  DLY_ORDERITEMS_CURRENCY.CURRENCY_GRPID,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP
FROM
  TRANSIENT_DL.DLY_ORDERITEMS_PRODUCT DLY_ORDERITEMS_PRODUCT
  INNER JOIN TRANSIENT_DL.DLY_ORDERITEMS_STORE DLY_ORDERITEMS_STORE 
  ON DLY_ORDERITEMS_PRODUCT.ORDER_ID = DLY_ORDERITEMS_STORE.ORDER_ID
  AND DLY_ORDERITEMS_PRODUCT.ORDER_ITEM_ID = DLY_ORDERITEMS_STORE.ORDER_ITEM_ID
  INNER JOIN TRANSIENT_DL.DLY_ORDERITEMS_SHIPSTORE DLY_ORDERITEMS_SHIPSTORE 
  ON DLY_ORDERITEMS_PRODUCT.ORDER_ID = DLY_ORDERITEMS_SHIPSTORE.ORDER_ID
  AND DLY_ORDERITEMS_PRODUCT.ORDER_ITEM_ID = DLY_ORDERITEMS_SHIPSTORE.ORDER_ITEM_ID
  INNER JOIN TRANSIENT_DL.DLY_ORDERITEMS_CURRENCY DLY_ORDERITEMS_CURRENCY 
  ON DLY_ORDERITEMS_PRODUCT.ORDER_ID = DLY_ORDERITEMS_CURRENCY.ORDER_ID  
  AND DLY_ORDERITEMS_PRODUCT.ORDER_ITEM_ID = DLY_ORDERITEMS_CURRENCY.ORDER_ITEM_ID;

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DLY_ORDERITEMS;
--97492

-- COMMAND ----------

-- INSERT ECOM:ORDER_ITEMS INTO FINAL TABLE : SLV_RETAIL_SALES.DTL_RETAIL_SALES

INSERT INTO
  SLV_RETAIL_SALES.DTL_RETAIL_SALES
SELECT
  ORDER_ID,
  ORDER_ITEM_ID,
  ORDER_STATUS_CODE,
  ORDER_CHANNEL_CODE,
  TRANSACTION_TYPE_CODE,
  ORDER_TIMESTAMP,
  ORDER_DATE_KEY,
  COLOR_RHID,
  COLOR_GRPID,
  PRODUCT_RHID,
  PRODUCT_GRPID,
  SIZE_RHID,
  SIZE_GRPID,
  STYLE_RHID,
  STYLE_GRPID,
  DIVISION_RHID,
  DIVISION_GRPID,
  CATALOG_ITEM_CODE,
  STORE_RHID,
  STORE_GRPID,
  SHIP_STORE_RHID,
  SHIP_STORE_GRPID,
  ITEM_TYPE_DESC,
  UNIT_PRICE_AMT,
  SALES_QTY,
  TAX_TYPE1_CODE,
  TAX_TYPE1_AMT,
  TAX_TYPE2_CODE,
  TAX_TYPE2_AMT,
  TAX_TYPE3_CODE,
  TAX_TYPE3_AMT,
  TAX_TYPE4_CODE,
  TAX_TYPE4_AMT,
  DISCOUNT1_REASON_CODE,
  CANCEL_DATE,
  RETURN_DATE,
  VERSION_NBR,
  CARRIER_NAME,
  SHIPPING_VENDOR_NAME,
  TRACKING_CODE,
  CHARGE_IND,
  PICK_CODE,
  AVAILABLE_DATE,
  PICK_DATE,
  EMAIL_NOTIFIED_IND,
  ORDER_FAILED_NOTIFIED_IND,
  ORDER_FAILED_NOTIFIED_DATE,
  ORDER_NOTES_TEXT,
  REVIEW_NOTIFIED_IND,
  CREDITED_IND,
  COMMENTS_TEXT,
  SHIPPING_METHOD_CODE,
  SHIP_DATE,
  CHARGE_TIMESTAMP,
  CURRENCY_RHID,
  CURRENCY_GRPID,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP,
  'DL_JOB_NAME' REC_CREATE_USER_NAME,
  CURRENT_TIMESTAMP REC_UPDATE_TIMESTAMP,
  'DL_JOB_NAME' REC_UPDATE_USER_NAME,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyMMdd') REC_CREATE_DATE_KEY,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'HHmm') REC_CREATE_TIME_KEY
FROM
  TRANSIENT_DL.DLY_ORDERITEMS DLY_ORDERITEMS;

-- COMMAND ----------

UPDATE DELTA_LOGS.USECASE_AUDIT_LOGS
SET LOAD_STATUS = 'COMPLETED' , 
REC_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
REC_UPDATE_USER_NAME = 'SJ3-SLV dtl_retail_sales_ecom'
WHERE JOBNAME = 'SJ3-SLV dtl_retail_sales_ecom'
AND REC_CREATE_TIMESTAMP = ( SELECT MAX(REC_CREATE_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                              WHERE JOBNAME = 'SJ3-SLV dtl_retail_sales_ecom' AND LOAD_STATUS IS NULL)
AND LOAD_STATUS IS NULL;

-- COMMAND ----------

--input count
select count(*) row_count from  TRANSIENT_DL.ORDER_ITEMS_BASE_DAILY ;
--97492

-- COMMAND ----------

--- output count 
SELECT count(*) row_count FROM SLV_RETAIL_SALES.DTL_RETAIL_SALES 
WHERE REC_UPDATE_TIMESTAMP = (SELECT max(REC_UPDATE_TIMESTAMP) FROM SLV_RETAIL_SALES.DTL_RETAIL_SALES WHERE  ORDER_CHANNEL_CODE ='ECOM') 
AND ORDER_CHANNEL_CODE ='ECOM' 
AND REC_UPDATE_TIMESTAMP > (SELECT MAX(REC_CREATE_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                              WHERE JOBNAME = 'SJ3-SLV dtl_retail_sales_ecom' AND LOAD_STATUS IS NOT NULL ) ;
--97492                              
