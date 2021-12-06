-- Databricks notebook source
CREATE TABLE IF NOT EXISTS TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY
USING DELTA 
LOCATION 's3://skx-dataeng-nonprod-datalake-staging/transient/datalake/integrated/dtl_retail_sales_base_daily' 
TBLPROPERTIES ('LAYER' = 'SILVER')
AS 
SELECT * 
FROM SLV_RETAIL_SALES.DTL_RETAIL_SALES 
WHERE REC_UPDATE_TIMESTAMP > NVL(( SELECT MAX(SRC_LAST_UPD_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                                    WHERE JOBNAME='SJ4-GOLD agg_store_metrics_daily' AND LOAD_STATUS = 'COMPLETED'),
                                    TO_DATE('1900-01-01','yyyy-MM-dd'));

-- COMMAND ----------

INSERT OVERWRITE TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY
SELECT * FROM SLV_RETAIL_SALES.DTL_RETAIL_SALES 
WHERE REC_UPDATE_TIMESTAMP > NVL(( SELECT MAX(SRC_LAST_UPD_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                                      WHERE JOBNAME = 'SJ4-GOLD agg_store_metrics_daily' AND LOAD_STATUS = 'COMPLETED'),
                                   TO_DATE('1900-01-01', 'yyyy-MM-dd'));

-- COMMAND ----------

select count(*) row_count from TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY;

-- COMMAND ----------

INSERT INTO DELTA_LOGS.USECASE_AUDIT_LOGS 
SELECT 
'SJ4-GOLD agg_store_metrics_daily',
MAX(REC_UPDATE_TIMESTAMP),
NULL,
CURRENT_TIMESTAMP ,
'SJ4-GOLD agg_store_metrics_daily', 
NULL,
NULL
FROM FROM TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY
;

-- COMMAND ----------

INSERT INTO GLD_PERFORMANCE.AGG_STORE_METRICS_DAILY
SELECT
  ORDER_DATE_KEY AS DATE_KEY,
  NVL(SALES.STORE_RHID,'-1') AS STORE_RHID,
  MAX(STORE.STORE_OPEN_DATE) LATEST_OPEN_DATE,
  MAX(STORE.STORE_CLOSE_DATE) LATEST_CLOSE_DATE,
  SUM(VISITS.ENTRY_COUNT) AS TOTAL_VISIT_COUNT,
  COUNT(SALES.ORDER_ID) TOTAL_TRANSACTION_COUNT,
  SUM(NVL(SALES.SALES_QTY,0)) TOTAL_SALES_QUANTITY,
  NULL AS TOTAL_RETURN_QUANTITY,
  SUM(NVL(SALES.UNIT_PRICE_AMT,0)) TOTAL_SALES_USD_AMOUNT,
  SUM(NVL(SALES.TAX_TYPE1_AMT,0) + NVL(SALES.TAX_TYPE2_AMT,0) + NVL(SALES.TAX_TYPE3_AMT,0) + NVL(SALES.TAX_TYPE4_AMT,0)) TOTAL_TAX_USD_AMOUNT,
  NULL AS TOTAL_DISCOUNT_USD_AMOUNT,
  NULL AS TOTAL_PROMO_USD_AMOUNT,
  NULL AS TOTAL_SHIPPING_USD_AMOUNT,
  COUNT (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'FOOTWEAR' AND  PRODUCT_LINE_CODE ='4' THEN 1 end)  as FOOTWEAR_TRANSACTION_COUNT, 
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'FOOTWEAR' AND  PRODUCT_LINE_CODE ='4' 
		THEN NVL(SALES.SALES_QTY,0) 
        ELSE 0 
	   end ) FOOTWEAR_SALES_QUANTITY,
  NULL AS FOOTWEAR_RETURN_QUANTITY,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'FOOTWEAR' AND  PRODUCT_LINE_CODE ='4' 
		THEN NVL(SALES.UNIT_PRICE_AMT,0)  
        ELSE 0 
	   end) FOOTWEAR_SALES_USD_AMOUNT,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'FOOTWEAR' AND  PRODUCT_LINE_CODE ='4' 
        THEN NVL(SALES.TAX_TYPE1_AMT,0) + NVL(SALES.TAX_TYPE2_AMT,0) + NVL(SALES.TAX_TYPE3_AMT,0) + NVL(SALES.TAX_TYPE4_AMT,0)
        ELSE 0 
       end) AS FOOTWEAR_TAX_USD_AMOUNT,
  NULL AS FOOTWEAR_DISCOUNT_USD_AMOUNT,
  NULL AS FOOTWEAR_PROMO_USD_AMOUNT,
  NULL AS FOOTWEAR_SHIPPING_USD_AMOUNT,
  COUNT (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'APPAREL' AND PRODUCT_LINE_CODE ='2' THEN 1 end) as APPAREL_TRANSACTION_COUNT,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'APPAREL' AND  PRODUCT_LINE_CODE ='2' 
		THEN NVL(SALES.SALES_QTY,0) 
        ELSE 0 
		end ) APPAREL_SALES_QUANTITY,
  NULL AS APPAREL_RETURN_QUANTITY,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'APPAREL' AND  PRODUCT_LINE_CODE ='2' 
		THEN NVL(SALES.UNIT_PRICE_AMT,0)  
        ELSE 0 
	   end) APPAREL_SALES_USD_AMOUNT,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'APPAREL' AND  PRODUCT_LINE_CODE ='2' 
        THEN NVL(SALES.TAX_TYPE1_AMT,0) + NVL(SALES.TAX_TYPE2_AMT,0) + NVL(SALES.TAX_TYPE3_AMT,0) + NVL(SALES.TAX_TYPE4_AMT,0) 
        ELSE 0 
       end) AS APPAREL_TAX_USD_AMOUNT,            
  NULL AS APPAREL_DISCOUNT_USD_AMOUNT,
  NULL AS APPAREL_PROMO_USD_AMOUNT,
  NULL AS APPAREL_SHIPPING_USD_AMOUNT,
  COUNT (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'ACCESSORIES' AND PRODUCT_LINE_CODE ='1' THEN 1 end) as ACCESSORIES_TRANSACTION_COUNT,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'ACCESSORIES' AND  PRODUCT_LINE_CODE ='1' 
		THEN NVL(SALES.SALES_QTY,0) 
        ELSE 0 
	   end ) ACCESSORIES_SALES_QUANTITY,
  NULL AS ACCESSORIES_RETURN_QUANTITY,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'ACCESSORIES' AND  PRODUCT_LINE_CODE ='1' 
		THEN NVL(SALES.UNIT_PRICE_AMT,0) 
        ELSE 0 
		end) ACCESSORIES_SALES_USD_AMOUNT,
  SUM (case when PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC = 'ACCESSORIES' AND  PRODUCT_LINE_CODE ='1' 
        THEN NVL(SALES.TAX_TYPE1_AMT,0) + NVL(SALES.TAX_TYPE2_AMT,0) + NVL(SALES.TAX_TYPE3_AMT,0) + NVL(SALES.TAX_TYPE4_AMT,0)
        ELSE 0 
       end) AS ACCESSORIES_TAX_USD_AMOUNT,
  NULL AS ACCESSORIES_DISCOUNT_USD_AMOUNT,
  NULL AS ACCESSORIES_PROMO_USD_AMOUNT,
  NULL AS ACCESSORIES_SHIPPING_USD_AMOUNT,
  CURRENT_TIMESTAMP AS REC_CREATE_TIMESTAMP,
  'DL_JOB_NAME' REC_CREATE_USER_NAME,
  CURRENT_TIMESTAMP REC_UPDATE_TIMESTAMP,
  'DL_JOB_NAME' REC_UPDATE_USER_NAME,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd') REC_CREATE_DATE_KEY,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'HHmm') REC_CREATE_TIME_KEY
FROM
  TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY SALES    
  LEFT OUTER JOIN REF_HUB.MST_STORE STORE
  ON NVL(SALES.STORE_RHID,-1) = STORE.STORE_RHID
  LEFT OUTER JOIN SLV_RETAIL_SALES.AGG_STORE_VISITS_DAILY VISITS
  ON STORE.STORE_RHID = VISITS.STORE_RHID
  LEFT OUTER JOIN REF_HUB.MST_PRODUCT PRODUCT_FOOTWEAR 
  ON NVL(SALES.PRODUCT_RHID,-1) = PRODUCT_FOOTWEAR.PRODUCT_RHID
  LEFT OUTER JOIN REF_HUB.MST_PRODUCT_LINE PRODUCT_LINE_FOOTWEAR 
  ON PRODUCT_FOOTWEAR.PRODUCT_LINE_RHID = PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_RHID
  AND PRODUCT_LINE_FOOTWEAR.PRODUCT_LINE_DESC in ('FOOTWEAR','APPAREL','ACCESSORIES')
  AND PRODUCT_LINE_CODE in (1,2,4)
GROUP BY
  NVL(SALES.STORE_RHID,'-1'),
  ORDER_DATE_KEY;

-- COMMAND ----------

-- Update the LOAD_STATUS = 'COMPLETED' after loading to target is completed.

UPDATE DELTA_LOGS.USECASE_AUDIT_LOGS
SET LOAD_STATUS = 'COMPLETED' , 
REC_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP,
REC_UPDATE_USER_NAME = 'SJ4-GOLD agg_store_metrics_daily'
WHERE JOBNAME = 'SJ4-GOLD agg_store_metrics_daily'
AND REC_CREATE_TIMESTAMP = ( SELECT MAX(REC_CREATE_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS 
                              WHERE JOBNAME='SJ4-GOLD agg_store_metrics_daily' AND LOAD_STATUS IS NULL)
AND LOAD_STATUS IS NULL;


-- COMMAND ----------

--Input Row count
select count(*) 
from (
select count(*) , NVL(SALES.STORE_RHID,'-1'),  ORDER_DATE_KEY 
from TRANSIENT_DL.DTL_RETAIL_SALES_BASE_DAILY SALES
GROUP BY
  NVL(SALES.STORE_RHID,'-1'),
  ORDER_DATE_KEY
 );

-- COMMAND ----------

--Outout Row count
select count(*) row_count from GLD_PERFORMANCE.AGG_STORE_METRICS_DAILY 
where REC_UPDATE_TIMESTAMP = (select max(REC_UPDATE_TIMESTAMP) from GLD_PERFORMANCE.AGG_STORE_METRICS_DAILY)
AND REC_UPDATE_TIMESTAMP > (SELECT MAX(REC_CREATE_TIMESTAMP) FROM DELTA_LOGS.USECASE_AUDIT_LOGS WHERE JOBNAME = 'SJ4-GOLD agg_store_metrics_daily' AND LOAD_STATUS IS NOT NULL ) ;
