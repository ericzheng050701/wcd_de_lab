-- this script is incomplete, it is only a sample.

-------------------------------------------------------
------------------1. prepare new data------------------
-------------------------------------------------------
USE SCHEMA WCD_LCT4.LAND;

-- change new data in raw dim table LAND.PRODUCT.
------ change 2 rows in raw data, and add 1 new row;
SELECT * FROM LAND.PRODUCT;

UPDATE LAND.PRODUCT SET PROD_NAME='CHANGE-1' WHERE PROD_KEY=657768;
UPDATE LAND.PRODUCT SET PROD_NAME='CHANGE-2' WHERE PROD_KEY=293693;
INSERT INTO LAND.PRODUCT VALUES (999999,'ADD-1',2.22, 88.88, 'brand-999', 1, 'active', 4, 'category-4', 1, 'subcategory-1');

SELECT * FROM LAND.PRODUCT;
-----------------------------------------------------------------------------
------------------2. update the ENTP.product_dim table-----------------------
-----------------------------------------------------------------------------

---1) create a staging table first to hold all up to date data from raw table.
DROP TABLE IF EXISTS WCD_LCT4.ENTP.PRODUCT_RAW_STG;
CREATE TRANSIENT TABLE WCD_LCT4.ENTP.PRODUCT_RAW_STG
AS 
(SELECT 
	prod_key,
	prod_name,
	vol,
	wgt,
	brand_name, 
	status_code,
	status_code_name,
	category_key,
	category_name,
	subcategory_key,
	subcategory_name,
    CURRENT_DATE() AS START_DATE,
    NULL AS END_DATE,
	TRUE AS tlog_active_flg
FROM LAND.PRODUCT);

SELECT * FROM WCD_LCT4.ENTP.PRODUCT_RAW_STG;
SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM;

---1) compare the existing product_dim table with the staging table, and update difference.


-------1.1) In order to avoid updating by mistake, we create a versioning staging table to work as PRUDUCT_DIM.
TRUNCATE TABLE IF EXISTS WCD_LCT4.ENTP.PRODUCT_DIM_VER;
CREATE OR REPLACE TRANSIENT TABLE WCD_LCT4.ENTP.PRODUCT_DIM_VER AS SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM;
SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM_VER;

-------1.2) Insert all the changes from new raw table to PRODUCT_DIM_VER
MERGE INTO WCD_LCT4.ENTP.PRODUCT_DIM_VER t1
USING ENTP.PRODUCT_RAW_STG t2
ON  t1.prod_key=t2.prod_key
    AND t1.prod_name=t2.prod_name 
    AND t1.vol=t2.vol 
    AND t1.wgt=t2.wgt
    AND t1.brand_name=t2.brand_name
    AND t1.status_code=t2.status_code
    AND t1.status_code_name=t2.status_code_name
    AND t1.category_key=t2.category_key
    AND t1.category_name=t2.category_name
    AND t1.subcategory_key=t2.subcategory_key
    AND t1.subcategory_name=t2.subcategory_name
WHEN NOT MATCHED 
THEN INSERT (
    prod_key,
    prod_name,
    vol,
    wgt,
    brand_name,
    status_code,
    status_code_name,
    category_key,
    category_name,
    subcategory_key,
    subcategory_name,
    tlog_active_flg,
    start_date,
    end_date)
VALUES (
    t2.prod_key,
    t2.prod_name,
    t2.vol,
    t2.wgt,
    t2.brand_name,
    t2.status_code,
    t2.status_code_name,
    t2.category_key,
    t2.category_name,
    t2.subcategory_key,
    t2.subcategory_name,
    TRUE,
    CURRENT_DATE(),
    NULL
)
;

SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM_VER;


-------1.3) In PRODUCT_DIM_VER, label the inactivate rows to be false

MERGE INTO WCD_LCT4.ENTP.PRODUCT_DIM_VER t1
USING ENTP.PRODUCT_RAW_STG t2
ON t1.prod_key=t2.prod_key
WHEN MATCHED
    AND (
    t1.prod_name!=t2.prod_name 
    OR t1.vol!=t2.vol 
    OR t1.wgt!=t2.wgt
    OR t1.brand_name!=t2.brand_name
    OR t1.status_code!=t2.status_code
    OR t1.status_code_name!=t2.status_code_name
    OR t1.category_key!=t2.category_key
    OR t1.category_name!=t2.category_name
    OR t1.subcategory_key!=t2.subcategory_key
    OR t1.subcategory_name!=t2.subcategory_name
)
THEN UPDATE SET end_date = current_date(), tlog_active_flg=FALSE
;

SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM_ver;

-------1.4) Update the real PRODUCT_DIM in WCD_LCT4.ENTP.
TRUNCATE TABLE WCD_LCT4.ENTP.PRODUCT_DIM;
INSERT INTO WCD_LCT4.ENTP.PRODUCT_DIM SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM_VER;

SELECT * FROM WCD_LCT4.ENTP.PRODUCT_DIM;
