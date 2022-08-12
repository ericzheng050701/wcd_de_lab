USE DATABASE walmart_dev;

DROP TABLE IF EXISTS waletp.PRODUCT_RAW_STG;

CREATE OR REPLACE TRANSIENT TABLE waletp.PRODUCT_RAW_STG
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
	TRUE AS tlog_active_flg
FROM WALLND.PRODUCT)
;

--
--SELECT * FROM waletp.PRODUCT_RAW_STG;
--SELECT * FROM waletp.PRODUCT_DIM;
--

-------1.1) In order to avoid updating by mistake, we create a versioning staging table to work as PRUDUCT_DIM.
DROP TABLE IF EXISTS waletp.PRODUCT_DIM_VER;
CREATE OR REPLACE TRANSIENT TABLE waletp.PRODUCT_DIM_VER AS SELECT * FROM waletp.PRODUCT_DIM;
--SELECT * FROM waletp.PRODUCT_DIM_VER;


MERGE INTO waletp.PRODUCT_DIM_VER t1
USING waletp.PRODUCT_RAW_STG t2
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

--SELECT count(*) FROM waletp.PRODUCT_DIM_VER;


------1.3) In PRODUCT_DIM_VER, label the inactivate rows to be false

MERGE INTO waletp.PRODUCT_DIM_VER t1
USING waletp.PRODUCT_RAW_STG t2
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

--SELECT * FROM waletp.PRODUCT_DIM_ver;

-------1.4) Update the real PRODUCT_DIM in waletp.
TRUNCATE TABLE waletp.PRODUCT_DIM;
INSERT INTO waletp.PRODUCT_DIM SELECT * FROM waletp.PRODUCT_DIM_VER;

--SELECT count(*) FROM waletp.PRODUCT_DIM;


