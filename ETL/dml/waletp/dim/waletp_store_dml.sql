USE DATABASE walmart_dev;

DROP TABLE IF EXISTS waletp.STORE_RAW_STG;

CREATE OR REPLACE TRANSIENT TABLE waletp.STORE_RAW_STG
AS 
(SELECT
	store_key,
	store_desc AS store_name,
	NULL AS status_code,
	NULL AS status_cd_name,
	NULL AS open_dt,
	NULL AS close_dt,
	addr AS addr,
	city AS city,
	region AS region,
	cntry_cd AS cntry_cd,
	cntry_nm AS cntry_nm,
	postal_zip_cd AS postal_zip_cd,
	prov_state_desc AS prov_name ,
	prov_state_cd AS prov_code,
	market_key AS market_key,
	market_name AS market_name,
    submarket_key AS submarket_key,
	submarket_name AS submarket_name,
	latitude AS latitude,
	longitude AS longitude,
	TRUE AS tlog_active_flg
FROM WALLND.STORE)
;

--
SELECT * FROM waletp.STORE_RAW_STG;
--SELECT * FROM waletp.STORE_DIM;
--

-------1.1) In order to avoid updating by mistake, we create a versioning staging table to work as PRUDUCT_DIM.
DROP TABLE IF EXISTS waletp.STORE_DIM_VER;
CREATE TRANSIENT TABLE waletp.STORE_DIM_VER AS SELECT * FROM waletp.STORE_DIM;
--SELECT * FROM waletp.STORE_DIM_VER;


MERGE INTO waletp.STORE_DIM_VER t1
USING waletp.STORE_RAW_STG t2
ON  t1.store_key=t2.store_key
	AND t1.store_name=t2.store_name
	AND t1.addr=t2.addr
	AND t1.city=t2.city
	AND t1.cntry_cd=t2.cntry_cd
	AND t1.cntry_nm=t2.cntry_nm
	AND t1.prov_name=t2.prov_name
	AND t1.prov_code=t2.prov_code
	AND t1.market_key=t2.market_key
	AND t1.market_name=t2.market_name
    AND t1.submarket_key=t2.submarket_key
	AND t1.submarket_name=t2.submarket_name
	AND t1.latitude=t2.latitude
	AND t1.longitude=t2.longitude
WHEN NOT MATCHED 
THEN INSERT (
	store_key,
	store_name,
	addr,
	city,
	region,
	cntry_cd,
	cntry_nm,
	postal_zip_cd,
	prov_name,
	prov_code,
	market_key,
	market_name,
	submarket_key,
	submarket_name,
	latitude,
	longitude,
	tlog_active_flg,
	start_date,
	end_date)
VALUES (
	t2.store_key,
	t2.store_name,
	t2.addr,
	t2.city,
	t2.region,
	t2.cntry_cd,
	t2.cntry_nm,
	t2.postal_zip_cd,
	t2.prov_name,
	t2.prov_code,
	t2.market_key,
	t2.market_name,
	t2.submarket_key,
	t2.submarket_name,
	t2.latitude,
	t2.longitude,
	TRUE,
	current_date(),
	NULL
)
;

--SELECT count(*) FROM waletp.STORE_DIM_VER;


------1.3) In STORE_DIM_VER, label the inactivate rows to be false

MERGE INTO waletp.STORE_DIM_VER t1
USING waletp.STORE_RAW_STG t2
ON t1.store_key=t2.store_key
WHEN MATCHED
    AND (
	t1.store_name!=t2.store_name
	OR t1.addr!=t2.addr
	OR t1.city!=t2.city
	OR t1.region!=t2.region
	OR t1.cntry_cd!=t2.cntry_cd
	OR t1.cntry_nm!=t2.cntry_nm
	OR t1.postal_zip_cd!=t2.postal_zip_cd
	OR t1.prov_name!=t2.prov_name
	OR t1.prov_code!=t2.prov_code
	OR t1.market_key!=t2.market_key
	OR t1.market_name!=t2.market_name
    OR t1.submarket_key!=t2.submarket_key
	OR t1.submarket_name!=t2.submarket_name
	OR t1.latitude!=t2.latitude
	OR t1.longitude!=t2.longitude
)
THEN UPDATE SET end_date = current_date(), tlog_active_flg=FALSE
;

SELECT count(*) FROM waletp.STORE_DIM_ver;

-------1.4) Update the real STORE_DIM in waletp.
TRUNCATE TABLE waletp.STORE_DIM;
INSERT INTO waletp.STORE_DIM SELECT * FROM waletp.STORE_DIM_VER;

SELECT count(*) FROM waletp.STORE_DIM;



--SELECT * FROM waletp.STORE_RAW_STG WHERE STORE_KEY=248;
--SELECT * FROM waletp.STORE_DIM_VER WHERE STORE_KEY=248;

