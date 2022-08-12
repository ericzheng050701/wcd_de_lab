USE DATABASE walmart_dev;

TRUNCATE walsup.store_dim;
INSERT INTO walsup.store_dim 
(
	store_key,
	store_name,
	status_code,
	status_cd_name,
	open_dt,
	close_dt,
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
	tlog_active_flg
	)
SELECT
	store_key,
	store_name,
	status_code,
	status_cd_name,
	open_dt,
	close_dt,
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
	tlog_active_flg
FROM waletp.store_dim
WHERE tlog_active_flg=true
;

--SELECT * FROM walsup.store_dim;
