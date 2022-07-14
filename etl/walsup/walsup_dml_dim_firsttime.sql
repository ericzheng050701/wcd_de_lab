--this script is incomplete. It is only a sample, don't use as script directly.

USE SCHEMA walmart_dev.walsup;


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
WHERE tlog_active_flg=TRUE
;

--SELECT * FROM walsup.store_dim;

INSERT INTO walsup.product_dim
(
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
	tlog_active_flg
)
SELECT 
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
	tlog_active_flg
FROM waletp.PRODUCT_dim
WHERE tlog_active_flg=TRUE
;


INSERT INTO walsup.calendar_dim
(	
	cal_dt,
	cal_type_name,
	day_of_wk_num,
	year_num,
	week_num,
	year_wk_num,
	month_num,
	year_month_num,
	qtr_num,
	yr_qtr_num
)
SELECT 
	cal_dt,
	cal_type_name,
	day_of_wk_num,
	year_num,
	week_num,
	year_wk_num,
	month_num,
	year_month_num,
	qtr_num,
	yr_qtr_num
FROM waletp.calendar_dim
;

