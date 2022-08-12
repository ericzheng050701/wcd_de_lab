USE DATABASE walmart_dev;

TRUNCATE TABLE walsup.product_dim;
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
WHERE tlog_active_flg=true
;