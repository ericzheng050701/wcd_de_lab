USE SCHEMA walmart_dev.waletp;


INSERT INTO waletp.store_dim 
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
	start_dt,
	end_dt,
	tlog_active_flg
	)
SELECT
	store_key	AS store_key,
	store_desc 	AS store_name,
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
	prov_state_desc AS prov_name,
	prov_state_cd AS prov_code,
	market_key AS market_key,
	market_name	 AS market_name,
	submarket_key AS submarket_key,
	submarket_name AS submarket_name,
	latitude AS latitude,
	longitude AS longitude,
	CURRENT_DATE() AS start_dt,
	NULL AS end_dt,
	TRUE AS tlog_active_flg
FROM wallnd.store
;

--SELECT * FROM waletp.store_dim;

INSERT INTO waletp.product_dim
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
	start_dt,
	end_dt,
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
	CURRENT_DATE() AS start_dt,
	NULL AS end_dt,
	TRUE AS tlog_active_flg
FROM WALLND.PRODUCT
;


INSERT INTO waletp.calendar_dim
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
	cal_dt AS cal_dt,
	cal_type_desc AS cal_type_name,
	day_of_wk_num AS day_of_wk_num,
	yr_num AS year_num,
	wk_num AS week_num,
	yr_wk_num AS year_wk_num,
	mnth_num AS month_num,
	yr_mnth_num AS year_month_num,
	qtr_num AS qtr_num,
	yr_qtr_num AS yr_qtr_num
FROM wallnd.calendar
;

INSERT INTO waletp.daily_inventory
(	
cal_dt,
store_key,
prod_key,
stock_on_hand_qty,
ordered_stock,
out_of_stock_flg,
waste_qty,
promotion_flg,
next_delivery_dt 
)
SELECT 
cal_dt   AS cal_dt,
store_key   AS store_key,
prod_key   AS prod_key,
inventory_on_hand_qty   AS stock_on_hand_qty,
inventory_on_order_qty      AS ordered_stock,
out_of_stock_flg   AS out_of_stock_flg,
waste_qty   AS waste_qty,
promotion_flg   AS promotion_flg,
next_delivery_dt   AS next_delivery_dt
FROM wallnd.inventory;


INSERT INTO waletp.daily_sales
(	
	cal_dt,
	store_key,
	prod_key,
	sales_qty,
	sales_amt,
	sales_price,
	sales_cost,
	sales_mgrn,
	discount,
	ship_cost
)
SELECT 
	trans_dt AS cal_dt,
	store_key AS store_key,
	prod_key AS prod_key,
	sum(sales_qty ) AS sales_qty,
	sum(sales_amt ) AS sales_amt,
	avg(sales_price ) AS sales_price,
	sum(sales_cost) AS sales_cost,
	sum(sales_mgrn) AS sales_mgrn,
	avg(discount) AS discount,
	sum(ship_cost) AS ship_cost
FROM wallnd.sales
GROUP BY 1,2,3
ORDER BY 1,2,3;