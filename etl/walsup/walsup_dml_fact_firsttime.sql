USE SCHEMA walmart_dev.walsup;



INSERT INTO walsup.sales_inv_store_dy 
(
	CAL_DT,
	STORE_KEY,
	PROD_KEY,
	SALES_QTY,
	SALES_PRICE,
	SALES_AMT,
	DISCOUNT,
	SALES_COST,
	SALES_MGRN,
	STOCK_ON_HAND_QTY,
	ORDERED_STOCK_QTY,
	OUT_OF_STOCK_FLG,
	IN_STOCK_FLG,
	Low_STOCK_FLG
)
SELECT
	s.CAL_DT,
	s.STORE_KEY,
	s.PROD_KEY,
	s.SALES_QTY,
	s.SALES_PRICE,
	s.SALES_AMT,
	s.DISCOUNT,
	s.SALES_COST,
	s.SALES_MGRN,
	i.STOCK_ON_HAND_QTY,
	i.ORDERED_STOCK,
	i.OUT_OF_STOCK_FLG,
	CASE WHEN i.OUT_OF_STOCK_FLG=TRUE THEN FALSE ELSE TRUE END AS in_stock_flg,
	CASE WHEN i.STOCK_ON_HAND_QTY<s.SALES_QTY THEN TRUE ELSE FALSE END AS low_stock_flg
FROM waletp.daily_sales s
FULL OUTER JOIN waletp.daily_inventory i USING (cal_dt, store_key, prod_key)
;


-- in calendar_dim if day_of_wk_num=6 then it is the end of the week
INSERT INTO walsup.sales_inv_store_wk 
(
	YR_NUM,
	WK_NUM,
	STORE_KEY,
	PROD_KEY,
	WK_SALES_QTY,
	AVG_SALES_PRICE,
	WK_SALES_AMT,
	WK_DISCOUNT,
	WK_SALES_COST,
	WK_SALES_MGRN,
	EOP_STOCK_ON_HAND_QTY,
	EOP_ORDERED_STOCK_QTY,
	OUT_OF_STOCK_TIMES,
	IN_STOCK_TIMES,
	Low_STOCK_TIMES,
	potential_low_stock_impact
)
SELECT
	c.year_num AS yr_num,
	c.week_num AS wk_num,
	s.store_key,
	s.prod_key,
	SUM(SALES_QTY) AS WK_SALES_QTY,
	AVG(SALES_PRICE) AS AVG_SALES_PRICE,
	SUM(SALES_AMT) AS WK_SALES_AMT,
	SUM(DISCOUNT) AS WK_DISCOUNT,
	SUM(SALES_COST) AS WK_SALES_COST,
	SUM(SALES_MGRN) AS WK_SALES_MGRN,
	sum(CASE WHEN c.day_of_wk_num=6 THEN s.STOCK_ON_HAND_QTY ELSE 0 end) AS EOP_STOCK_ON_HAND_QTY,
	sum(CASE WHEN c.day_of_wk_num=6 THEN s.ORDERED_STOCK_QTY ELSE 0 end) AS EOP_ORDERED_STOCK_QTY,
	count(CASE WHEN s.out_of_stock_flg=TRUE THEN 1 ELSE 0 end) AS OUT_OF_STOCK_TIMES,
	count(CASE WHEN s.in_stock_flg=TRUE THEN 1 ELSE 0 end) AS IN_STOCK_TIMES,
	count(CASE WHEN s.low_stock_flg=TRUE THEN 1 ELSE 0 end) AS LOW_STOCK_TIMES,
	sum(CASE WHEN s.low_stock_flg=TRUE THEN (s.sales_amt - (s.STOCK_ON_HAND_QTY*s.SALES_PRICE)) ELSE 0 END) AS potential_low_stock_impact
FROM walsup.sales_inv_store_dy s
JOIN walsup.calendar_dim c USING (cal_dt)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;

