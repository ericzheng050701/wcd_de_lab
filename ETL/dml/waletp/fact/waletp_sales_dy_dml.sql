USE DATABASE walmart_dev;

--------1) set latest date as a variable
SET LAST_DATE = (SELECT MAX(cal_dt) FROM waletp.DAILY_SALES);
SELECT $LAST_DATE;

--------2) use the variable in the variable to filter the raw data records. But in order avoid possibility of imcomplete records of the latest date,
-----------we need to delete the original latest date in DAILY_SALES and append the new records from that date.

DELETE FROM waletp.DAILY_SALES WHERE cal_dt=$LAST_DATE;
select max(cal_dt) from waletp.DAILY_SALES;

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
WHERE cal_dt>=NVL($LAST_DATE, '1900-01-01')
GROUP BY 1,2,3
ORDER BY 1,2,3
;

--select max(cal_dt) from waletp.DAILY_SALES;
--SELECT count(*) FROM waletp.DAILY_SALES;


