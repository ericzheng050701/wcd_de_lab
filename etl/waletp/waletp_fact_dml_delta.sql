-- this script is incomplete, it is only a sample.

-------------------------------------------------------
------------------1. prepare new data------------------
-------------------------------------------------------
USE SCHEMA WCD_LCT4.LAND;

-- add new data in raw fact table LAND.SALES.
------ add 2 new dates records in raw data;
INSERT INTO WCD_LCT4.LAND.SALES
SELECT * FROM WALMART_DEV.WALLND.SALES WHERE TRANS_DT BETWEEN TO_DATE('2012-01-01') AND TO_DATE('2012-01-02');

----previously, before this action, the date range is from 2011-01-01 to 2011-12-31. let's check the corrent date range:
SELECT MIN(trans_dt), MAX(trans_dt) FROM WCD_LCT4.LAND.SALES;
-------------this means: the raw sales data has 2 more days records

-----------------------------------------------------------------------------
------------------2. append new data into ENTP.daily_sales-------------------
-----------------------------------------------------------------------------
--------1) set latest date as a variable
SET LAST_DATE = (SELECT MAX(cal_dt) FROM WCD_LCT4.ENTP.DAILY_SALES);
SELECT $LAST_DATE;

--------2) use the variable in the variable to filter the raw data records. But in order avoid possibility of imcomplete records of the latest date,
-----------we need to delete the original latest date in DAILY_SALES and append the new records from that date.

DELETE FROM WCD_LCT4.ENTP.DAILY_SALES WHERE cal_dt=$LAST_DATE;
select max(cal_dt) from WCD_LCT4.ENTP.DAILY_SALES;

INSERT INTO entp.daily_sales
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
FROM land.sales
WHERE trans_dt>=$LAST_DATE
GROUP BY 1,2,3
ORDER BY 1,2,3;

select max(cal_dt) from WCD_LCT4.ENTP.DAILY_SALES;
select count(*) from WCD_LCT4.ENTP.DAILY_SALES where cal_dt=to_date('2011-12-31');

