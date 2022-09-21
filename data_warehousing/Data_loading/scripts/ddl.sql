
USE DATABASE wcd_lab;

CREATE SCHEMA IF NOT EXISTS walmart_anl;

------DIMENSION TABLES
CREATE OR REPLACE TABLE walmart_anl.product_dim
(	
	prod_key	Integer,
	prod_name	varchar(150),
	vol	numeric(19,3),
	wgt	numeric(19,3),
	brand_name	varchar,
	status_code	varchar(30),
	status_code_name 	varchar(30),
	category_key	integer,
	category_name	varchar(150),
	subcategory_key	integer,
	subcategory_name	varchar(150),
	start_dt date,
	end_dt date,	
	tlog_active_flg	boolean,
	update_time timestamp default CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE walmart_anl.store_dim
(
	store_key	INTEGER,
	store_name	varchar(150),
	status_code	varchar(10),
	status_cd_name	varchar(100),
	open_dt	date,
	close_dt	date,
	addr	varchar(500),
	city	varchar(50),
	region varchar(100),
	cntry_cd	varchar(30),
	cntry_nm	varchar(150),
	postal_zip_cd	varchar(10),
	prov_name	varchar(30),
	prov_code	varchar(30),
	market_key	integer,
	market_name	varchar(150),
	submarket_key	integer,
	submarket_name	varchar(150),
	latitude	NUMERIC(19, 6),
	longitude	NUMERIC(19, 6),
	tlog_active_flg boolean,
	start_dt date,
	end_dt date,
	update_time	timestamp default CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE walmart_anl.calendar_dim
(	
	cal_dt	date NOT NULL,
	cal_type_name	varchar(20),
	day_of_wk_num 	integer,
	year_num	integer,
	week_num	integer,
	year_wk_num	 integer,
	month_num	integer,
	year_month_num	integer,
	qtr_num	integer,
	yr_qtr_num	integer,
	update_time	timestamp default CURRENT_TIMESTAMP()
);



-------FACT TABLES
CREATE OR REPLACE TABLE walmart_anl.sales_inv_store_dy
(	
CAL_DT	date,
STORE_KEY	integer,
PROD_KEY	integer,
SALES_QTY	number(38,2),
SALES_PRICE	number(38,2),
SALES_AMT	number(38,2),
DISCOUNT	number(38,2),
SALES_COST	number(38,2),
SALES_MGRN	number(38,2),
STOCK_ON_HAND_QTY	number(38,2),
ORDERED_STOCK_QTY	number(38,2),
OUT_OF_STOCK_FLG	boolean,
IN_STOCK_FLG	boolean,
Low_STOCK_FLG	boolean,
UPDATE_TIME	timestamp default CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE walmart_anl.sales_inv_store_wk
(	
YR_NUM	integer,
WK_NUM	integer,
STORE_KEY integer,
PROD_KEY	integer,
WK_SALES_QTY	number(38,2),
AVG_SALES_PRICE	number(38,2),
WK_SALES_AMT	number(38,2),
WK_DISCOUNT	number(38,2),
WK_SALES_COST	number(38,2),
WK_SALES_MGRN	number(38,2),
EOP_STOCK_ON_HAND_QTY	number(38,2),
EOP_ORDERED_STOCK_QTY	number(38,2),
OUT_OF_STOCK_TIMES	integer,
IN_STOCK_TIMES	integer,
Low_STOCK_TIMES	integer,
UPDATE_TIME	timestamp default CURRENT_TIMESTAMP()
);
