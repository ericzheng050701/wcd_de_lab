USE DATABASE walmart_dev;


CREATE OR REPLACE TABLE waletp.product_dim
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
	tlog_active_flg	boolean,
	start_date date,
	end_date date,
	update_time timestamp default CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE waletp.store_dim
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
	start_date date,
	end_date date,
	update_time	timestamp default CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE waletp.calendar_dim
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


CREATE OR REPLACE TABLE waletp.daily_inventory
(	
cal_dt date,
store_key int,
prod_key int,
stock_on_hand_qty number(38,2),
ordered_stock number(38,2),
out_of_stock_flg boolean,
waste_qty number(38,2),
promotion_flg boolean,
next_delivery_dt date, 
update_time timestamp default CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE waletp.daily_sales
(	
cal_dt date,
store_key int,
prod_key int,
sales_qty number(38,2),
sales_amt number(38,2),
sales_price number(38,2),
sales_cost number(38,2),
sales_mgrn number(38,2),
discount number(38,2),
ship_cost number(38,2),
update_time timestamp default CURRENT_TIMESTAMP()
);

