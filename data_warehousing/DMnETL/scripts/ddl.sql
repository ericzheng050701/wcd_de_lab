USE DATABASE wcd_lab;

CREATE SCHEMA IF NOT EXISTS sakila_anl;


CREATE OR REPLACE TABLE sakila_anl.customer_dim (
customer_id	int PRIMARY KEY,
first_name	varchar(45),
last_name	varchar(45),
email	varchar(500),
create_date	timestamp,
address	varchar(500),
address2	varchar(500),
district	varchar(20),
city_name	varchar(50),
postal_code	varchar(10),
phone	varchar(20),
coutry_name	varchar(50),
active	boolean);

CREATE OR REPLACE TABLE sakila_anl.staff_dim (
staff_id	int PRIMARY KEY,
first_name	varchar(45),
last_name	varchar(45),
address	varchar(500),
address2	varchar(500),
picture	varchar(200),
email	varchar(500),
username	varchar(500),
password	varchar(500),
district	varchar(20),
city_name	varchar(50),
postal_code	varchar(10),
phone	varchar(20),
country_name	varchar(50),
active	boolean);

CREATE OR REPLACE TABLE sakila_anl.store_dim (
store_id	int PRIMARY KEY,
manager_firstname	varchar(45),
manager_lastname	varchar(45),
address	varchar(500),
address2	varchar(500),
district	varchar(20),
city_name	varchar(50),
postal_code	varchar(10),
phone	varchar(20),
country_name	varchar(50));

CREATE OR REPLACE TABLE sakila_anl.film_dim (
film_id	int PRIMARY KEY,
title	varchar(500),
description	 TEXT,
released_year	int,
language	varchar(20),
original_language	varchar(20),
rental_duration	 int,
rental_rate	NUMERIC,
length	 int,
replace_cost	NUMERIC,
rating	varchar(100),
special_features	varchar(100),
actor_first_name	varchar(45),
actor_last_name	varchar(45),
category_name	varchar(45));

CREATE OR REPLACE TABLE sakila_anl.calendar_dim (
cal_dt	date PRIMARY KEY,
day_of_wk_num	 int,
day_of_wk_desc	varchar(30),
yr_num	integer,
wk_num	integer,
yr_wk_num	integer,
mnth_num	integer,
yr_mnth_num	integer);

CREATE OR REPLACE TABLE sakila_anl.TRANSACTION (
trans_dt	date REFERENCES sakila_anl.calendar_dim(cal_dt),
customer_id	int REFERENCES sakila_anl.customer_dim(customer_id),
staff_id	int REFERENCES sakila_anl.staff_dim(staff_id),
store_id	int REFERENCES sakila_anl.store_dim(store_id),
film_id	int REFERENCES sakila_anl.film_dim(film_id),
amount	NUMERIC,
is_decline	boolean);
