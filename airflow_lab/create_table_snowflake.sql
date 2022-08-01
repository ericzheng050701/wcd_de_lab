CREATE DATABASE IF NOT EXISTS AIRFLOW_DEMO;
CREATE SCHEMA IF NOT EXISTS EMR;

create or replace table  orders
(
order_num INT,
order_date DATE,
order_amount numeric(19,2),
big_order boolean
)