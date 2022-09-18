
USE DATABASE wcd_lab;


----- populate dimension tables.
TRUNCATE TABLE IF EXISTS sakila_anl.customer_dim;
INSERT INTO sakila_anl.customer_dim (
	customer_id,
	first_name,
	last_name,
	email,
	create_date,
	address,
	address2,
	district,
	city_name,
	postal_code,
	phone,
	coutry_name,
	active)
SELECT 
	c.customer_id,
	c.first_name,
	c.last_name,
	c.email,
	c.create_date,
	a.address,
	a.address2,
	a.district,
	ct.city AS city_name,
	a.postal_code,
	a.phone,
	cn.country AS country_name,
	c.active
FROM sakila.customer c
LEFT JOIN sakila.address a USING (address_id)
LEFT JOIN sakila.city ct USING (city_id)
LEFT JOIN sakila.country cn USING (country_id);


TRUNCATE TABLE IF EXISTS sakila_anl.staff_dim;
INSERT INTO sakila_anl.staff_dim (
	staff_id,
	first_name,
	last_name,
	address,
	address2,
	picture,
	email,
	username,
	password,
	district,
	city_name,
	postal_code,
	phone,
	country_name,
	active)
SELECT 
	s.staff_id,
	s.first_name,
	s.last_name,
	a.address,
	a.address2,
	s.picture,
	s.email,
	s.username,
	s.password,
	a.district,
	ct.city AS city_name,
	a.postal_code,
	a.phone,
	cn.country AS country_name,
	s.active
FROM sakila.staff s
LEFT JOIN sakila.address a USING (address_id)
LEFT JOIN sakila.city ct USING (city_id)
LEFT JOIN sakila.country cn USING (country_id);


TRUNCATE TABLE IF EXISTS sakila_anl.store_dim;
INSERT INTO sakila_anl.store_dim (
	store_id,
	manager_firstname,
	manager_lastname,
	address,
	address2,
	district,
	city_name,
	postal_code,
	phone,
	country_name)
SELECT 
	s.store_id,
	st.first_name,
	st.last_name,
	a.address,
	a.address2,
	a.district,
	ct.city AS city_name,
	a.postal_code,
	a.phone,
	cn.country AS country_name
FROM sakila.store s
LEFT JOIN sakila.staff st ON s.manager_staff_id=st.staff_id
LEFT JOIN sakila.address a ON s.address_id=a.address_id
LEFT JOIN sakila.city ct USING (city_id)
LEFT JOIN sakila.country cn USING (country_id);


TRUNCATE TABLE IF EXISTS sakila_anl.film_dim;
INSERT INTO sakila_anl.film_dim (
	film_id,
	title,
	description,
	released_year,
	LANGUAGE,
	original_language,
	rental_duration,
	rental_rate,
	LENGTH,
	replace_cost,
	rating,
	special_features,
	actor_first_name,
	actor_last_name,
	category_name)
SELECT 
	f.film_id,
	f.title,
	f.description,
	f.release_year AS released_year,
	l.NAME AS LANGUAGE,
	ll.NAME AS original_language,
	f.rental_duration,
	f.rental_rate,
	f.LENGTH,
	f.replacement_cost AS replace_cost,
	f.rating,
	f.special_features,
	a.first_name AS actor_first_name,
    a.last_name AS actor_last_name,
    c.NAME AS category_name
FROM sakila.film f
LEFT JOIN sakila.LANGUAGE l USING (language_id)
LEFT JOIN sakila.LANGUAGE ll USING (language_id)
LEFT JOIN sakila.film_actor fa USING (film_id)
LEFT JOIN sakila.actor a ON fa.actor_id=a.actor_id
LEFT JOIN sakila.film_category fc USING (film_id)
LEFT JOIN sakila.category c ON fc.category_id=c.category_id;


----Populate the fact table
-------step 1. we first join the payment, inventory and retal tables to create a base transient table

CREATE OR REPLACE TRANSIENT TABLE sakila_anl.trans_base_stg AS  
SELECT
	p.payment_date AS trans_dt,
	p.customer_id,
	p.staff_id,
	i.store_id,
	i.film_id,
	p.amount
FROM sakila.payment p 
JOIN sakila.rental r USING (rental_id, customer_id, staff_id)
JOIN sakila.inventory i ON r.inventory_id = i.inventory_id;

---- step 2. generate the is_decline column
-------------The steps are:
------------------- 1. find the latest date of the transaction
------------------- 2. get he last 4 weeks, the week number and date range
--------------------3. use the 4 weeks to filter the transactions, and sum the sales amount group by store by week
--------------------4. if the week amount less than the previous amount, we will label as 1 otherwise 0, if the sum of the label is 3, then it is_decline.


----- find the latest date of the transaction
SET max_dt = (SELECT max(payment_date) FROM sakila.payment);


----- get the latest 4 weeks number and date range to create a transient table
CREATE OR REPLACE TRANSIENT TABLE sakila_anl.last_4_wk_stg AS 
SELECT 
	c.cal_dt,
	c.yr_wk_num
FROM sakila_anl.calendar_dim c
JOIN 
		(SELECT 
			yr_wk_num
		FROM sakila_anl.calendar_dim
		WHERE cal_dt <=$max_dt
		GROUP BY yr_wk_num
		ORDER BY yr_wk_num desc
		LIMIT 4) USING (yr_wk_num)
;

----build a week + store framework. The reason why we cross join week and store, is because we want to create a framework to make sure the all weeks and stores
----are listed no matter there are any transction or not in that date in the payment table.
CREATE OR REPLACE TRANSIENT TABLE sakila_anl.last_4_wk_store_stg AS 
SELECT 
	w.yr_wk_num,
	s.store_id,
	w.cal_dt
FROM sakila_anl.last_4_wk_stg w
CROSS JOIN sakila_anl.store_dim s;



----------- filter out the sum transaction with the latest 4 weeks
CREATE OR REPLACE TRANSIENT TABLE sakila_anl.last_4_wk_trans_stg AS 
SELECT 
	w.store_id,
	w.yr_wk_num,
	nvl(sum(t.amount),0) AS wk_amount
FROM 
sakila_anl.last_4_wk_store_stg w 
LEFT JOIN sakila_anl.trans_base_stg T  ON T.trans_dt=w.cal_dt
GROUP BY 1,2
ORDER BY 1,2;


----------- find out if the late week amount less than that in previous week
------------in the subquery there are several steps:
 ------------- 1)  based on the "last_4_wk_trans_stg" table create a new column 'the last wk_amount' 
------------- 2)   make the wk amount minus last wk amount, if negtive then we label it as -1, this will be a new table called 'wk_decline'
------------- 3)  sum total 'wk_decline' of a store, if it is -3, it means last 3 weeks all less than the later week, this means totally 4 weeks decline, so we can label
----------------- the is_decline column true.

CREATE OR REPLACE TRANSIENT TABLE sakila_anl.last_4_wk_decline AS 
SELECT 
		store_id,
		CASE WHEN sum_decline=-3 THEN TRUE ELSE FALSE END AS is_decline 
FROM 
           (select  store_id, sum(wk_decline) as sum_decline
		    from  
		             (SELECT *, CASE WHEN wk_amount - last_wk_amount<=0 THEN -1 ELSE 0 END AS wk_decline
				       FROM  
				        		(select  *, lead(wk_amount) over (partition by store_id order by yr_wk_num) as last_wk_amount 
				        		 from sakila_anl.last_4_wk_trans_stg)
				       WHERE last_wk_amount IS NOT NULL)
		  group by 1)
;

------- finally we join this column with the sakila_anl.trans_base_stg table, to create the final transcaction transient table.
CREATE OR REPLACE TRANSIENT TABLE sakila_anl.transaction_stg AS 
SELECT T.*,
			  w.is_decline
FROM sakila_anl.trans_base_stg t
JOIN sakila_anl.last_4_wk_decline w USING (store_id);

--------- replace the current transaction table with new transaction transient table
TRUNCATE TABLE IF EXISTS sakila_anl.transaction;
INSERT INTO sakila_anl.transaction (
		trans_dt,
		customer_id,
		staff_id,
		store_id,
		film_id,
		amount,
		is_decline)
SELECT 
		trans_dt,
		customer_id,
		staff_id,
		store_id,
		film_id,
		amount,
		is_decline
FROM sakila_anl.transaction_stg;



	
