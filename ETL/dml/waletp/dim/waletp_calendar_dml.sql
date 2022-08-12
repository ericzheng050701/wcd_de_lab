USE DATABASE walmart_dev;

TRUNCATE TABLE waletp.calendar_dim;
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