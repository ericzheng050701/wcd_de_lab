USE DATABASE walmart_dev;

TRUNCATE TABLE walsup.calendar_dim;
INSERT INTO walsup.calendar_dim
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
FROM waletp.calendar_dim
;
