USE DATABASE walmart_dev;

--------1) set latest date as a variable
SET LAST_DATE = (SELECT MAX(cal_dt) FROM waletp.DAILY_INVENTORY);
SELECT $LAST_DATE;

--------2) use the variable in the variable to filter the raw data records. But in order avoid possibility of imcomplete records of the latest date,
-----------we need to delete the original latest date in DAILY_INVENTORY and append the new records from that date.

DELETE FROM waletp.DAILY_INVENTORY WHERE cal_dt=$LAST_DATE;

select max(cal_dt) from waletp.DAILY_INVENTORY;


INSERT INTO waletp.daily_inventory
(	
cal_dt,
store_key,
prod_key,
stock_on_hand_qty,
ordered_stock,
out_of_stock_flg,
waste_qty,
promotion_flg,
next_delivery_dt 
)
SELECT 
cal_dt   AS cal_dt,
store_key   AS store_key,
prod_key   AS prod_key,
inventory_on_hand_qty   AS stock_on_hand_qty,
inventory_on_order_qty      AS ordered_stock,
out_of_stock_flg   AS out_of_stock_flg,
waste_qty   AS waste_qty,
promotion_flg   AS promotion_flg,
next_delivery_dt   AS next_delivery_dt
FROM wallnd.inventory
WHERE cal_dt>=NVL($LAST_DATE, '1900-01-01');