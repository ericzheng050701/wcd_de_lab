# 2. Business Requirements 
**Each week**, **on store level**, calculate the following metrics for **each product**:
- **total sales quantity of a product** : Sum(sales_qty)
- **total sales amount of a product** : Sum(sales_amt)
- **Average sales Price**: Sum(sales_amt)/Sum(sales_qty)
- **stock level by then end of the week** : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
- **Store on Order level by then end of the week**: ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week) 
- **Total cost of the week**: Sum(cost_amt)
- **the percentage of Store In-Stock**: (how many times of out_of_stock in a week) / days of a week (7 days)
- **Total Low Stock Impact**: sum (out_of+stock_flg + Low_Stock_flg)
- **Potential Low Stock Impact**: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
- **No Stock Impact**: if out_of_stock_flg=true, then sum(sales_amt)
- **Low Stock Instances**: Caluclate how many times of Low_Stock_Flg in a week
- **No Stock Instances**: Caluclate then how many times of out_of_Stock_Flg in a week
- **how many weeks the on hand stock can supply**: (stock_on_hand_qty at the end of the week) / sum(sales_qty)
