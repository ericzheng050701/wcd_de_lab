#!/usr/bin python3
"""
This is the main file for the py_cloud project. It can be used at any situation
"""

import sqlalchemy as db
import pandas as pd
engine = db.create_engine('mysql+mysqlconnector://root:root@34.200.90.230:3306/superstore')

sql="""
select customerID, sum(sales) sum_sales
from orders
group by 1
order by 2 desc
limit 10;
"""
df = pd.read_sql(sql, con = engine)
# 
print(df[["customerID"]].head())
