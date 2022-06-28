#!/usr/bin python3
"""
This is the main file for the lambda project. It will be used in EC2 IAM role situation
"""

import sqlalchemy as db
import pandas as pd
import subprocess

engine = db.create_engine('mysql+mysqlconnector://root:root@34.200.90.230:3306/superstore')

sql="""
select customerID, sum(sales) sum_sales
from orders
group by 1
order by 2 desc
limit 10;
"""
df = pd.read_sql(sql, con = engine)
df[["customerID"]].head()
df[["customerID"]].to_json('data/cus_id.json')

'jobs2.csv', 's3://de-exercise-data-bucket/input/job2.csv'

subprocess.run(["aws", "s3", "cp", "./data/cus_id.json", "s3://de-exercise-data-bucket/input/cus_id.json"])
# 
