import requests
import pandas as pd
import json 
import sqlalchemy as db
from datetime import datetime


cus_id_url='./data/cus_id.json'

li=[]
with open(cus_id_url,'rb') as f:
    data = json.load(f)
    for value in data["customerID"].values():
        li.append(str(value))

id_string = "("+ ",".join(li)+")"



engine = db.create_engine('mysql+mysqlconnector://root:root@34.200.90.230:3306/superstore')

sql=f"""select customerID, CustomerName
      from customers
      where customerID in {id_string} ;"""

result = engine.execute(sql)

records=[]
for row in result:
    doc={"id":row[0], "name":row[1], "date":datetime.today().strftime('%Y-%m-%d')}
    records.append(doc)

file_path = f"./customer_{datetime.today().strftime('%Y%m%d')}.json"
with open(file_path, 'w') as file:
    json.dump(records, file)

with open(file_path, 'r') as f:
    data=json.load(f)




url = "https://virtserver.swaggerhub.com/wcd_de_lab/top10/1.0.0/add"

request = requests.post(url, data=result)

print(request.status_code)
