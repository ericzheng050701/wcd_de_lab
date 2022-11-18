![airflow_lab_diagram](https://user-images.githubusercontent.com/62180522/202745575-a43c6e52-f818-40a8-90a5-aeb58ec3cb83.png)

## Intro
The project including several steps:
Step 1. Save query result from RDS to a S3 folder.
Step 2. Save the query result of average order amount to Airflow XCOM.
Step 3. Fetch the file from S3 and value of average order amount from Airflow XCOM, and tranform in EMR and save the result to a new S3 folder. 
Step 4. the new S3 folder should Integrate with Snowflake by S3_Integration, then we use Copy Command in Snowflake to copy the file into Snowflake.


## Step 1: 
  - 1) We first create a S3 folder to store the result from RDS; The bucket and the folder name and the folder and file name should be saved into **Variable** in Airflow.
  - 2)  We create a connection to AWS and store the connection id into Airflow. In the demo, the conn_id is called "aws_conn".
  - 3) We create a connection to RDS and store the connection id into Airflow. In the demo, the conn_id is called "mysql_rds_ariflowlab".
  - 4) We create the below query called "sql_orderAmount", this query is used to feltch the result of **order_number + order_date + order_amount**:
      ![2022-11-18 11_14_36-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202750844-14736eb1-8170-4030-b9f9-a646537fc0d2.jpg)
  - 5) Then we run above query with **t1 = SqlToS3Operator**. This is the Operator specifically design for fetching result from databases like mysql, postgres to S3. In the operator, **sql_conn_id** is the connection to RDS which we've just created in step iii, the **aws_conn_id** is the connection to AWS which we've just created in step ii. **query** is the query "sql_orderAmount" we create in step iiii. **s3_key** is the folder name in S3. We stored this S3 Key in Airflow **Variable** in step i.
![2022-11-18 11_21_07-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202752633-14d1c3fd-a5c8-4fae-8843-6d88c0101d35.jpg)



