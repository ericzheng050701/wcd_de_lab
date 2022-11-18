![airflow_lab_diagram](https://user-images.githubusercontent.com/62180522/202745575-a43c6e52-f818-40a8-90a5-aeb58ec3cb83.png)

## Intro
The project including several steps:
Step 1. Save query result from RDS to a S3 folder.
Step 2. Save the query result of average order amount to Airflow XCOM.
Step 3. Fetch the file from S3 and value of average order amount from Airflow XCOM, and tranform in EMR and save the result to a new S3 folder. 
Step 4. the new S3 folder should Integrate with Snowflake by S3_Integration, then we use Copy Command in Snowflake to copy the file into Snowflake.


## Step 1: 
  - 1) We first create a S3 folder to store the result from RDS; In this demo, we we defined the file called "orders_amount.csv" and the file is saved into 'input' folder. So the value of **input/orders_amount.csv** is saved in **Variable** if Airflow.
  - 2)  We create a connection to AWS and store the connection id into Airflow. In the demo, the conn_id is called "aws_conn".
  - 3) We create a connection to RDS and store the connection id into Airflow. In the demo, the conn_id is called "mysql_rds_ariflowlab".
  - 4) We create the below query called "sql_orderAmount", this query is used to feltch the result of **order_number + order_date + order_amount**:
      ![2022-11-18 11_14_36-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202750844-14736eb1-8170-4030-b9f9-a646537fc0d2.jpg)
      ![sales_order_amount_final](https://user-images.githubusercontent.com/62180522/202755636-56273c41-b501-4dcd-817d-136e9f29cee0.jpg)

  - 5) Then we run above query with **t1 = SqlToS3Operator**. This is the Operator specifically design for fetching result from databases like mysql, postgres to S3. In the operator, **sql_conn_id** is the connection to RDS which we've just created in step iii, the **aws_conn_id** is the connection to AWS which we've just created in step ii. **query** is the query "sql_orderAmount" we create in step iiii. The we save the file into S3 folder with name == **s3_key**. In this demo, we we defined the file called "orders_amount.csv" and the file is saved into 'input' folder. So the value of "input/orders_amount.csv" is saved in **Variable** if Airflow in Step i.
  
![2022-11-18 11_21_07-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202752633-14d1c3fd-a5c8-4fae-8843-6d88c0101d35.jpg)


## Step 2: 
- 1) We first create a sql query to fetch the data of average order amount from RDS with name **sql_avgOrderAmount**.
    
![2022-11-18 11_41_38-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202756788-8652f820-24d6-4c33-b8e7-d64ad524e9e7.jpg)

- 2) We create a python function to send the query **sql_avgOrderAmount** to RDS to run. In the function, instead of using operators we use Hook. We use the same RDS conn_id --"mysql_rds_ariflowlab" as **my_conn_id** in **MysqlHook**. Fetch the result and same the result to XCOM key='avg_order_amount' with **kwargs['ti'].xcom_push(key='avg_order_amount', value=data)**. The reason why we save the value in Xcom is because in the next step the EMR will get this value and use it in the pyspark transformation. 
![2022-11-18 11_39_43-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202757080-4caa9847-d15b-4c44-9b8a-a748a1bc80ab.jpg)

## Step 3:
- 1) When the files "input/orders_amount.csv" and value of "avg_order_amount" are ready, we will start to run EMR, with Operator **t3 = EmrAddStepsOperator** and **EmrStepSensor**. 
- 2) **t3 = EmrAddStepsOperator** is the Operator to add the **Steps** into EMR. We first need to define all the parameters defined for the **Steps**. This steps will be used in the EMR. In the code, you need to fill in your own '--input_file_url', '--output_file_url', and the '-avg_order_amount' is the value we create in the STEP 2 and saved in the **Variable** of Airflow.
![2022-11-18 13_04_23-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202774518-80449a22-f924-42ab-8764-adc5a1ce58d7.jpg)

- 3) If you take a look at the pyspark script, the parameters defined in the above step are interpreted in the pyspark code.
![2022-11-18 13_14_31-wcd_de_lab_pyspark py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202774706-0697bc62-9e99-4693-89c2-493ec1a42f37.jpg)

- 4) Then we add the **SAPRK_STEPS** into Operator **t3 = EmrAddStepsOperator**. In the operator we also need to input the AWS connection which we defined in the previous step. The task_id for this step is 'add_emr_steps', we will use it in the next step.You also need to input the EMR Cluser_id which you created intially.
 ![2022-11-18 13_21_52-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202776326-69c44d71-c13d-4fb7-baaa-63e3be12dd68.jpg)

- 5) **t4 = EmrStepSensor** this step is the real step to run EMR. In this step, you need to input the EMR Cluser_id which you created initially. and also we also need to connect AWS with **aws_conn_id** which you define in the first Step.  You can write the **step_id** just like this **step_id = "{{ task_instance.xcom_pull('add_emr_steps', key='return_value')[0] }}"** this means you need to fetch the STEP ID from the task_id= 'add_emr_steps' which is defined in our last step. 
![2022-11-18 13_16_58-wcd_de_lab_dag py at master · ericzheng050701_wcd_de_lab](https://user-images.githubusercontent.com/62180522/202775022-8660a40d-7005-465e-964c-f0a8486ac460.jpg)




