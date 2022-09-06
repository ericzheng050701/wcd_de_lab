aws emr create-cluster \
 --service-role EMR_DefaultRole \
 --release-label emr-5.36.0 \
 --name EMR_Lab \
 --applications Name=Spark \
 --ec2-attributes KeyName=demo1,InstanceProfile=EMR_EC2_DefaultRole \
 --instance-groups InstanceType=m5.xlarge,InstanceGroupType=MASTER,InstanceCount=1 InstanceType=m5.xlarge,InstanceGroupType=CORE,InstanceCount=2 \
 --region us-east-1 \
 --managed-scaling-policy ComputeLimits='{MinimumCapacityUnits=2,MaximumCapacityUnits=4,UnitType=Instances}' \
 --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://de-exercise-data-bucket/scripts/pyspark_emr_full_script.py","--output_path","s3://de-exercise-data-bucket/emrlab_output.parquet"],"Type":"CUSTOM_JAR","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]'
