1. set all secrets in .env
2. set all config in config.toml
3. run **init.sh** to create a virtual environment
4. the python script enbeded in **run.sh**, and use **run.sh**  to control job
5. there are two versions of python script:
    - run_v1.py: A regular version using boto3 and aws secrets, the version can be used in any platform any condition.
    - run_v2.py: This is a version for srcipts running on AWS EC2. Because we grand the EC2 a role to access the S3, so no secrets and boto3 required. 

## Instruction
- The run script most copy the template from the linux lab
- some parameter need for run.sh also come from config.toml