# you can make this layer in Cloudshell

# Open Cloudshell
# create layer
mkdir -p lambda_layers/python/lib/python3.9/site-packages
python3 -m venv venv
source venv/bin/activate
# 1) install the dependencies in the desired folder
# You will realize that we are not using requirements.txt file here and pip3 install command is also using multiple flags. This is because lambda backend containers are using older packages which makes it difficult to install it using other methods.
# Please review this link (https://stackoverflow.com/questions/75472308/aws-lambda-returns-lib64-libc-so-6-version-glibc-2-28-not-found) for more information.
pip3 install --platform manylinux2010_x86_64 --implementation cp --python 3.9 --only-binary=:all: --upgrade --target lambda_layers/python/lib/python3.9/site-packages snowflake-connector-python==2.7.9
# 2) Zip the lambda_layers folder
cd lambda_layers
zip -r snowflake_lambda_layer.zip *
# 3) publish layer
aws lambda publish-layer-version \
    --layer-name fl-snowflake-lambda-layer \
    --compatible-runtimes python3.9 \
    --zip-file fileb://snowflake_lambda_layer.zip
