mkdir my-lambda-layer && cd my-lambda-layer
mkdir -p aws-layer/python/lib/python3.8/site-packages
pip3 install -r requirements.txt --target aws-layer/python/lib/python3.8/site-packages
cd aws-layer
zip -r9 lambda-layer.zip .
