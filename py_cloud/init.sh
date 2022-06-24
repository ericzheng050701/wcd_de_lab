# Create a virtual environment
python3 -m venv sandbox  
source sandbox/bin/activate 

# Install dependencies
pip install -r requirements.txt

deactivate # deactivate your sandbox

chmod a+x run.sh # make run.sh executable

mkdir -p log # create log directory if it doesn't exist