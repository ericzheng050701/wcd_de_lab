## Step 1. Create Virtual machine and Storage Container.
## Step 2. In the Storage Container create access token.
## Step 3. In the VM, download the Azcopy with below command. We use AzCopy to copy file from VM to storage containers.
```
wget https://s3.amazonaws.com/weclouddata/data/data/install-AzCopy.sh
```
```
chmod 400 install-AzCopy.sh
```
```
bash install-AzCopy.sh
```
Test azcopy by typing `azcopy` to see if the command exists.

### Step 4. Test the azcopy command by the following commands:
```
touch file1.txt
```
```
azcopy copy "file1.txt" "<your access token>"  --recursive=true
```
Go to check your storage container to see if the file has been uploaded into the container.

### Step 5. In VM, set a virtual environment:
```
sudo apt-get install software-properties-common
```
```
sudo apt-add-repository universe
```
```
sudo apt-get update
```
```
sudo apt install python3-virtualenv -y
```
```
virtualenv --python="/usr/bin/python3.8" sandbox
```
```
source sandbox/bin/activate
```
You will create a virtual enviroment and activated it.

### Step 6. Install the requirements file, here we use a file called `az_vm_requirements.txt` with command:
```
pip install -r az_vm_requirements.txt
```

### Step 7. Now let's run the `main.py` file:
```
python3 main.py
```

The file jobs2.csv should be uploaded into the storage container. 

Don't forget to turn off your VM. 


