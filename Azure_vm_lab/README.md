Step 1. Create Virtual machine and Storage Container.
Step 2. In the Storage Container create access token.
Step 3. In the VM, download the Azcopy with below command. We use AzCopy to copy file from VM to storage containers.
```
wget https://s3.amazonaws.com/weclouddata/data/data/install-AzCopy.sh
```
```
chmod 400 install-AzCopy.sh
```
```
bash install-AzCopy.sh
```


