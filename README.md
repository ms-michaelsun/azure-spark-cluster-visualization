# Japan Visa Batch Analysis Pipeline with Azure VM, Spark cluster (docker containers) and visualization with Plotly (python).

## Overview
This project analyzes Japan's visa issuance dataset using Apache Spark for distributed data processing on an Azure Virtual Machine. The pipeline includes data cleaning, transformation, and visualization generation through HTML reports. The analysis provides insights into visa issuance patterns, trends, and demographics. The Spark cluster is containerized using Docker for easy deployment and scaling.

## Architecture


### Infrastructure
- **Azure VM**: Primary compute instance
  - Size: Standard D2s v3 (2 vcpus, 8 GiB memory)
  - Region: North Europe (Zone 1)
  - OS: ubuntu-24_04-lts

- **Containerized Spark Cluster**
  - 1 Spark Master Container
  - 4 Spark Worker Containers
  - Docker network for inter-container communication

### Data Source
- Dataset: Japan Visa Issuance Records
- Source: Kaggle.com
- Format: CSV

## Running the Pipeline

1. Create an Azure VM and download the private key (eg. azure-spark-cluster.pem)

2. Move the private key to project folder and give it private permission.

```bash
chmod 400 spark-cluster-key.pem
```

3. Get the VM ip address to set up the connection.

4. Connect to the VM using SSH.

```bash
ssh -X azureuser@xx.xx.xxx.xx -i spark-cluster-key.pem
```

5. Install docker and docker compose in the VM

```bash
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```
```bash
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

6. Setup the Spark cluster with the docker compose file by uplading the the VM.

7. With the Spark cluster set up and running, we can trigger the spark job using:
```bash
sudo docker exec -it azureuser-spark-worker-1 spark-submit --master spark://172.18.0.2:7077 jobs/visualization.py
```

8. After processing we can use the "download_file.sh" to download the finished html visualization files to our output directory.

