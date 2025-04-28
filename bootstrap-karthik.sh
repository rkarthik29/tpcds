#!/bin/bash

#Download spark binary
sudo wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
# Extract the downloaded tarball
python -m venv /opt/spark/venv
# install python libraries

sudo mkdir -p /opt/spark
sudo chown -R 777 /opt/spark
sudo tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt/spark

/opt/spark/venv/bin/pip install pyspark==3.5.1 s3fs
# Remove the tarball after extraction
sudo rm spark-3.5.1-bin-hadoop3.tgz

# install git
sudo yum install -y git

cd /opt
# clone the tpcds demo
sudo git clone https://github.com/rkarthik29/tpcds.git

#sudo aws s3 cp s3://sridp/gpu/gtm-spark-integration/base-jars/gluten-velox-bundle-spark3.5_2.12-amzn_2023_x86_64-1.4.0-SNAPSHOT-nolic.jar /opt/tpcds/jars/
sudo aws s3 cp s3://sridp/gpu/gtm-spark-integration/base-jars/datapelago-gv-bundle-spark3.5_2.12-amzn_2023_x86_64-1.4.0-SNAPSHOT-s3-7apr.jar /opt/tpcds/jars/
#set permissions for the tpcds folder
sudo chmod -R 777 /opt/tpcds
#create an alias for dp-submit to make it easier to run
# source the bashrc to make the changes effective   

#adding the license hack
#echo "DP_LF_564871=dummy" >> sudo /opt/spark/spark-3.5.1-bin-hadoop3/conf/spark-env.sh