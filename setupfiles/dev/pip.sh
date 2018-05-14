sudo apt-get update
sudo apt-get install python3-pip
pip3 install --upgrade pip
pip3 install pyspark
pip3 install elasticsearch
pip3 install awscli
# install boto on all servers
pip3 install boto3
pip3 install boto
# on all servers
sudo apt install awscli
aws configure
# hadoop-elasticsearch
cd ~/Downloads
wget https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-6.2.4.zip
unzip elasticsearch-hadoop-6.2.4.zip 
cp ~/Downloads/elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar ~/insight/jars
