#!/usr/bin/env bash

sudo apt-get update
sudo mkdir /usr/local/hadoop
cd /usr/local/hadoop
sudo wget http://download.nextag.com/apache/hadoop/common/hadoop-2.9.0/hadoop-2.9.0.tar.gz
sudo tar xvf hadoop-2.9.0.tar.gz
cd hadoop-2.9.0/etc/hadoop/
sudo apt install awscli
sudo apt install awscli -y
aws s3 sync s3://hadoop-venku/hadoop_config .
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

sudo add-apt-repository ppa:webupd8team/java -y
sudo apt update
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get install oracle-java8-installer -y
export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
export PATH=$PATH:$JAVA_HOME/bin
source ~/.profile


cd /usr/local/hadoop/hadoop-2.9.0
bin/hdfs namenode -format
sbin/start-dfs.sh

cd /usr/local/hadoop/hadoop-2.9.0/
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/ubuntu
sbin/start-yarn.sh
sbin/mr-jobhistory-daemon.sh start historyserver

cd ~
mkdir mp_jar
cd mp_jar/

aws configure set s3.signature_version s3v4
aws s3 cp s3://hadoop-venku/build/buildfiles/hadoop-map-reduce-1.0-SNAPSHOT.jar .
mkdir ~/mp_jar/data

cd ~/mp_jar/data
aws s3 cp s3://hadoop-venku/data/input.txt .
mkdir ~/mp_jar/output
