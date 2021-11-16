#!/bin/bash
cd /tmp
sudo yum install -y https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/linux_amd64/amazon-ssm-agent.rpm
sudo systemctl enable amazon-ssm-agent
sudo systemctl start amazon-ssm-agent
sudo yum install python3 -y
sudo echo 'alias python=python3' >> ~/.bashrc
sudo source ~/.bashrc
sudo pip install -U awscli
sudo python3 -m pip install boto3
sudo python3 -m pip install pypandoc
sudo python3 -m pip install requests
sudo python3 -m pip install pydeequ
sudo python3 -m pip install configparser
sudo python3 -m pip install multiprocessing
sudo yum install dos2unix -y
sudo mkdir /home/hadoop/pydeequ
sudo aws s3 cp s3://gd-gopaysecure-prod-gopay-scripts/enterprise_data/sellbrite/scripts/clean/ /home/hadoop/ --recursive
sudo aws s3 cp s3://gd-gopaysecure-prod-gopay-scripts/enterprise_data/sellbrite/scripts/pydeequ/  /home/hadoop/pydeequ/ --recursive
sudo dos2unix /home/hadoop/*
sudo dos2unix /home/hadoop/pydeequ/*