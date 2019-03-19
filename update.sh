#!/bin/bash
cd /home/ubuntu/dailyinterface
ssh-agent bash -c 'ssh-add ~/.ssh/<private-key>.pem; git pull --rebase'
cp -f -R ~/dailyinterface/* /opt/dailyinterface/