#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS with env: $1"
python src/autopos_jda_20174.py $1
echo "===== END"
