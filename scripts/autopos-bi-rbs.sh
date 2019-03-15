#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS with env: $1"
python src/autopos_bi_rbs.py $1
echo "===== END"
