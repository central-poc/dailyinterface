#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS with env: $1"
python src/autopos_jda_57002.py $1
exit_code=$?
echo "===== END"
exit "$exit_code"
