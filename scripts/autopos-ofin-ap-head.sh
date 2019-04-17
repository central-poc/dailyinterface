#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS OFIN AP Head with env: $1"
python src/autopos_ofin_ap_head.py $1
exit_code=$?
echo "===== END"
exit "$exit_code"
