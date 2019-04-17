#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS OFIN GL ZN with env: $1"
python src/autopos_ofin_gl_zn_cgo.py $1
exit_code=$?
echo "===== END"
exit "$exit_code"
