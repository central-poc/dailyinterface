#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS OFIN with env: $1"
python src/autopos_ofin_gl_zn.py $1
python src/autopos_ofin_gl_zy.py $1
python src/autopos_ofin_ap_head.py $1
python src/autopos_ofin_ap_line.py $1
echo "===== END"

