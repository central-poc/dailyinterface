#!/bin/bash
cd /opt/dailyinterface
source env/bin/activate
echo "===== Starting generate text AutoPOS OFIN CGO with env: $1"
python src/autopos_ofin_gl_zn_cgo.py $1
python src/autopos_ofin_ap_head_cgo.py $1
python src/autopos_ofin_ap_line_cgo.py $1
echo "===== END"
