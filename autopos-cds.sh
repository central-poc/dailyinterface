#!/bin/bash
echo "===== Starting generate text AutoPOS with date: $1"
cd `pwd`
python src/autopos_ofin_zn.py $1
python src/autopos_ofin_zl.py $1
python src/autopos_ofin_h.py $1
python src/autopos_ofin_l.py $1
python src/autopos_ofin_zy.py $1
python src/autopos_jda.py $1
python src/autopos_siebel.py $1
python src/autopos_bi_cds.py $1
python src/autopos_bi_ssp.py $1
python src/autopos_vendor.py $1
echo "===== END"