#! /bin/bash
cd /opt/dailyinterface
source env/bin/activate
python src/sbl_cgo_product_master_full.py
python src/sbl_ofm__product_master_full.py
