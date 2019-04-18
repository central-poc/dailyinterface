#! /bin/bash
cd /opt/dailyinterface
source env/bin/activate
python src/sbl_cgo_product_master_inc.py
python src/sbl_ofm_product_master_inc.py
