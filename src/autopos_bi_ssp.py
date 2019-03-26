from common import config, connect_psql, get_file_seq, insert_transaction, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback

text_format = {
    'sale_detail': [
        'store_code',
        'receipt_date',
        'receipt_time',
        'transaction_type',
        'pos_id',
        'receipt_no',
        'line_number',
        'sku',
        'quantity',
        'price_before_override',
        'price_after_override',
        'price_override_flag',
        'total_sale',
        'vat_rate',
        'vat_amt',
        'discount_code1',
        'discount_amt1',
        'discoune_flag1',
        'discount_code2',
        'discount_amt2',
        'discoune_flag2',
        'discount_code3',
        'discount_amt3',
        'discoune_flag3',
        'discount_code4',
        'discount_amt4',
        'discoune_flag4',
        'discount_code5',
        'discount_amt5',
        'discoune_flag5',
        'discount_code6',
        'discount_amt6',
        'discoune_flag6',
        'discount_code7',
        'discount_amt7',
        'discoune_flag7',
        'discount_code8',
        'discount_amt8',
        'discoune_flag8',
        'discount_code9',
        'discount_amt9',
        'discoune_flag9',
        'discount_code10',
        'discount_amt10',
        'discoune_flag10',
        'ref_receipt_no',
        'ref_date',
        'return_reason_code',
        'dept_id',
        'subdept_id',
        'class_id',
        'subclass_id',
        'vendor_id',
        'brand_id',
        'itemized',
        'dtype',
        'member_id',
        'cashier_id',
        'sale_id',
        'guide_id',
        'last_modify_date',
    ],
    'tendor_detail': [
        'store_code', 'receipt_date', 'receipt_time', 'transaction_type',
        'pos_id', 'receipt_no', 'line_number', 'media_member_code',
        'media_member_desc', 'tendor_amt', 'credit_cardno'
    ],
    'installment': [
        'store_code', 'receipt_date', 'receipt_time', 'transaction_type',
        'pos_id', 'receipt_no', 'vendor_id', 'brand_id', 'dept_id',
        'subdept_id', 'sku', 'tendor_type', 'installment_period',
        'credit_cardno', 'interest_rate', 'tendor_amt'
    ],
    'dcpn': [
        'store_code', 'receipt_date', 'receipt_time', 'transaction_type',
        'pos_id', 'receipt_no', 'coupon_id'
    ],
}


def prepare_data(data, fields, str_date):
  result = []
  for field in fields:
    result.append(data[field])
  if str_date:
    result.append(str_date)

  return "|".join(result)


def generate_trans_sale_detail(output_path, str_date, store, data):
  prefix = 'BISSP_' + store + '_Sales_' + str_date[:12] + "_"
  seq = get_file_seq(prefix, output_path, '.DATA')
  file_name = '{}{:0>2}.DATA'.format(prefix, seq)
  file_fullpath = os.path.join(output_path, file_name)
  log_name = '{}{:0>2}.LOG'.format(prefix, seq)
  log_fullpath = os.path.join(output_path, log_name)
  result = [
      prepare_data(d, text_format['sale_detail'], '') for d in data
  ]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    f.write("\n")
    l.write('{}|{}|{}'.format(str_date[:8], str_date[-6:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI SSP[{}] transaction sale detail create files completed..'.format(store))
  return [file_name, log_name]


def generate_trans_tendor_detail(output_path, str_date, store, data):
  prefix = 'BISSP_' + store + '_Tendor_' + str_date[:12] + "_"
  seq = get_file_seq(prefix, output_path, '.DATA')
  file_name = '{}{:0>2}.DATA'.format(prefix, seq)
  file_fullpath = os.path.join(output_path, file_name)
  log_name = '{}{:0>2}.LOG'.format(prefix, seq)
  log_fullpath = os.path.join(output_path, log_name)
  result = [
      prepare_data(d, text_format['tendor_detail'], str_date[:8]) for d in data
  ]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    f.write("\n")
    l.write('{}|{}|{}'.format(str_date[:8], str_date[-6:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI SSP[{}] transaction tendor detail create files completed..'.format(store))
  return [file_name, log_name]


def generate_trans_installment(output_path, str_date, store, data):
  prefix = 'BISSP_' + store + '_Installment_' + str_date[:12] + "_"
  seq = get_file_seq(prefix, output_path, '.DATA')
  file_name = '{}{:0>2}.DATA'.format(prefix, seq)
  file_fullpath = os.path.join(output_path, file_name)
  log_name = '{}{:0>2}.LOG'.format(prefix, seq)
  log_fullpath = os.path.join(output_path, log_name)
  result = [
      prepare_data(d, text_format['installment'], str_date[:8]) for d in data
  ]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    f.write("\n")
    l.write('{}|{}|{}'.format(str_date[:8], str_date[-6:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI SSP[{}] transaction installment create files completed..'.format(store))
  return [file_name, log_name]


def generate_trans_dcpn(output_path, str_date, store, data):
  prefix = 'BISSP_' + store + '_DCPN_' + str_date[:12] + "_"
  seq = get_file_seq(prefix, output_path, '.DATA')
  file_name = '{}{:0>2}.DATA'.format(prefix, seq)
  file_fullpath = os.path.join(output_path, file_name)
  log_name = '{}{:0>2}.LOG'.format(prefix, seq)
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['dcpn'], str_date[:8]) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    f.write("\n")
    l.write('{}|{}|{}'.format(str_date[:8], str_date[-6:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI SSP[{}] transaction dpcn create files completed..'.format(store))
  return [file_name, log_name]


def main():
  env = sys.argv[1] if len(sys.argv) > 1 else 'local'
  print("\n===== Start Siebel [{}] =====".format(env))
  cfg = config(env)
  now = datetime.now()
  query_date = cfg['run_date'] if cfg['run_date'] else (now - timedelta(days=1)).strftime('%Y%m%d')
  str_date = cfg['bissp_date'] if cfg['bissp_date'] else now.strftime('%Y%m%d%H%M%S')
  
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path_tendor = os.path.join(parent_path, 'output/autopos/{}/bissp/tendor'.format(env), str_date)
  if not os.path.exists(target_path_tendor):
    os.makedirs(target_path_tendor)
  target_path_sale = os.path.join(parent_path, 'output/autopos/{}/bissp/sale'.format(env), str_date)
  if not os.path.exists(target_path_sale):
    os.makedirs(target_path_sale)
  target_path_installment = os.path.join(parent_path, 'output/autopos/{}/bissp/installment'.format(env), str_date)
  if not os.path.exists(target_path_installment):
    os.makedirs(target_path_installment)
  target_path_dcpn = os.path.join(parent_path, 'output/autopos/{}/bissp/dcpn'.format(env), str_date)
  if not os.path.exists(target_path_dcpn):
    os.makedirs(target_path_dcpn)

  try:
    dbfms = cfg['fms']
    stores = [
        x['store_code']
        for x in query_all(dbfms, 
            "select store_code from businessunit where businessunit_code = 'SSP' and status = 'AT' group by store_code"
        )
    ]
    for store in stores:
      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_sale_detail"
      sql = "select * from mv_autopos_bi_ssp_trans_sale_detail where store_code = '{}' and interface_date = '{}'".format(
          store, query_date)
      data = query_matview(dbfms, refresh_view, sql)
      sale = generate_trans_sale_detail(target_path_sale, str_date, store, data)
      sql_insert = "insert into transaction_bi_ssp_sale_detail {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_tendor_detail"
      sql = "select * from mv_autopos_bi_ssp_trans_tendor_detail where store_code = '{}' and interface_date = '{}'".format(
          store, query_date)
      data = query_matview(dbfms, refresh_view, sql)
      tendor = generate_trans_tendor_detail(target_path_tendor, str_date, store, data)
      sql_insert = "insert into transaction_bi_ssp_tendor_detail {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_installment"
      sql = "select * from mv_autopos_bi_ssp_trans_installment where store_code = '{}' and interface_date = '{}'".format(
          store, query_date)
      data = query_matview(dbfms, refresh_view, sql)
      installment = generate_trans_installment(target_path_installment, str_date, store, data)
      sql_insert = "insert into transaction_bi_ssp_installment {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_dpcn"
      sql = "select * from mv_autopos_bi_ssp_trans_dpcn where store_code = '{}' and interface_date = '{}'".format(
          store, query_date)
      data = query_matview(dbfms, refresh_view, sql)
      dcpn = generate_trans_dcpn(target_path_dcpn, str_date, store, data)
      sql_insert = "insert into transaction_bi_ssp_dpcn {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      if cfg['ftp']['is_enable']:
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_tendor, 'incoming/bissp/tendor', tendor)
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_sale, 'incoming/bissp/sale', sale)
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_installment,'incoming/bissp/installment', installment)
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_dcpn, 'incoming/bissp/dcpn', dcpn)
  except Exception as e:
    print('[RBS AutoPOS] - BI SSP Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
