from common import connect_psql, get_file_seq, query_matview
from datetime import datetime, timedelta
import os
import traceback

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
    'store_code',
    'receipt_date',
    'receipt_time',
    'transaction_type',
    'pos_id',
    'receipt_no',
    'line_number',
    'media_member_code',
    'media_member_desc',
    'tendor_amt',
    'credit_cardno'
  ],
  'installment': [
    'store_code',
    'receipt_date',
    'receipt_time',
    'transaction_type',
    'pos_id',
    'receipt_no',
    'vendor_id',
    'brand_id',
    'dept_id',
    'subdept_id',
    'sku',
    'tendor_type',
    'installment_period',
    'credit_cardno',
    'interest_rate',
    'tendor_amt'
  ],
  'dcpn': [
    'store_code',
    'receipt_date',
    'receipt_time',
    'transaction_type',
    'pos_id',
    'receipt_no',
    'coupon_id'
  ],
}


def prepare_data(data, fields, str_date):
  result = []
  for field in fields:
    result.append(data[field])
  if str_date:
    result.append(str_date)

  return "|".join(result)


def generate_trans_sale_detail(output_path, str_date, str_time, str_stime,
                               store, data):
  prefix = 'BISSP_' + store + '_Sales_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['sale_detail'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{}|{}|{}'.format(str_date, str_stime, len(result)))
    print('[AutoPOS] - BI SSP transaction sale detail create files completed..')
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def generate_trans_tendor_detail(output_path, str_date, str_time, str_stime,
                                 store, data):
  prefix = 'BISSP_' + store + '_Tendor_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['tendor_detail'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{}|{}|{}'.format(str_date, str_stime, len(result)))
    print('[AutoPOS] - BI SSP transaction tendor detail create files completed..')
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def generate_trans_installment(output_path, str_date, str_time, str_stime,
                               store, data):
  prefix = 'BISSP_' + store + '_Installment_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['tendor_detail'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{}|{}|{}'.format(str_date, str_stime, len(result)))
    print('[AutoPOS] - BI SSP transaction installment create files completed..')
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def generate_trans_dcpn(output_path, str_date, str_time, str_stime, store,
                        data):
  prefix = 'BISSP_' + store + '_DCPN_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['tendor_detail'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{}|{}|{}'.format(str_date, str_stime, len(result)))
    print('[AutoPOS] - BI SSP transaction dpcn create files completed..')
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def main():
  now = datetime.now()
  str_date = (now - timedelta(days=1)).strftime('%Y%m%d')
  str_time = (now - timedelta(days=1)).strftime('%H%M')
  str_stime = (now - timedelta(days=1)).strftime('%H%M%S')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/bissp',
                             str_date + str_time)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_sale_detail"
    sql = "select store_code from mv_autopos_bi_ssp_trans_sale_detail group by store_code"
    data = query_matview(refresh_view, sql)
    stores = [x['store_code'] for x in data]
    for store in stores:
      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_sale_detail"
      sql = "select * from mv_autopos_bi_ssp_trans_sale_detail where store_code = '{}' and interface_date = '{}'".format(store, str_date)
      data = query_matview(refresh_view, sql)
      generate_trans_sale_detail(target_path, str_date, str_time, str_stime, str_stime, store, data)

      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_tendor_detail"
      sql = "select * from mv_autopos_bi_ssp_trans_tendor_detail where store_code = '{}' and interface_date = '{}'".format(store, str_date)
      data = query_matview(refresh_view, sql)
      generate_trans_tendor_detail(target_path, str_date, str_time, str_stime, store, data)

      refresh_view = "refresh materialized view mv_autopos_bi_ssp_trans_installment"
      sql = "select * from mv_autopos_bi_ssp_trans_installment where store_code = '{}' and interface_date = '{}'".format(store, str_date)
      data = query_matview(refresh_view, sql)
      generate_trans_installment(target_path, str_date, str_time, str_stime, store, data)

      refresh_view = "refresh materialized view mv_autopos_bi_cds_trans_payment"
      sql = "select * from mv_autopos_bi_ssp_trans_dpcn where store_code = '{}' and interface_date = '{}'".format(store, str_date)
      data = query_matview(refresh_view, sql)
      generate_trans_dcpn(target_path, str_date, str_time, str_stime, store, data)
  except Exception as e:
    print('[AutoPOS] - BI SSP Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
