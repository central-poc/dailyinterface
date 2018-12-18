from common import connect_psql, get_file_seq, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os
import traceback

text_format = {
  'promo': [
    'transaction_date',
    'store_code',
    'barcode',
    'brand_id',
    'vendor_id',
    'dept_id',
    'subdept_id',
    'global_campaing_id',
    'discount_code1',
    'discount_amt_incvat1',
    'discount_amt_excvat1',
    'discount_code2',
    'discount_amt_incvat2',
    'discount_amt_excvat2',
    'discount_code3',
    'discount_amt_incvat3',
    'discount_amt_excvat3',
    'discount_code4',
    'discount_amt_incvat4',
    'discount_amt_excvat4',
    'discount_code5',
    'discount_amt_incvat5',
    'discount_amt_excvat5',
    'discount_code6',
    'discount_amt_incvat6',
    'discount_amt_excvat6',
    'discount_code7',
    'discount_amt_incvat7',
    'discount_amt_excvat7',
    'discount_code8',
    'discount_amt_incvat8',
    'discount_amt_excvat8',
    'discount_code9',
    'discount_amt_incvat9',
    'discount_amt_excvat9',
    'discount_code10',
    'discount_amt_incvat10',
    'discount_amt_excvat10',
    'net_amt_incvat',
    'net_amt_excvat'
  ],
  'discount': [
    'transaction_date',
    'store_code',
    'barcode',
    'brand_id',
    'vendor_id',
    'dept_id',
    'subdept_id',
    'discount_host_code',
    'global_campaing_id',
    'discount_amt_incvat',
    'discount_amt_excvat'
  ],
  'payment': [
    'transaction_date',
    'store_code',
    'media_member_code',
    'pay_amt_incvat',
    'pay_amt_excvat'
  ],
}


def prepare_data(data, fields, str_date):
  result = []
  for field in fields:
    result.append(data[field])
  if str_date:
    result.append(str_date)

  return "|".join(result)


def generate_trans_payment(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Payment_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['payment'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{:8}|{:4}|{}'.format(str_date, str_time, len(result)))
    print('[AutoPOS] - BI CDS[{}] payment create files completed..'.format(store))

  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))

def generate_trans_promo(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Promotion_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['promo'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{:8}|{:4}|{}'.format(str_date, str_time, len(result)))
    print('[AutoPOS] - BI CDS[{}] promotion create files completed..'.format(store))
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def generate_trans_discount(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Discount_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['discount'], str_date) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    f.write("\n".join(result))
    l.write('{:8}|{:4}|{}'.format(str_date, str_time, len(result)))
    print('[AutoPOS] - BI CDS[{}] discount create files completed..'.format(store))
  
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


def main():
  now = datetime.now()
  str_date = (now - timedelta(days=1)).strftime('%Y%m%d')
  str_time = (now - timedelta(days=1)).strftime('%H%M')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path_payment = os.path.join(parent_path, 'output/autopos/bicds/payment', str_date + str_time)
  if not os.path.exists(target_path_payment):
    os.makedirs(target_path_payment)
  target_path_promotion = os.path.join(parent_path, 'output/autopos/bicds/promotion', str_date + str_time)
  if not os.path.exists(target_path_promotion):
    os.makedirs(target_path_promotion)
  target_path_discount = os.path.join(parent_path, 'output/autopos/bicds/discount', str_date + str_time)
  if not os.path.exists(target_path_discount):
    os.makedirs(target_path_discount)
  target_path_master = os.path.join(parent_path, 'output/autopos/bicds/master', str_date + str_time)
  if not os.path.exists(target_path_master):
    os.makedirs(target_path_master)

  try:
    stores = [x['store_code'] for x in query_all("select store_code from businessunit group by store_code")]
    for store in stores:
      refresh_view = "refresh materialized view mv_autopos_bi_cds_trans_payment"
      sql = "select * from mv_autopos_bi_cds_trans_payment where interface_date = '{}' and store_code = '{}'".format(str_date, store)
      data = query_matview(refresh_view, sql)
      generate_trans_payment(target_path_payment, str_date, str_time, store, data)

      refresh_view = "refresh materialized view mv_autopos_bi_cds_trans_promo"
      sql = "select * from mv_autopos_bi_cds_trans_promo where interface_date = '{}' and store_code = '{}'".format(str_date, store)
      data = query_matview(refresh_view, sql)
      generate_trans_promo(target_path_promotion, str_date, str_time, store, data)

      refresh_view = "refresh materialized view mv_autopos_bi_cds_trans_discount"
      sql = "select * from mv_autopos_bi_cds_trans_discount where interface_date = '{}' and store_code = '{}'".format(str_date, store)
      data = query_matview(refresh_view, sql)
      generate_trans_discount(target_path_discount, str_date, str_time, store, data)

    sftp('autopos.cds-uat', target_path_payment, 'incoming/bicds/payment')
    sftp('autopos.cds-uat', target_path_promotion, 'incoming/bicds/promotion')
    sftp('autopos.cds-uat', target_path_discount, 'incoming/bicds/discount')
    sftp('autopos.cds-uat', target_path_master, 'incoming/bicds/master')
  except Exception as e:
    print('[AutoPOS] - BI CDS Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
