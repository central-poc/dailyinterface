from common import config, connect_psql, get_file_seq, insert_transaction, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback

text_format = {
    'promo': [
        'transaction_date', 'store_code', 'barcode', 'brand_id', 'vendor_id',
        'dept_id', 'subdept_id', 'global_campaing_id',
        'discount_code1', 'discount_amt_excvat1', 'discount_amt_incvat1', 
        'discount_code2', 'discount_amt_excvat2', 'discount_amt_incvat2', 
        'discount_code3', 'discount_amt_excvat3', 'discount_amt_incvat3', 
        'discount_code4', 'discount_amt_excvat4', 'discount_amt_incvat4', 
        'discount_code5', 'discount_amt_excvat5', 'discount_amt_incvat5', 
        'discount_code6', 'discount_amt_excvat6', 'discount_amt_incvat6', 
        'discount_code7', 'discount_amt_excvat7', 'discount_amt_incvat7', 
        'discount_code8', 'discount_amt_excvat8', 'discount_amt_incvat8', 
        'discount_code9', 'discount_amt_excvat9', 'discount_amt_incvat9', 
        'discount_code10', 'discount_amt_excvat10', 'discount_amt_incvat10', 
        'net_amt_excvat', 'net_amt_incvat'
    ],
    'discount': [
        'transaction_date', 'store_code', 'barcode', 'brand_id', 'vendor_id',
        'dept_id', 'subdept_id', 'discount_host_code', 'global_campaing_id',
        'discount_amt_excvat', 'discount_amt_incvat'
    ],
    'payment': [
        'transaction_date', 'store_code', 'media_member_code',
        'pay_amt_incvat', 'pay_amt_excvat'
    ],
}


def prepare_data(data, fields, str_date):
  result = []
  for field in fields:
    result.append(data[field])
  if str_date:
    result.append(str_date)

  return "|".join(result)


def generate_trans_payment(output_path, str_date, store, data):
  bu = 'BIRBS_'
  prefix = bu + store + '_Payment_' + str_date + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['payment'], str_date[:8]) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    if len(result) > 0:
      f.write("\n".join(result))
    else:
      f.write("NO RECORD")
    f.write("\n")
    l.write('{:8}|{:4}|{}'.format(str_date[:8], str_date[-4:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI RBS[{}] payment create files completed..'.format(store))
  return [file_name, log_name]


def generate_trans_promo(output_path, str_date, store, data):
  bu = 'BIRBS_'
  prefix = bu + store + '_Promotion_' + str_date + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['promo'], str_date[:8]) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    if len(result) > 0:
      f.write("\n".join(result))
    else:
      f.write("NO RECORD")
    f.write("\n")
    l.write('{:8}|{:4}|{}'.format(str_date[:8], str_date[-4:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI RBS[{}] promotion create files completed..'.format(store))
  return [file_name, log_name]


def generate_trans_discount(output_path, str_date, store, data):
  bu = 'BIRBS_'
  prefix = bu + store + '_Discount_' + str_date + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '.LOG'
  log_fullpath = os.path.join(output_path, log_name)
  result = [prepare_data(d, text_format['discount'], str_date[:8]) for d in data]

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    if len(result) > 0:
      f.write("\n".join(result))
    else:
      f.write("NO RECORD")
    f.write("\n")
    l.write('{:8}|{:4}|{}'.format(str_date[:8], str_date[-4:], len(result)))
    l.write("\n")
  print('[RBS AutoPOS] - BI RBS[{}] discount create files completed..'.format(store))
  return [file_name, log_name]


def main():
  env = sys.argv[1] if len(sys.argv) > 1 else 'local'
  print("\n===== Start Siebel [{}] =====".format(env))
  cfg = config(env)
  now = datetime.now()
  query_date = cfg['run_date'] if cfg['run_date'] else (now - timedelta(days=1)).strftime('%Y%m%d')
  str_date = cfg['birbs_date'] if cfg['birbs_date'] else now.strftime('%Y%m%d%H%M')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path_payment = os.path.join(
      parent_path, 'output/autopos/{}/birbs/payment'.format(env), str_date)
  if not os.path.exists(target_path_payment):
    os.makedirs(target_path_payment)
  target_path_promotion = os.path.join(
      parent_path, 'output/autopos/{}/birbs/promotion'.format(env), str_date)
  if not os.path.exists(target_path_promotion):
    os.makedirs(target_path_promotion)
  target_path_discount = os.path.join(
      parent_path, 'output/autopos/{}/birbs/discount'.format(env), str_date)
  if not os.path.exists(target_path_discount):
    os.makedirs(target_path_discount)

  try:
    dbfms = cfg['fms']
    stores = [
        x['store_code']
        for x in query_all(dbfms,
            "select store_code from businessunit where businessunit_code in ('RBS') and status = 'AT' group by store_code"
        )
    ]
    for store in stores:
      refresh_view = "refresh materialized view mv_autopos_bi_rbs_trans_payment"
      sql = "select * from mv_autopos_bi_rbs_trans_payment where interface_date = '{}' and store_code = '{}'".format(
          query_date, store)
      data = query_matview(dbfms, refresh_view, sql)
      payment = generate_trans_payment(target_path_payment, str_date, store, data)
      sql_insert = "insert into transaction_bi_rbs_payment {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      refresh_view = "refresh materialized view mv_autopos_bi_rbs_trans_promo"
      sql = "select * from mv_autopos_bi_rbs_trans_promo where interface_date = '{}' and store_code = '{}'".format(
          query_date, store)
      data = query_matview(dbfms, refresh_view, sql)
      promo = generate_trans_promo(target_path_promotion, str_date, store, data)
      sql_insert = "insert into transaction_bi_rbs_promo {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      refresh_view = "refresh materialized view mv_autopos_bi_rbs_trans_discount"
      sql = "select * from mv_autopos_bi_rbs_trans_discount where interface_date = '{}' and store_code = '{}'".format(
          query_date, store)
      data = query_matview(dbfms, refresh_view, sql)
      discount = generate_trans_discount(target_path_discount, str_date, store, data)
      sql_insert = "insert into transaction_bi_rbs_discount {}".format(sql)
      insert_transaction(dbfms, sql_insert)

      if cfg['ftp']['is_enable']:
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_payment, 'incoming/birbs/payment', payment)
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_promotion, 'incoming/birbs/promotion', promo)
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path_discount, 'incoming/birbs/discount', discount)
  except Exception as e:
    print('[RBS AutoPOS] - BI RBS Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
