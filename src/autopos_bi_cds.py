from common import connect_psql
from datetime import datetime, timedelta
import os
import psycopg2.extras


def get_file_seq(prefix, output_path, ext):
  files = [
      f.split('.')[0] for f in os.listdir(output_path)
      if os.path.isfile(os.path.join(output_path, f)) and f.endswith(ext)
  ]
  return 1 if not files else max(
      int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def generate_trans_payment(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Payment_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '_.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write("{}|{}|{}|{}|{}|{}".format(
            d['transaction_date'], d['store_code'], d['media_member_code'],
            d['pay_amt_incvat'], d['pay_amt_excvat'], str_date))
        count = count + 1

      l.write('{:8}|{:4}|{}'.format(str_date, str_time, count))
      print('[AutoPOS] - BI CDS payment create files complete..')
    except Exception as e:
      print('[AutoPOS] - BI CDS payment create files error: {}: '.format(e))


def generate_trans_promo(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Promotion_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '_.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".
            format(d['transaction_date'], d['store_code'], d['barcode'],
                   d['brand_id'], d['vendor_id'], d['dept_id'],
                   d['subdept_id'], d['global_campaing_id'],
                   d['discount_code1'], d['discount_amt_incvat1'],
                   d['discount_amt_excvat1'], d['discount_code2'],
                   d['discount_amt_incvat2'], d['discount_amt_excvat2'],
                   d['discount_code3'], d['discount_amt_incvat3'],
                   d['discount_amt_excvat3'], d['discount_code4'],
                   d['discount_amt_incvat4'], d['discount_amt_excvat4'],
                   d['discount_code5'], d['discount_amt_incvat5'],
                   d['discount_amt_excvat5'], d['discount_code6'],
                   d['discount_amt_incvat6'], d['discount_amt_excvat6'],
                   d['discount_code7'], d['discount_amt_incvat7'],
                   d['discount_amt_excvat7'], d['discount_code8'],
                   d['discount_amt_incvat8'], d['discount_amt_excvat8'],
                   d['discount_code9'], d['discount_amt_incvat9'],
                   d['discount_amt_excvat9'], d['discount_code10'],
                   d['discount_amt_incvat10'], d['discount_amt_excvat10'],
                   d['net_amt_incvat'], d['net_amt_excvat'], str_date))
        count = count + 1

      l.write('{:8}|{:4}|{}'.format(str_date, str_time, count))
      print('[AutoPOS] - BI CDS promotion create files complete..')
    except Exception as e:
      print('[AutoPOS] - BI CDS promotion create files error: {}: '.format(e))


def generate_trans_discount(output_path, str_date, str_time, store, data):
  prefix = 'BICDS_' + store + '_Discount_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.TXT')
  file_name = prefix + str(seq) + '_.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
            d['transaction_date'], d['store_code'], d['barcode'],
            d['brand_id'], d['vendor_id'], d['dept_id'], d['subdept_id'],
            d['discount_host_code'], d['global_campaing_id'],
            d['discount_amt_incvat'], d['discount_amt_excvat'], str_date))
        count = count + 1

      l.write('{:8}|{:4}|{}'.format(str_date, str_time, count))
      print('[AutoPOS] - BI CDS discount create files complete..')
    except Exception as e:
      print('[AutoPOS] - BI CDS discount create files error: {}: '.format(e))


def query_transaction_payment(str_date, store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_cds_trans_payment where transaction_date = %s and store_code = %s"
      cursor.execute(sql, (str_date, store, ))

      return cursor.fetchall()


def query_transaction_promotion(str_date, store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_cds_trans_promo where transaction_date = %s and store_code = %s"
      cursor.execute(sql, (str_date, store, ))

      return cursor.fetchall()


def query_transaction_discount(str_date, store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_cds_trans_discount where transaction_date = %s and store_code = %s"
      cursor.execute(sql, (str_date, store, ))

      return cursor.fetchall()


def query_store():
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute(
          "select store_code from mv_autopos_bi_cds_trans_payment group by store_code"
      )

      return cursor.fetchall()


def main():
  now = datetime.now()
  str_date = (now - timedelta(days=1)).strftime('%Y%m%d')
  str_time = (now - timedelta(days=1)).strftime('%H%M')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/bicds',
                             str_date + str_time)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    stores = [x['store_code'] for x in query_store()]
    for store in stores:
      generate_trans_payment(target_path, str_date, str_time, store,
                             query_transaction_payment(str_date, store))
      generate_trans_promo(target_path, str_date, str_time, store,
                           query_transaction_promotion(str_date, store))
      generate_trans_discount(target_path, str_date, str_time, store,
                              query_transaction_discount(str_date, store))
  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
