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


def generate_data_file(output_path, store, data):
  file_name = 'SD' + store + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)

  with open(file_fullpath, 'w') as f:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write(
            "{:0>5}{:0>8}{:0>4}{:0>2}{:9}{:0>3}{:0>16}{:0>16}{:1}{:0>6}{:0>12}{:0>12}{:1}{:0>12}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:21}{:9}{:0>8}{:0>2}{:0>6}{:0>3}{:0>3}{:0>16}{:1}{:16}{:21}{:0>8}{:8}{:8}".
            format(
                d['store_code'], d['transaction_date'], d['transaction_time'],
                d['transaction_type'], d['ticket_no'], d['seq_no'], d['sku'],
                d['barcode'], d['qty_sign'], d['quantity'], d['jda_price'],
                d['price_override'], d['price_override_flag'],
                d['total_net_amt'], d['vat_amt'], d['discount_type1'],
                d['discount_amt1'], d['discount_type2'], d['discount_amt2'],
                d['discount_type3'], d['discount_amt3'], d['discount_type4'],
                d['discount_amt4'], d['discount_type5'], d['discount_amt5'],
                d['discount_type6'], d['discount_amt6'], d['discount_type7'],
                d['discount_amt7'], d['discount_type8'], d['discount_amt8'],
                d['discount_type9'], d['discount_amt9'], d['discount_type10'],
                d['discount_amt10'], d['ref_id'], d['ref_ticket'],
                d['ref_date'], d['reason_code'], d['event_no'], d['dept_id'],
                d['subdept_id'], d['itemized'], d['dtype'], d['credit_cardno'],
                d['customer_id'], d['member_point'], d['cashier_id'],
                d['sale_person']))
        count = count + 1
      print('[AutoPOS] - JDA Create Files Complete..')
    except Exception as e:
      print('[AutoPOS] - JDA Create Files Error: {}: '.format(e))


def query_store():
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute("select store_code from businessunit")

      return cursor.fetchall()


def query_data_by_store(store, transaction_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "refresh materialized view mv_autopos_jda"
      cursor.execute(sql)
      sql = "select * from mv_autopos_jda where store_code = %s and transaction_date = %s"
      cursor.execute(sql, (store, transaction_date, ))

      return cursor.fetchall()


def main():
  str_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/jda', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    stores = [x['store_code'] for x in query_store()]
    for store in stores:
      data = query_data_by_store(store, str_date)
      generate_data_file(target_path, store, data)
  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
