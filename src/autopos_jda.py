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

def prepare_data(data):
  result = []
  result.append("{:0>5}".data['store_code'][:5])
  result.append("{:0>8}".data['transaction_date'][:8])
  result.append("{:0>4}".data['transaction_time'][:4])
  result.append("{:0>2}".data['transaction_type'][:2])
  result.append("{:9}".data['ticket_no'][:9])
  result.append("{:0>3}".data['seq_no'][:3])
  result.append("{:0>16}".data['sku'][:16])
  result.append("{:0>16}".data['barcode'][:16])
  result.append("{:1}".data['qty_sign'][:1])
  result.append("{:0>6}".data['quantity'][:6])
  result.append("{:0>12}".data['jda_price'][:12])
  result.append("{:0>12}".data['price_override'][:12])
  result.append("{:1}".data['price_override_flag'][:1])
  result.append("{:0>12}".data['total_net_amt'][:12])
  result.append("{:0>12}".data['vat_amt'][:12])
  result.append("{:4}".data['discount_type1'][:4])
  result.append("{:0>12}".data['discount_amt1'][:12])
  result.append("{:4}".data['discount_type2'][:4])
  result.append("{:0>12}".data['discount_amt2'][:12])
  result.append("{:4}".data['discount_type3'][:4])
  result.append("{:0>12}".data['discount_amt3'][:12])
  result.append("{:4}".data['discount_type4'][:4])
  result.append("{:0>12}".data['discount_amt4'][:12])
  result.append("{:4}".data['discount_type5'][:4])
  result.append("{:0>12}".data['discount_amt5'][:12])
  result.append("{:4}".data['discount_type6'][:4])
  result.append("{:0>12}".data['discount_amt6'][:12])
  result.append("{:4}".data['discount_type7'][:4])
  result.append("{:0>12}".data['discount_amt7'][:12])
  result.append("{:4}".data['discount_type8'][:4])
  result.append("{:0>12}".data['discount_amt8'][:12])
  result.append("{:4}".data['discount_type9'][:4])
  result.append("{:0>12}".data['discount_amt9'][:12])
  result.append("{:4}".data['discount_type10'][:4])
  result.append("{:0>12}".data['discount_amt10'][:12])
  result.append("{:21}".data['ref_id'][:21])
  result.append("{:9}".data['ref_ticket'][:9])
  result.append("{:0>8}".data['ref_date'][:8])
  result.append("{:0>2}".data['reason_code'][:2])
  result.append("{:0>6}".data['event_no'][:6])
  result.append("{:0>3}".data['dept_id'][:3])
  result.append("{:0>3}".data['subdept_id'][:3])
  result.append("{:0>16}".data['itemized'][:16])
  result.append("{:1}".data['dtype'][:1])
  result.append("{:16}".data['credit_cardno'][:16])
  result.append("{:21}".data['customer_id'][:21])
  result.append("{:0>8}".data['member_point'][:8])
  result.append("{:8}".data['cashier_id'][:8])
  result.append("{:8}".data['sale_person'][:8])


def generate_data_file(output_path, store, data):
  file_name = 'SD' + store + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  result_fullpath = os.path.join(output_path, 'SD' + store + '.csv')

  with open(file_fullpath, 'w') as f:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write(
            "{:0>5}{:0>8}{:0>4}{:0>2}{:9}{:0>3}{:0>16}{:0>16}{:1}{:0>6}{:0>12}{:0>12}{:1}{:0>12}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:21}{:9}{:0>8}{:0>2}{:0>6}{:0>3}{:0>3}{:0>16}{:1}{:16}{:21}{:0>8}{:8}{:8}.".
            format(
                d['store_code'][:5], d['transaction_date'][:8], d['transaction_time'][:4],
                d['transaction_type'][:2], d['ticket_no'][:9], d['seq_no'][:3], d['sku'][:16],
                d['barcode'][:16], d['qty_sign'][:1], d['quantity'][:6], d['jda_price'][:12],
                d['price_override'][:12], d['price_override_flag'][:1],
                d['total_net_amt'][:12], d['vat_amt'][:12], d['discount_type1'][:4],
                d['discount_amt1'][:12], d['discount_type2'][:4], d['discount_amt2'][:12],
                d['discount_type3'][:4], d['discount_amt3'][:12], d['discount_type4'][:4],
                d['discount_amt4'][:12], d['discount_type5'][:4], d['discount_amt5'][:12],
                d['discount_type6'][:4], d['discount_amt6'][:12], d['discount_type7'][:4],
                d['discount_amt7'][:12], d['discount_type8'][:4], d['discount_amt8'][:12],
                d['discount_type9'][:4], d['discount_amt9'][:12], d['discount_type10'][:4],
                d['discount_amt10'][:12], d['ref_id'][:21], d['ref_ticket'][:9],
                d['ref_date'][:8], d['reason_code'][:2], d['event_no'][:6], d['dept_id'][:3],
                d['subdept_id'][:3], d['itemized'][:16], d['dtype'][:1], d['credit_cardno'][:16],
                d['customer_id'][:21], d['member_point'][:8], d['cashier_id'][:8],
                d['sale_person'][:8]))
        count = count + 1
      print('[AutoPOS] - JDA Create Files Complete..')
    except Exception as e:
      print('[AutoPOS] - JDA Create Files Error: {}: '.format(e))
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(line[-1])


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
