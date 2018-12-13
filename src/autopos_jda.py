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
  result.append("{:0>5}".format(data['store_code'][:5]))
  result.append("{:0>8}".format(data['transaction_date'][:8]))
  result.append("{:0>4}".format(data['transaction_time'][:4]))
  result.append("{:0>2}".format(data['transaction_type'][:2]))
  result.append("{:9}".format(data['ticket_no'][:9]))
  result.append("{:0>3}".format(data['seq_no'][:3]))
  result.append("{:0>16}".format(data['sku'][:16]))
  result.append("{:0>16}".format(data['barcode'][:16]))
  result.append("{:1}".format(data['qty_sign'][:1]))
  result.append("{:0>6}".format(data['quantity'][:6]))
  result.append("{:0>12}".format(data['jda_price'][:12]))
  result.append("{:0>12}".format(data['price_override'][:12]))
  result.append("{:1}".format(data['price_override_flag'][:1]))
  result.append("{:0>12}".format(data['total_net_amt'][:12]))
  result.append("{:0>12}".format(data['vat_amt'][:12]))
  result.append("{:4}".format(data['discount_type1'][:4]))
  result.append("{:0>12}".format(data['discount_amt1'][:12]))
  result.append("{:4}".format(data['discount_type2'][:4]))
  result.append("{:0>12}".format(data['discount_amt2'][:12]))
  result.append("{:4}".format(data['discount_type3'][:4]))
  result.append("{:0>12}".format(data['discount_amt3'][:12]))
  result.append("{:4}".format(data['discount_type4'][:4]))
  result.append("{:0>12}".format(data['discount_amt4'][:12]))
  result.append("{:4}".format(data['discount_type5'][:4]))
  result.append("{:0>12}".format(data['discount_amt5'][:12]))
  result.append("{:4}".format(data['discount_type6'][:4]))
  result.append("{:0>12}".format(data['discount_amt6'][:12]))
  result.append("{:4}".format(data['discount_type7'][:4]))
  result.append("{:0>12}".format(data['discount_amt7'][:12]))
  result.append("{:4}".format(data['discount_type8'][:4]))
  result.append("{:0>12}".format(data['discount_amt8'][:12]))
  result.append("{:4}".format(data['discount_type9'][:4]))
  result.append("{:0>12}".format(data['discount_amt9'][:12]))
  result.append("{:4}".format(data['discount_type10'][:4]))
  result.append("{:0>12}".format(data['discount_amt10'][:12]))
  result.append("{:21}".format(data['ref_id'][:21]))
  result.append("{:9}".format(data['ref_ticket'][:9]))
  result.append("{:0>8}".format(data['ref_date'][:8]))
  result.append("{:0>2}".format(data['reason_code'][:2]))
  result.append("{:0>6}".format(data['event_no'][:6]))
  result.append("{:0>3}".format(data['dept_id'][:3]))
  result.append("{:0>3}".format(data['subdept_id'][:3]))
  result.append("{:0>16}".format(data['itemized'][:16]))
  result.append("{:1}".format(data['dtype'][:1]))
  result.append("{:16}".format(data['credit_cardno'][:16]))
  result.append("{:21}".format(data['customer_id'][:21]))
  result.append("{:0>8}".format(data['member_point'][:8]))
  result.append("{:8}".format(data['cashier_id'][:8]))
  result.append("{:8}".format(data['sale_person'][:8]))

  return "".join(result)


def generate_data_file(output_path, store, data):
  result = [prepare_data(d) for d in data]
  file_name = 'SD' + store + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)
  result_fullpath = os.path.join(output_path, 'SD' + store + '.csv')

  with open(file_fullpath, 'w') as f:
    f.write("\n".join(result))
    print('[AutoPOS] - JDA Create Files Complete..')
  with open(file_fullpath, 'r') as f:
    for line in f.read().splitlines():
      print(len(line))


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

  stores = [x['store_code'] for x in query_store()]
  for store in stores:
    data = query_data_by_store(store, str_date)
    generate_data_file(target_path, store, data)


if __name__ == '__main__':
  main()
