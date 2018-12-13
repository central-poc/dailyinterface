from common import connect_psql, get_file_seq
from datetime import datetime, timedelta
import os
import psycopg2.extras


def prepare_data(data):
  result = []
  for d in data:
    temp = []
    temp.append("{:3}".format(d['source'][:3]))
    temp.append("{:50}".format(d['invoice_no'][:50]))
    temp.append("{:1}".format(d['invoice_type'][:1]))
    temp.append("{:30}".format(d['vendor_id'][:30]))
    temp.append("{:1}".format(d['line_type'][:1]))
    temp.append("{:240}".format(d['item_description'][:240]))
    temp.append("{:014.2f}".format(d['amount']))
    temp.append("{:14}".format(d['item_qty'][:14]))
    temp.append("{:14}".format(d['item_cost'][:14]))
   
    result.append("".join(temp))

  return result


def generate_data_file(output_path, str_date, data):
  prefix = 'L' + str_date
  seq = get_file_seq(prefix, output_path, '.MER')
  dat_file = prefix + str(seq) + '.MER'
  dat_file_path = os.path.join(output_path, dat_file)

  with open(dat_file_path, 'w') as dat:
    dat.write("\n".join(prepare_data(data)))
    print('Create Files L .MER Complete..')


def query_data(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_ofin_line where invoice_date = %s"
      cursor.execute(sql, (str_date, ))
      
      return cursor.fetchall()


def main():
  str_date = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofindaily', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    generate_data_file(target_path, str_date, query_data(str_date))

  except Exception as e:
    print('[L] - Error: %s' % str(e))


if __name__ == '__main__':
  main()
