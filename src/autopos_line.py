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


def generate_data_file(output_path, str_date, data):
  prefix = 'L' + str_date
  seq = get_file_seq(prefix, output_path, '.MER')
  dat_file = prefix + str(seq) + '.MER'
  dat_file_path = os.path.join(output_path, dat_file)

  with open(dat_file_path, 'w') as dat:
    try:
      count = 0
      for line in data:
        if count > 0:
          dat.write('\n')
        dat.write(
            "{:3}{:50}{:1}{:30}{:1}{:240}{:014.2f}{:14}{:14}".
            format(line['source'], line['invoice_no'], line['invoice_type'], 
                   line['vendor_id'], line['line_type'], line['item_description'],
                   line['amount'], line['item_qty'], line['item_cost'])) 
        count = count + 1
      print('Create Files L .MER Complete..')
    except Exception as e:
      print('Create Files L .MER Error .. : ')
      print(str(e))


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
