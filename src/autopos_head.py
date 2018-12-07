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
  prefix = 'H' + str_date
  seq = get_file_seq(prefix, output_path, '.MER')
  dat_file = prefix + str(seq) + '.MER'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.LOG'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    try:
      count = 0
      sum_invoice = 0
      for line in data:
        if count > 0:
          dat.write('\n')
        line_invoice = line['invoice_total']
        dat.write(
            "{:3}{:50}{:30}{:6}{:014.2f}{:15}{:1}{:1}{:2}{:5}{:50}{:50}{:10}{:3}{:4}{:2}{:15}{:50}{:14}{:6}{:10}{:50}".
            format(line['source'][:3], line['invoice_no'][:50], 
                   line['vendor_id'][:30], line['invoice_date'],
                   line_invoice, line['store_id'][:15], 
                   line['invoice_type'][:1], line['imported_goods'][:1], 
                   line['hold_reason01'][:2], line['invoice_tax_name'][:5], 
                   line['tax_inv_running_no'][:50], line['blank1'][:50], 
                   line['rtv_auth_no'][:10], line['currency_code'][:3],
                   line['terms'][:4], line['blank2'][:2], 
                   line['gr_tran_no'][:15], line['ass_tax_invoice_num'][:50],
                   line['blank3'][:14], line['tax_invoice_date'], 
                   line['invoice_rtv_type'][:10], line['currency_rate'][:50]))
        sum_invoice = sum_invoice + line_invoice
        count = count + 1

      val.write('{:14}{:0>5}{:0>5}{:0>10}'.format(dat_file, count, count, sum_invoice))
      print('Create Files H .MER & .LOG Complete..')
    except Exception as e:
      print('Create Files H .MER & .LOG Error .. : ')
      print(str(e))


def query_data(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_ofin_head where invoice_date = %s"
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
    print('[H] - Error: %s' % str(e))


if __name__ == '__main__':
  main()
