from common import connect_psql, get_file_seq, query_matview
from datetime import datetime, timedelta
import os
import traceback


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
    print('[AutoPOS] - L create file .MER completed..')


def main():
  batch_date = datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofindaily', batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    refresh_view = "refresh materialized view mv_autopos_ofin_line"
    sql = "select * from mv_autopos_ofin_line where interface_date = '{}'".format(batch_date.strftime('%Y%m%d'))
    data = query_matview(refresh_view, sql)
    generate_data_file(target_path, batch_date.strftime('%d%m%y'), data)
  except Exception as e:
    print('[AutoPOS] - L Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
