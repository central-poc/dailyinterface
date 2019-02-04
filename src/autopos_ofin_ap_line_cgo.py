from common import config, connect_psql, get_file_seq, insert_transaction, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback


def prepare_data(data):
  result = []
  sum_amount = 0
  for d in data:
    amount = d['amount']
    sum_amount = sum_amount + amount
    temp = []
    temp.append("{:3}".format(d['source'][:3]))
    temp.append("{:50}".format(d['invoice_no'][:50]))
    temp.append("{:1}".format(d['invoice_type'][:1]))
    temp.append("{:30}".format(d['vendor_id'][:30]))
    temp.append("{:1}".format(d['line_type'][:1]))
    temp.append("{:240}".format(d['item_description'][:240]))
    temp.append("{:014.2f}".format(amount))
    temp.append("{:14}".format(d['item_qty'][:14]))
    temp.append("{:14}".format(d['item_cost'][:14]))
    temp.append("{:35}".format(d['temp_field'][:35]))
    temp.append("{:5}".format(d['cost_center'][:5]))
    temp.append("{:8}".format(d['gl_account'][:8]))

    result.append("".join(temp))

  return result, sum_amount


def generate_data_file(output_path, str_date, data):
  prefix = 'L' + str_date + 'FMS'
  seq = get_file_seq(prefix, output_path, '.MER')
  dat_file = prefix + str(seq) + '.MER'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.LOG'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result, sum_amount = prepare_data(data)
    dat.write("\n".join(result))
    val.write('{:20}{:0>10}{:015.2f}'.format(dat_file, len(result), sum_amount))
  print('[AutoPOS] - L create file .MER completed..')
  return [dat_file, val_file]


def main():
  env = sys.argv[1] if len(sys.argv) > 1 else 'local'
  print("\n===== Start OFIN AP LINE CGO [{}] =====".format(env))
  cfg = config(env)
  batch_date = datetime.strptime(cfg['run_date'], '%Y%m%d') if cfg['run_date'] else datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/{}/ofin/ap/cgo'.format(env), batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    refresh_view = "refresh materialized view mv_autopos_ofin_line_cgo"
    sql = "select * from mv_autopos_ofin_line_cgo where interface_date = '{}'".format(batch_date.strftime('%Y%m%d'))
    data = query_matview(cfg['fms'], refresh_view, sql)
    files = generate_data_file(target_path, batch_date.strftime('%y%m%d'), data)
    if cfg['ftp']['is_enable']:
      destination = 'incoming/ofin/ap/cgo'
      sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path, destination, files)    
    sql_insert = "insert into transaction_ofin_line_cgo {}".format(sql)
    insert_transaction(cfg['fms'], sql_insert)
  except Exception as e:
    print('[AutoPOS] - L CGO Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
