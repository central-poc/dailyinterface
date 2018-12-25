from common import connect_psql, get_file_seq, query_matview, sftp
from datetime import datetime, timedelta
import os
import traceback


def prepare_data(data):
  result = []
  sum_invoice = 0
  for d in data:
    line_invoice = d['invoice_total']
    sum_invoice = sum_invoice + line_invoice
    temp = []
    temp.append("{:3}".format(d['source'][:3]))
    temp.append("{:50}".format(d['invoice_no'][:50]))
    temp.append("{:30}".format(d['vendor_id'][:30]))
    temp.append("{:6}".format(d['invoice_date'][:6]))
    temp.append("{:014.2f}".format(line_invoice))
    temp.append("{:15}".format(d['store_id'][:14]))
    temp.append("{:1}".format(d['invoice_type'][:1]))
    temp.append("{:1}".format(d['imported_goods'][:1]))
    temp.append("{:2}".format(d['hold_reason01'][:2]))
    temp.append("{:5}".format(d['invoice_tax_name'][:5]))
    temp.append("{:50}".format(d['tax_inv_running_no'][:50]))
    temp.append("{:50}".format(d['blank1'][:50]))
    temp.append("{:10}".format(d['rtv_auth_no'][:10]))
    temp.append("{:3}".format(d['currency_code'][:3]))
    temp.append("{:4}".format(d['terms'][:4]))
    temp.append("{:2}".format(d['blank2'][:2]))
    temp.append("{:15}".format(d['gr_tran_no'][:15]))
    temp.append("{:50}".format(d['ass_tax_invoice_num'][:50]))
    temp.append("{:14}".format(d['blank3'][:14]))
    temp.append("{:6}".format(d['tax_invoice_date'][:6]))
    temp.append("{:10}".format(d['invoice_rtv_type'][:10]))
    temp.append("{:50}".format(d['currency_rate'][:50]))
   
    result.append("".join(temp))

  return result, sum_invoice


def generate_data_file(output_path, str_date, data):
  prefix = 'H' + str_date + 'FMS'
  seq = get_file_seq(prefix, output_path, '.MER')
  dat_file = prefix + str(seq) + '.MER'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.LOG'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result, invoice = prepare_data(data)
    dat.write("\n".join(result))
    val.write('{:14}{:0>5}{:0>5}{:0>10}'.format(dat_file, len(result), len(result), invoice))
    print('[AutoPOS] - H create files .MER & .LOG completed..')


def main():
  batch_date = datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin/ap', batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    refresh_view = "refresh materialized view mv_autopos_ofin_head"
    sql = "select * from mv_autopos_ofin_head where interface_date = '{}'".format(batch_date.strftime('%Y%m%d'))
    data = query_matview(refresh_view, sql)
    generate_data_file(target_path, batch_date.strftime('%y%m%d'), data)
    destination = 'incoming/ofin/ap'
    sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - H Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
