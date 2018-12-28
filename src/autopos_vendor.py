from common import connect_psql, get_file_seq, query_all, sftp
from datetime import datetime, timedelta
import os
import traceback


def prepare_data(data):
  result = []
  for d in data:
    temp = []
    temp.append('{:1}'.format('A'))
    temp.append('{:6}'.format(''))
    temp.append("{:6}".format(d['id'][:6]))
    temp.append("{:60}".format(d['name'][:60]))
    temp.append("{:1}".format(d['type'][:1]))
    temp.append("{:13}".format(d['tax_payer_id'][:13]))
    temp.append("{:1}".format(d['payment_type'][:1]))
    temp.append("{:5}".format(d['tax_rate'][:5]))
    temp.append("{:1}".format(d['withholding_tax'][:1]))
    temp.append("{:60}".format(d['address_line1'][:60]))
    temp.append("{:60}".format(d['address_line2'][:60]))
    temp.append("{:60}".format(d['address_line3'][:60]))
    temp.append("{:10}".format(d['zip_code'][:10]))
    temp.append("{:3}".format(d['country_code'][:3]))
    temp.append("{:25}".format(d['country'][:25]))
    temp.append("{:15}".format(d['phone'][:15]))
    temp.append("{:15}".format(d['fax'][:15]))
    temp.append("{:15}".format(d['telex'][:15]))
    temp.append("{:15}".format(d['first_name'][:15]))
    temp.append("{:20}".format(d['last_name'][:20]))
    temp.append("{:1}".format(d['is_oversea'][:1]))
    temp.append("{:3}".format(d['bank_number'][:3]))
    temp.append("{:30}".format(d['bank_account_number'][:30]))
    temp.append("{:65}".format(d['bank_account_name'][:65]))
    temp.append("{:50}".format(d['email'][:50]))
    temp.append("{:20}".format(d['remittance_fax'][:20]))
    temp.append("{:2}".format(d['cycle_payment'][:2]))
    temp.append("{:3}".format(d['currency'][:3]))
    temp.append("{:10}".format(d['tax_branch_no'][:10]))
    temp.append("{:30}".format(d['edi_no'][:30]))
   
    result.append("".join(temp))

  return result


def generate_data_file(output_path, str_date, str_time, data):
  prefix = 'S' + str_date
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result = prepare_data(data)
    dat.write("\n".join(result))
    val.write('{:3}{:12}{:9}{:6}{:6}{:15}{:15}{:15}{:15}'.format('HDR', dat_file, len(result), str_date, str_time, '0', '0', '0', '0'))
    print('[AutoPOS] - Vendor .DAT & .VAL Completed..')


def main():
  batch_date = datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin/vendor', batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    sql = "select * from vendor"
    data = query_all(sql)
    generate_data_file(target_path, batch_date.strftime('%y%m%d'), batch_date.strftime('%H%M%S'), data)
    destination = 'incoming/ofin/vendor'
    sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - Vendor Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
