from common import connect_psql, get_file_seq, query_all, sftp
from datetime import datetime, timedelta
import os, sys, traceback


def prepare_data(data):
  result = []
  for d in data:
    temp = []
    temp.append('{:1}'.format(d['status'][:1]))
    temp.append('{:6}'.format(d['delete_date'][:6]))
    temp.append("{:6}".format(d['vendor_id'][:6]))
    temp.append("{:60}".format(d['vendor_name'][:60]))
    temp.append("{:1}".format(d['vendor_type'][:1]))
    temp.append("{:13}".format(d['tax_payer_id'][:13]))
    temp.append("{:4}".format(d['term_of_payment'][:4]))
    temp.append("{:1}".format(d['payment_type'][:1]))
    temp.append("{:5}".format(d['tax_rate'][:5]))
    temp.append("{:1}".format(d['withholding_tax'][:1]))
    temp.append("{:60}".format(d['address_line1'][:60]))
    temp.append("{:60}".format(d['address_line2'][:60]))
    temp.append("{:60}".format(d['address_line3'][:60]))
    temp.append("{:25}".format(''))
    temp.append("{:4}".format(''))
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


def genrate_report(file_with_path):
  with open(file_with_path, 'r') as f:
    for line in f.read().splitlines():
      print('Status: {}.'.format(line[0:1]))
      print('Delete Date: {}.'.format(line[1:7]))
      print("ID: {}.".format(line[7:13]))
      print("Name: {}.".format(line[13:73]))
      print("Type: {}.".format(line[73:74]))
      print("Tax Payer: {}.".format(line[74:87]))
      print("Term: {}.".format(line[87:91]))
      print("Payment: {}.".format(line[91:92]))
      print("Tax Rate: {}.".format(line[92:97]))
      print("Withholding: {}.".format(line[97:98]))
      print("Line1: {}.".format(line[98:158]))
      print("Line2: {}.".format(line[158:218]))
      print("Line3: {}.".format(line[218:278]))
      print("City: {}.".format(line[278:303]))
      print("State: {}.".format(line[303:307]))
      print("Zip: {}.".format(line[307:317]))
      print("Country Code: {}.".format(line[317:320]))
      print("Country: {}.".format(line[320:345]))
      print("Phone: {}.".format(line[345:360]))
      print("Fax: {}.".format(line[360:375]))
      print("Telex: {}.".format(line[375:390]))
      print("First Name: {}.".format(line[390:405]))
      print("Last Name: {}.".format(line[405:425]))
      print("Oversea: {}.".format(line[425:426]))
      print("Bank: {}.".format(line[426:429]))
      print("Account Number: {}.".format(line[429:459]))
      print("Account Name: {}.".format(line[459:524]))
      print("Email: {}".format(line[524:574]))
      print("Remittance: {}.".format(line[574:594]))
      print("Cycle Payment: {}.".format(line[594:596]))
      print("Currency: {}.".format(line[596:599]))
      print("Tax Branch: {}.".format(line[599:609]))
      print("EDI: {}.".format(line[609:639]))


def main():
  batch_date = datetime.strptime(sys.argv[1], '%Y%m%d') if len(sys.argv) > 1 else datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin/vendor', batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    sql = "select case when status = 'D' then to_char(updated_on, 'DDMMYY') else '' end as delete_date, * from vendor where created_on <> updated_on"
    data = query_all(sql)
    generate_data_file(target_path, batch_date.strftime('%y%m%d'), batch_date.strftime('%H%M%S'), data)
    destination = 'incoming/ofin/vendor'
    sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - Vendor Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
