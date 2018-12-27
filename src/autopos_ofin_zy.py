from common import connect_psql, get_file_seq, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os
import traceback


def prepare_data(data):
  result = []
  debit_accum = 0
  credit_accum = 0
  for d in data:
    debit = d['debit']
    credit = d['credit']
    temp = []
    temp.append("{:6}".format(d['ofin_branch_code'][:6]))
    temp.append("{:5}".format(d['ofin_cost_profit_center'][:5]))
    temp.append("{:8}".format(d['account_code'][:8]))
    temp.append("{:6}".format(d['subaccount_code'][:6]))
    temp.append("{:9}".format(d['invoice_date'][:9]))
    temp.append("{:012.2f}".format(debit))
    temp.append("{:012.2f}".format(credit))
    temp.append("{:4}".format(d['bu'][:4]))
    temp.append("{:25}".format(d['journal_category_name'][:25]))
    temp.append("{:25}".format(d['journal_source_name'][:25]))
    temp.append("{:25}".format(d['batch_name'][:25]))
    temp.append("{:1}".format(d['seq'][:1]))
    temp.append("{:10}".format(d['ofin_for_cfs'][:10]))
    temp.append("{:240}".format(d['account_description'][:240]))
   
    debit_accum = debit_accum + debit
    credit_accum = credit_accum + credit
    result.append("".join(temp))

  return result, debit_accum, credit_accum


def generate_data_file(output_path, str_date, bu, data):
  prefix = 'ZY' + str_date + bu[:2]
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result, debit, credit = prepare_data(data)
    dat.write("\n".join(result))
    val.write('{:15}{:0>10}{:015.2f}{:015.2f}'.format(dat_file, len(result), debit, credit))
    print('[AutoPOS] - OFIN[{}] create files completed..'.format(bu))


def main():
  batch_date = datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  try:
    bus = ['CDS', 'CBN', 'SPB', 'B2N']
    for bu in bus:
      target_path = os.path.join(parent_path, 'output/autopos/ofin/zy/{}'.format(bu.lower()), batch_date.strftime('%Y%m%d'))
      if not os.path.exists(target_path):
        os.makedirs(target_path)

      refresh_view = "refresh materialized view mv_autopos_ofin"
      sql = "select * from mv_autopos_ofin where (credit + debit) > 0 and interface_date = '{}' and bu = '{}'".format(batch_date.strftime('%Y%m%d'), bu)
      data = query_matview(refresh_view, sql)
      generate_data_file(target_path, batch_date.strftime('%y%m%d'), bu, data)
  
      destination = 'incoming/ofin/zy/{}'.format(bu.lower())
      sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - OFIN Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
