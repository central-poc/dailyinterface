from common import connect_psql, get_file_seq, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback


def is_debit_equals_credit(data_zn):
  sum_debit = sum([row['debit'] for row in data_zn])
  sum_credit = sum([row['credit'] for row in data_zn])
  print('[AutoPOS] - ZN Debit: {}, Credit: {}'.format(sum_debit, sum_credit))
  return False if sum_debit != sum_credit else True


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
    temp.append("{:6}".format(d['business_date'][:6]))
    temp.append("{:012.2f}".format(debit))
    temp.append("{:012.2f}".format(credit))
    temp.append("{:20}".format(d['journal_source_name'][:20]))
    temp.append("{:20}".format(d['journal_category_name'][:20]))
    temp.append("{:20}".format(d['batch_name'][:20]))
    temp.append("{:10}".format(d['ofin_for_cfs'][:10]))
    temp.append("{:240}".format(d['account_description'][:240]))
    temp.append("{:80}".format(d['journal_name'][:80]))

    debit_accum = debit_accum + debit
    credit_accum = credit_accum + credit
    result.append("".join(temp))

  return result, debit_accum, credit_accum


def generate_data_file(output_path, str_date, data):
  prefix = 'ZN' + str_date + 'CD'
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result, debit, credit = prepare_data(data)
    dat.write("\n".join(result))
    val.write('{:15}{:0>10}{:015.2f}{:015.2f}'.format(dat_file, len(result),
                                                      debit, credit))
    print('[AutoPOS] - ZN .DAT & .VAL Completed..')


def main():
  batch_date = datetime.strptime(
      sys.argv[1],
      '%Y%m%d') if len(sys.argv) > 1 else datetime.now() - timedelta(days=1)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin/gl',
                             batch_date.strftime('%Y%m%d'))
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    refresh_view = "refresh materialized view mv_autopos_ofin_zn"
    sql = "select * from mv_autopos_ofin_zn where (credit + debit) > 0 and interface_date = '{}'".format(
        batch_date.strftime('%Y%m%d'))
    data = query_matview(refresh_view, sql)
    if not is_debit_equals_credit(data):
      return

    generate_data_file(target_path, batch_date.strftime('%y%m%d'), data)

    destination = 'incoming/ofin/gl'
    sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - ZN Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
