from common import connect_psql, get_file_seq
from datetime import datetime, timedelta
import os
import psycopg2.extras


def is_debit_equals_credit(data):
  sum_debit = sum([row[5] for row in data])
  sum_credit = sum([row[6] for row in data])
  print('[ZL] - Debit: {}, Credit: {}'.format(sum_debit, sum_credit))
  return False if sum_debit != sum_credit else True


def prepare_data(data):
  result = []
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
   
    result.append("".join(temp))

  return result


def generate_data_file(output_path, str_date, data):
  prefix = 'ZL' + str_date
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    result = prepare_data(data)
    dat.write("\n".join(data))
    val.write('{:14}{:10}'.format(dat_file, len(result)))
    print('Create Files ZL .DAT & .VAL Complete..')


def query_data(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "refresh materialized view mv_autopos_ofin_zl"
      cursor.execute(sql)
      sql = "select * from mv_autopos_ofin_zl where business_date = %s"
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
    data = query_data(str_date)
    if not is_debit_equals_credit(data):
      return

    generate_data_file(target_path, str_date, data)

  except Exception as e:
    print('Get Data ZL From Stored Procedure Error: %s' % str(e))


if __name__ == '__main__':
  main()
