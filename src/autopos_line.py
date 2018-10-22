from common import connect_psql
from datetime import datetime
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
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    try:
      count = 0
      for line in data:
        if count > 0:
          dat.write('\n')
        dat.write(
            "{:6}{:5}{:8}{:6}{:6}{:012.2f}{:012.2f}{:20}{:20}{:20}{:10}{:240}".
            format(line['ofin_branch_code'], line['ofin_cost_profit_center'],
                   line['account_code'], line['subaccount_code'],
                   line['business_date'], line['debit'], line['credit'],
                   line['journal_source_name'], line['journal_category_name'],
                   line['batch_name'], line['ofin_for_cfs'],
                   line['account_description']))
        count = count + 1

      val.write('{:14}{:10}'.format(dat_file, count))
      print('Create Files ZL .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZL .DAT & .VAL Error .. : ')
      print(str(e))


def query_data(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_ofin_line where to_char(trans_date, 'YYYYMMDD') = %s"
      cursor.execute(sql, (str_date, ))
      
      return cursor.fetchall()


def main():
  str_date = datetime.today().strftime('%Y%m%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofindaily', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    data = query_data(str_date)
    generate_data_file(target_path, str_date, data)

  except Exception as e:
    print('[L] - Error: %s' % str(e))


if __name__ == '__main__':
  main()
