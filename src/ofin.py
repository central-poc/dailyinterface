from common import connect_psql
from datetime import datetime
import os
import psycopg2.extras


def generate_data_file(output_path, str_date, data):
  dat_file = 'ZY' + str_date + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = 'ZY' + str_date + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    try:
      count = 0
      for d in data:
        if count > 0:
          dat.write('\n')
        dat.write(
            "{:6}{:5}{:8}{:6}{:6}{:012.2f}{:012.2f}{:4}{:25}{:25}{:25}{:1}{:10}{:240}".
            format(d['ofin_branch_code'], d['ofin_cost_profit_center'],
                   d['account_code'], d['subaccount_code'], d['business_date'],
                   d['debit'], d['credit'], d['bu'],
                   d['journal_category_name'], d['journal_source_name'],
                   d['batch_name'], d['seq'], d['ofin_for_cfs'],
                   d['account_description']))
        count = count + 1

      val.write('{:14}{:10}'.format(dat_file, count))
      print('[AutoPOS] - OFIN Create Files Complete..')
    except Exception as e:
      print('[AutoPOS] - OFIN Create Files Error: {}: '.format(e))


def query_data(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_ofin where to_char(trans_date, 'YYYYMMDD') = %s"
      cursor.execute(sql, (str_date, ))

      return cursor.fetchall()


def main():
  str_date = datetime.today().strftime('%Y%m%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    data = query_data(str_date)
    generate_data_file(target_path, str_date, data)

  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
