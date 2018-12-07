from common import connect_psql
from datetime import datetime, timedelta
import os
import psycopg2.extras


def get_file_seq(prefix, output_path, ext):
  files = [
      f.split('.')[0] for f in os.listdir(output_path)
      if os.path.isfile(os.path.join(output_path, f)) and f.endswith(ext)
  ]
  return 1 if not files else max(
      int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def generate_data_file(output_path, str_date, bu, data):
  prefix = 'ZY' + str_date + bu[:2]
  seq = get_file_seq(prefix, output_path, '.DAT')
  dat_file = prefix + str(seq) + '.DAT'
  dat_file_path = os.path.join(output_path, dat_file)
  val_file = prefix + str(seq) + '.VAL'
  val_file_path = os.path.join(output_path, val_file)

  with open(dat_file_path, 'w') as dat, open(val_file_path, 'w') as val:
    try:
      count = 0
      debit_accum = 0
      credit_accum = 0
      for d in data:
        if count > 0:
          dat.write('\n')
        debit = d['debit']
        credit = d['credit']
        dat.write(
            "{:6}{:5}{:8}{:6}{:9}{:012.2f}{:012.2f}{:4}{:25}{:25}{:25}{:1}{:10}{:240}".
            format(d['ofin_branch_code'][:6], d['ofin_cost_profit_center'][:5],
                   d['account_code'][:8], d['subaccount_code'][:6], d['invoice_date'][:9],
                   debit, credit, d['bu'][:4],
                   d['journal_category_name'][:25], d['journal_source_name'][:25],
                   d['batch_name'][:25], d['seq'][:1], d['ofin_for_cfs'][:10],
                   d['account_description'][:240]))
        count = count + 1
        debit_accum = debit_accum + debit
        credit_accum = credit_accum + credit

      val.write('{:15}{:10}{:015.2f}{:015.2f}'.format(dat_file, count, debit_accum, credit_accum))
      print('[AutoPOS] - OFIN Create Files Complete..')
    except Exception as e:
      print('[AutoPOS] - OFIN Create Files Error: {}: '.format(e))


def query_data(str_date, bu):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "refresh materialized view mv_autopos_ofin"
      cursor.execute(sql)
      sql = "select * from mv_autopos_ofin where business_date = %s and bu = %s"
      cursor.execute(sql, (str_date, bu, ))

      return cursor.fetchall()


def query_bu(str_date):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "refresh materialized view mv_autopos_ofin"
      cursor.execute(sql)
      cursor.execute(
          "select bu from mv_autopos_ofin where business_date = %s group by bu",
          (str_date, )
      )

      return cursor.fetchall()


def main():
  str_date = (datetime.now() - timedelta(days=1)).strftime('%y%m%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/ofin', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    bus = [x['bu'] for x in query_bu(str_date)]
    for bu in bus:
      data = query_data(str_date, bu)
      generate_data_file(target_path, str_date, bu, data)

  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
