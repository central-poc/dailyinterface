from common import connect_psql
import os


def is_debit_equals_credit(data_zn):
  sum_debit = sum([row[5] for row in data_zn])
  sum_credit = sum([row[6] for row in data_zn])
  print('[ZN] - Debit: {}, Credit: {}'.format(sum_debit, sum_credit))
  return False if sum_debit != sum_credit else True


def get_file_seq(prefix, output_path, ext):
  files = [f.split('.')[0] for f in os.listdir(output_path)
            if os.path.isfile(os.path.join(output_path,f)) and f.endswith(ext)]
  return 1 if not files else max(int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def generate_data_file(output_path, str_date, data):
  prefix = 'ZN' + str_date
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
        dat.write('''
          {:<6}{:<5}{:<8}{:<6}{:<6}{:012.2f}{:012.2f}
          {:<20}{:<20}{:<20}{:<10}{:<240}
        '''.format(
          line[0], line[1], line[2], line[3], line[4], line[5], line[6],
          line[7], line[8], line[9], line[10], line[11]
        ))
        count = count + 1

      val.write('{:<14}{:<10}'.format(dat_file, count))
      print('Create Files ZN .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZN .DAT & .VAL Error .. : ')
      print(str(e))


def query_data(str_date):
  with connect_psql() as conn:
      with conn.cursor() as cursor:
        sql = """
        select 
          '056401' as ofin_branch_code, cost_center as ofin_cost_profit_center, 
          account_code, subaccount_code, to_char(trans_date, 'DDMMYY') as business_date, 
          sum(debit + additional_amt) as debit, sum(credit + additional_amt) as credit, 
          'CMOS-Receipts' as journal_source_name, 'RV-Receipts' as journal_category_name, 
          'RECEIPT-' || to_char(trans_date, 'YYMMDD') as batch_name,
          ofin_for_cfs, account_description
        from artransaction
        where doc_type = 'INV'
        and trans_type = 'S'
        and to_char(trans_date, 'YYYYMMDD') = %s
        group by account_code, subaccount_code, cost_center, account_description, trans_date, ofin_for_cfs
        union all
        select 
          '056401' as ofin_branch_code, cost_center as ofin_cost_profit_center, 
          account_code, subaccount_code, to_char(trans_date, 'DDMMYY') as business_date, 
          sum(debit + additional_amt) as debit, sum(credit + additional_amt) as credit, 
          'CMOS-Receipts' as journal_source_name, 'RC-Return' as journal_category_name, 
          'RECEIPT-' || to_char(trans_date, 'YYMMDD') as batch_name,
          ofin_for_cfs, account_description
        from artransaction
        where doc_type = 'CN'
        and trans_type = 'R'
        and to_char(trans_date, 'YYYYMMDD') = %s
        group by account_code, subaccount_code, cost_center, account_description, trans_date, ofin_for_cfs
        """
        cursor.execute(sql, (str_date, str_date,))
        return cursor.fetchall()


def main():
  str_date = '20180820'
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/ofin', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    data = query_data(str_date)
    if not is_debit_equals_credit(data):
      return

    generate_data_file(target_path, str_date, data)

  except Exception as e:
    print('Get Data ZN From Stored Procedure Error: %s' % str(e))


if __name__ == '__main__':
  main()
