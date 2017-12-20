import os
import pymssql
from datetime import datetime, timedelta

_date = datetime.strptime('2017-11-19', '%Y-%m-%d')


def connect_db():
  return pymssql.connect('10.17.221.173', 'coreii', 'co@2ii!', 'DBMKP')


def check_debit_equal_to_credit(dtZL):
  sum_debit = sum(row['Debit'] for row in dtZL)
  sum_credit = sum(row['Credit'] for row in dtZL)

  if sum_debit > sum_credit:
    print('SaleAndSaleReturn_ZL : Debit > Credit : %.2f' %
          (sum_debit - sum_credit))
    return False
  elif sum_debit < sum_credit:
    print('SaleAndSaleReturn_ZL : Debit < Credit : %.2f' %
          (sum_credit - sum_debit))
    return False
  return True


def check_all_records_has_CPCID(dtZL):
  if any(not row['CPCID'] for row in dtZL):
    print('CPCID is null')
    return False
  return True


def validate_dtZL(dtZL):
  return check_debit_equal_to_credit(dtZL) & check_all_records_has_CPCID(dtZL)


def get_next_seq(files, prefix_filename, prefix_length):
  if not files:
    return 1
  else:
    try:
      seq = max(
          int(filename[prefix_length:prefix_length + 1])
          if filename.startswith(prefix_filename) else 0 for filename in files)
      return seq + 1
    except Exception as e:
      print('GenSeqNumber Error %s' % str(e))


def generate_data(date):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('SPC_GenARTRNDTL_3PL')
      cursor.callproc('spc_OFIN_SaleAndSaleReturn', (date, ))
      data = [row for row in cursor]

      cursor.execute('SELECT * FROM TBOFINSaleAndSaleReturnUFMT_Temp')
      dtZL = [row for row in cursor]

  return data, dtZL


def generate_report(output_path, date, data):
  date = date.strftime('%y%m%d')
  prefix_filename_ZL = 'ZL' + date
  seq = get_next_seq(
      [filename.split('.')[0]
       for filename in os.listdir(output_path)], prefix_filename_ZL, 8)
  ZL_name_dat = prefix_filename_ZL + str(seq) + '.DAT'
  ZL_name_dat_file_path = os.path.join(output_path, ZL_name_dat)
  ZL_name_val = prefix_filename_ZL + str(seq) + '.VAL'
  ZL_name_val_file_path = os.path.join(output_path, ZL_name_val)

  with open(ZL_name_dat_file_path, 'w') as dat_writer, open(
      ZL_name_val_file_path, 'w') as val_writer:
    try:
      line_count = 0
      for line in data:
        if line_count > 0:
          dat_writer.write('\n')
        dat_writer.write(
            '%-6s%-5s%-8s%-6s%-6s%012.2f%012.2f%-20s%-20s%-20s%-10s%-240s' %
            (line['Store'], line['CPCID'], line['AccountCode'],
             line['SubAccount'], line['AccountDate'].strftime('%d%m%y'),
             line['Debit'], line['Credit'], line['JournalSourceName'],
             line['JournalCategoryName'], line['BatchName'], line['CFSFlag'],
             line['Description']))
        line_count = line_count + 1

      val_writer.write('%-14s%-10s' % (ZL_name_dat,
                                       '{:0>10}'.format(str(line_count))))
      print('Create Files ZL .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZL .DAT & .VAL Error .. : ')
      print(str(e))


def get_report_path(date):
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 'D' + date.strftime('%Y-%m-%d')
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)
  return target_path


def main():
  date = _date
  data, dtZL = generate_data(date)

  if not validate_dtZL(dtZL):
    return

  target_path = get_report_path(date)
  generate_report(target_path, date, data)


if __name__ == '__main__':
  main()
