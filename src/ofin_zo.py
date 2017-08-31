import os
import pymssql
from datetime import datetime, timedelta

_date = datetime.strptime('2017-08-03', '%Y-%m-%d')


def generate_data(date):
  with pymssql.connect('10.17.221.173', 'coreii', 'co@2ii!', 'DBMKP') as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_OFIN_GoodsReceiveAndRTV', (date, ))
      data = [row for row in cursor]

      cursor.execute('Select * From TBOFINGoodReceiveAndRTVUFMT_Temp')
      dtZO = [row for row in cursor]
  return data, dtZO


def check_debit_equal_to_credit(dtZO):
  sum_debit = sum(row['Debit'] for row in dtZO)
  sum_credit = sum(row['Credit'] for row in dtZO)

  if sum_debit > sum_credit:
    print('GoodsReceiving_ZO : Debit > Credit : %.2f' %
          (sum_debit - sum_credit))
    return False
  elif sum_debit < sum_credit:
    print('GoodsReceiving_ZO : Debit < Credit : %.2f' %
          (sum_credit - sum_debit))
    return False
  return True


def check_all_records_has_CPCID(dtZO):
  if any(not row['CPCID'] for row in dtZO):
    print('CPCID is null')
    return False
  return True


def validate_dtZO(dtZO):
  return check_debit_equal_to_credit(dtZO) & check_all_records_has_CPCID(dtZO)


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


def generate_report(output_path, date, data):
  date = date.strftime('%y%m%d')
  prefix_filename_ZO = 'ZO' + date
  seq = get_next_seq(
      [filename.split('.')[0]
       for filename in os.listdir(output_path)], prefix_filename_ZO, 8)
  ZO_name_dat = prefix_filename_ZO + str(seq) + '.DAT'
  ZO_name_dat_file_path = os.path.join(output_path, ZO_name_dat)
  ZO_name_val = prefix_filename_ZO + str(seq) + '.VAL'
  ZO_name_val_file_path = os.path.join(output_path, ZO_name_val)

  with open(ZO_name_dat_file_path, 'w') as dat_writer, open(
      ZO_name_val_file_path, 'w') as val_writer:
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

      val_writer.write('%-14s%-10s' % (ZO_name_dat, str(line_count)))
      print('Create Files ZO .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZO .DAT & .VAL Error .. : ')
      print(str(e))


def get_report_path(date):
  dir_path = os.path.dirname(os.path.realpath(__file__))
  target_dir = 'D' + date.strftime('%Y-%m-%d')
  target_path = os.path.join(dir_path, 'BACKUP_OFIN', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)
  return target_path


def main():
  date = _date
  data, dtZO = generate_data(date)

  if not validate_dtZO(dtZO):
    return

  target_path = get_report_path(date)
  generate_report(target_path, date, data)


if __name__ == '__main__':
  main()
