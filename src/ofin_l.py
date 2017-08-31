from datetime import datetime, timedelta
import os
import sys
import pymssql

_date = datetime.strptime('2017-08-03', '%Y-%m-%d')


def connect_db():
  return pymssql.connect('10.17.221.173', 'coreii', 'co@2ii!', 'DBMKP')


def ofin_ap_trans_line(fromdate, todate):
  '''
    ofin_ap_trans_line call store spc_OFIN_APTransLine and return
    rows that return from store procedure
    '''
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_OFIN_APTransLine', (fromdate, todate))
      return [row for row in cursor]


def checkrule_goodsreceiving_rtv_mer_line():
  '''
    checkrule_goodsreceiving_rtv_mer_line query table TBOFINAPTransLneUFMT_Temp
    and return rows
    '''
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.execute('SELECT * FROM TBOFINAPTransLineUFMT_Temp')
      return [row for row in cursor]


def check_vendor_namel(dt_merl):
  vendor_num_key = 'Vendor_number'
  return len([
      row for row in dt_merl
      if row[vendor_num_key] == ('0' * 6) or row[vendor_num_key] == ('0' * 5)
  ]) > 0


def main_checkrule_merl(dt_merl):
  return check_vendor_namel(dt_merl)


def gen_seq_number(files, prefix_file_name, prefix_len):
  f_has_prefix = [f for f in files if f[0:prefix_len] == prefix_file_name]
  if len(f_has_prefix) > 0:
    return max([int(date[1:prefix_len]) for date in f_has_prefix]) + 1
  else:
    return 1


def generate_text_files_mer_line(data, fromdate, todate):
  # dt_merl is data table from TBOFINAPTransLineUFMT_Temp
  dt_merl = checkrule_goodsreceiving_rtv_mer_line()
  if main_checkrule_merl(dt_merl):
    print('main_checkrule_merl failed')
    return

  parent_folder = os.path.dirname(os.path.realpath(__file__))
  dpath = parent_folder + '/BACKUP_OFIN/' + 'D' + _date.strftime('%Y-%m-%d')
  if not os.path.exists(dpath):
    os.makedirs(dpath)
  sp = fromdate.strftime('%y%m%d')
  prefix_filename_mer = 'L{0}'.format(sp)
  # TODO: change 7 to len from prefix_filename_mer
  seq = gen_seq_number([
      filename for filename in os.listdir(dpath) if filename.endswith('.MER')
  ], prefix_filename_mer, len(sp))
  mer_name_line = '{}{}{}'.format(prefix_filename_mer, seq, '.MER')
  path = parent_folder + '/BACKUP_OFIN/' + 'D' + _date.strftime(
      '%Y-%m-%d') + '/' + mer_name_line
  with open(path, 'w') as writer:
    try:
      for line in data:
        writer.write('%-3s%-50s%-1s%-30s%-1s%-240s%-1s%-13s%-14s%-14s\n' %
                     (line['Source'], line['Invoice_num'],
                      line['Invoice_type'], line['Vendor_number'],
                      line['Line_type'], line['Item_description'],
                      get_invoice_type_dash_or_zero(line['Invoice_type']),
                      '{:0>13}'.format(line['Amount']), ""
                      if line['Item_qty'] == None else line['Item_qty'], ""
                      if line['Item_cost'] == None else line['Item_cost']))

      print(' Create Files AP(Line) .MER Complete..')
    except Exception as e:
      print(' Create Files AP(Line) .MER Error .. : ')
      print(str(e))
  create_data_mer_line(path)


def create_data_mer_line(path):
  data = get_data_from_file(path)
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.execute('TRUNCATE TABLE TBOFINAPTransLineFMT_Temp')
      query = """
            INSERT INTO TBOFINAPTransLineFMT_Temp
            (Source, Invoice_num, Invoice_type, Vendor_number, Line_type, Item_description, Amount, Item_qty, Item_cost)
            VALUES
            (%s, %s, %s, %s, %s, %s, %d, %d, %d)
            """
      cursor.executemany(query, generate_data_mer_line(data))


def get_invoice_type_dash_or_zero(invoice_type):
  if invoice_type in [
      '4', '5', '6', '7', 'c', 'C', 'd', 'D', 'e', 'E', 'f', 'F'
  ]:
    return '-'
  else:
    return '0'


def get_data_from_file(path):
  with open(path) as f:
    return [line.rstrip('\n') for line in f]


def generate_data_mer_line(data):
  return [(sub_string_merline(1, 3, line), sub_string_merline(
      4, 53, line), sub_string_merline(54, 54, line), sub_string_merline(
          55, 84, line), sub_string_merline(85, 85, line), sub_string_merline(
              86, 325, line), sub_string_merline(326, 339, line),
           sub_string_merline(340, 353, line), sub_string_merline(
               354, 367, line)) for line in data]


def sub_string_merline(start, end, line):
  try:
    return line[start - 1:end]
  except Exception as e:
    return line[start - 1:len(line)]


if __name__ == '__main__':
  fromdate = _date
  todate = _date
  dir_path = os.path.dirname(os.path.realpath(__file__))

  if fromdate > todate:
    print('Please check date because from date greater than to date')
    sys.exit(1)

  data = ofin_ap_trans_line(fromdate, todate)
  generate_text_files_mer_line(data, fromdate, todate)
