from datetime import datetime, timedelta
import os
import sys
import pymssql

_date = datetime.strptime('2017-08-03', '%Y-%m-%d')


def connect_db():
  return pymssql.connect('10.17.221.173', 'coreii', 'co@2ii!', 'DBMKP')


def ofin_ap_trans_header(fromdate, todate):
  '''
    ofin_ap_trans_header call store spc_OFIN_APTranHead and return
    rows that return from store procedure
    '''
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_OFIN_APTranHead', (fromdate, todate))
      return [row for row in cursor]


def checkrule_goodsreceiving_rtv_mer_header():
  '''
    checkrule_goodsreceiving_rtv_mer_header query table TBOFINAPTransUFMT_Temp
    and return rows
    '''
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.execute('SELECT * FROM TBOFINAPTransUFMT_Temp')
      return [row for row in cursor]


def check_vendor_numberH(dt_merl):
  vendor_num_key = 'Vendor_num'
  return len([
      row for row in dt_merl
      if row[vendor_num_key] == ('0' * 6) or row[vendor_num_key] == ('0' * 5)
  ]) > 0


def main_checkrule_merh(dt_merh):
  return check_vendor_numberH(dt_merh)


def gen_seq_number(files, prefix_file_name, prefix_len):
  f_has_prefix = [f for f in files if f[0:prefix_len] == prefix_file_name]
  if len(f_has_prefix) > 0:
    return max([int(date[1:prefix_len]) for date in f_has_prefix]) + 1
  else:
    return 1


def generate_text_files_mer_header(data, fromdate, todate):
  # dt_merh is data table from TBOFINAPTransUFMT_Temp
  dt_merh = checkrule_goodsreceiving_rtv_mer_header()
  if main_checkrule_merh(dt_merh):
    print('main_checkrule_merh failed')
    return

  parent_folder = os.path.dirname(os.path.realpath(__file__))
  dpath = parent_folder + '/BACKUP_OFIN/' + 'D' + _date.strftime('%Y-%m-%d')
  if not os.path.exists(dpath):
    os.makedirs(dpath)
  sp = fromdate.strftime('%y%m%d')
  prefix_filename_mer = 'H{0}'.format(sp)
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
        writer.write(
            '%-3s%-50s%-30s%-6s%-1s%-13s%-6s%-1s%-1s%-2s%-5s%-50s%-50s%-10s%-3s%-4s%-2s%-15s%-50s%-14s%-6s%-10s%-50s\n'
            % (line['Source'], line['Invoice_num'], line['Vendor_num'],
               line['Invoice_date'].strftime('%d%m%y'),
               get_invoice_type_dash_or_zero(line['Invoice_type']),
               '{:0>13}'.format(line['Invoice_total']), line['Store_id'],
               line['Invoice_type'], line['Imported_goods'],
               line['Hold_reason01'], line['Invoice_tax_name'],
               line['Tax_inv_running_no'], line['Blank1'], line['RTV_auth_no'],
               line['Currency_code'], line['Terms'], line['Blank2'],
               line['Gr_tran_no'], line['Ass_tax_invoice_num'], line['Blank3'],
               "" if line['Tax_inv_running_no'] == "" else
               line['Tax_Invoice_Date'].strftime("%d%m%y"),
               line['Invoice_RTV_Type'], line['CurrencyRate']))

      print(' Create Files AP(Header) .MER Complete..')
    except Exception as e:
      print(' Create Files AP(Header) .MER Error .. : ')
      print(str(e))
  create_data_mer_header(path)


def create_data_mer_header(path):
  data = get_data_from_file(path)
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.execute('TRUNCATE TABLE TBOFINAPTransFMT_Temp')
      query = """
            INSERT INTO TBOFINAPTransFMT_Temp
            (Source, Invoice_num, Vendor_num, Invoice_date, Invoice_total, Store_id,Invoice_type, Imported_goods,
            Hold_reason01, Invoice_tax_name, Tax_inv_running_no, Blank1, RTV_auth_no,
            Currency_code, Terms, Blank2, Gr_tran_no,Ass_tax_invoice_num, Blank3,
            Tax_Invoice_Date, Invoice_RTV_Type, CurrencyRate)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %d, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %d)
            """
      cursor.executemany(query, generate_data_mer_header(data))


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


def generate_data_mer_header(data):
  return [(sub_string_mer(1, 3, line), sub_string_mer(4, 53, line),
           sub_string_mer(54, 83, line), sub_string_mer(84, 89, line),
           sub_string_mer(90, 103, line), sub_string_mer(104, 109, line),
           sub_string_mer(110, 110, line), sub_string_mer(111, 111, line),
           sub_string_mer(112, 113, line), sub_string_mer(114, 118, line),
           sub_string_mer(119, 168, line), sub_string_mer(169, 218, line),
           sub_string_mer(219, 228, line), sub_string_mer(229, 231, line),
           sub_string_mer(232, 235, line), sub_string_mer(236, 237, line),
           sub_string_mer(238, 247, line), sub_string_mer(248, 297, line),
           sub_string_mer(298, 311, line), sub_string_mer(312, 317, line),
           sub_string_mer(318, 327, line), sub_string_mer(328, 377, line))
          for line in data]


def sub_string_mer(start, end, line):
  try:
    return line[start - 1:to]
  except Exception as e:
    return line[end - 1:len(line)]


if __name__ == '__main__':
  fromdate = _date
  todate = _date
  dir_path = os.path.dirname(os.path.realpath(__file__))

  if fromdate > todate:
    print('Please check date because from date greater than to date')
    sys.exit(1)

  data = ofin_ap_trans_header(fromdate, todate)
  generate_text_files_mer_header(data, fromdate, todate)
