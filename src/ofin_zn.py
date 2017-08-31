import os
from datetime import datetime, timedelta
import pymssql

_date = datetime.strptime('2017-08-29', '%Y-%m-%d')


def connect_db():
  return pymssql.connect('10.17.220.173', 'coreii', 'co@2ii!', 'DBMKP')


def getorderid_genARTranMST(curr_date, order_type):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      if order_type != "Normal":
        command = '''
                  select orderid from tborderhead t1
                    inner join  tbcustpayment t2 on t1.paymenttype = t2.paymenttype
                    where IsOnlinePayment = 'Yes'
                    and	IsGenSale = 'No'
                    and OrderType = 'Preorder'
                    and cast(PaymentDate as date) = cast('{}' as date)'''.format(
            curr_date)
      else:
        command = '''
                  select orderid from tborderhead t1
                    inner join  tbcustpayment t2 on t1.paymenttype = t2.paymenttype
                    where IsOnlinePayment = 'Yes'
                    and	IsGenSale = 'No'
                    and OrderType = 'Normal'
                    and cast(PaymentDate as date) = cast('{}' as date)'''.format(
            curr_date)

      cursor.execute(command)
      dt = [row for row in cursor]
  return dt


def genARTranMST(orderid, is_prepaid):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_GenARTranMST', (
          orderid,
          'Server',
          is_prepaid,
          'Sale',
          'No', ))
      return cursor.fetchone()


def get_return_agent(flag):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      if flag:
        command = '''
                    select suborderid
                        from TBSubOrderHead
                        where status in ('ReadyToShip', 'Shipping', 'Delivery')
                        and IsGenRTC = 'No'
                        and shippingid <> 4
                        and netamt <> oldnetamt
                '''
      else:
        command = '''
                    select t1.suborderid
                        from TBSubOrderHead t1
                        inner join  tbcustpayment t2 on t1.paymenttype = t2.paymenttype
                        where t2.IsOnlinePayment = 'Yes'
                        and t1.status = 'Canceled'
                        and t1.IsGenRTC = 'No'
                '''

      cursor.execute(command)
      dt = [row for row in cursor]
  return dt


def get_return_agentservice():
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      command = '''
                SELECT ServiceNo
                FROM TBOtherServiceHead
                WHERE Status = 'Canceled'
                AND IsGenRTC = 'No'
            '''
      cursor.execute(command)
      dt = [row for row in cursor]
  return dt


def gen_return_agent(suborderid, flag=False):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      parameter = (suborderid, )
      if flag:
        parameter = parameter + ('SER', )
      else:
        parameter = parameter + ('INV', )
      cursor.callproc('SPC_GENTBReturnAgent', parameter)


def gen_receipt_and_adjust(curr_date):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_OFIN_CustomerReceiptAndAdjust', (curr_date, ))


def check_debit_equal_to_credit(dtZN):
  sum_debit = sum([row['Debit'] for row in dtZN])
  sum_credit = sum([row['Credit'] for row in dtZN])

  if sum_debit > sum_credit:
    print('ReceiptAndAdjust_ZN : Debit > Credit : %.2f' %
          (sum_debit - sum_credit))
    return False
  elif sum_debit < sum_credit:
    print('ReceiptAndAdjust_ZN : Debit < Credit : %.2f' %
          (sum_credit - sum_debit))
    return False
  return True


def check_all_records_has_CPCID(dtZN):
  is_CPCID_empty_or_null = [1 if not row['CPCID'] else 0 for row in dtZN]

  if sum(is_CPCID_empty_or_null) > 0:
    print('CPCID is null')
    return False
  return True


def validate_dtZN(dtZN):
  return check_debit_equal_to_credit(dtZN) & check_all_records_has_CPCID(dtZN)


def generate_data(date):
  with connect_db() as conn:
    with conn.cursor(as_dict=True) as cursor:
      cursor.callproc('spc_OFIN_CustomerReceiptAndAdjust', (date, ))
      data = [row for row in cursor]

      cursor.execute('Select * From TBOFINCustomerReceiptAndAdjust_Temp')
      dtZN = [row for row in cursor]

  return data, dtZN


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
  prefix_filename_ZN = 'ZN' + date
  seq = get_next_seq(
      [filename.split('.')[0]
       for filename in os.listdir(output_path)], prefix_filename_ZN, 8)
  ZN_name_dat = prefix_filename_ZN + str(seq) + '.DAT'
  ZN_name_dat_file_path = os.path.join(output_path, ZN_name_dat)
  ZN_name_val = prefix_filename_ZN + str(seq) + '.VAL'
  ZN_name_val_file_path = os.path.join(output_path, ZN_name_val)

  with open(ZN_name_dat_file_path, 'w') as writerDAT, open(
      ZN_name_val_file_path, 'w') as writerVAL:
    try:
      line_count = 0
      for line in data:
        if line_count > 0:
          writerDAT.write('\n')
        writerDAT.write(
            '%-6s%-5s%-8s%-6s%-6s%012.2f%012.2f%-20s%-20s%-20s%-10s%-240s' %
            (line['Store'], line['CPCID'], line['AccountCode'],
             line['SubAccount'], line['AccountDate'].strftime('%d%m%y'),
             line['Debit'], line['Credit'], line['JournalSourceName'],
             line['JournalCategoryName'], line['BatchName'], line['CFSFlag'],
             line['Description']))
        line_count = line_count + 1

      writerVAL.write('%-14s%-10s' % (ZN_name_dat, '{:0>10}'.format(str(line_count))))
      print('Create Files ZN .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZN .DAT & .VAL Error .. : ')
      print(str(e))


def main():
  curr_date = _date
  last_date = _date
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 'D' + last_date.strftime('%Y-%m-%d')
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    dt_orderid_normal = getorderid_genARTranMST(last_date, "Normal")
    suborders_normal = [
        row['orderid'] for row in dt_orderid_normal
        if genARTranMST(row['orderid'], "No") != 0
    ]

    dt_orderid_preorder = getorderid_genARTranMST(last_date, "Preorder")
    suborders_preorder = [
        row['orderid'] for row in dt_orderid_preorder
        if genARTranMST(row['orderid'], "Yes") != 0
    ]

    suborders = suborders_normal + suborders_preorder
    if suborders:
      print(
          'CMOS Interface To Oracle (OFIN) ({}) : Text ZN Error SubOrderId {}'
          .format(datetime.now(), ','.join(suborders)))

    dt_suborders = get_return_agent(False)
    [gen_return_agent(suborderid['suborderid']) for suborderid in dt_suborders]

    dt_suborders = get_return_agent(True)
    [gen_return_agent(suborderid['suborderid']) for suborderid in dt_suborders]

    dt_suborders = get_return_agentservice()
    [
        gen_return_agent(suborderid['ServiceNo'], True)
        for suborderid in dt_suborders
    ]

    data, dtZN = generate_data(last_date)

    if not validate_dtZN(dtZN):
      return

    generate_report(target_path, curr_date, data)

  except Exception as e:
    print('Get Data ZN From Stored Procedure Error: %s' % str(e))


if __name__ == '__main__':
  main()
