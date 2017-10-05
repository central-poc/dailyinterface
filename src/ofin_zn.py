import os
from datetime import datetime, timedelta
import _mssql

_date = datetime.strptime('2017-08-27', '%Y-%m-%d')


def mssql_db():
  return _mssql.connect(
      server='10.17.220.173',
      user='coreii',
      password='co@2ii!',
      database='DBMKP')


def get_orders_by_date_type(curr_date, order_type):
  with mssql_db() as conn:
    if order_type == "Preorder":
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
                  and OrderType in ('Normal', 'ByOrder')
                  and cast(PaymentDate as date) = cast('{}' as date)'''.format(
          curr_date)

    conn.execute_query(command)
    data = [row for row in conn]
  return data


def execute_ar_transaction(orderid, is_prepaid):
  with mssql_db() as conn:
    sql = """
      DECLARE @out INT;
      EXEC dbo.spc_GenARTranMST %s, %s, %s, %s, %s, @out OUT;
      SELECT @out;
    """
    out = conn.execute_scalar(sql, (
        orderid,
        'Server',
        is_prepaid,
        'Sale',
        'No', ))
  return out


def get_return_agent():
  with mssql_db() as conn:
    command = '''
      select suborderid
            from TBSubOrderHead
            where status in ('ReadyToShip', 'Shipping', 'Delivery')
            and IsGenRTC = 'No'
            and shippingid <> 4
            and netamt <> oldnetamt
      union all
      select t1.suborderid
          from TBSubOrderHead t1
          inner join  tbcustpayment t2 on t1.paymenttype = t2.paymenttype
          where t2.IsOnlinePayment = 'Yes'
          and t1.status = 'Canceled'
          and t1.IsGenRTC = 'No'
    '''
    conn.execute_query(command)
    data = [row for row in conn]
  return data


def get_return_agentservice():
  with mssql_db() as conn:
    command = '''
              SELECT ServiceNo
              FROM TBOtherServiceHead
              WHERE Status = 'Canceled'
              AND IsGenRTC = 'No'
          '''
    conn.execute_query(command)
    data = [row for row in conn]
  return data


def execut_return_agent(suborderid, return_type):
  with mssql_db() as conn:
    sql = "EXEC dbo.SPC_GENTBReturnAgent %s, %s;"
    conn.execute_scalar(sql, (
        suborderid,
        return_type, ))


def generate_temp_data(curr_date):
  with mssql_db() as conn:
    sql = "EXEC dbo.spc_OFIN_CustomerReceiptAndAdjust %s;"
    conn.execute_row(sql, (curr_date, ))

    conn.execute_query('Select * From TBOFINCustomerReceiptAndAdjust_Temp')
    data = [row for row in conn]

  return data


def is_debit_equals_credit(data_zn):
  sum_debit = sum([row['Debit'] for row in data_zn])
  sum_credit = sum([row['Credit'] for row in data_zn])

  if sum_debit > sum_credit:
    print('ReceiptAndAdjust_ZN : Debit > Credit : %.2f' %
          (sum_debit - sum_credit))
    return False
  elif sum_debit < sum_credit:
    print('ReceiptAndAdjust_ZN : Debit < Credit : %.2f' %
          (sum_credit - sum_debit))
    return False
  return True


def check_all_records_has_CPCID(data):
  is_CPCID_empty_or_null = [1 if not row['CPCID'] else 0 for row in data]

  if sum(is_CPCID_empty_or_null) > 0:
    print('CPCID is null')
    return False
  return True


def validate_data(data):
  return is_debit_equals_credit(data) & check_all_records_has_CPCID(data)


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


def generate_data_file(output_path, date, data):
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

      writerVAL.write('%-14s%-10s' % (ZN_name_dat,
                                      '{:0>10}'.format(str(line_count))))
      print('Create Files ZN .DAT & .VAL Complete..')
    except Exception as e:
      print('Create Files ZN .DAT & .VAL Error .. : ')
      print(str(e))


def main():
  curr_date = _date
  last_date = _date
  str_date = last_date.strftime('%Y-%m-%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 'D' + str_date
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    orders_normal = get_orders_by_date_type(str_date, "Normal")
    suborders_normal = [
        row['orderid'] for row in orders_normal
        if execute_ar_transaction(row['orderid'], "No") != 0
    ]

    orders_preorder = get_orders_by_date_type(str_date, "Preorder")
    suborders_preorder = [
        row['orderid'] for row in orders_preorder
        if execute_ar_transaction(row['orderid'], "Yes") != 0
    ]

    error_suborders = suborders_normal + suborders_preorder
    if error_suborders:
      print(
          'CMOS Interface To Oracle (OFIN) ({}) : Text ZN Error SubOrderId {}'
          .format(datetime.now(), ','.join(suborders)))

    returns = get_return_agent()
    [execut_return_agent(row['suborderid'], "INV") for row in returns]

    returns = get_return_agentservice()
    [execut_return_agent(row['ServiceNo'], "SER") for row in returns]

    data = generate_temp_data(str_date)

    if not validate_data(data):
      return

    generate_data_file(target_path, curr_date, data)

  except Exception as e:
    print('Get Data ZN From Stored Procedure Error: %s' % str(e))


if __name__ == '__main__':
  main()
