import pymssql
import sys
import csv
import os
import zipfile
from datetime import datetime, timedelta
from ftplib import FTP
import random

CFG_SALES_TRANSACTION_PATH = "SaleTransaction/"
CFG_TENDER_MEMBER_PATH = "TenderMemberTransaction/"
CFG_TENDER_NON_MEMBER_PATH = "TenderTransaction/"
CFG_REPORTT1C_PAHT = "ReportT1CPath/"

_year = ""
_month = ""
_day = ""
_file_name = ""
enum_bu = ['B2S', 'CDS', 'CGO', 'MSL', 'RBS', 'SSP']
enum_shopgroup = ['BU', 'IN', 'ME']
_time = datetime.strptime('2018-03-27 10:00', '%Y-%m-%d %H:%M')
order_ids = []

# time = datetime.now()


def connect_db():
  return pymssql.connect("10.17.220.173", "app-t1c", "Zxcv123!", "DBMKP")


def generate_report():
  print("--- Begin: GenerateText ---")
  dt_batch_date_get('', '', 'GR')
  data = get_order_data()
  create_report(data)


def get_order_data():
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(
        'SELECT OrderId,BU,ShopId,ShopGroup,InvNo,InvDate,TenderType,NetAmt,T1CNoEarn FROM vw_DailyT1CInterface'
    )
    return [[
        order['OrderId'], order['BU'],
        str(order['ShopId']), order['ShopGroup'], order['InvNo'],
        order['InvDate'].strftime('%Y%m%d'), order['TenderType'],
        '{:04.14f}'.format(order['NetAmt']), order['T1CNoEarn']
    ] for order in cursor]


def create_report(rows):
  head = [
      "OrderId,BU,ShopId,ShopGroup,InvNo,InvDate,TenderType,NetAmt,T1CNoEarn"
  ]
  with open(CFG_REPORTT1C_PAHT + _file_name, 'w') as csvfile:
    writer = csv.writer(
        csvfile, delimiter=' ', quotechar=',', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
        'OrderId', 'BU', 'ShopId', 'ShopGroup', 'InvNo', 'InvDate',
        'TenderType', 'NetAmt', 'T1CNoEarn'
    ])
    for row in rows:
      writer.writerow(row)


def generate_text():
  print("--- Begin: GenerateText ---")
  shop_master = get_shop_master()

  gen_text_bu(shop_master)
  gen_text_indy(shop_master)
  gen_text_mer(shop_master)

  print('--- End: GenerateText ---')


def get_shop_master():
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(
        'SELECT ShopId, ShopGroup, AccountCode, BU FROM TBShopMaster')
    return [{
        'ShopId': str(shop['ShopId']),
        'ShopGroup': shop['ShopGroup'],
        'AccountCode': shop['AccountCode'],
        'BU': shop['BU']
    } for shop in cursor]


def get_shop_master_by_bu_list():
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(
        'SELECT DISTINCT ShopGroup, AccountCode, BU FROM TBShopMaster WHERE BU IN (\''
        + str.join('\',\'', enum_bu) + '\')')
    return [{
        'ShopGroup': shop['ShopGroup'],
        'AccountCode': shop['AccountCode'],
        'BU': shop['BU']
    } for shop in cursor]


def gen_text_bu(shop_master):
  shop_master_by_bus = get_shop_master_by_bu_list()
  bu_shops = [
      shop for shop in shop_master
      if (shop['ShopGroup'] in enum_shopgroup) & (shop['BU'] in enum_bu)
  ]
  for item in shop_master_by_bus:
    bu_shop = str.join(',', [
        bu['ShopId'] for bu in bu_shops
        if bu['AccountCode'] == item['AccountCode']
    ])
    generate_text_t1c('CGO', item['AccountCode'], bu_shop, 'BU')


def gen_text_indy(shop_master):
  indy_shops = [shop for shop in shop_master if shop['ShopGroup'] == 'IN']
  indy_shop = str.join(',', [indy['ShopId'] for indy in indy_shops])
  generate_text_t1c('CGO', '99564', indy_shop, 'IN')


def gen_text_mer(shop_master):
  mer_shops = [shop for shop in shop_master if shop['ShopGroup'] == 'ME']
  mer_shop = str.join(',', [mer['ShopId'] for mer in mer_shops])
  generate_text_t1c('CGO', '99998', mer_shop, 'ME')


def generate_text_t1c(bu, store_number, shop_id, shop_group):
  dt_batch_date_get(store_number, bu, 'GT')
  sale_transaction(bu, store_number, shop_id, shop_group, 'SF' + _file_name)
  tender_member_only(bu, store_number, shop_id, shop_group, 'TD' + _file_name)
  tender_non_member(bu, store_number, shop_id, shop_group, 'TF' + _file_name)


def dt_batch_date_get(store_number, bu, gen_type):
  global _file_name
  dt_batch_date = get_batch_date()
  month = '{:02}'.format(dt_batch_date.month)
  day = '{:02}'.format(dt_batch_date.day)
  if gen_type == 'GT':
    year = str(dt_batch_date.year)[2:4]
    _file_name = '{}{}{}{}.{}'.format(year, month, day, store_number, bu)
  else:
    year = str(dt_batch_date.year)
    _file_name = '{}{}{}_{}'.format(year, month, day, 'ReportT1C_CGO.csv')


def sale_transaction(bu, store_number, shop_id, shop_group, file_name):
  print('--- Begin: SaleTransaction ---')
  sale_transactions = get_sale_tran(shop_id, shop_group, store_number)
  total = len(sale_transactions)
  with open(CFG_SALES_TRANSACTION_PATH + file_name, 'w') as text_file:
    for transaction in sale_transactions:
      text_file.write(transaction + os.linesep)

  total_sale_transaction(total, file_name, bu, store_number)
  print('--- End: SaleTransaction ---')


def total_sale_transaction(total, file_name, bu, store_number):
  print('--- Begin: TotalSaleTransaction ---')
  file_name_total = 'SO' + _file_name
  total = str(total).zfill(10)
  main_text = _time.strftime('%Y%m%d%H%M') + total
  zip_sale_transaction_path = CFG_SALES_TRANSACTION_PATH
  with open(CFG_SALES_TRANSACTION_PATH + file_name_total, 'w') as text:
    text.write(main_text)
  create_directory(zip_sale_transaction_path + 'ZipFile')
  with zipfile.ZipFile(
      zip_sale_transaction_path + 'ZipFile/' + file_name + '.ZIP',
      'w') as myzip:
    myzip.write(zip_sale_transaction_path + file_name)
    myzip.write(zip_sale_transaction_path + file_name_total)
  file_path = zip_sale_transaction_path + 'ZipFile' + file_name + '.ZIP'
  ftp_server = '10.0.18.96'
  ftp_user_name = 'ftpcrclytpos'
  ftp_password = 'posftp*01'

  upload_file_to_ftp(file_path, ftp_server, ftp_user_name, ftp_password, 'SF/')
  print('--- End: TotalSaleTransaction ---')


def upload_file_to_ftp(file_path, ftp_server, ftp_user_name, ftp_password,
                       folder):
  print('--- Begin UploadFileToFtp ---')
  print('--- End: UploadToFtp ---')


def get_sale_tran(shop_id, shop_group, store_number):
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    query = """
      SELECT
          Head.Suborderid as id,
          Head.ShopID,
          Head.ShopGroup,
          Head.SubOrderId,
          (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
          Head.InvDate,
          Head.CreateOn,
          '00' AS SaleType,
          Head.Status,
          Detail.PID,
          Detail.Quantity,
          Detail.UnitPrice ,
          Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
          Head.VatAmt ,
          emp.eEmpID AS CreateBy,
          (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
          (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS ItemDiscountAmount,
          (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
          ProMas.DEPT,
          ProMas.SUB_DPT,
          Head.T1CNoEarn
      FROM TBSubOrderHead Head
      INNER JOIN TBSubOrderDetail Detail ON Head.Suborderid = Detail.Suborderid
      LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
      LEFT JOIN [dbo].[cmeEmp] emp ON Head.CreateBy = emp.eEMailInternal
      WHERE Head.IsGenT1c = 'No'
      AND Head.InvNo != ''
      AND head.ShopGroup = %(ShopGroup)s {0}
      UNION ALL
      SELECT
          Head.SubSRNo as id,
          Head.shopid,
          Head.ShopGroup,
          Head.SubOrderId,
          Head.CnNo AS InvNo,
          CONVERT(VARCHAR(19),Head.CnDate,111) AS InvDate,
          Head.CreateOn,
          '20' AS SaleType,
          Head.Status,
          Detail.PID,
          Detail.Quantity,
          Detail.UnitPrice ,
          Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
          Head.VatAmt ,
          emp.eEmpID AS CreateBy,
          (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
          (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS ItemDiscountAmount,
          (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
          ProMas.DEPT,
          ProMas.SUB_DPT,
          Head.T1CNoEarn
      FROM TBSubSaleReturnHead Head
      INNER JOIN TBSubSaleReturnDetail Detail ON Head.SubSRNo = Detail.SubSRNo
      LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
      LEFT JOIN [dbo].[cmeEmp] emp ON Head.CreateBy = emp.eEMailInternal
      WHERE Head.SubSaleReturnType IN ('CN', 'Exchange')
      AND Head.Status = 'Completed'
      AND Head.CnNo != ''
      AND Head.IsGenT1c = 'No'
      AND Head.ShopGroup = %(ShopGroup)s {0}
      ORDER BY InvNo
    """
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID in ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(ShopGroup=shop_group))
    return [
        gen_sale_tran_data(data, index + 1, store_number)
        for index, data in enumerate(cursor)
    ]


def gen_sale_tran_data(data, index, store_number):
  global order_ids
  order_ids.append(data['id'])
  store_number = store_number.zfill(5)

  if data['InvDate'] == None:
    transaction_date = '        '
  else:
    transaction_date = datetime.strptime(data['InvDate'],
                                         '%Y-%m-%d').strftime('%Y%m%d')

  if data['CreateOn'] == None:
    transaction_time = '    '
  else:
    transaction_time = data['CreateOn'].strftime('%H%M')

  status = get_option_str(data['Status'])
  sale_type = get_option_str(data['SaleType'])

  pos_number = get_option_str(data['ShopID'])
  pos_number = '{:0>4}'.format(pos_number)

  inv_no = get_option_str(data['InvNo'])
  ticket_running_number = get_tracking_number(inv_no)

  pid = get_option_str(data['PID'])
  sku_code = pid.zfill(16)
  upc_code = sku_code

  qty_sign = '+'
  quantity = get_option_str(data['Quantity'])
  quantity = (str(quantity) + '00').zfill(6)

  unit_pos_price = get_option_str(data['UnitPrice'])
  unit_pos_price = split_price(unit_pos_price)

  unit_sale_price = get_option_str(data['UnitSalesPrice'])
  unit_sale_price = split_price(unit_sale_price)

  flag_price_override = 'N'
  sales_amount = get_option_str(data['NetAmt'])
  sales_amount = split_price(sales_amount)

  vat_amount = get_option_str(data['VatAmt'])
  vat_amount = split_price(vat_amount)

  item_discount_code = '{: ^4}'.format('')
  item_discount_amount = get_option_str(data['ItemDiscountAmount'])
  item_discount_amount = split_price(item_discount_amount)

  transaction_discount_code = '{: ^4}'.format('')
  transaction_discount_amount = get_option_str(
      data['TransactionDiscountAmount'])
  transaction_discount_amount = split_price(transaction_discount_amount)

  reference_id = '{: ^21}'.format('')
  reference_receipt_number = '{: ^9}'.format('')
  reference_tran_date = transaction_date
  reason_code = ''.zfill(2)
  promotion_event_code = ''.zfill(6)
  dept_code = get_option_str(data['DEPT'])
  dept_code = dept_code.zfill(3) if len(dept_code) < 3 else dept_code[0:3]
  sub_dept_code = get_option_str(data['SUB_DPT'])
  sub_dept_code = sub_dept_code.zfill(
      3) if len(sub_dept_code) < 3 else sub_dept_code[0:3]
  itemize_upc_code = ''.zfill(16)
  dtype_code = '{: ^1}'.format('')
  credit_customer_id = '{: ^16}'.format('')

  member_id = get_option_str(data['T1CNoEarn'])
  member_id = '{: <21}'.format(
      member_id) if member_id == None else '{: <21}'.format(
          member_id) if len(member_id) < 21 else member_id[0:21]

  member_point = ''.zfill(8)
  cashier_id = ''.zfill(8)
  sales_person_id = get_option_str(data['CreateBy'])
  sales_person_id = sales_person_id.zfill(
      8) if len(sales_person_id) < 8 else sales_person_id[0:8]
  shop_id = '' if data['ShopID'] == None else data['ShopID']
  shopGroup = '' if data['ShopGroup'] == None else data['ShopGroup']

  raw_sale_tran = store_number + transaction_date + transaction_time + sale_type \
  + pos_number + ticket_running_number + sku_code + upc_code + qty_sign + quantity \
  + unit_pos_price + unit_sale_price + flag_price_override + sales_amount + vat_amount \
  + item_discount_code + item_discount_amount + transaction_discount_code + transaction_discount_amount \
  + reference_id + reference_receipt_number + reference_tran_date + reason_code + promotion_event_code \
  + dept_code + sub_dept_code + itemize_upc_code + dtype_code + credit_customer_id + member_id + member_point \
  + cashier_id + sales_person_id
  if len(raw_sale_tran) == 281:
    return raw_sale_tran
  else:
    print(index, ": Miss Length")


def split_price(price):
  price_array = str.split(str(price), ".")
  if len(price_array) == 2:
    price = price_array[0] + '{:0<2}'.format(price_array[1][0:2])
  if len(price) < 12:
    price = '{:0>12}'.format(price)
  return price


def get_tracking_number(inv_no):
  pad_char = '0'
  suffix = inv_no[6:13]
  bu = inv_no[0:3]
  if len(inv_no) < 8:
    if bu == 'CDS':
      pad_char = '1'
    elif bu == 'CNC':
      pad_char = '2'
    elif bu == 'ABB':
      pad_char = '3'
    elif bu == 'RBS' or bu == 'CNR':
      pad_char = '4'
    elif bu == 'SSP' or bu == 'CNS':
      pad_char = '5'
    elif bu == 'B2S' or bu == 'CNB':
      pad_char = '6'
    else:
      pad_char = '0'
  return ('{:' + pad_char + '>8}').format(suffix)


def tender_member_only(bu, store_number, shop_id, shop_group, file_name):
  print('--- Begin: TenderMemberOnly ---')
  tender_member_file_path = CFG_TENDER_MEMBER_PATH
  tender_member_only = get_tender_mem(shop_id, shop_group, store_number)
  total = len(tender_member_only)
  with open(tender_member_file_path + file_name, 'w') as text_file:
    for tender in tender_member_only:
      text_file.write(tender + os.linesep)
  total_tender_member(total, bu, store_number)
  print('--- End: TenderMemberOnly ---')


def tender_non_member(bu, store_number, shop_id, shop_group, file_name):
  print('--- Begin: TenderNonMember ---')
  tender_non_member_file_path = CFG_TENDER_NON_MEMBER_PATH
  tender_non_member = get_tender_non_mem(shop_id, shop_group, store_number)
  total = len(tender_non_member)
  with open(tender_non_member_file_path + file_name, 'w') as text_file:
    for tender in tender_non_member:
      text_file.write(tender + os.linesep)
  total_tender_non_member(total, file_name, bu, store_number)
  print('--- End: TenderNonMember ---')


def get_tender_mem(shop_id, shop_group, store_number):
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    query = """SELECT
                  Head.Suborderid as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  (CASE WHEN Head.shopGroup = 'ME' THEN Concat('0', SUBSTRING(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
                  Head.InvDate,
                  Head.CreateOn,
                  '00' AS SaleType,
                  'CASH' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  Head.redeemamt,
                  OrderHead.NetAmt AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
							FROM TBsubOrderHead Head
							INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId
							WHERE Head.IsGenT1c = 'No'
              AND Head.InvNo != ''
              AND Head.T1CNoEarn != ''
              AND Head.ShopGroup = %(shop_group)s {0}
              UNION ALL
              SELECT
                  Head.Suborderid as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
                  Head.InvDate,
                  Head.CreateOn,
                  '00' AS SaleType,
                  'T1PM' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  Head.redeemamt,
                  OrderHead.NetAmt AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
              FROM TBsubOrderHead Head
              INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId
              WHERE Head.IsGenT1c = 'No'
              AND Head.InvNo != ''
              AND Head.T1CNoEarn != ''
              AND Head.redeempoint <> 0
              AND Head.ShopGroup = %(shop_group)s {0}
              UNION ALL
              SELECT top 1
                  Head.SubSRNo as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  Head.CnNo AS InvNo,
                  CONVERT(VARCHAR(19), Head.CnDate, 111) AS InvDate,
                  Head.CreateOn,
                  '20' AS SaleType,
                  'CASH' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  '0' AS redeemamt,
                  '0' AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
              FROM TBSubSaleReturnHead Head WHERE Head.SubSaleReturnType IN ('CN', 'Exchange')
              AND Head.Status = 'Completed'
              AND Head.CnNo != ''
              AND Head.IsGenT1c = 'No'
              AND Head.T1CNoEarn <> ''
              AND Head.ShopGroup = %(shop_group)s {0}
							ORDER BY InvNo"""
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID IN ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(shop_group=shop_group))
    return [
        gen_tender(data, index + 1, store_number)
        for index, data in enumerate(cursor)
    ]


def gen_tender(data, index, store_number):
  global order_ids
  order_ids.append(data['id'])
  store_number.zfill(5)
  transaction_date = ' ' * 8 if data['InvDate'] == None else datetime.strptime(
      data['InvDate'], '%Y-%m-%d').strftime('%Y%m%d')
  transaction_time = '{: >4}'.format() if data['CreateOn'] == None else data[
      'CreateOn'].strftime('%H%M')
  status = get_option_str(data['Status'])
  sale_type = get_option_str(data['SaleType'])
  pos_number = get_option_str(data['ShopID']).zfill(4)
  ticket_running_number = get_tracking_number(get_option_str(data['InvNo']))
  tender_type = get_option_str(data['TenderType'])
  sign = '+'

  sub_net_amt = get_option_number(data['NetAmt'])
  total_net_amt = get_option_number(data['TotalNetAmt'])
  redeem_amt = get_option_number(data['redeemamt'])

  percent = 0 if total_net_amt == 0 else sub_net_amt * 100 / total_net_amt
  point_discount = redeem_amt if percent == 0 else redeem_amt * percent / 100

  if tender_type == 'CASH':
    tender_amt = 0 if sub_net_amt < point_discount else sub_net_amt - point_discount
  elif tender_type == 'T1PM':
    tender_amt = sub_net_amt if sub_net_amt < point_discount else point_discount
  else:
    tender_amt = 0

  if data['SaleType'] == 20:
    tender_amt == sub_net_amt

  tender_amt = str(tender_amt)
  tender_amt = split_price(tender_amt)

  payment_discount_code = ' ' * 4
  payment_discount_label = ' ' * 10
  payment_discount_amt = get_option_str(
      data['TransactionDiscountAmount']).replace('.', '').zfill(12)
  reference_id_1 = ' ' * 21
  credit_customer_id = ' ' * 16
  approve_code = ' ' * 6

  member_id = '{: <21}'.format(get_option_str(data['T1CNoEarn']))
  member_point = '0' * 8
  cashier_id = ' ' * 8

  system_date = _time.strftime('%Y%m%d')
  system_time = _time.strftime('%H%M')

  shop_id = get_option_str(data['ShopID'])
  shop_group = get_option_str(data['ShopGroup'])

  raw_tender_mem = store_number + transaction_date + transaction_time \
  + sale_type + pos_number + ticket_running_number + tender_type \
  + sign + tender_amt + payment_discount_code + payment_discount_label \
  + payment_discount_amt + reference_id_1 + credit_customer_id \
  + approve_code + member_id + member_point + cashier_id \
  + system_date + system_time

  if len(raw_tender_mem) == 166:
    return raw_tender_mem
  else:
    print(index, 'Miss Length:', len(raw_tender_mem))


def get_option_str(data):
  return '' if data == None else str(data)


def get_option_number(data):
  try:
    return float(data)
  except Exception as e:
    return 0


def get_tender_non_mem(shop_id, shop_group, store_number):
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    query = """SELECT
                  Head.Suborderid as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
                  Head.InvDate,
                  Head.CreateOn,
                  '00' AS SaleType,
                  'CASH' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  Head.redeemamt,
                  OrderHead.NetAmt AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
							FROM TBsubOrderHead Head
							INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId
							WHERE Head.IsGenT1c = 'No'
              AND Head.InvNo != ''
              AND Head.ShopGroup = %(shop_group)s {0}
              UNION ALL
              SELECT top 1
                  Head.Suborderid as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
                  Head.InvDate,
                  Head.CreateOn,
                  '00' AS SaleType,
                  'T1PM' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  Head.redeemamt,
                  OrderHead.NetAmt AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
              FROM TBsubOrderHead Head
              INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId
              WHERE Head.IsGenT1c = 'No'
              AND Head.InvNo != ''
              AND Head.redeempoint <> 0
              AND Head.ShopGroup = %(shop_group)s {0}
              UNION ALL
              SELECT
                  Head.SubSRNo as id,
                  Head.ShopID,
                  Head.ShopGroup,
                  Head.CnNo AS InvNo,
                  CONVERT(VARCHAR(19),Head.CnDate,111) AS InvDate,
                  Head.CreateOn,
                  '20' AS SaleType,
                  'CASH' AS TenderType,
                  Head.Status ,
                  Head.VatAmt,
                  Head.NetAmt,
                  '0' AS redeemamt,
                  '0' AS TotalNetAmt ,
                  (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                  Head.T1CNoEarn
              FROM TBSubSaleReturnHead Head WHERE Head.SubSaleReturnType IN ('CN', 'Exchange')
              AND Head.Status = 'Completed'
              AND Head.CnNo != ''
              AND Head.IsGenT1c = 'No'
              AND Head.ShopGroup = %(shop_group)s {0}
							ORDER BY InvNo"""
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID IN ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(
                       shop_group=shop_group))
    return [
        gen_tender(data, index + 1, store_number)
        for index, data in enumerate(cursor)
    ]


def total_tender_member(total, bu, store_number):
  print('--- Begin: TotalTenderMemberOnly ---')
  dt_batch_date_get(store_number, bu, 'GT')
  file_name_total = 'TL' + _file_name
  total = str(total).zfill(10)
  main_text = _time.strftime('%Y%m%d%H%M') + total
  with open(CFG_TENDER_MEMBER_PATH + file_name_total, 'w') as text:
    text.write(main_text)

  print('--- End: TotalTenderMemberOnly ---')


def total_tender_non_member(total, file_name, bu, store_number):
  print('--- Begin: TotalTenderNonMember ---')
  dt_batch_date_get(store_number, bu, 'GT')
  file_name_total = 'TO' + _file_name
  total = str(total).zfill(10)
  main_text = _time.strftime('%Y%m%d%H%M') + total
  with open(CFG_TENDER_NON_MEMBER_PATH + file_name_total, 'w') as text:
    text.write(main_text)

  zip_non_member_path = CFG_TENDER_NON_MEMBER_PATH
  create_directory(zip_non_member_path + 'ZipFile')
  with zipfile.ZipFile(zip_non_member_path + 'ZipFile/' + file_name + '.ZIP',
                       'w') as myzip:
    myzip.write(zip_non_member_path + file_name)
    myzip.write(zip_non_member_path + file_name_total)
  file_path = zip_non_member_path + 'ZipFile' + _file_name + '.ZIP'

  print('--- End: TotalTenderNonMember ---')


def get_batch_date():
  return _time - timedelta(days=1)


def get_sql_batch_date():
  return _time


def create_directory(path):
  if not os.path.exists(path):
    os.makedirs(path)

def update_order():
    sale = []
    sr = []
    for id in order_ids:
        if id[:2] == 'CR':
            sr.append(id)
        else:
            sale.append(id)
    query ="""
    BEGIN TRANSACTION A
        BEGIN TRY
            UPDATE TBSubOrderHead SET IsGenT1c = 'Yes' WHERE Suborderid in ('%s');
            UPDATE TBSubSaleReturnHead SET IsGenT1c = 'Yes' WHERE SubSRNo in ('%s')
            COMMIT TRANSACTION A
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION A
        END CATCH
    GO
    """ % ("','".join(sale),"','".join(sr))
    print(query)
    # with connect_db() as conn:
    #     with conn.cursor(as_dict=True) as cursor:
    #         cursor.execute(query)

if __name__ == "__main__":
  create_directory('SaleTransaction')
  create_directory('TenderMemberTransaction')
  create_directory('TenderTransaction')
  create_directory('ReportT1CPath')
  generate_text()
  generate_report()
  update_order()
