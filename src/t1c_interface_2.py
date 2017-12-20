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

partner_code = 'CGO-PARTNER-CODE'

_year = ""
_month = ""
_day = ""
_file_name = ""
enum_bu = ['B2S', 'CDS', 'CGO', 'MSL', 'RBS', 'SSP']
enum_shopgroup = ['BU', 'IN', 'ME']
_time = datetime.strptime('2017-10-06 10:00', '%Y-%m-%d %H:%M')

# time = datetime.now()


def connect_db():
  return pymssql.connect("10.17.221.173", "app-t1c", "Zxcv123!", "DBMKP")


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
  sale_transaction(bu, store_number, shop_id, shop_group, _file_name)
  # tender_member_only(bu, store_number, shop_id, shop_group, 'TD' + _file_name)
  # tender_non_member(bu, store_number, shop_id, shop_group, 'TF' + _file_name)


def dt_batch_date_get(store_number, bu, gen_type):
  global _file_name
  dt_batch_date = get_batch_date()
  month = '{:02}'.format(dt_batch_date.month)
  day = '{:02}'.format(dt_batch_date.day)
  if gen_type == 'GT':
    year = str(dt_batch_date.year)[2:4]
    _file_name = 'BCH_{}_T1C_NRTSales_{}{}{}.dat'.format(bu, year, month, day)
  else:
    year = str(dt_batch_date.year)
    _file_name = '{}{}{}_{}'.format(year, month, day, 'ReportT1C_CGO.csv')


def sale_transaction(bu, store_number, shop_id, shop_group, file_name):
  print('--- Begin: SaleTransaction ---')
  sale_transactions = get_sale_tran(shop_id, shop_group, store_number)
  total = len(sale_transactions)
  with open(CFG_SALES_TRANSACTION_PATH + file_name, 'a') as text_file:
    for transaction in sale_transactions:
      text_file.write(transaction + os.linesep)

  # total_sale_transaction(total, file_name, bu, store_number)
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
    query = """SELECT Head.ShopID,
                                   Head.ShopGroup,
                                   Head.SubOrderId,
                                   (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
                                   Head.InvDate,
                                   Head.CreateOn,
                                   Head.PaymentDate as BusinessDate,
                                   Head.DeliveryDate,
                                   '00' AS SaleType,
                                   Head.Status,
                                   Detail.PID,
                                   Detail.Quantity,
                                   Detail.UnitPrice ,
                                   Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
                                   Detail.SeqNo,
                                   Head.VatAmt ,
                                   emp.eEmpID AS CreateBy,
                                   (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
                                   (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS ItemDiscountAmount,
                                   (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                                   ProMas.DEPT,
                                   ProMas.SUB_DPT,
                                   ProMas.ProdBarcode,
                                   Head.T1CNoEarn
                            FROM TBSubOrderHead Head
                            INNER JOIN TBSubOrderDetail Detail ON Head.Suborderid = Detail.Suborderid
                            LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
                            LEFT JOIN [dbo].[cmeEmp] emp ON Head.CreateBy = emp.eEMailInternal
                            WHERE Head.InvDate = CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
                              AND Head.InvNo != ''
                              AND head.ShopGroup = %(ShopGroup)s {0}
                              UNION ALL
                              SELECT Head.shopid,
                                     Head.ShopGroup,
                                     Head.SubOrderId,
                                     Head.CnNo AS InvNo,
                                     CONVERT(VARCHAR(19),Head.CnDate,111) AS InvDate,
                                     Head.CreateOn,
                                     Head.CnDate as BusinessDate,
                                     Head.CnDate as DeliveryDate,
                                     '20' AS SaleType,
                                     Head.Status,
                                     Detail.PID,
                                     Detail.Quantity,
                                     Detail.UnitPrice ,
                                     Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
                                     Detail.SeqNo,
                                     Head.VatAmt ,
                                     emp.eEmpID AS CreateBy,
                                     (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
                                     (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS ItemDiscountAmount,
                                     (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
                                     ProMas.DEPT,
                                     ProMas.SUB_DPT,
                                     ProMas.ProdBarcode,
                                     Head.T1CNoEarn
                              FROM TBSubSaleReturnHead Head
                              INNER JOIN TBSubSaleReturnDetail Detail ON Head.SubSRNo = Detail.SubSRNo
                              LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
                              LEFT JOIN [dbo].[cmeEmp] emp ON Head.CreateBy = emp.eEMailInternal WHERE Head.SubSaleReturnType IN ('CN', 'Exchange')
                              AND Head.Status = 'Complete'
                              AND Head.CnNo != ''
                              AND CONVERT(VARCHAR(19),Head.CnDate,111) =CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
                              AND Head.ShopGroup = %(ShopGroup)s {0}
                            ORDER BY InvNo"""
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID in ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(ShopGroup=shop_group, batch_date=get_sql_batch_date()))
    return [
        gen_sale_tran_data(data, index + 1, store_number)
        for index, data in enumerate(cursor)
    ]


def gen_sale_tran_data(data, index, store_number):
  store_number = store_number.zfill(5)

  inv_no = get_option_str(data['InvNo'])
  bu = inv_no[0:3]

  source_trans_id = partner_code + store_number + datetime.strptime(data['InvDate'],
                                         '%Y-%m-%d').strftime('%Y%m%d')
  if data['InvDate'] == None:
    transaction_date = '        '
  else:
    transaction_date = datetime.strptime(data['InvDate'],
                                         '%Y-%m-%d').strftime('%Y%m%d')

  if data['CreateOn'] == None:
    transaction_time = '    '
  else:
    transaction_time = data['CreateOn'].strftime('%H%M')

  trans_type = get_option_str(data['SaleType'])
  trans_sub_type = 'P'
  trans_date = transaction_date + transaction_time

  pos_number = get_option_str(data['ShopID'])
  pos_number = '{:0>4}'.format(pos_number)

  ticket_running_number = get_tracking_number(inv_no)
  receipt_number = store_number + pos_number + ticket_running_number \
        + transaction_date

  business_date = get_option_str(data['BusinessDate'])
  inv_date = transaction_date
  delivery_date = get_option_str(data['DeliveryDate'])
  earn_online_flag = 'N'
  t1c_card_no = get_option_str(data['T1CNoEarn'])
  t1c_card_no = '{: <21}'.format(t1c_card_no) if t1c_card_no == None else '{: <16}'.format(
          t1c_card_no) if len(t1c_card_no) < 16 else t1c_card_no[0:16]
  mobile_no = '0' * 10
  user_id = 'POS'
  item_seq_no = str(index)

  pid = get_option_str(data['PID'])
  product_code = pid
  product_barcode = get_option_str(data['ProdBarcode'])

  quantity = get_option_str(data['Quantity'])

  price_unit = get_option_str(data['UnitPrice'])
  price_total = get_option_str(data['Quantity'] * data['UnitPrice'])

  net_price_unit = get_option_str(data['UnitSalesPrice'])

  net_price_total = get_option_str(data['NetAmt'])

  discount_total = get_option_str(
      data['TransactionDiscountAmount'])

  vat_amount = get_option_str(data['VatAmt'])
  tender_type = ''
  tender_ref_no = ''
  original_receipt_no = ''
  original_item_seq_no = get_option_str(data['SeqNo'])

  res = []
  res.append('1')
  res.append(source_trans_id)
  res.append(store_number)
  res.append(pos_number)
  res.append(receipt_number)
  res.append(trans_type)
  res.append(trans_sub_type)
  res.append(trans_date)
  res.append(business_date)
  res.append(inv_date)
  res.append(delivery_date)
  res.append(earn_online_flag)
  res.append(t1c_card_no)
  res.append(mobile_no)
  res.append(user_id)
  res.append(item_seq_no)
  res.append(product_code)
  res.append(product_barcode)
  res.append(quantity)
  res.append(price_unit)
  res.append(price_total)
  res.append(net_price_unit)
  res.append(net_price_total)
  res.append(discount_total)
  res.append(vat_amount)
  res.append(tender_type)
  res.append(tender_ref_no)
  res.append(original_receipt_no)
  res.append(original_item_seq_no)
  # + display_receipt_no + return_all_flag + sblc_ncl_redeem_txn_id
  # if len(raw_sale_tran) == 281:
  return "|".join(res)
  # else:
  #   print(index, ": Miss Length")


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
    query = """SELECT Head.ShopID,
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
                            WHERE Head.invDate = CONVERT(VARCHAR(19), DateAdd(dd,-1,%(batch_date)s), 111)
                              AND Head.InvNo != ''
                              AND Head.T1CNoEarn != ''
                              AND Head.ShopGroup = %(shop_group)s {0}
                              UNION ALL
                              SELECT Head.ShopID,
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
                              INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId WHERE Head.invDate = CONVERT(VARCHAR(19), DateAdd(dd,-1,%(batch_date)s), 111)
                              AND Head.InvNo != ''
                              AND Head.T1CNoEarn != ''
                              AND Head.redeempoint <> 0
                              AND Head.ShopGroup = %(shop_group)s {0}
                              UNION ALL
                              SELECT Head.ShopID,
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
                              AND Head.Status = 'Complete'
                              AND Head.CnNo != ''
                              AND CONVERT(VARCHAR(19), Head.CnDate, 111) = CONVERT(VARCHAR(19), DateAdd(dd,-1,%(batch_date)s), 111)
                              AND Head.T1CNoEarn <> ''
                              AND Head.ShopGroup = %(shop_group)s {0}
                            ORDER BY InvNo"""
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID IN ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(
                       shop_group=shop_group, batch_date=get_sql_batch_date()))
    return [
        gen_tender(data, index + 1, store_number)
        for index, data in enumerate(cursor)
    ]


def gen_tender(data, index, store_number):
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
    query = """SELECT Head.ShopID,
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
                            WHERE Head.invDate = CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
                              AND Head.InvNo != ''
                              AND Head.ShopGroup = %(shop_group)s {0}
                              UNION ALL
                              SELECT Head.ShopID,
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
                              INNER JOIN TBOrderHead OrderHead ON Head.OrderId = OrderHead.OrderId WHERE Head.invDate = CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
                              AND Head.InvNo != ''
                              AND Head.redeempoint <> 0
                              AND Head.ShopGroup = %(shop_group)s {0}
                              UNION ALL
                              SELECT Head.ShopID,
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
                              AND Head.Status = 'Complete'
                              AND Head.CnNo != ''
                              AND CONVERT(VARCHAR(19),Head.CnDate,111) = CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
                              AND Head.ShopGroup = %(shop_group)s {0}
                            ORDER BY InvNo"""
    if shop_group == 'BU':
      query = query.format('{} ({})'.format(' AND Head.ShopID IN ', shop_id))
    else:
      query = query.format('')
    cursor.execute(query,
                   dict(
                       shop_group=shop_group, batch_date=get_sql_batch_date()))
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


if __name__ == "__main__":
  create_directory('SaleTransaction')
  # create_directory('TenderMemberTransaction')
  # create_directory('TenderTransaction')
  # create_directory('ReportT1CPath')
  generate_text()
  # generate_report()
