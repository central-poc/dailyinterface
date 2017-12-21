import pymssql
import sys
import csv
import os
from datetime import datetime, timedelta
import random

CFG_SALES_TRANSACTION_PATH = "SaleTransaction/"

partner_code = 'CGO-PARTNER-CODE'
total_row = 0

_year = ""
_month = ""
_day = ""
_file_name = ""
enum_bu = ['B2S', 'CDS', 'CGO', 'MSL', 'RBS', 'SSP']
enum_shopgroup = ['BU', 'IN', 'ME']
_time = datetime.strptime('2017-11-03 10:00', '%Y-%m-%d %H:%M')

# time = datetime.now()

def connect_db():
  return pymssql.connect("10.17.221.173", "app-t1c", "Zxcv123!", "DBMKP")

def generate_text():
  print("--- Begin: GenerateText ---")
  shop_master = get_shop_master()

  gen_text_bu(shop_master)
  gen_text_indy(shop_master)
  gen_text_mer(shop_master)

  gen_ctrl_text()

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
  dt_batch_date_get(store_number, bu)
  sale_transaction(bu, store_number, shop_id, shop_group, _file_name)

def dt_batch_date_get(store_number, bu):
  global _file_name
  dt_batch_date = get_batch_date()
  month = '{:02}'.format(dt_batch_date.month)
  day = '{:02}'.format(dt_batch_date.day)
  year = str(dt_batch_date.year)[2:4]
  _file_name = 'BCH_{}_T1C_NRTSales_{}{}{}.dat'.format(bu, year, month, day)

def sale_transaction(bu, store_number, shop_id, shop_group, file_name):
  print('--- Begin: SaleTransaction ---')
  global total_row
  sale_transactions = get_sale_tran(shop_id, shop_group, store_number)
  total = len(sale_transactions)
  with open(CFG_SALES_TRANSACTION_PATH + file_name, 'a') as text_file:
    for transaction in sale_transactions:
      text_file.write(transaction + os.linesep)

  total_row = total_row+total
  print('--- End: SaleTransaction ---')

def get_sale_tran(shop_id, shop_group, store_number):
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    query = """
    SELECT
        Head.ShopID,
        (case when Head.shopGroup = 'ME' then Concat('0', substring(Head.subOrderId,1,12)) else Head.InvNo end) as InvNo,
        Head.InvDate,
        Head.CreateOn,
        Head.PaymentDate as BusinessDate,
        Head.DeliveryDate,
        '01' AS SaleType,
        Detail.PID,
        Detail.Quantity,
        Detail.UnitPrice ,
        Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
        Detail.SeqNo,
        Head.VatAmt ,
        (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
        (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
        ProMas.ProdBarcode,
        Head.T1CNoEarn,
        Head.RedeemAmt,
        Head.PaymentRefNo
    FROM
        TBSubOrderHead Head
    INNER JOIN
        TBSubOrderDetail Detail ON Head.Suborderid = Detail.Suborderid
    LEFT JOIN
        TBProductMaster ProMas ON Detail.PID = ProMas.PID
    WHERE
        Head.InvDate = CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
        AND Head.InvNo != ''
        AND head.ShopGroup = %(ShopGroup)s {0}

    UNION ALL

    SELECT
        Head.ShopID,
        Head.CnNo AS InvNo,
        CONVERT(VARCHAR(19),Head.CnDate,111) AS InvDate,
        Head.CreateOn,
        Head.CnDate as BusinessDate,
        Head.CnDate as DeliveryDate,
        '07' AS SaleType,
        Detail.PID,
        Detail.Quantity,
        Detail.UnitPrice ,
        Detail.UnitPrice - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS UnitSalesPrice,
        Detail.SeqNo,
        Head.VatAmt ,
        (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
        (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
        ProMas.ProdBarcode,
        Head.T1CNoEarn,
        Head.RedeemAmt,
        Head.PaymentRefNo
    FROM
        TBSubSaleReturnHead Head
    INNER JOIN
        TBSubSaleReturnDetail Detail ON Head.SubSRNo = Detail.SubSRNo
    LEFT JOIN
        TBProductMaster ProMas ON Detail.PID = ProMas.PID
    WHERE
        Head.SubSaleReturnType IN ('CN', 'Exchange')
        AND Head.Status = 'Complete'
        AND Head.CnNo != ''
        AND CONVERT(VARCHAR(19),Head.CnDate,111) =CONVERT(VARCHAR(19),DateAdd(dd,-1,%(batch_date)s),111)
        AND Head.ShopGroup = %(ShopGroup)s {0}
    ORDER BY
        InvNo
    """
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
  t1c_card_no = '{: <16}'.format(t1c_card_no) if t1c_card_no == None else '{: <16}'.format(
          t1c_card_no) if len(t1c_card_no) < 16 else t1c_card_no[0:16]
  redeem_amt = get_option_str(data['RedeemAmt'])
  trans_sub_type = 'T' if t1c_card_no != '' or redeem_amt != "0.000'" else 'P'

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
  tender_type = '' if trans_sub_type == 'P' else 'T1PM' if redeem_amt != '0.000' else 'CASH'
  tender_ref_no = '' if trans_sub_type == 'P' else t1c_card_no if redeem_amt != '0.000' else get_option_str(data['PaymentRefNo'])
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

def gen_ctrl_text():
  global total_row
  interface_name = 'BCH_PRT_T1C_NRTSales'
  filedatetime = datetime.now().strftime('%d%m%Y_%H:%M:%S')
  ctrlfile = '%s_%s.ctrl' % (interface_name, filedatetime)
  filepath = os.path.join(CFG_SALES_TRANSACTION_PATH, ctrlfile)
  with open(filepath, 'w') as outfile:
    outfile.write('%s|%s|Online|1|%d|%s|CGO|||' % (interface_name, partner_code, total_row,
                                                  filedatetime))

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

def get_option_str(data):
  return '' if data == None else str(data)

def get_batch_date():
  return _time - timedelta(days=1)

def get_sql_batch_date():
  return _time

def create_directory(path):
  if not os.path.exists(path):
    os.makedirs(path)

if __name__ == "__main__":
  create_directory('SaleTransaction')
  generate_text()
