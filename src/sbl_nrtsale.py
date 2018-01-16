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
_t_index = 1
_p_index = 1

# time = datetime.now()

def connect_db():
  return pymssql.connect("10.17.220.173", "app-t1c", "Zxcv123!", "DBMKP")

def generate_text_t1c():
  dt_batch_date_get()
  sale_transaction(_file_name)
  gen_ctrl_text()

def dt_batch_date_get():
  global _file_name
  dt_batch_date = get_batch_date()
  month = '{:02}'.format(dt_batch_date.month)
  day = '{:02}'.format(dt_batch_date.day)
  year = str(dt_batch_date.year)[2:4]
  _file_name = 'BCH_CGO_T1C_NRTSales_{}{}{}.dat'.format(year, month, day)

def sale_transaction(file_name):
  print('--- Begin: SaleTransaction ---')
  global total_row
  sale_transactions = get_sale_tran()
  total = len(sale_transactions)
  with open(CFG_SALES_TRANSACTION_PATH + file_name, 'a') as text_file:
    text_file.write(('0|%d' % (len(sale_transactions)))+ os.linesep)
    for transaction in sale_transactions:
      text_file.write(transaction + os.linesep)
    text_file.write('9|END')

  total_row = total_row+total
  print('--- End: SaleTransaction ---')

def get_sale_tran():
  with connect_db() as conn:
    cursor = conn.cursor(as_dict=True)
    query = """
        SELECT
         S.AccountCode as StoreNo,
         S.StroeCode as POSNo,
         Head.ShopID,
         Head.InvNo,
         Head.InvDate,
         Head.CreateOn,
         Head.PaymentDate as BusinessDate,
         Head.DeliveryDate,
         '01' AS TransType,
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
         Head.PaymentRefNo,
         Head.TrackingId as DisplayReceipt
        FROM TBSubOrderHead Head
        INNER JOIN TBShopMaster S on S.ShopID = Head.ShopID
        INNER JOIN TBSubOrderDetail Detail ON Head.Suborderid = Detail.Suborderid
        LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
        WHERE cast(Head.InvDate as date) = cast(getdate() - 1 as date)
        AND Head.InvNo != ''

        UNION ALL

        SELECT
         S.AccountCode as StoreNo,
         S.StroeCode as POSNo,
         Head.ShopID,
         Head.CnNo AS InvNo,
         CONVERT(VARCHAR(19),Head.CnDate,111) AS InvDate,
         Head.CreateOn,
         Head.CnDate as BusinessDate,
         Head.CnDate as DeliveryDate,
         '07' AS TransType,
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
         Head.PaymentRefNo,
         Head.SubSRNo as DisplayReceipt
        FROM TBSubSaleReturnHead Head
        INNER JOIN TBShopMaster S on S.ShopID = Head.ShopID
        INNER JOIN TBSubSaleReturnDetail Detail ON Head.SubSRNo = Detail.SubSRNo
        LEFT JOIN TBProductMaster ProMas ON Detail.PID = ProMas.PID
        WHERE Head.SubSaleReturnType IN ('CN', 'Exchange')
        AND Head.Status = 'Complete'
        AND Head.CnNo != ''
        AND cast(Head.InvDate as date) = cast(getdate() - 1 as date)
        ORDER BY InvNo
    """
    cursor.execute(query)
    return [
        gen_sale_tran_data(data)
        for data in cursor
    ]

def gen_sale_tran_data(data):
  global _t_index, _p_index
  store_number = get_option_str(data['StoreNo'])

  inv_no = get_option_str(data['InvNo'])
  bu = inv_no[0:3]

  source_trans_id = partner_code + store_number + datetime.strptime(data['InvDate'],
                                         '%Y-%m-%d').strftime('%Y%m%d')
  transaction_date = datetime.strptime(data['InvDate'],
                                         '%Y-%m-%d').strftime('%Y%m%d')

  transaction_time = data['CreateOn'].strftime('%H%M')

  trans_type = get_option_str(data['TransType'])
  trans_date = transaction_date + transaction_time

  pos_number = get_option_str(data['POSNo'])

  ticket_running_number = get_tracking_number(inv_no)
  receipt_number = store_number + pos_number + ticket_running_number \
        + transaction_date

  business_date = get_option_str(data['BusinessDate'])
  inv_date = transaction_date
  delivery_date = get_option_str(data['DeliveryDate'])
  earn_online_flag = 'N'
  t1c_card_no = get_option_str(data['T1CNoEarn'])
  redeem_amt = get_option_str(data['RedeemAmt'])
  trans_sub_type = 'T' if t1c_card_no != '' or redeem_amt != "0.000'" else 'P'

  mobile_no = '0' * 10
  user_id = 'POS'
  if trans_sub_type == 'T':
      item_seq_no = str(_t_index)
      _t_index = _t_index+ 1
  else :
      item_seq_no = str(_p_index)
      _p_index = _p_index+ 1

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
  display_receipt_no = get_option_str(data['DisplayReceipt'])

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
  res.append(display_receipt_no)
  # + return_all_flag + sblc_ncl_redeem_txn_id
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

def create_directory(path):
  if not os.path.exists(path):
    os.makedirs(path)

if __name__ == "__main__":
  create_directory('SaleTransaction')
  generate_text_t1c()
