from common import sftp
from datetime import datetime, timedelta
import pymssql
import sys
import csv
import os
import random


def generate_text_t1c():
  sale_transactions = get_sale_tran()
  total_row = len(sale_transactions)

  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 'siebel/nrtsale'
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  interface_name = 'BCH_CGO_T1C_NRTSales'
  now = datetime.now()
  batchdatetime = now.strftime('%d%m%Y_%H:%M:%S:%f')[:-3]
  filedatetime = now.strftime('%d%m%Y_%H%M%S')
  datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, 1)
  filepath = os.path.join(target_path, datfile)

  with open(filepath, 'w') as text_file:
    text_file.write('0|{}\n'.format(total_row))
    for transaction in sale_transactions:
      text_file.write('{}\n'.format(transaction))
    text_file.write('9|END')

  ctrlfile = '{}_{}.ctrl'.format(interface_name, filedatetime)
  filepath = os.path.join(target_path, ctrlfile)
  attribute1 = ""
  attribute2 = ""
  with open(filepath, 'w') as outfile:
    outfile.write('{}|CGO|001|1|{}|{}|CGO|{}|{}'.format(
      interface_name, total_row, batchdatetime, attribute1, attribute2))

  destination = '/inbound/BCH_SBL_NRTSales/req'
  sftp(target_path, destination)

def get_sale_tran():
  with pymssql.connect("10.17.220.173", "app-t1c", "Zxcv123!", "DBMKP") as conn:
    with conn.cursor(as_dict=True) as cursor:
      query = """
          SELECT
          concat(Head.ShopID, '-', NewID()) as SourceTransID,
          S.AccountCode as StoreNo,
          S.StroeCode as POSNo,
          Head.ShopID,
          Head.InvNo,
          format(Head.InvDate, 'ddMMyyyy', 'en-us') as InvDate,
          format(Head.PaymentDate, 'ddMMyyyy', 'en-us') as BusinessDate,
          format(Head.DeliveryDate, 'ddMMyyyy', 'en-us') as DeliveryDate,
          '01' AS TransType,
          format(Head.suborderdate, 'ddMMyyyy_HH:mm:ss:fff', 'en-us') as TransDate,
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
          Head.ShipMobileNo as Mobile,
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
          concat(Head.ShopID, '-', NewID()) as SourceTransID,
          S.AccountCode as StoreNo,
          S.StroeCode as POSNo,
          Head.ShopID,
          Head.CnNo AS InvNo,
          format(Head.CnDate, 'ddMMyyyy', 'en-us') AS InvDate,
          format(Head.CnDate, 'ddMMyyyy', 'en-us') AS BusinessDate,
          format(Head.CnDate, 'ddMMyyyy', 'en-us') AS DeliveryDate,
          '07' AS TransType,
          format(Head.subsrdate, 'ddMMyyyy_HH:mm:ss:fff', 'en-us') as TransDate,
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
          Head.ShipMobileNo as Mobile,
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
      """
      cursor.execute(query)
      return [gen_sale_tran_data(data) for data in cursor]

def gen_sale_tran_data(data):
  _t_index = 1
  _p_index = 1
  source_trans_id = data['SourceTransID']
  store_number = data['StoreNo']
  pos_number = data['POSNo']
  receipt_number = data['InvNo']
  trans_type = data['TransType']
  trans_date = data['TransDate']
  business_date = data['BusinessDate']
  inv_date = data['InvDate']
  delivery_date = data['DeliveryDate']
  earn_online_flag = 'N'
  t1c_card_no = data['T1CNoEarn']
  mobile_no = data['Mobile']
  user_id = 'POS'
  redeem_amt = str(data['RedeemAmt'])
  trans_sub_type = 'T' if t1c_card_no != '' or redeem_amt != '0.000' else 'P'

  if trans_sub_type == 'T':
      item_seq_no = str(_t_index)
      _t_index = _t_index + 1
  else :
      item_seq_no = str(_p_index)
      _p_index = _p_index + 1

  product_code = str(data['PID'])
  product_barcode = str(data['ProdBarcode'])
  quantity = str(data['Quantity'])
  price_unit = str(data['UnitPrice'])
  price_total = str(data['Quantity'] * data['UnitPrice'])
  net_price_unit = str(data['UnitSalesPrice'])
  net_price_total = str(data['NetAmt'])
  discount_total = str(data['TransactionDiscountAmount'])
  vat_amount = str(data['VatAmt'])
  tender_type = '' if trans_sub_type == 'P' else 'T1PM' if redeem_amt != '0.000' else 'CASH'
  tender_ref_no = '' if trans_sub_type == 'P' else t1c_card_no if redeem_amt != '0.000' else data['PaymentRefNo']
  original_receipt_no = ''
  original_item_seq_no = ''
  display_receipt_no = data['DisplayReceipt']
  return_all_flag = ''
  sbl_cancel_redeem = ''

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
  res.append(return_all_flag)
  res.append(sbl_cancel_redeem)
  return "|".join(res)

if __name__ == "__main__":
  generate_text_t1c()
