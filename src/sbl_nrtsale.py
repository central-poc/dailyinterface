from common import connect_cmos, mssql_cmos, sftp, cleardir
from datetime import datetime, timedelta
import sys
import csv
import os
import uuid

order_ids = []


def generate_text_t1c():
  sale_transactions = gen_tender(get_sale_tran())
  total_row = len(sale_transactions)

  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 'siebel/nrtsale'
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)
  cleardir(target_path)

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

  destination = 'incoming/nrtsale'
  sftp('cgo-prod', target_path, destination)


def get_sale_tran():
  with connect_cmos() as conn:
    with conn.cursor(as_dict=True) as cursor:
      query = """
            SELECT result.* FROM (
              SELECT
                Head.Suborderid as id,
                Head.OrderId as ParentID,
                Case
                  WHEN S.ShopID in ('254', '255') Then '99101'
                  WHEN S.ShopID = '256' Then '99570'
                  ELSE S.AccountCode
                END as StoreNo,
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
                CONVERT(DECIMAL(10,3),Detail.TotalAmt/Detail.Quantity) AS UnitSalesPrice,
                Detail.SeqNo,
                CASE
                  WHEN Detail.IsVat = 1 THEN CONVERT(DECIMAL(10,3),(Detail.UnitPrice - Detail.UnitPrice / 1.07))
                  ELSE Detail.UnitPrice
                END as VatAmt ,
                (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
                (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS TransactionDiscountAmount ,
                CASE WHEN Head.SubOrderId like 'MO%'
                  THEN
                    ISNULL(rbsprod.Upc, '')
                  ELSE
                    isnull(nullif(LTRIM(RTRIM(cdsmap.sbc)),''), cdsmap.ibc)
                END AS ProdBarcode,
                Head.T1CNoEarn as T1CRefNo,
                Head.ShipMobileNo as Mobile,
                oh.CreditCardNo  as PaymentRefNo,
                Head.OrderId as DisplayReceipt,
                Case
                    WHEN Head.TenderType = 'T1PM' THEN Head.TenderType
                    ELSE Head.PaymentType
                END as TenderType,
                Head.NetAmt as OrderNetAmt,
                Head.VatAmt as OrderVatAmt,
                isnull(CONVERT(DECIMAL(10,3),Head.RedeemAmt * Head.GrandTotalAmt  / case when oh.GrandTotalAmt=0 THEN 1 else oh.GrandTotalAmt end),0) as RedeemAmt,
                isnull(CONVERT(DECIMAL(10,3),Head.RedeemCash * Head.GrandTotalAmt / case when oh.GrandTotalAmt=0 THEN 1 else oh.GrandTotalAmt end),0) as RedeemCash
              FROM TBSubOrderHead Head
              INNER JOIN TBShopMaster S on S.ShopID = Head.ShopID
              INNER JOIN TBSubOrderDetail Detail ON Head.Suborderid = Detail.Suborderid
              LEFT JOIN [10.17.220.55].DBCDSContent.dbo.tbproductmapping cdsmap on cdsmap.pidnew = Detail.PID
              LEFT JOIN [mssql.production.thecentral.com].DBMKPOnline.dbo.Product rbsprod on rbsprod.pid = Detail.PID
              INNER JOIN TBOrderHead oh on Head.OrderId = oh.OrderId
              WHERE 1 = 1
              AND Head.IsGenT1c = 'No'
              AND cast(head.InvDate as date) = cast(getdate() - 1 as date)
              AND Head.InvNo != ''
              UNION ALL

              SELECT
                Head.Suborderid as id,
                Head.OrderId as ParentID,
                Case
                  WHEN S.ShopID in ('254', '255') Then '99101'
                  WHEN S.ShopID = '256' Then '99570'
                  ELSE S.AccountCode
                END as StoreNo,
                S.StroeCode as POSNo,
                '' as ShopID,
                Head.InvNo,
                format(Head.InvDate, 'ddMMyyyy', 'en-us') as InvDate,
                format(Head.PaymentDate, 'ddMMyyyy', 'en-us') as BusinessDate,
                format(Head.DeliveryDate, 'ddMMyyyy', 'en-us') as DeliveryDate,
                '01' AS TransType,
                format(Head.suborderdate, 'ddMMyyyy_HH:mm:ss:fff', 'en-us') as TransDate,
                '' as PID,
                1 as Quantity,
                0 as UnitPrice ,
                0 as UnitSalesPrice,
                1 as SeqNo,
                0 as VatAmt ,
                0 as NetAmt,
                0 as TransactionDiscountAmount ,
                '' as ProdBarcode,
                Head.T1CNoEarn as T1CRefNo,
                Head.ShipMobileNo as Mobile,
                dis.PromotionNo as PaymentRefNo,
                Head.OrderId as DisplayReceipt,
                'Coupon' as TenderType,
                0 as OrderNetAmt,
                0 as OrderVatAmt,
                0 as RedeemAmt,
                0 as RedeemCash
              FROM TBSubOrderHead head
              JOIN TBSubOrderDetail d ON head.SubOrderId= d.SubOrderId
              INNER JOIN TBShopMaster S on S.ShopID = Head.ShopID
              JOIN TBOrderDiscount dis ON head.OrderId = dis.OrderId AND d.PID = dis.PId
              WHERE 1 = 1
              AND Head.IsGenT1c = 'No'
              AND cast(head.InvDate as date) = cast(getdate() - 1 as date)
              AND Head.InvNo != ''
            ) result
            UNION ALL

            SELECT
              Head.SubSRNo as id,
              Head.SRNo as ParentID,
              Case
                WHEN S.ShopID in ('254', '255') Then '99101'
                WHEN S.ShopID = '256' Then '99570'
                ELSE S.AccountCode
              END as StoreNo,
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
              Detail.UnitPrice - CONVERT(DECIMAL(10,3),(Detail.ItemDiscAmt + Detail.OrdDiscAmt)/Detail.Quantity) AS UnitSalesPrice,
              Detail.SeqNo,
              CASE
                WHEN Detail.IsVat = 1 THEN CONVERT(DECIMAL(10,3),(Detail.UnitPrice - Detail.UnitPrice / 1.07))
                ELSE Detail.UnitPrice
              END as VatAmt ,
              (Detail.UnitPrice * Detail.Quantity) - (Detail.ItemDiscAmt + Detail.OrdDiscAmt) AS NetAmt,
              (Head.ItemDiscAmt + Head.OrdDiscAmt) AS TransactionDiscountAmount ,
              CASE WHEN Head.SubOrderId like 'MO%'
                THEN
                  ISNULL(rbsprod.Upc, '')
                ELSE
                  isnull(nullif(LTRIM(RTRIM(cdsmap.sbc)),''), cdsmap.ibc)
              END AS ProdBarcode,
              Head.T1CNoEarn as T1CRefNo,
              Head.ShipMobileNo as Mobile,
              oh.CreditCardNo as PaymentRefNo,
              Head.SRNo as DisplayReceipt,
                Case
                    WHEN oh.TenderType = 'T1PM' THEN oh.TenderType
                    ELSE Head.PaymentType
                END as TenderType,
              Head.NetAmt as OrderNetAmt,
              Head.VatAmt as OrderVatAmt,
              isnull(CONVERT(DECIMAL(10,3),Head.RedeemAmt * Detail.TotalAmt / oh.GrandTotalAmt),0) as RedeemAmt,
              isnull(CONVERT(DECIMAL(10,3),Head.RedeemCash * Detail.TotalAmt / oh.GrandTotalAmt),0) as RedeemCash
            FROM TBSubSaleReturnHead Head
            INNER JOIN TBShopMaster S on S.ShopID = Head.ShopID
            INNER JOIN TBSubSaleReturnDetail Detail ON Head.SubSRNo = Detail.SubSRNo
            LEFT JOIN [10.17.220.55].DBCDSContent.dbo.tbproductmapping cdsmap on cdsmap.pidnew = Detail.PID
            LEFT JOIN [mssql.production.thecentral.com].DBMKPOnline.dbo.Product rbsprod on rbsprod.pid = Detail.PID
            INNER JOIN TBOrderHead oh on Head.OrderId = oh.OrderId
            WHERE 1 = 1
            AND Head.IsGenT1c = 'No'
            AND cast(head.CnDate as date) = cast(getdate() - 1 as date)
            AND Head.SubSaleReturnType IN ('CN', 'Exchange')
            AND Head.Status = 'Completed'
            AND Head.CnNo != ''
            Order By ParentID
            """
      cursor.execute(query)
      return [gen_sale_tran_data(data) for data in cursor]


def gen_sale_tran_data(data):
  global order_ids
  order_ids.append(data['id'])
  order_id = data['ParentID']
  source_trans_id = ''
  store_number = data['StoreNo']
  pos_number = data['POSNo']
  receipt_number = data['InvNo']
  trans_type = data['TransType']
  trans_date = data['TransDate']
  business_date = data['BusinessDate']
  inv_date = data['InvDate']
  delivery_date = data['DeliveryDate']
  earn_online_flag = 'N'
  t1c_card_no = data['T1CRefNo']
  mobile_no = data['Mobile']
  user_id = 'POS'
  redeem_amt = str(data['RedeemAmt'])

  product_code = str(data['PID'])
  product_barcode = str(data['ProdBarcode'])
  quantity = str(data['Quantity'])
  price_unit = str(data['UnitPrice'])
  price_total = str(data['Quantity'] * data['UnitPrice'])
  net_price_unit = str(data['UnitSalesPrice'])
  net_price_total = str(data['NetAmt'])
  discount_total = str(data['TransactionDiscountAmount'])
  vat_amount = str(data['VatAmt'])

  order_tender_type = str(data['TenderType'])
  if order_tender_type != "Coupon":
    trans_sub_type = 'P'
  else:
    trans_sub_type = 'C'
    order_tender_type = ''
  tender_type = '' if trans_sub_type == 'P' or trans_sub_type == 'C' else 'T1PM' if redeem_amt != '0.000' else 'CASH'
  tender_ref_no = str(data['PaymentRefNo'])
  original_receipt_no = ''
  original_item_seq_no = ''
  display_receipt_no = data['DisplayReceipt']
  return_all_flag = ''
  sbl_cancel_redeem = ''

  order_net_amt = data['OrderNetAmt']
  order_redeem_amt = data['RedeemAmt']
  order_redeem_cash = data['RedeemCash']

  res = []
  res.append('1')
  res.append(source_trans_id)
  res.append(store_number)
  res.append(pos_number)
  res.append(data['id'])
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
  res.append("1")
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
  res.append(order_tender_type)
  res.append(order_net_amt)
  res.append(order_redeem_amt)
  res.append(order_redeem_cash)
  res.append(order_id)
  res.append(data['id'])
  return res


def gen_tender(input):
  values = set(map(lambda x: x[37], input))
  groups = [[y for y in input if y[37] == x] for x in values]
  for g in groups:
    net_amt = 0
    redeem_amt = 0
    order_redeem_cash = 0
    temp_suborder_id = ""
    product_index = 0
    for index, sub in enumerate(g):
      if sub[6] == "P":
        product_index = index
      if sub[6] == "C" or temp_suborder_id == sub[37]:
        continue
      net_amt = net_amt + sub[33]
      redeem_amt = redeem_amt + sub[34]
      order_redeem_cash = order_redeem_cash + sub[35]
      temp_suborder_id = sub[37]

    if redeem_amt == 0:
      g.append(tender(g[product_index][:], net_amt, False))
    else:
      g.append(tender(g[product_index][:], redeem_amt, True))
      if order_redeem_cash > 0:
        g.append(tender(g[product_index][:], order_redeem_cash, False))

    total = g[0][:]
    total[6] = "A"
    total[15:27] = ["1", "", "", "1", "", "", "", str(net_amt), "", "", "", ""]
    g.append(total)

  for g in groups:
    index = 1
    for o in g:
      o[1] = str(uuid.uuid4()).upper()
      o[2] = '{:0>6}'.format(o[2])
      if o[6] != "A" and o[6] != "C":
        o[15] = str(index)
        index = index + 1
        if o[6] == "P":
          o[26] = ''

  out = [item[:32] for sublist in groups for item in sublist]

  return ['|'.join(row) for row in out]


def tender(data, amount, isT1C):
  t = data
  t[6] = "T"
  t[16:26] = ["", "", "1", "", "", "", str(amount), "", "", t[32]]
  if isT1C and len(t[12]) > 0:
    t[26] = t[12]
    t[25] = 'T1CRedeem'

  return t


def update_order():
  sale = []
  sr = []
  for id in order_ids:
    if id[:2] in ['CR', 'SR']:
      sr.append(id)
    else:
      sale.append(id)
  with mssql_cmos() as conn:
    query = "UPDATE TBSubOrderHead SET IsGenT1c = 'Yes' WHERE Suborderid in ('{}')".format(
        "','".join(sale))
    conn.execute_non_query(query)
    query = "UPDATE TBSubSaleReturnHead SET IsGenT1c = 'Yes' WHERE SubSRNo in ('{}')".format(
        "','".join(sr))
    conn.execute_non_query(query)


if __name__ == "__main__":
  generate_text_t1c()
  update_order()
