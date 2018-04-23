from common import sftp_ofm
from datetime import datetime, timedelta
import pymssql
import sys
import csv
import os
import random
import uuid

def generate_text_t1c():
    sale_transactions = gen_tender(get_sale_tran())
    total_row = len(sale_transactions)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
    target_dir = 'siebel/nrtsale'
    target_path = os.path.join(parent_path, 'output', target_dir)
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    interface_name = 'BCH_OFM_T1C_NRTSales'
    now = datetime.now()
    batchdatetime = now.strftime('%d%m%Y_%H:%M:%S:%f')[:-3]
    filedatetime = now.strftime('%d%m%Y_%H%M%S')
    datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, 1)
    filepath = os.path.join(target_path, datfile)
    print(filepath)
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
      outfile.write('{}|OFM|001|1|{}|{}|OFM|{}|{}'.format(
        interface_name, total_row, batchdatetime, attribute1, attribute2))

    destination = '/inbound/BCH_SBL_NRTSales/req'
    sftp_ofm(target_path, destination)


def get_sale_tran():
    with pymssql.connect("10.17.1.23", "CTOAI", "CTO@Ai", "DBInterfaceSiebel") as conn:
        with conn.cursor(as_dict=True) as cursor:
            sql = """
                SELECT top 1
                BatchID,
                TotalRecord
                FROM tb_Control_Transaction
                WHERE Type = 'S'
                ORDER BY BatchID DESC
            """
            cursor.execute(sql)
            data = cursor.fetchone()
            batch_id = data["BatchID"]
            print(batch_id)
            query = """
            Select top 500 * from (
                SELECT
                    '1' AS LNIdentifier,
                    'OFM-' + h.ReceiptNo + '-' + CAST(NEWID() AS NVARCHAR(36)) AS SourceTransID,
                    h.StoreCode as StoreNo,
                    ISNULL(h.PosNo,'') as PosNo,
                    h.ReceiptNo,
                    h.TransType,
                    'P' as TransSubType,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') + '_00:00:00:000' as TransDate,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') as BusinessDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.InvoiceDate, 103), '/', ''),'') as InvoiceDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.DeliveryDate, 103), '/', ''),'') as DeliveryDate,
                    h.OnlineFlg as EarnOnlineFlag,
                    h.T1CNumber as T1CCardNo,
                    ISNULL(h.Mobile,'') as MobileNo,
                    h.[User] as UserID,
                    d.SeqNo as ItemSeqNo,
                    d.RTLProductCode as ProductCode,
                    d.Barcode as ProductBarcode,
                    d.ItemQty as Quantity,
                    d.PriceUnit,
                    CONVERT(DECIMAL(10,3),d.PriceUnit * d.ItemQty) as PriceTotal,
                    CASE WHEN d.ItemQty = 0
                        THEN d.NetPriceTotal
                        ELSE CONVERT(DECIMAL(10,3),d.NetPriceTotal / d.ItemQty)
                    END as NetPriceUnit,
                    d.NetPriceTotal,
                    d.DiscountAmount as DiscountTotal,
                    d.VatAmount,
                    '' as TenderType,
                    '' as TenderRefNo,
                    ISNULL(H.OriginalReceipt,'') as OriginalReceiptNo,
                    ISNULL(d.OriginalSeq,'') as OriginalItemSequenceNo,
                    h.DisplayReceiptNo,
                    h.ReturnAllFlag,
                    ISNULL(h.SBLCnclRedeemTxnID,'') as SBLCnclRedeemTxnID,
                    h.TotalAmount as TotalAmount
                FROM tb_saleH h
                JOIN tb_SaleD d ON h.ReceiptNo = d.ReceiptNo
                WHERE h.BatchID = %(BatchID)s

                Union All

                SELECT
                    '1' AS LNIdentifier,
                    'OFM-' + c.ReceiptNo + '-' + CAST(NEWID() AS NVARCHAR(36)) AS SourceTransID,
                    '000001' as StoreNo,
                    '' as PosNo,
                    c.ReceiptNo,
                    '01'as TransType,
                    'C' as TransSubType,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') + '_00:00:00:000' as TransDate,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') as BusinessDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.InvoiceDate, 103), '/', ''),'') as InvoiceDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.DeliveryDate, 103), '/', ''),'') as DeliveryDate,
                    '' as EarnOnlineFlag,
                    h.T1CNumber as T1CCardNo,
                    ISNULL(h.Mobile,'') as MobileNo,
                    h.[User] as UserID,
                    SeqNo as  ItemSeqNo,
                    '' as ProductCode,
                    '' as ProductBarcode,
                    1 as Quantity,
                    0 as PriceUnit,
                    0 as PriceTotal,
                    0 as NetPriceUnit,
                    0 as NetPriceTotal,
                    0 as DiscountTotal,
                    0 as VatAmount,
                    '' as TenderType,
                    ISNULL(CouponNumber,'') as TenderRefNo,
                    ISNULL(H.OriginalReceipt,'') as OriginalReceiptNo,
                    ISNULL(SeqNo,'') as OriginalItemSequenceNo,
                    h.DisplayReceiptNo,
                    h.ReturnAllFlag,
                    ISNULL(h.SBLCnclRedeemTxnID,'') as SBLCnclRedeemTxnID,
                    h.TotalAmount as TotalAmount

                FROM tb_SaleDiscCpn c
                JOIN tb_saleH h ON c.ReceiptNo = h.ReceiptNo
                WHERE c.BatchID = %(BatchID)s

                Union All

                SELECT
                    '1' AS LNIdentifier,
                    'OFM-' + t.ReceiptNo + '-' + CAST(NEWID() AS NVARCHAR(36)) AS SourceTransID,
                    '000001' as StoreNo,
                    '' as PosNo,
                    t.ReceiptNo,
                    CASE WHEN t.Amount > 0 THEN '01' ELSE '07' END as TransType,
                    'T' as TransSubType,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') + '_00:00:00:000' as TransDate,
                    REPLACE(CONVERT(VARCHAR(10), h.TransDate, 103), '/', '') as BusinessDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.InvoiceDate, 103), '/', ''),'') as InvoiceDate,
                    ISNULL(REPLACE(CONVERT(VARCHAR(10), h.DeliveryDate, 103), '/', ''),'') as DeliveryDate,
                    '' as EarnOnlineFlag,
                    h.T1CNumber as T1CCardNo,
                    ISNULL(h.Mobile,'') as MobileNo,
                    h.[User] as UserID,
                    SeqNo as  ItemSeqNo,
                    '' as ProductCode,
                    '' as ProductBarcode,
                    1 as Quantity,
                    0 as PriceUnit,
                    0 as PriceTotal,
                    0 as NetPriceUnit,
                    Amount as NetPriceTotal,
                    0 as DiscountTotal,
                    0 as VatAmount,
                    TenderType,
                    ISNULL(TenderRefNo,'') as TenderRefNo,
                    ISNULL(H.OriginalReceipt,'') as OriginalReceiptNo,
                    ISNULL(SeqNo,'') as OriginalItemSequenceNo,
                    h.DisplayReceiptNo,
                    h.ReturnAllFlag,
                    ISNULL(h.SBLCnclRedeemTxnID,'') as SBLCnclRedeemTxnID,
                    h.TotalAmount as TotalAmount

                FROM tb_SaleTender t
                JOIN tb_saleH h ON t.ReceiptNo = h.ReceiptNo
                WHERE t.BatchID = %(BatchID)s) a
                ORDER BY a.ReceiptNo asc, a.TransSubType asc
                  """
            cursor.execute(query, dict(BatchID=batch_id))
            return [gen_sale_tran_data(data) for data in cursor]


def gen_sale_tran_data(data):
    line_identifier = data['LNIdentifier']
    source_trans_id = data['SourceTransID']
    store_number = data['StoreNo']
    pos_number = data['PosNo']
    receipt_number = data['ReceiptNo']
    trans_type = data['TransType']
    trans_sub_type = data['TransSubType']
    trans_date = str(data['TransDate'])
    business_date = str(data['BusinessDate'])
    inv_date = str(data['InvoiceDate'])
    delivery_date = str(data['DeliveryDate'])
    earn_online_flag = data['EarnOnlineFlag']
    t1c_card_no = data['T1CCardNo']
    mobile_no = data['MobileNo']
    user_id = data['UserID']
    item_seq_no = str(data['ItemSeqNo'])

    product_code = str(data['ProductCode'])
    product_barcode = str(data['ProductBarcode'])
    quantity = str(data['Quantity'])
    price_unit = str(data['PriceUnit'])
    price_total = str(data['PriceTotal'])
    net_price_unit = str(data['NetPriceUnit'])
    net_price_total = str(data['NetPriceTotal'])
    discount_total = str(data['DiscountTotal'])
    vat_amount = str(data['VatAmount'])
    order_tender_type = str(data['TenderType'])

    tender_type = data['TenderType']
    tender_ref_no = str(data['TenderRefNo'])
    original_receipt_no = str(data['OriginalReceiptNo'])
    original_item_seq_no = str(data['OriginalItemSequenceNo'])
    display_receipt_no = str(data['DisplayReceiptNo'])
    return_all_flag = data['ReturnAllFlag']
    sbl_cancel_redeem = str(data['SBLCnclRedeemTxnID'])
    order_net_amt = str(data['TotalAmount'])

    res = []
    res.append(line_identifier)
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
    res.append(order_net_amt)
    return res


def gen_tender(input):
    # [a(row[4])=a(row[4])+row for row in input]
    values = set(map(lambda x: x[4], input))
    groups = [[y for y in input if y[4] == x] for x in values]
    for g in groups:
        for index, data in enumerate(g):
            if data[6] == 'P':
                break

        total = g[index][:]
        total[1] = "OFM-" + total[4]+ "-" + str(uuid.uuid4()).upper()
        total[2] = "000001"
        total[6] = "A"
        total[15:27] = [
            "1", "", "", "1", "", "", "", total[32], "", "", "",""
        ]
        g.append(total)

    out = [item[:32] for sublist in groups for item in sublist]

    return ['|'.join(row) for row in out]

if __name__ == "__main__":
    generate_text_t1c()
    # update_order()
