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
