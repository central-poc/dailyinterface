create table transaction_siebel
(
  line_identifier     varchar(100),
  source_trans_id     varchar(100),
  bu                  varchar(20),
  store_code          text,
  pos_no              text,
  receipt_no          text,
  trans_type          text,
  trans_sub_type      text,
  trans_date          text,
  business_date       text,
  invoice_date        text,
  delivery_date       text,
  earn_online_flag    varchar(100),
  t1c_cardno          varchar,
  mobile_no           varchar(50),
  pos_user_id         varchar(100),
  item_seq_no         varchar(100),
  product_code        varchar,
  product_barcode     varchar,
  quantity            text,
  price_unit          text,
  price_total         text,
  net_price_unit      text,
  net_price_total     text,
  discount_total      text,
  vat_amt             text,
  tendor_type         text,
  tendor_ref          varchar,
  display_receipt_no  varchar,
  orginal_receipt_no  varchar(100),
  orginal_item_seq_no varchar(100),
  return_all_flag     varchar(100),
  sbl_cancel_redeem   varchar(100),
  net_amt             numeric,
  redeem_amt          numeric,
  redeem_cash         numeric,
  interface_date      text
);


create materialized view mv_autopos_siebel as
  SELECT '1'                                                                                                   AS line_identifier,
         ''                                                                                                    AS source_trans_id,
         siebel.bu,
         siebel.store_code,
         siebel.store_code                                                                                     AS pos_no,
         (((siebel.bu) :: text || (siebel.order_id) :: text) ||
          to_char(timezone('Asia/Bangkok' :: text, now()), 'HH24MISSMS' :: text))                              AS receipt_no,
         siebel.trans_type,
         siebel.trans_sub_type,
         siebel.trans_date,
         siebel.business_date,
         siebel.invoice_date,
         siebel.delivery_date,
         'N'                                                                                                   AS earn_online_flag,
         siebel.t1c_cardno,
         siebel.mobile_no,
         'FMS'                                                                                                 AS pos_user_id,
         '1'                                                                                                   AS item_seq_no,
         siebel.product_code,
         siebel.product_barcode,
         to_char(sum(siebel.quantity), 'fm999999999999999990.9990' :: text)                                    AS quantity,
         to_char(siebel.full_price, 'fm999999999999999990.9990' :: text)                                       AS price_unit,
         to_char(sum((siebel.full_price * (siebel.quantity) :: numeric)),
                 'fm999999999999999990.9990' :: text)                                                          AS price_total,
         to_char(siebel.sale_price, 'fm999999999999999990.9990' :: text)                                       AS net_price_unit,
         to_char(sum(siebel.total_amt), 'fm999999999999999990.9990' :: text)                                   AS net_price_total,
         to_char(sum(siebel.item_discountamt), 'fm999999999999999990.9990' :: text)                            AS discount_total,
         to_char(sum(siebel.vat_amt), 'fm999999999999999990.9990' :: text)                                     AS vat_amt,
         siebel.tendor_type,
         siebel.tendor_ref,
         siebel.display_receipt_no,
         ''                                                                                                    AS orginal_receipt_no,
         ''                                                                                                    AS orginal_item_seq_no,
         ''                                                                                                    AS return_all_flag,
         ''                                                                                                    AS sbl_cancel_redeem,
         sum(siebel.net_amt)                                                                                   AS net_amt,
         sum(siebel.redeem_amt)                                                                                AS redeem_amt,
         sum(siebel.redeem_cash)                                                                               AS redeem_cash,
         siebel.interface_date
  FROM (SELECT b.bu,
               CASE
                 WHEN ((b.bu) :: text = 'RBS' :: text) THEN '20174' :: text
                 WHEN ((b.bu) :: text = 'RBS9' :: text) THEN '20181' :: text
                 ELSE '' :: text
                   END                                                                                               AS store_code,
               a.order_id,
               '01' :: text                                                                                          AS trans_type,
               'P' :: text                                                                                           AS trans_sub_type,
               to_char(timezone('Asia/Bangkok' :: text, a.order_date),
                       'DDMMYYYY_HH24:MI:SS:MS' :: text)                                                             AS trans_date,
               to_char(timezone('Asia/Bangkok' :: text, a.payment_date),
                       'DDMMYYYY' :: text)                                                                           AS business_date,
               to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'DDMMYYYY' :: text)                          AS invoice_date,
               COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.delivered_date), 'DDMMYYYY' :: text),
                        '' :: text)                                                                                  AS delivery_date,
               a.t1c_earn_cardno                                                                                     AS t1c_cardno,
               a.bill_phoneno                                                                                        AS mobile_no,
               COALESCE(b.jda_sku, '' :: character varying)                                                          AS product_code,
               b.barcode                                                                                             AS product_barcode,
               b.quantity,
               b.full_price,
               b.sale_price,
               b.total_amt,
               b.item_discountamt,
               b.vat_amt,
               CASE
                 WHEN (("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                        (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text])) AND
                       ((b.bu) :: text = 'RBS' :: text)) THEN '2CEN' :: text
                 WHEN (("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                        (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text])) AND
                       ((b.bu) :: text <> 'RBS' :: text)) THEN 'CENT' :: text
                 WHEN ((a.payment_code) :: text = '2C2P123' :: text) THEN 'W123' :: text
                 WHEN ((a.payment_code) :: text = 'COD' :: text) THEN 'COD' :: text
                 WHEN ((a.payment_code) :: text = 'T1C' :: text) THEN 'T1CP' :: text
                 WHEN ((a.payment_code) :: text = 'LinePay' :: text) THEN 'LINE' :: text
                 ELSE '2C2P' :: text
                   END                                                                                               AS tendor_type,
               a.credit_cardno                                                                                       AS tendor_ref,
               a.order_id                                                                                            AS display_receipt_no,
               b.total_amt                                                                                           AS net_amt,
               round(((b.total_amt * a.redeem_amt) / a.total_amt), 2)                                                AS redeem_amt,
               round(((b.total_amt * a.customer_pay_amt) / a.total_amt), 2)                                          AS redeem_cash,
               to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text)                          AS interface_date
        FROM ((sale_order a
            JOIN sale_order_detail b ON (((a.order_id) :: text = (b.order_id) :: text)))
            JOIN ticket c ON ((((c.ticket_no) :: text = (b.ticket_no) :: text) AND
                               ((c.ticket_type) :: text <> 'D' :: text))))
        WHERE ((b.is_genticket = true) AND (a.total_amt > (0) :: numeric))
        UNION ALL
        SELECT b.bu,
               CASE
                 WHEN ((b.bu) :: text = 'RBS' :: text) THEN '20174' :: text
                 WHEN ((b.bu) :: text = 'RBS9' :: text) THEN '20181' :: text
                 ELSE '' :: text
                   END                                                                                               AS store_code,
               a.order_id,
               '01' :: text                                                                                          AS trans_type,
               'C' :: text                                                                                           AS trans_sub_type,
               to_char(timezone('Asia/Bangkok' :: text, a.order_date),
                       'DDMMYYYY_HH24:MI:SS:MS' :: text)                                                             AS trans_date,
               to_char(timezone('Asia/Bangkok' :: text, a.payment_date),
                       'DDMMYYYY' :: text)                                                                           AS business_date,
               to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'DDMMYYYY' :: text)                          AS invoice_date,
               COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.delivered_date), 'DDMMYYYY' :: text),
                        '' :: text)                                                                                  AS delivery_date,
               a.t1c_earn_cardno                                                                                     AS t1c_cardno,
               a.bill_phoneno                                                                                        AS mobile_no,
               '' :: character varying                                                                               AS product_code,
               '' :: character varying                                                                               AS product_barcode,
               1                                                                                                     AS quantity,
               0                                                                                                     AS full_price,
               0                                                                                                     AS sale_price,
               0                                                                                                     AS total_amt,
               0                                                                                                     AS item_discountamt,
               0                                                                                                     AS vat_amt,
               'Coupon' :: text                                                                                      AS tendor_type,
               c.promotion_code                                                                                      AS tendor_ref,
               a.order_id                                                                                            AS display_receipt_no,
               0                                                                                                     AS net_amt,
               0                                                                                                     AS redeem_amt,
               0                                                                                                     AS redeem_cash,
               to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text)                          AS interface_date
        FROM (((sale_order a
            JOIN sale_order_detail b ON (((a.order_id) :: text = (b.order_id) :: text)))
            JOIN ticket d ON ((((d.ticket_no) :: text = (b.ticket_no) :: text) AND
                               ((d.ticket_type) :: text <> 'D' :: text))))
            JOIN sale_order_discount c ON ((
          ((c.order_id) :: text = (b.order_id) :: text) AND (c.line_number = b.line_number) AND
          ((c.line_id) :: text = (b.line_id) :: text) AND (length((c.promotion_code) :: text) > 0))))
        WHERE ((b.is_genticket = true) AND (c.discount_amt_incvat > (0) :: numeric))
        UNION ALL
        SELECT a.bu,
               CASE
                 WHEN ((a.bu) :: text = 'RBS' :: text) THEN '20174' :: text
                 WHEN ((a.bu) :: text = 'RBS9' :: text) THEN '20181' :: text
                 ELSE '' :: text
                   END                                                                                          AS store_code,
               a.sub_return_no                                                                                  AS order_id,
               '07' :: text                                                                                     AS trans_type,
               'P' :: text                                                                                      AS trans_sub_type,
               to_char(timezone('Asia/Bangkok' :: text, a.return_date),
                       'DDMMYYYY_HH24:MI:SS:MS' :: text)                                                        AS trans_date,
               to_char(timezone('Asia/Bangkok' :: text, a.create_on), 'DDMMYYYY' :: text)                       AS business_date,
               to_char(timezone('Asia/Bangkok' :: text, a.create_on), 'DDMMYYYY' :: text)                       AS invoice_date,
               COALESCE(to_char(timezone('Asia/Bangkok' :: text, a.create_on), 'DDMMYYYY' :: text),
                        '' :: text)                                                                             AS delivery_date,
               a.t1c_earn_cardno                                                                                AS t1c_cardno,
               a.bill_phoneno                                                                                   AS mobile_no,
               b.jda_sku                                                                                        AS product_code,
               b.barcode                                                                                        AS product_barcode,
               b.quantity,
               b.full_price,
               b.sale_price,
               b.total_amt,
               b.item_discountamt,
               b.vat_amt,
               CASE
                 WHEN (("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                        (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text])) AND
                       ((a.bu) :: text = 'RBS' :: text)) THEN '2CEN' :: text
                 WHEN (("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                        (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text])) AND
                       ((a.bu) :: text <> 'RBS' :: text)) THEN 'CENT' :: text
                 WHEN ((a.payment_code) :: text = '2C2P123' :: text) THEN 'W123' :: text
                 WHEN ((a.payment_code) :: text = 'COD' :: text) THEN 'COD' :: text
                 WHEN ((a.payment_code) :: text = 'T1C' :: text) THEN 'T1CP' :: text
                 WHEN ((a.payment_code) :: text = 'LinePay' :: text) THEN 'LINE' :: text
                 ELSE '2C2P' :: text
                   END                                                                                          AS tendor_type,
               a.credit_cardno                                                                                  AS tendor_ref,
               a.return_no                                                                                      AS display_receipt_no,
               b.total_amt                                                                                      AS net_amt,
               round(((b.total_amt * a.return_redeem_amt) / a.total_amt), 2)                                    AS redeem_amt,
               round(((b.total_amt * a.return_cash_amt) / a.total_amt), 2)                                      AS redeem_cash,
               to_char(timezone('Asia/Bangkok' :: text, a.create_on), 'YYYYMMDD' :: text)                       AS interface_date
        FROM ((sale_return a
            JOIN ticket c ON ((((c.ticket_no) :: text = (a.ticket_no) :: text) AND
                               ((c.ticket_type) :: text <> 'D' :: text))))
            JOIN (SELECT sale_order_detail.sub_salereturn_no,
                         COALESCE(sale_order_detail.jda_sku, '' :: character varying) AS jda_sku,
                         sale_order_detail.barcode,
                         sale_order_detail.full_price,
                         sale_order_detail.sale_price,
                         sale_order_detail.item_discountamt,
                         sale_order_detail.vat_amt,
                         sum(sale_order_detail.quantity)                              AS quantity,
                         sum(sale_order_detail.total_amt)                             AS total_amt
                  FROM sale_order_detail
                  WHERE (length((sale_order_detail.sub_salereturn_no) :: text) > 0)
                  GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.jda_sku, sale_order_detail.barcode,
                           sale_order_detail.full_price, sale_order_detail.sale_price,
                           sale_order_detail.item_discountamt, sale_order_detail.vat_amt) b ON ((
          (a.sub_return_no) :: text = (b.sub_salereturn_no) :: text)))
        WHERE (((a.status) :: text = 'Completed' :: text) AND (a.total_amt > (0) :: numeric))) siebel
  GROUP BY siebel.bu, siebel.store_code, siebel.order_id, siebel.trans_type, siebel.trans_sub_type, siebel.trans_date,
           siebel.business_date, siebel.invoice_date, siebel.delivery_date, siebel.t1c_cardno, siebel.mobile_no,
           siebel.product_code, siebel.product_barcode, siebel.full_price, siebel.sale_price, siebel.tendor_type,
           siebel.tendor_ref, siebel.display_receipt_no, siebel.interface_date;
