-- auto-generated definition
create table transaction_bi_rbs_sale_detail
(
  store_code            varchar(20),
  receipt_date          text,
  receipt_time          text,
  transaction_type      varchar(100),
  pos_id                varchar(30),
  receipt_no            text,
  line_number           text,
  sku                   varchar(50),
  quantity              text,
  price_before_override text,
  price_after_override  text,
  price_override_flag   text,
  total_sale            text,
  vat_rate              text,
  vat_amt               text,
  discount_code1        varchar,
  discount_amt1         text,
  discoune_flag1        varchar(100),
  discount_code2        varchar,
  discount_amt2         text,
  discoune_flag2        varchar(100),
  discount_code3        varchar,
  discount_amt3         text,
  discoune_flag3        varchar(100),
  discount_code4        varchar,
  discount_amt4         text,
  discoune_flag4        varchar(100),
  discount_code5        varchar,
  discount_amt5         text,
  discoune_flag5        varchar(100),
  discount_code6        varchar,
  discount_amt6         text,
  discoune_flag6        varchar(100),
  discount_code7        varchar,
  discount_amt7         text,
  discoune_flag7        varchar(100),
  discount_code8        varchar,
  discount_amt8         text,
  discoune_flag8        varchar(100),
  discount_code9        varchar,
  discount_amt9         text,
  discoune_flag9        varchar(100),
  discount_code10       varchar,
  discount_amt10        text,
  discoune_flag10       varchar(100),
  ref_receipt_no        varchar,
  ref_date              text,
  return_reason_code    varchar(100),
  dept_id               text,
  subdept_id            text,
  class_id              text,
  subclass_id           text,
  vendor_id             varchar(50),
  brand_id              varchar(50),
  itemized              varchar,
  dtype                 varchar(1),
  member_id             varchar,
  cashier_id            varchar(100),
  sale_id               varchar(100),
  guide_id              varchar(100),
  last_modify_date      text,
  interface_date        text
);

-- auto-generated definition
create table transaction_bi_rbs_tendor_detail
(
  store_code        varchar(20),
  receipt_date      text,
  receipt_time      text,
  transaction_type  varchar(100),
  pos_id            varchar(30),
  receipt_no        text,
  line_number       text,
  media_member_code varchar(100),
  media_member_desc varchar(100),
  tendor_amt        text,
  credit_cardno     varchar(16),
  interface_date    text
);

create table transaction_bi_rbs_installment
(
  store_code         varchar(20),
  receipt_date       text,
  receipt_time       text,
  transaction_type   varchar(100),
  pos_id             varchar(30),
  receipt_no         text,
  vendor_id          varchar(50),
  brand_id           varchar(50),
  dept_id            text,
  subdept_id         text,
  sku                varchar(50),
  tendor_type        varchar(100),
  installment_period varchar(100),
  credit_cardno      varchar(16),
  interest_rate      text,
  tendor_amt         text,
  interface_date     text
);

create table transaction_bi_rbs_dpcn
(
  store_code       varchar(20),
  receipt_date     text,
  receipt_time     text,
  transaction_type varchar(100),
  pos_id           varchar(30),
  receipt_no       text,
  coupon_id        varchar(50),
  interface_date   text
);

create table transaction_ofin_zn_cgo
(
  ofin_branch_code        varchar(6),
  ofin_cost_profit_center varchar(10),
  account_code            text,
  subaccount_code         varchar(20),
  business_date           text,
  debit                   numeric,
  credit                  numeric,
  journal_source_name     varchar(100),
  journal_category_name   varchar(100),
  batch_name              text,
  journal_name            text,
  account_type            varchar(1),
  branch_id               varchar(6),
  ofin_for_cfs            varchar(6),
  account_description     varchar(255),
  interface_date          text
);

create table transaction_ofin_head_cgo
(
  source              text,
  invoice_no          varchar(50),
  vendor_id           varchar(6),
  invoice_date        text,
  invoice_total       numeric,
  store_id            text,
  invoice_type        text,
  imported_goods      text,
  hold_reason01       text,
  invoice_tax_name    text,
  tax_inv_running_no  text,
  blank1              text,
  rtv_auth_no         text,
  currency_code       text,
  terms               varchar(4),
  blank2              text,
  gr_tran_no          varchar(50),
  ass_tax_invoice_num text,
  blank3              text,
  tax_invoice_date    text,
  invoice_rtv_type    text,
  currency_rate       text,
  interface_date      text
);


create materialized view mv_autopos_bi_rbs_trans_tendor_detail as
  SELECT b.store_code,
         to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text)              AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'HH24MISS' :: text)              AS receipt_time,
         '00' :: text                                                                              AS transaction_type,
         c.active_posno                                                                            AS pos_id,
         "right"((b.ticket_no) :: text, 6)                                                         AS receipt_no,
         to_char(row_number() OVER (PARTITION BY b.ticket_no ORDER BY b.ticket_no), 'fm0' :: text) AS line_number,
         CASE
           WHEN ("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                 (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text]))
                   THEN '2CEN' :: text
           WHEN ((a.payment_code) :: text = '2C2P123' :: text) THEN 'W123' :: text
           WHEN ((a.payment_code) :: text = 'COD' :: text) THEN 'COD' :: text
           WHEN ((a.payment_code) :: text = 'T1C' :: text) THEN 'T1CP' :: text
           WHEN ((a.payment_code) :: text = 'LinePay' :: text) THEN 'LINE' :: text
           ELSE '2C2P' :: text
             END                                                                                   AS media_member_code,
         '' :: text                                                                                AS media_member_desc,
         to_char(sum(a.customer_pay_amt), 'fm9999999990.9990' :: text)                             AS tendor_amt,
         a.credit_cardno,
         to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text)              AS interface_date
  FROM ((sale_order a
      JOIN sale_order_detail b ON ((((a.order_id) :: text = (b.order_id) :: text) AND
                                    ((b.bu) :: text = 'RBS' :: text))))
      JOIN businessunit c ON (((c.store_code) :: text = (b.store_code) :: text)))
  WHERE (b.is_genticket = true)
  GROUP BY b.store_code, b.ticket_date, b.ticket_no, c.active_posno, a.credit_cardno, a.payment_code
  UNION ALL
  SELECT a.store_code,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)              AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'HH24MISS' :: text)              AS receipt_time,
         '20' :: text                                                                              AS transaction_type,
         c.active_posno                                                                            AS pos_id,
         "right"((a.ticket_no) :: text, 6)                                                         AS receipt_no,
         to_char(row_number() OVER (PARTITION BY a.ticket_no ORDER BY a.ticket_no), 'fm0' :: text) AS line_number,
         CASE
           WHEN ("substring"((a.credit_cardno) :: text, 0, 7) = ANY
                 (ARRAY['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text]))
                   THEN '2CEN' :: text
           WHEN ((a.payment_code) :: text = '2C2P123' :: text) THEN 'rTRN' :: text
           WHEN ((a.payment_code) :: text = 'COD' :: text) THEN 'rCOD' :: text
           WHEN ((a.payment_code) :: text = 'T1C' :: text) THEN 'T1CP' :: text
           WHEN ((a.payment_code) :: text = 'LinePay' :: text) THEN 'LINE' :: text
           ELSE '2C2P' :: text
             END                                                                                   AS media_member_code,
         '' :: text                                                                                AS media_member_desc,
         to_char(b.total_amt, 'fm9999999990.9990' :: text)                                         AS tendor_amt,
         a.credit_cardno,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)              AS interface_date
  FROM ((sale_return a
      JOIN (SELECT sale_order_detail.sub_salereturn_no,
                   sale_order_detail.sku,
                   sale_order_detail.barcode,
                   sale_order_detail.sale_price,
                   sale_order_detail.ticket_date,
                   sum(sale_order_detail.quantity)  AS quantity,
                   sum(sale_order_detail.total_amt) AS total_amt
            FROM sale_order_detail
            WHERE (length((sale_order_detail.sub_salereturn_no) :: text) > 0)
            GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.sku, sale_order_detail.barcode,
                     sale_order_detail.sale_price, sale_order_detail.ticket_date) b ON (((a.sub_return_no) :: text =
                                                                                         (b.sub_salereturn_no) :: text)))
      JOIN businessunit c ON ((((c.store_code) :: text = (a.store_code) :: text) AND ((a.bu) :: text = 'RBS' :: text))))
  WHERE (((a.status) :: text = 'Confirmed' :: text) OR
         (to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) =
          to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)));


create materialized view mv_autopos_bi_rbs_trans_installment as
  SELECT b.store_code,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS receipt_date,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'HH24MISS' :: text),
                  '' :: text)                                                                               AS receipt_time,
         '00' :: text                                                                                       AS transaction_type,
         c.active_posno                                                                                     AS pos_id,
         "right"((b.ticket_no) :: text, 6)                                                                  AS receipt_no,
         b.vendor_id,
         b.brand_id,
         lpad((b.dept_id) :: text, 3, '0' :: text)                                                          AS dept_id,
         lpad((b.subdept_id) :: text, 3, '0' :: text)                                                       AS subdept_id,
         b.sku,
         '' :: text                                                                                         AS tendor_type,
         '' :: text                                                                                         AS installment_period,
         a.credit_cardno,
         to_char(0, 'fm9999999990.9990' :: text)                                                            AS interest_rate,
         to_char(sum(a.customer_pay_amt), 'fm9999999990.9990' :: text)                                      AS tendor_amt,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS interface_date
  FROM ((sale_order a
      JOIN sale_order_detail b ON ((((a.order_id) :: text = (b.order_id) :: text) AND
                                    ((b.bu) :: text = 'RBS' :: text))))
      JOIN businessunit c ON (((c.store_code) :: text = (b.store_code) :: text)))
  WHERE (b.is_genticket = true)
  GROUP BY b.store_code, b.ticket_date, b.ticket_no, c.active_posno, a.credit_cardno, b.vendor_id, b.brand_id,
           b.dept_id, b.subdept_id, b.sku
  UNION ALL
  SELECT a.store_code,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text) AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'HH24MISS' :: text) AS receipt_time,
         '20' :: text                                                                 AS transaction_type,
         c.active_posno                                                               AS pos_id,
         "right"((a.ticket_no) :: text, 6)                                            AS receipt_no,
         b.vendor_id,
         b.brand_id,
         lpad((b.dept_id) :: text, 3, '0' :: text)                                    AS dept_id,
         lpad((b.subdept_id) :: text, 3, '0' :: text)                                 AS subdept_id,
         b.sku,
         '' :: text                                                                   AS tendor_type,
         '' :: text                                                                   AS installment_period,
         a.credit_cardno,
         to_char(0, 'fm9999999990.9990' :: text)                                      AS interest_rate,
         to_char(b.total_amt, 'fm9999999990.9990' :: text)                            AS tendor_amt,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text) AS interface_date
  FROM ((sale_return a
      JOIN (SELECT sale_order_detail.sub_salereturn_no,
                   sale_order_detail.sku,
                   sale_order_detail.barcode,
                   sale_order_detail.sale_price,
                   sale_order_detail.ticket_date,
                   sum(sale_order_detail.quantity)  AS quantity,
                   sum(sale_order_detail.total_amt) AS total_amt,
                   sale_order_detail.vendor_id,
                   sale_order_detail.brand_id,
                   sale_order_detail.dept_id,
                   sale_order_detail.subdept_id
            FROM sale_order_detail
            WHERE (length((sale_order_detail.sub_salereturn_no) :: text) > 0)
            GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.sku, sale_order_detail.barcode,
                     sale_order_detail.sale_price, sale_order_detail.ticket_date, sale_order_detail.vendor_id,
                     sale_order_detail.brand_id, sale_order_detail.dept_id, sale_order_detail.subdept_id) b ON ((
    (a.sub_return_no) :: text = (b.sub_salereturn_no) :: text)))
      JOIN businessunit c ON ((((c.store_code) :: text = (a.store_code) :: text) AND ((a.bu) :: text = 'RBS' :: text))))
  WHERE (((a.status) :: text = 'Confirmed' :: text) OR
         (to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) =
          to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)));


create materialized view mv_autopos_bi_rbs_trans_dpcn as
  SELECT b.store_code,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS receipt_date,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'HH24MISS' :: text),
                  '' :: text)                                                                               AS receipt_time,
         '00' :: text                                                                                       AS transaction_type,
         d.active_posno                                                                                     AS pos_id,
         "right"((b.ticket_no) :: text, 6)                                                                  AS receipt_no,
         c.jda_short_name                                                                                   AS coupon_id,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS interface_date
  FROM (((sale_order a
      JOIN sale_order_detail b ON ((((a.order_id) :: text = (b.order_id) :: text) AND
                                    ((b.bu) :: text = 'RBS' :: text))))
      JOIN businessunit d ON (((d.store_code) :: text = (b.store_code) :: text)))
      LEFT JOIN sale_order_discount c ON ((
    ((c.order_id) :: text = (b.order_id) :: text) AND (c.line_number = b.line_number) AND
    ((c.line_id) :: text = (b.line_id) :: text) AND (length((c.jda_discount_code) :: text) > 0))))
  WHERE (b.is_genticket = true)
  GROUP BY b.store_code, b.ticket_date, b.ticket_no, d.active_posno, c.jda_short_name
  UNION ALL
  SELECT a.store_code,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text) AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'HH24MISS' :: text) AS receipt_time,
         '20' :: text                                                                 AS transaction_type,
         c.active_posno                                                               AS pos_id,
         "right"((a.ticket_no) :: text, 6)                                            AS receipt_no,
         '' :: character varying                                                      AS coupon_id,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text) AS interface_date
  FROM ((sale_return a
      JOIN (SELECT sale_order_detail.sub_salereturn_no,
                   sale_order_detail.sku,
                   sale_order_detail.barcode,
                   sale_order_detail.sale_price,
                   sale_order_detail.ticket_date,
                   sum(sale_order_detail.quantity)  AS quantity,
                   sum(sale_order_detail.total_amt) AS total_amt,
                   sale_order_detail.vendor_id,
                   sale_order_detail.brand_id,
                   sale_order_detail.dept_id,
                   sale_order_detail.subdept_id
            FROM sale_order_detail
            WHERE (length((sale_order_detail.sub_salereturn_no) :: text) > 0)
            GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.sku, sale_order_detail.barcode,
                     sale_order_detail.sale_price, sale_order_detail.ticket_date, sale_order_detail.vendor_id,
                     sale_order_detail.brand_id, sale_order_detail.dept_id, sale_order_detail.subdept_id) b ON ((
    (a.sub_return_no) :: text = (b.sub_salereturn_no) :: text)))
      JOIN businessunit c ON ((((c.store_code) :: text = (a.store_code) :: text) AND ((a.bu) :: text = 'RBS' :: text))))
  WHERE (((a.status) :: text = 'Confirmed' :: text) OR
         (to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) =
          to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)));


create materialized view mv_autopos_ofin_head as
  SELECT 'FMS' :: text                                                                          AS source,
         b.doc_no                                                                               AS invoice_no,
         d.vendor_id,
         to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'DDMMYY' :: text)   AS invoice_date,
         sum(((b.quantity) :: numeric * b.total_amt))                                           AS invoice_total,
         '020174' :: text                                                                       AS store_id,
         CASE
           WHEN ((c.status) :: text <> 'Canceled' :: text) THEN 'A' :: text
           WHEN ((c.status) :: text = 'Canceled' :: text) THEN 'F' :: text
           ELSE '0' :: text
             END                                                                                AS invoice_type,
         'N' :: text                                                                            AS imported_goods,
         '' :: text                                                                             AS hold_reason01,
         '00.00' :: text                                                                        AS invoice_tax_name,
         '' :: text                                                                             AS tax_inv_running_no,
         '' :: text                                                                             AS blank1,
         '' :: text                                                                             AS rtv_auth_no,
         'THB' :: text                                                                          AS currency_code,
         d.term_of_payment                                                                      AS terms,
         '' :: text                                                                             AS blank2,
         b.doc_no                                                                               AS gr_tran_no,
         '' :: text                                                                             AS ass_tax_invoice_num,
         '' :: text                                                                             AS blank3,
         '' :: text                                                                             AS tax_invoice_date,
         '' :: text                                                                             AS invoice_rtv_type,
         '' :: text                                                                             AS currency_rate,
         to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'YYYYMMDD' :: text) AS interface_date
  FROM (((sale_order a
      JOIN sale_order_detail b ON (((a.order_id) :: text = (b.order_id) :: text)))
      JOIN ticket c ON (((c.ticket_no) :: text = (b.ticket_no) :: text)))
      JOIN vendor d ON (((d.store_code) :: text = (b.store_code) :: text)))
  WHERE ((b.is_genticket = true) AND (b.is_confirm_paid = true) AND ((b.doc_no) :: text <> '' :: text))
  GROUP BY b.doc_no, (to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'DDMMYY' :: text)), c.status,
           d.vendor_id, b.is_salereturn, b.is_rtc,
           (to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'YYYYMMDD' :: text)), d.term_of_payment
  UNION ALL
  SELECT 'FMS' :: text                                                                AS source,
         a.doc_no                                                                     AS invoice_no,
         d.vendor_id,
         to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'DDMMYY' :: text)   AS invoice_date,
         (a.total_amt * ('-1' :: integer) :: numeric)                                 AS invoice_total,
         '020174' :: text                                                             AS store_id,
         CASE
           WHEN ((c.status) :: text <> 'Canceled' :: text) THEN 'D' :: text
           WHEN ((c.status) :: text = 'Canceled' :: text) THEN 'H' :: text
           ELSE '0' :: text
             END                                                                      AS invoice_type,
         'N' :: text                                                                  AS imported_goods,
         '' :: text                                                                   AS hold_reason01,
         '00.00' :: text                                                              AS invoice_tax_name,
         '' :: text                                                                   AS tax_inv_running_no,
         '' :: text                                                                   AS blank1,
         '' :: text                                                                   AS rtv_auth_no,
         'THB' :: text                                                                AS currency_code,
         d.term_of_payment                                                            AS terms,
         '' :: text                                                                   AS blank2,
         a.doc_no                                                                     AS gr_tran_no,
         a.ticket_sale_no                                                             AS ass_tax_invoice_num,
         '' :: text                                                                   AS blank3,
         '' :: text                                                                   AS tax_invoice_date,
         '' :: text                                                                   AS invoice_rtv_type,
         '' :: text                                                                   AS currency_rate,
         to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) AS interface_date
  FROM ((sale_return a
      JOIN ticket c ON ((((c.ticket_no) :: text = (a.ticket_no) :: text) AND
                         ((c.order_id) :: text = (a.order_id) :: text))))
      JOIN vendor d ON (((d.store_code) :: text = (a.store_code) :: text)))
  WHERE ((a.is_settled = true) AND ((a.doc_no) :: text <> '' :: text));











