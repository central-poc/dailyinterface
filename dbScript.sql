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






