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

create materialized view mv_autopos_bi_rbs_trans_sale_detail as
  SELECT b.store_code,
         to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text)                       AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'HH24MISS' :: text)                       AS receipt_time,
         '00' :: text                                                                                       AS transaction_type,
         d.active_posno                                                                                     AS pos_id,
         "right"((b.ticket_no) :: text, 6)                                                                  AS receipt_no,
         to_char(row_number() OVER (PARTITION BY b.ticket_no ORDER BY b.ticket_no),
                 'fm0' :: text)                                                                             AS line_number,
         b.sku,
         to_char(sum(b.quantity), 'fm0' :: text)                                                            AS quantity,
         to_char(b.jda_price, 'fm9999999990.9990' :: text)                                                  AS price_before_override,
         to_char(sum(b.total_amt), 'fm9999999990.9990' :: text)                                             AS price_after_override,
         'N' :: text                                                                                        AS price_override_flag,
         to_char(sum(b.total_amt), 'fm9999999990.9990' :: text)                                             AS total_sale,
         to_char(b.vat_rate, 'fm9999999990.9990' :: text)                                                   AS vat_rate,
         to_char(sum(((b.total_amt * (7) :: numeric) / (107) :: numeric)), 'fm9999999990.9990' :: text)     AS vat_amt,
         COALESCE(c.discount1, '' :: character varying)                                                     AS discount_code1,
         '' :: text                                                                                         AS discoune_flag1,
         to_char(sum(COALESCE(c.discount1_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt1,
         COALESCE(c.discount2, '' :: character varying)                                                     AS discount_code2,
         '' :: text                                                                                         AS discoune_flag2,
         to_char(sum(COALESCE(c.discount2_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt2,
         COALESCE(c.discount3, '' :: character varying)                                                     AS discount_code3,
         '' :: text                                                                                         AS discoune_flag3,
         to_char(sum(COALESCE(c.discount3_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt3,
         COALESCE(c.discount4, '' :: character varying)                                                     AS discount_code4,
         '' :: text                                                                                         AS discoune_flag4,
         to_char(sum(COALESCE(c.discount4_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt4,
         COALESCE(c.discount5, '' :: character varying)                                                     AS discount_code5,
         '' :: text                                                                                         AS discoune_flag5,
         to_char(sum(COALESCE(c.discount5_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt5,
         COALESCE(c.discount6, '' :: character varying)                                                     AS discount_code6,
         '' :: text                                                                                         AS discoune_flag6,
         to_char(sum(COALESCE(c.discount6_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt6,
         COALESCE(c.discount7, '' :: character varying)                                                     AS discount_code7,
         '' :: text                                                                                         AS discoune_flag7,
         to_char(sum(COALESCE(c.discount7_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt7,
         COALESCE(c.discount8, '' :: character varying)                                                     AS discount_code8,
         '' :: text                                                                                         AS discoune_flag8,
         to_char(sum(COALESCE(c.discount8_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt8,
         COALESCE(c.discount9, '' :: character varying)                                                     AS discount_code9,
         '' :: text                                                                                         AS discoune_flag9,
         to_char(sum(COALESCE(c.discount9_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt9,
         COALESCE(c.discount10, '' :: character varying)                                                    AS discount_code10,
         '' :: text                                                                                         AS discoune_flag10,
         to_char(sum(COALESCE(c.discount10_amt, (0) :: numeric)),
                 'fm9999999990.9990' :: text)                                                               AS discount_amt10,
         COALESCE(b.ticket_no, '' :: character varying)                                                     AS ref_receipt_no,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text), '' :: text) AS ref_date,
         '' :: text                                                                                         AS return_reason_code,
         lpad((b.dept_id) :: text, 3, '0' :: text)                                                          AS dept_id,
         lpad((b.subdept_id) :: text, 3, '0' :: text)                                                       AS subdept_id,
         lpad((b.class_id) :: text, 3, '0' :: text)                                                         AS class_id,
         lpad((b.subclass_id) :: text, 3, '0' :: text)                                                      AS subclass_id,
         b.vendor_id,
         b.brand_id,
         COALESCE(b.itemized_itemid, '' :: character varying)                                               AS itemized,
         b.dtype,
         COALESCE(a.customer_id, '' :: character varying)                                                   AS member_id,
         '' :: text                                                                                         AS cashier_id,
         '' :: text                                                                                         AS sale_id,
         '' :: text                                                                                         AS guide_id,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.update_on), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS last_modify_date,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                               AS interface_date
  FROM (((sale_order a
      JOIN sale_order_detail b ON ((((a.order_id) :: text = (b.order_id) :: text) AND
                                    ((b.bu) :: text = 'RBS' :: text))))
      JOIN businessunit d ON (((d.store_code) :: text = (b.store_code) :: text)))
      LEFT JOIN vw_sale_order_discount c ON ((
    ((c.order_id) :: text = (b.order_id) :: text) AND ((c.sku) :: text = (b.sku) :: text) AND (c.is_genticket = true))))
  WHERE (b.is_genticket = true)
  GROUP BY b.store_code, b.ticket_date, b.ticket_no, d.active_posno, a.credit_cardno, b.sku, b.jda_price, b.vat_rate,
           'N' :: text, c.discount1, c.discount2, c.discount3, c.discount4, c.discount5, c.discount6, c.discount7,
           c.discount8, c.discount9, c.discount10, b.dept_id, b.subdept_id, b.class_id, b.subclass_id, b.vendor_id,
           b.brand_id, b.itemized_itemid, b.dtype, a.customer_id,
           COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.update_on), 'YYYYMMDD' :: text), '' :: text)
  UNION ALL
  SELECT a.store_code,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)                         AS receipt_date,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'HH24MISS' :: text)                         AS receipt_time,
         '20' :: text                                                                                         AS transaction_type,
         d.active_posno                                                                                       AS pos_id,
         "right"((a.ticket_no) :: text, 6)                                                                    AS receipt_no,
         to_char(row_number() OVER (PARTITION BY a.ticket_no ORDER BY a.ticket_no),
                 'fm0' :: text)                                                                               AS line_number,
         '' :: character varying                                                                              AS sku,
         to_char(b.quantity, 'fm0' :: text)                                                                   AS quantity,
         to_char(b.sale_price, 'fm9999999990.9990' :: text)                                                   AS price_before_override,
         to_char(b.sale_price, 'fm9999999990.9990' :: text)                                                   AS price_after_override,
         'N' :: text                                                                                          AS price_override_flag,
         to_char(b.total_amt, 'fm9999999990.9990' :: text)                                                    AS total_sale,
         to_char(b.vat_rate, 'fm9999999990.9990' :: text)                                                     AS vat_rate,
         to_char(((b.total_amt * b.vat_rate) / ((100) :: numeric + b.vat_rate)),
                 'fm9999999990.9990' :: text)                                                                 AS vat_amt,
         '' :: character varying                                                                              AS discount_code1,
         '' :: text                                                                                           AS discoune_flag1,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt1,
         '' :: character varying                                                                              AS discount_code2,
         '' :: text                                                                                           AS discoune_flag2,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt2,
         '' :: character varying                                                                              AS discount_code3,
         '' :: text                                                                                           AS discoune_flag3,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt3,
         '' :: character varying                                                                              AS discount_code4,
         '' :: text                                                                                           AS discoune_flag4,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt4,
         '' :: character varying                                                                              AS discount_code5,
         '' :: text                                                                                           AS discoune_flag5,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt5,
         '' :: character varying                                                                              AS discount_code6,
         '' :: text                                                                                           AS discoune_flag6,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt6,
         '' :: character varying                                                                              AS discount_code7,
         '' :: text                                                                                           AS discoune_flag7,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt7,
         '' :: character varying                                                                              AS discount_code8,
         '' :: text                                                                                           AS discoune_flag8,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt8,
         '' :: character varying                                                                              AS discount_code9,
         '' :: text                                                                                           AS discoune_flag9,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt9,
         '' :: character varying                                                                              AS discount_code10,
         '' :: text                                                                                           AS discoune_flag10,
         to_char(0, 'fm9999999990.9990' :: text)                                                              AS discount_amt10,
         COALESCE(a.ticket_sale_no, '' :: text)                                                               AS ref_receipt_no,
         COALESCE(to_char(timezone('Asia/Bangkok' :: text, b.ticket_date), 'YYYYMMDD' :: text),
                  '' :: text)                                                                                 AS ref_date,
         '' :: text                                                                                           AS return_reason_code,
         lpad((b.dept_id) :: text, 3, '0' :: text)                                                            AS dept_id,
         lpad((b.subdept_id) :: text, 3, '0' :: text)                                                         AS subdept_id,
         lpad((b.class_id) :: text, 3, '0' :: text)                                                           AS class_id,
         lpad((b.subclass_id) :: text, 3, '0' :: text)                                                        AS subclass_id,
         b.vendor_id,
         b.brand_id,
         COALESCE(b.itemized_itemid, '' :: character varying)                                                 AS itemized,
         b.dtype,
         '' :: character varying                                                                              AS member_id,
         '' :: text                                                                                           AS cashier_id,
         '' :: text                                                                                           AS sale_id,
         '' :: text                                                                                           AS guide_id,
         to_char(timezone('Asia/Bangkok' :: text, a.update_on), 'YYYYMMDD' :: text)                           AS last_modify_date,
         to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)                         AS interface_date
  FROM ((sale_return a
      JOIN (SELECT sale_order_detail.sub_salereturn_no,
                   sale_order_detail.sku,
                   sale_order_detail.barcode,
                   sale_order_detail.sale_price,
                   sale_order_detail.ticket_date,
                   sum(sale_order_detail.quantity)  AS quantity,
                   sum(sale_order_detail.total_amt) AS total_amt,
                   sale_order_detail.vat_rate,
                   sale_order_detail.dept_id,
                   sale_order_detail.subdept_id,
                   sale_order_detail.class_id,
                   sale_order_detail.subclass_id,
                   sale_order_detail.vendor_id,
                   sale_order_detail.brand_id,
                   sale_order_detail.itemized_itemid,
                   sale_order_detail.dtype
            FROM sale_order_detail
            WHERE (length((sale_order_detail.sub_salereturn_no) :: text) > 0)
            GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.sku, sale_order_detail.barcode,
                     sale_order_detail.sale_price, sale_order_detail.ticket_date, sale_order_detail.vat_rate,
                     sale_order_detail.dept_id, sale_order_detail.subdept_id, sale_order_detail.class_id,
                     sale_order_detail.subclass_id, sale_order_detail.vendor_id, sale_order_detail.brand_id,
                     sale_order_detail.itemized_itemid, sale_order_detail.dtype) b ON ((
    ((a.sub_return_no) :: text = (b.sub_salereturn_no) :: text) AND ((a.bu) :: text = 'RBS' :: text))))
      JOIN businessunit d ON (((d.store_code) :: text = (a.store_code) :: text)))
  WHERE (((a.status) :: text = 'Confirmed' :: text) OR
         (to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) =
          to_char(timezone('Asia/Bangkok' :: text, a.return_date), 'YYYYMMDD' :: text)));


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

-- Order that already create ticket
-- Prepaid & COD
-- Sale & Return
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

-- Summary of SKU discount by coupon
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
  WHERE (b.is_genticket = true) AND (COALESCE(c.discount_amt_incvat, (0)::numeric) > (0)::numeric)
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




create table transaction_bi_rbs_payment
(
  transaction_date  text,
  store_code        varchar(20),
  media_member_code text,
  pay_amt_incvat    text,
  pay_amt_excvat    text,
  interface_date    text
);

create table transaction_bi_rbs_promo
(
  transaction_date      text,
  store_code            varchar(20),
  barcode               text,
  brand_id              varchar(50),
  vendor_id             varchar(50),
  dept_id               varchar(50),
  subdept_id            varchar(50),
  global_campaing_id    varchar(100),
  discount_code1        varchar,
  discount_amt_incvat1  text,
  discount_amt_excvat1  text,
  discount_code2        varchar,
  discount_amt_incvat2  text,
  discount_amt_excvat2  text,
  discount_code3        varchar,
  discount_amt_incvat3  text,
  discount_amt_excvat3  text,
  discount_code4        varchar,
  discount_amt_incvat4  text,
  discount_amt_excvat4  text,
  discount_code5        varchar,
  discount_amt_incvat5  text,
  discount_amt_excvat5  text,
  discount_code6        varchar,
  discount_amt_incvat6  text,
  discount_amt_excvat6  text,
  discount_code7        varchar,
  discount_amt_incvat7  text,
  discount_amt_excvat7  text,
  discount_code8        varchar,
  discount_amt_incvat8  text,
  discount_amt_excvat8  text,
  discount_code9        varchar,
  discount_amt_incvat9  text,
  discount_amt_excvat9  text,
  discount_code10       varchar,
  discount_amt_incvat10 text,
  discount_amt_excvat10 text,
  net_amt_incvat        text,
  net_amt_excvat        text,
  interface_date        text
);

create table transaction_bi_rbs_discount
(
  transaction_date    text,
  store_code          varchar(20),
  barcode             text,
  brand_id            varchar(50),
  vendor_id           varchar(50),
  dept_id             varchar(50),
  subdept_id          varchar(50),
  discount_host_code  varchar,
  global_campaing_id  varchar(2),
  discount_amt_incvat text,
  discount_amt_excvat text,
  interface_date      text
);

create materialized view mv_autopos_bi_rbs_trans_discount as
SELECT to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text) AS transaction_date,
       b.store_code,
       lpad((b.barcode)::text, 13, '0'::text)                                   AS barcode,
       b.brand_id,
       b.vendor_id,
       b.dept_id,
       b.subdept_id,
       CASE
         WHEN ((COALESCE(c.jda_discount_code, ''::character varying))::text = ''::text) THEN '00'::character varying
         ELSE c.jda_discount_code
         END                                                                    AS discount_host_code,
       '00'                                                                     AS global_campaing_id,
       to_char((sum(COALESCE(c.discount_amt_incvat, (0)::numeric)) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                       AS discount_amt_incvat,
       to_char((sum(COALESCE(c.discount_amt_excvat, (0)::numeric)) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                       AS discount_amt_excvat,
       to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text) AS interface_date
FROM ((sale_order a
  JOIN sale_order_detail b ON ((((a.order_id)::text = (b.order_id)::text) AND
                                ((b.bu)::text = ANY (ARRAY [('RBS'::character varying)::text])))))
       LEFT JOIN sale_order_discount c
                 ON ((((c.order_id)::text = (b.order_id)::text) AND (c.line_number = b.line_number) AND
                      ((c.line_id)::text = (b.line_id)::text) AND (length((c.jda_discount_code)::text) > 0))))
WHERE ((b.is_genticket = true) AND (COALESCE(c.discount_amt_incvat, (0)::numeric) > (0)::numeric))
GROUP BY (to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text)), b.store_code, b.barcode,
         b.brand_id, b.vendor_id, b.dept_id, b.subdept_id, c.jda_discount_code;

create materialized view mv_autopos_bi_rbs_trans_payment as
SELECT a.transaction_date,
       a.store_code,
       a.media_member_code,
       to_char(sum(a.pay_amt_incvat), 'fm9999999990.9990'::text) AS pay_amt_incvat,
       to_char(sum(a.pay_amt_excvat), 'fm9999999990.9990'::text) AS pay_amt_excvat,
       a.interface_date
FROM (SELECT to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text) AS transaction_date,
             b.store_code,
             t.tendor                                                                 AS media_member_code,
             sum(a_1.total_amt)                                                       AS pay_amt_incvat,
             sum((a_1.total_amt - ((a_1.total_amt * (7)::numeric) / (107)::numeric))) AS pay_amt_excvat,
             to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text) AS interface_date
      FROM ((sale_order a_1
        JOIN (SELECT sale_order.order_id,
                     CASE
                       WHEN ("substring"((sale_order.credit_cardno)::text, 0, 7) = ANY
                             (ARRAY ['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text]))
                         THEN '2CEN'::text
                       WHEN ((sale_order.payment_code)::text = '2C2P123'::text) THEN 'W123'::text
                       WHEN ((sale_order.payment_code)::text = 'COD'::text) THEN 'COD'::text
                       WHEN ((sale_order.payment_code)::text = 'T1C'::text) THEN 'T1CP'::text
                       WHEN ((sale_order.payment_code)::text = 'LinePay'::text) THEN 'LINE'::text
                       ELSE '2C2P'::text
                       END AS tendor
              FROM sale_order) t ON (((t.order_id)::text = (a_1.order_id)::text)))
             JOIN sale_order_detail b ON ((((a_1.order_id)::text = (b.order_id)::text) AND
                                           ((b.bu)::text = ANY (ARRAY [('RBS'::character varying)::text])))))
      WHERE (b.is_genticket = true)
      GROUP BY (to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text)), b.store_code, t.tendor
      UNION ALL
      SELECT to_char(timezone('Asia/Bangkok'::text, a_1.return_date), 'YYYYMMDD'::text)                        AS transaction_date,
             a_1.store_code,
             CASE
               WHEN ("substring"((a_1.credit_cardno)::text, 0, 7) = ANY
                     (ARRAY ['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text]))
                 THEN '2CEN'::text
               WHEN ((a_1.payment_code)::text = '2C2P123'::text) THEN 'rTRN'::text
               WHEN ((a_1.payment_code)::text = 'COD'::text) THEN 'rCOD'::text
               WHEN ((a_1.payment_code)::text = 'T1C'::text) THEN 'T1CP'::text
               WHEN ((a_1.payment_code)::text = 'LinePay'::text) THEN 'LINE'::text
               ELSE '2C2P'::text
               END                                                                                             AS media_member_code,
             (sum(b.total_amt) * ('-1'::integer)::numeric)                                                     AS pay_amt_incvat,
             (sum((b.total_amt - ((b.total_amt * (7)::numeric) / (107)::numeric))) *
              ('-1'::integer)::numeric)                                                                        AS pay_amt_excvat,
             to_char(timezone('Asia/Bangkok'::text, a_1.return_date),
                     'YYYYMMDD'::text)                                                                         AS interface_date
      FROM (sale_return a_1
             JOIN (SELECT sale_order_detail.sub_salereturn_no,
                          sale_order_detail.sku,
                          sale_order_detail.barcode,
                          sale_order_detail.sale_price,
                          sale_order_detail.ticket_date,
                          sum(sale_order_detail.quantity)  AS quantity,
                          sum(sale_order_detail.total_amt) AS total_amt
                   FROM sale_order_detail
                   WHERE (length((sale_order_detail.sub_salereturn_no)::text) > 0)
                   GROUP BY sale_order_detail.sub_salereturn_no, sale_order_detail.sku, sale_order_detail.barcode,
                            sale_order_detail.sale_price, sale_order_detail.ticket_date) b
                  ON (((a_1.sub_return_no)::text = (b.sub_salereturn_no)::text)))
      WHERE ((((a_1.status)::text = 'Confirmed'::text) AND
              ((a_1.bu)::text = ANY (ARRAY [('RBS'::character varying)::text]))) OR
             (to_char(timezone('Asia/Bangkok'::text, a_1.settle_date), 'YYYYMMDD'::text) =
              to_char(timezone('Asia/Bangkok'::text, a_1.return_date), 'YYYYMMDD'::text)))
      GROUP BY (to_char(timezone('Asia/Bangkok'::text, a_1.return_date), 'YYYYMMDD'::text)), a_1.store_code,
               CASE
                 WHEN ("substring"((a_1.credit_cardno)::text, 0, 7) = ANY
                       (ARRAY ['525667'::text, '525668'::text, '525669'::text, '528560'::text, '537798'::text]))
                   THEN '2CEN'::text
                 WHEN ((a_1.payment_code)::text = '2C2P123'::text) THEN 'rTRN'::text
                 WHEN ((a_1.payment_code)::text = 'COD'::text) THEN 'rCOD'::text
                 WHEN ((a_1.payment_code)::text = 'T1C'::text) THEN 'T1CP'::text
                 WHEN ((a_1.payment_code)::text = 'LinePay'::text) THEN 'LINE'::text
                 ELSE '2C2P'::text
                 END) a
GROUP BY a.transaction_date, a.store_code, a.media_member_code, a.interface_date;


create materialized view mv_autopos_bi_rbs_trans_promo as
SELECT to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text)                                     AS transaction_date,
       b.store_code,
       lpad((b.barcode)::text, 13, '0'::text)                                                                       AS barcode,
       b.brand_id,
       b.vendor_id,
       b.dept_id,
       b.subdept_id,
       '00'                                                                                                         AS global_campaing_id,
       COALESCE(c.discount_code1, '00'::character varying)                                                          AS discount_code1,
       to_char((COALESCE(c.discount1_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat1,
       to_char((COALESCE(c.discount_ex1_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat1,
       COALESCE(c.discount_code2, '00'::character varying)                                                          AS discount_code2,
       to_char((COALESCE(c.discount2_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat2,
       to_char((COALESCE(c.discount_ex2_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat2,
       COALESCE(c.discount_code3, '00'::character varying)                                                          AS discount_code3,
       to_char((COALESCE(c.discount3_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat3,
       to_char((COALESCE(c.discount_ex3_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat3,
       COALESCE(c.discount_code4, '00'::character varying)                                                          AS discount_code4,
       to_char((COALESCE(c.discount4_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat4,
       to_char((COALESCE(c.discount_ex4_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat4,
       COALESCE(c.discount_code5, '00'::character varying)                                                          AS discount_code5,
       to_char((COALESCE(c.discount5_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat5,
       to_char((COALESCE(c.discount_ex5_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat5,
       COALESCE(c.discount_code6, '00'::character varying)                                                          AS discount_code6,
       to_char((COALESCE(c.discount6_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat6,
       to_char((COALESCE(c.discount_ex6_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat6,
       COALESCE(c.discount_code7, '00'::character varying)                                                          AS discount_code7,
       to_char((COALESCE(c.discount7_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat7,
       to_char((COALESCE(c.discount_ex7_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat7,
       COALESCE(c.discount_code8, '00'::character varying)                                                          AS discount_code8,
       to_char((COALESCE(c.discount8_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat8,
       to_char((COALESCE(c.discount_ex8_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat8,
       COALESCE(c.discount_code9, '00'::character varying)                                                          AS discount_code9,
       to_char((COALESCE(c.discount9_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat9,
       to_char((COALESCE(c.discount_ex9_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat9,
       COALESCE(c.discount_code10, '00'::character varying)                                                         AS discount_code10,
       to_char((COALESCE(c.discount10_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_incvat10,
       to_char((COALESCE(c.discount_ex10_amt, (0)::numeric) * ('-1'::integer)::numeric),
               'fm9999999990.9990'::text)                                                                           AS discount_amt_excvat10,
       to_char(sum(a.net_amt), 'fm9999999990.9990'::text)                                                           AS net_amt_incvat,
       to_char(sum((a.net_amt - ((a.net_amt * (7)::numeric) / (107)::numeric))),
               'fm9999999990.9990'::text)                                                                           AS net_amt_excvat,
       to_char(timezone('Asia/Bangkok'::text, b.ticket_date),
               'YYYYMMDD'::text)                                                                                    AS interface_date
FROM ((sale_order a
  JOIN sale_order_detail b ON ((((a.order_id)::text = (b.order_id)::text) AND
                                ((b.bu)::text = ANY (ARRAY [('RBS'::character varying)::text])))))
       LEFT JOIN vw_sale_order_discount c
                 ON ((((c.order_id)::text = (b.order_id)::text) AND ((c.sku)::text = (b.sku)::text) AND
                      (c.is_genticket = true))))
WHERE (b.is_genticket = true)
GROUP BY (to_char(timezone('Asia/Bangkok'::text, b.ticket_date), 'YYYYMMDD'::text)), b.store_code, b.barcode,
         b.brand_id, b.vendor_id, b.dept_id, b.subdept_id, c.discount_code1, c.discount_code2, c.discount_code3,
         c.discount_code4, c.discount_code5, c.discount_code6, c.discount_code7, c.discount_code8, c.discount_code9,
         c.discount_code10, c.discount1_amt, c.discount2_amt, c.discount3_amt, c.discount4_amt, c.discount5_amt,
         c.discount6_amt, c.discount7_amt, c.discount8_amt, c.discount9_amt, c.discount10_amt, c.discount_ex1_amt,
         c.discount_ex2_amt, c.discount_ex3_amt, c.discount_ex4_amt, c.discount_ex5_amt, c.discount_ex6_amt,
         c.discount_ex7_amt, c.discount_ex8_amt, c.discount_ex9_amt, c.discount_ex10_amt;
