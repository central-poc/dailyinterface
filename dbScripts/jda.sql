create table transaction_jda
(
  order_id            varchar,
  store_code          varchar(20),
  transaction_date    text,
  transaction_time    text,
  transaction_type    text,
  ticket_no           varchar,
  seq_no              text,
  sku                 text,
  barcode             varchar(50),
  qty_sign            text,
  quantity            text,
  jda_price           text,
  price_override      text,
  price_override_flag text,
  total_net_amt       text,
  vat_amt             text,
  discount_type1      varchar,
  discount_amt1       text,
  discount_type2      varchar,
  discount_amt2       text,
  discount_type3      varchar,
  discount_amt3       text,
  discount_type4      varchar,
  discount_amt4       text,
  discount_type5      varchar,
  discount_amt5       text,
  discount_type6      varchar,
  discount_amt6       text,
  discount_type7      varchar,
  discount_amt7       text,
  discount_type8      varchar,
  discount_amt8       text,
  discount_type9      varchar,
  discount_amt9       text,
  discount_type10     varchar,
  discount_amt10      text,
  ref_id              text,
  ref_ticket          text,
  ref_date            text,
  reason_code         text,
  event_no            text,
  dept_id             text,
  subdept_id          text,
  itemized            text,
  dtype               text,
  credit_cardno       text,
  customer_id         text,
  member_point        text,
  cashier_id          text,
  sale_person         text,
  interface_date      text
);