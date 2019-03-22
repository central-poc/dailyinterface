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

create table transaction_ofin_line_cgo
(
  source           text,
  invoice_no       varchar(50),
  invoice_type     text,
  vendor_id        varchar(6),
  line_type        text,
  item_description varchar(50),
  amount           numeric,
  item_qty         text,
  item_cost        text,
  temp_field       text,
  cost_center      text,
  gl_account       text,
  interface_date   text
);


create materialized view mv_autopos_ofin_head_cgo as
  SELECT 'FMS' :: text                                                                          AS source,
         b.doc_no                                                                               AS invoice_no,
         d.vendor_id,
         to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'DDMMYY' :: text)   AS invoice_date,
         sum(((b.quantity) :: numeric * b.total_amt))                                           AS invoice_total,
         '010138' :: text                                                                       AS store_id,
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
  FROM ((((sale_order a
      JOIN sale_order_detail b ON (((a.order_id) :: text = (b.order_id) :: text)))
      JOIN ticket c ON (((c.ticket_no) :: text = (b.ticket_no) :: text)))
      JOIN vendor d ON (((d.store_code) :: text = (b.store_code) :: text)))
      JOIN payment e ON ((((e.payment_code) :: text = (a.payment_code) :: text) AND (e.is_online_payment = false))))
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
         '010138' :: text                                                             AS store_id,
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
  FROM (((sale_return a
      JOIN ticket c ON ((((c.ticket_no) :: text = (a.ticket_no) :: text) AND
                         ((c.order_id) :: text = (a.order_id) :: text))))
      JOIN vendor d ON (((d.store_code) :: text = (a.store_code) :: text)))
      JOIN payment e ON ((((e.payment_code) :: text = (a.payment_code) :: text) AND (e.is_online_payment = false))))
  WHERE ((a.is_settled = true) AND ((a.doc_no) :: text <> '' :: text));


create materialized view mv_autopos_ofin_line_cgo as
  SELECT 'FMS' :: text                                                                          AS source,
         b.doc_no                                                                               AS invoice_no,
         CASE
           WHEN ((c.status) :: text <> 'Canceled' :: text) THEN 'A' :: text
           WHEN ((c.status) :: text = 'Canceled' :: text) THEN 'F' :: text
           ELSE '0' :: text
             END                                                                                AS invoice_type,
         d.vendor_id,
         'I' :: text                                                                            AS line_type,
         b.ticket_no                                                                            AS item_description,
         sum(((b.quantity) :: numeric * b.total_amt))                                           AS amount,
         '1' :: text                                                                            AS item_qty,
         '' :: text                                                                             AS item_cost,
         '' :: text                                                                             AS temp_field,
         '' :: text                                                                             AS cost_center,
         '' :: text                                                                             AS gl_account,
         to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'YYYYMMDD' :: text) AS interface_date
  FROM ((((sale_order a
      JOIN sale_order_detail b ON (((a.order_id) :: text = (b.order_id) :: text)))
      JOIN ticket c ON ((((c.ticket_no) :: text = (b.ticket_no) :: text) AND
                         ((c.order_id) :: text = (b.order_id) :: text))))
      JOIN vendor d ON (((d.store_code) :: text = (b.store_code) :: text)))
      JOIN payment e ON ((((e.payment_code) :: text = (a.payment_code) :: text) AND (e.is_online_payment = false))))
  WHERE ((b.is_genticket = true) AND (b.is_confirm_paid = true) AND ((b.doc_no) :: text <> '' :: text))
  GROUP BY b.ticket_no, b.doc_no, c.status, d.vendor_id, b.is_salereturn, b.is_rtc,
           (to_char(timezone('Asia/Bangkok' :: text, b.confirm_received_date), 'YYYYMMDD' :: text))
  UNION ALL
  SELECT 'FMS' :: text                                                                AS source,
         a.doc_no                                                                     AS invoice_no,
         CASE
           WHEN ((c.status) :: text <> 'Canceled' :: text) THEN 'D' :: text
           WHEN ((c.status) :: text = 'Canceled' :: text) THEN 'H' :: text
           ELSE '0' :: text
             END                                                                      AS invoice_type,
         d.vendor_id,
         'I' :: text                                                                  AS line_type,
         a.ticket_no                                                                  AS item_description,
         (a.total_amt * ('-1' :: integer) :: numeric)                                 AS amount,
         '1' :: text                                                                  AS item_qty,
         '' :: text                                                                   AS item_cost,
         '' :: text                                                                   AS temp_field,
         '' :: text                                                                   AS cost_center,
         '' :: text                                                                   AS gl_account,
         to_char(timezone('Asia/Bangkok' :: text, a.settle_date), 'YYYYMMDD' :: text) AS interface_date
  FROM (((sale_return a
      JOIN ticket c ON ((((c.ticket_no) :: text = (a.ticket_no) :: text) AND
                         ((c.order_id) :: text = (a.order_id) :: text))))
      JOIN vendor d ON (((d.store_code) :: text = (a.store_code) :: text)))
      JOIN payment e ON ((((e.payment_code) :: text = (a.payment_code) :: text) AND (e.is_online_payment = false))))
  WHERE ((a.is_settled = true) AND ((a.doc_no) :: text <> '' :: text));

-- RPT 03
create materialized view mv_autopos_ofin_zn_cgo as
  SELECT a.branch_id                                                                                           AS ofin_branch_code,
         a.cost_center                                                                                         AS ofin_cost_profit_center,
         replace((a.account_code) :: text, '-' :: text, '' :: text)                                            AS account_code,
         a.subaccount_code,
         to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'DDMMYY' :: text)                             AS business_date,
         sum((a.debit + a.additional_amt))                                                                     AS debit,
         sum((a.credit + a.additional_amt))                                                                    AS credit,
         'FMS-SALES'                                                                                           AS journal_source_name,
         'FMS-SALES'                                                                                           AS journal_category_name,
         ('FMS-SALES-' :: text ||
          to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'YYYYMMDD' :: text))                         AS batch_name,
         (((a.branch_id) :: text || '-' :: text) ||
          to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'YYYYMMDD' :: text))                         AS journal_name,
         a.account_type,
         a.branch_id,
         a.ofin_for_cfs,
         a.account_description,
         to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'YYYYMMDD' :: text)                           AS interface_date
  FROM artransaction a
  WHERE (((a.doc_type) :: text = 'DLN' :: text) AND ((a.activity_type) :: text = 'ConfirmDeliveryCGO' :: text) AND
         ((a.trans_type) :: text = 'S' :: text) AND ((a.status) :: text = 'AT' :: text))
  GROUP BY a.branch_id, a.account_code, a.subaccount_code, a.cost_center, a.account_description, a.ofin_for_cfs,
           (to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'DDMMYY' :: text)),
           (to_char(timezone('Asia/Bangkok' :: text, a.trans_date), 'YYYYMMDD' :: text)), a.account_type;