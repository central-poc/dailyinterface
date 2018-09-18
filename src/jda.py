from common import connect_psql
import os
import psycopg2.extras


def get_file_seq(prefix, output_path, ext):
  files = [
      f.split('.')[0] for f in os.listdir(output_path)
      if os.path.isfile(os.path.join(output_path, f)) and f.endswith(ext)
  ]
  return 1 if not files else max(
      int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def generate_data_file(output_path, store, data):
  file_name = 'SD' + store + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)

  with open(file_fullpath, 'w') as f:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write('''
          {:0>5}{:0>8}{:0>4}{:0>2}{:9}{:0>3}{:0>16}{:0>16}{:1}{:0>6}
          {:0>12}{:0>12}{:1}{:0>12}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}
          {:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}{:0>12}{:4}
          {:0>12}{:4}{:0>12}{:4}{:0>12}{:21}{:9}{:0>8}{:0>2}{:0>6}
          {:0>3}{:0>3}{:0>16}{:1}{:16}{:21}{:0>8}{:8}{:8}
        '''.format(
            d['store_code'], d['transaction_date'], d['transaction_time'],
            d['transaction_type'], d['ticket_no'], d['seq_no'], d['sku'],
            d['barcode'], d['qty_sign'], d['quantity'], d['jda_price'],
            d['price_override'], d['is_price_override'], d['total_net_amt'],
            d['vat_amt'], d['discount_type1'], d['discount_amt1'],
            d['discount_type2'], d['discount_amt2'], d['discount_type3'],
            d['discount_amt3'], d['discount_type4'], d['discount_amt4'],
            d['discount_type5'], d['discount_amt5'], d['discount_type6'],
            d['discount_amt6'], d['discount_type7'], d['discount_amt7'],
            d['discount_type8'], d['discount_amt8'], d['discount_type9'],
            d['discount_amt9'], d['discount_type10'], d['discount_amt10'],
            d['ref_id'], d['ref_ticket'], d['ref_date'], d['reason_code'],
            d['event_no'], d['dept_id'], d['subdept_id'], d['itemized'],
            d['dtype'], d['credit_cardno'], d['customer_id'],
            d['member_point'], d['cashier_id'], d['sale_person']))
        count = count + 1
      print('[AutoPOS] - JDA Create Files Complete..')
    except Exception as e:
      print('[AutoPOS] - JDA Create Files Error: {}: '.format(e))


def query_store():
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute("select store_code from businessunit")

      return cursor.fetchall()


def query_data_by_store(store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = """
        select 
          b.store_code, '00' as transaction_type, coalesce(b.ticket_no, '') as ticket_no, 
          b.sku, b.barcode, '+' as qty_sign, 'N' as is_price_override, b.dept_id, b.subdept_id, b.dtype, 
          '' as ref_id, '' as ref_ticket, '' as reason_code, '' as event_no, '' as member_point, '' as cashier_id, '' as sale_person,
          coalesce(to_char(invoice_date, 'YYYYMMDD'), '00000000') as transaction_date, 
          coalesce(to_char(invoice_date, 'HH24MI'), '0000') as transaction_time,
          to_char(b.line_number, 'fm000') as seq_no, to_char(b.quantity, 'fm000000') as quantity,
          to_char(b.jda_price, 'fm000000000000') as jda_price, 
          to_char(b.total_amt * 100, 'fm000000000000') as price_override, 
          to_char(b.quantity * b.total_amt * 100, 'fm000000000000') as total_net_amt,
          to_char(coalesce(b.vat_amt, 0) * 100, 'fm000000000000') as vat_amt, 
          coalesce(to_char(a.payment_date, 'YYYYMMDD'), '00000000') as ref_date, 
          coalesce(b.itemized_itemid, '') as itemized, 
          coalesce(a.credit_cardno, '') as credit_cardno, coalesce(customer_id, '') as customer_id, 
          coalesce(dtyp1.host_code, '') as discount_type1, to_char(coalesce(dtyp1.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt1,
          coalesce(dtyp1.host_code, '') as discount_type2, to_char(coalesce(dtyp2.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt2,
          coalesce(dtyp1.host_code, '') as discount_type3, to_char(coalesce(dtyp3.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt3,
          coalesce(dtyp1.host_code, '') as discount_type4, to_char(coalesce(dtyp4.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt4,
          coalesce(dtyp1.host_code, '') as discount_type5, to_char(coalesce(dtyp5.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt5,
          coalesce(dtyp1.host_code, '') as discount_type6, to_char(coalesce(dtyp6.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt6,
          coalesce(dtyp1.host_code, '') as discount_type7, to_char(coalesce(dtyp7.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt7,
          coalesce(dtyp1.host_code, '') as discount_type8, to_char(coalesce(dtyp8.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt8,
          coalesce(dtyp1.host_code, '') as discount_type9, to_char(coalesce(dtyp9.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt9,
          coalesce(dtyp1.host_code, '') as discount_type10, to_char(coalesce(dtyp10.discount_amt_incvat, 0) * 100, 'fm000000000000') as discount_amt10
        from "order" a
        inner join "orderdetail" b on a.order_id = b.order_id
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp1 on dtyp1.order_id = a.order_id and b.line_number = dtyp1.line_number and dtyp1.line_number = 1
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp2 on dtyp2.order_id = a.order_id and b.line_number = dtyp2.line_number and dtyp2.line_number = 2
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp3 on dtyp3.order_id = a.order_id and b.line_number = dtyp3.line_number and dtyp3.line_number = 3
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp4 on dtyp4.order_id = a.order_id and b.line_number = dtyp4.line_number and dtyp4.line_number = 4
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp5 on dtyp5.order_id = a.order_id and b.line_number = dtyp5.line_number and dtyp5.line_number = 5
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp6 on dtyp6.order_id = a.order_id and b.line_number = dtyp6.line_number and dtyp6.line_number = 6
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp7 on dtyp7.order_id = a.order_id and b.line_number = dtyp7.line_number and dtyp7.line_number = 7
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp8 on dtyp8.order_id = a.order_id and b.line_number = dtyp8.line_number and dtyp8.line_number = 8
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp9 on dtyp9.order_id = a.order_id and b.line_number = dtyp9.line_number and dtyp9.line_number = 9
        left join (
          select order_id, line_number, host_code, discount_amt_incvat from "orderdiscount" 
          where length(jda_discount_code) > 0
        ) dtyp10 on dtyp10.order_id = a.order_id and b.line_number = dtyp10.line_number and dtyp10.line_number = 10
        where store_code = %s
        """
      cursor.execute(sql, (store, ))

      return cursor.fetchall()


def main():
  str_date = '20180820'
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/jda', str_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    stores = [x['store_code'] for x in query_store()]
    for store in stores:
      data = query_data_by_store(store)
      print(data)
      generate_data_file(target_path, store, data)
  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
