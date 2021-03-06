from common import connect_psql, insert_transaction, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback


def prepare_data(datas):
  result = []
  for data in datas:
    temp = []
    temp.append("{:0>5}".format(data['store_code'][:5]))
    temp.append("{:0>8}".format(data['transaction_date'][:8]))
    temp.append("{:0>4}".format(data['transaction_time'][:4]))
    temp.append("{:0>2}".format(data['transaction_type'][:2]))
    temp.append("{:9}".format(data['transaction_type'][:2] + data['ticket_no'][-7:]))
    temp.append("{:0>3}".format(data['seq_no'][:3]))
    temp.append("{:0>16}".format(data['sku'][:16]))
    temp.append("{:0>16}".format(data['barcode'][:16]))
    temp.append("{:1}".format(data['qty_sign'][:1]))
    temp.append("{:0>6}".format(data['quantity'][:6]))
    temp.append("{:0>12}".format(data['jda_price'][:12]))
    temp.append("{:0>12}".format(data['price_override'][:12]))
    temp.append("{:1}".format(data['price_override_flag'][:1]))
    temp.append("{:0>12}".format(data['total_net_amt'][:12]))
    temp.append("{:0>12}".format(data['vat_amt'][:12]))
    temp.append("{:4}".format(data['discount_type1'][:4]))
    temp.append("{:0>12}".format(data['discount_amt1'][:12]))
    temp.append("{:4}".format(data['discount_type2'][:4]))
    temp.append("{:0>12}".format(data['discount_amt2'][:12]))
    temp.append("{:4}".format(data['discount_type3'][:4]))
    temp.append("{:0>12}".format(data['discount_amt3'][:12]))
    temp.append("{:4}".format(data['discount_type4'][:4]))
    temp.append("{:0>12}".format(data['discount_amt4'][:12]))
    temp.append("{:4}".format(data['discount_type5'][:4]))
    temp.append("{:0>12}".format(data['discount_amt5'][:12]))
    temp.append("{:4}".format(data['discount_type6'][:4]))
    temp.append("{:0>12}".format(data['discount_amt6'][:12]))
    temp.append("{:4}".format(data['discount_type7'][:4]))
    temp.append("{:0>12}".format(data['discount_amt7'][:12]))
    temp.append("{:4}".format(data['discount_type8'][:4]))
    temp.append("{:0>12}".format(data['discount_amt8'][:12]))
    temp.append("{:4}".format(data['discount_type9'][:4]))
    temp.append("{:0>12}".format(data['discount_amt9'][:12]))
    temp.append("{:4}".format(data['discount_type10'][:4]))
    temp.append("{:0>12}".format(data['discount_amt10'][:12]))
    temp.append("{:21}".format(data['ref_id'][:21]))
    temp.append("{:9}".format(data['ref_ticket'][-9:]))
    temp.append("{:0>8}".format(data['ref_date'][:8]))
    temp.append("{:0>2}".format(data['reason_code'][:2]))
    temp.append("{:0>6}".format(data['event_no'][:6]))
    temp.append("{:0>3}".format(data['dept_id'][:3]))
    temp.append("{:0>3}".format(data['subdept_id'][:3]))
    temp.append("{:0>16}".format(data['itemized'][:16]))
    temp.append("{:1}".format(data['dtype'][:1]))
    temp.append("{:16}".format(data['credit_cardno'][:16]))
    temp.append("{:21}".format(data['customer_id'][:21]))
    temp.append("{:0>8}".format(data['member_point'][:8]))
    temp.append("{:8}".format(data['cashier_id'][:8]))
    temp.append("{:8}".format(data['sale_person'][:8]))

    gen_text = "".join(temp)
    print(gen_text)
    result.append(gen_text)

  return result


def prepare_data_b2s(datas):
  result = []
  for data in datas:
    temp = []
    temp.append("{:0>5}".format(data['store_code'][:5]))
    temp.append("{:0>8}".format(data['transaction_date'][:8]))
    temp.append("{:0>4}".format(data['transaction_time'][:4]))
    temp.append("{:0>2}".format(data['transaction_type'][:2]))
    temp.append("{:0>4}".format(data['ticket_no'][-9:][:3]))
    temp.append("{:0>4}".format(data['ticket_no'][-4:]))
    temp.append("{:0>4}".format(data['seq_no'][:4]))
    temp.append("{:0>16}".format(data['sku'][:16]))
    temp.append("{:0>16}".format(data['barcode'][:16]))
    temp.append("{:1}".format(data['qty_sign'][:1]))
    temp.append("{:0>6}".format(data['quantity'][:6]))
    temp.append("{:0>12}".format(data['jda_price'][:12]))
    temp.append("{:0>12}".format(data['price_override'][:12]))
    temp.append("{:1}".format(data['price_override_flag'][:1]))
    temp.append("{:0>12}".format(data['total_net_amt'][:12]))
    temp.append("{:0>12}".format(data['vat_amt'][:12]))
    temp.append("{:4}".format(data['discount_type1'][:4]))
    temp.append("{:0>12}".format(data['discount_amt1'][:12]))
    temp.append("{:4}".format(data['discount_type2'][:4]))
    temp.append("{:0>12}".format(data['discount_amt2'][:12]))
    temp.append("{:4}".format(data['discount_type3'][:4]))
    temp.append("{:0>12}".format(data['discount_amt3'][:12]))
    temp.append("{:4}".format(data['discount_type4'][:4]))
    temp.append("{:0>12}".format(data['discount_amt4'][:12]))
    temp.append("{:21}".format(data['ref_id'][:21]))
    temp.append("{:8}".format(data['ref_ticket'][-8:]))
    temp.append("{:0>8}".format(data['ref_date'][:8]))
    temp.append("{:0>2}".format(data['reason_code'][:2]))
    temp.append("{:0>6}".format(data['event_no'][:6]))
    temp.append("{:0>3}".format(data['dept_id'][:3]))
    temp.append("{:0>3}".format(data['subdept_id'][:3]))
    temp.append("{:0>16}".format(data['itemized'][:16]))
    temp.append("{:1}".format(data['dtype'][:1]))
    temp.append("{:16}".format(data['credit_cardno'][:16]))
    temp.append("{:21}".format(data['customer_id'][:21]))
    temp.append("{:0>8}".format(data['member_point'][:8]))
    temp.append("{:8}".format(data['cashier_id'][:8]))
    temp.append("{:8}".format(data['sale_person'][:8]))

    result.append("".join(temp))

  return result


def query_data(env, store, str_date):
  refresh_view = "refresh materialized view mv_autopos_jda"
  sql = """
    select * from mv_autopos_jda
    where store_code = '{}' 
    and interface_date = '{}'
    and not (
	    (cast(discount_amt1 as numeric) > 0 and discount_type1 = '')
	    or (cast(discount_amt2 as numeric) > 0 and discount_type2 = '')
	    or (cast(discount_amt3 as numeric) > 0 and discount_type3 = '')
	    or (cast(discount_amt4 as numeric) > 0 and discount_type4 = '')
	    or (cast(discount_amt5 as numeric) > 0 and discount_type5 = '')
	    or (cast(discount_amt6 as numeric) > 0 and discount_type6 = '')
	    or (cast(discount_amt7 as numeric) > 0 and discount_type7 = '')
	    or (cast(discount_amt8 as numeric) > 0 and discount_type8 = '')
	    or (cast(discount_amt9 as numeric) > 0 and discount_type9 = '')
	    or (cast(discount_amt10 as numeric) > 0 and discount_type10 = '')
    )
  """.format(store, str_date)
  sql_insert = "insert into transaction_jda {}".format(sql)
  insert_transaction(env, sql_insert)
  
  return query_matview(env, refresh_view, sql)


def generate_data_file(str_date, output_path, store, datas):
  file_name = 'SD' + store + '.TXT'
  file_fullpath = os.path.join(output_path, file_name)

  with open(file_fullpath, 'w') as f:
    f.write("\n".join(datas))
  print('[AutoPOS] - JDA[{}] create files completed..'.format(store))
  return [file_name]


def process(env, bu, store, cfg, run_date):
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/{}/jda/{}'.format(env, bu), run_date)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  #files = generate_data_file(target_path, store, prepare_data(query_data(cfg['fms'], store, run_date)))
  files = generate_data_file(run_date, target_path, store, prepare_data(query_data(cfg['fms'], store, run_date)))
  if cfg['ftp']['is_enable']:
      destination = 'incoming/jda/{}'.format(bu)
      sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path, destination, files)
