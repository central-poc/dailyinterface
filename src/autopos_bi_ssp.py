from common import connect_psql
from datetime import datetime, timedelta
import os
import psycopg2.extras


def get_file_seq(prefix, output_path, ext):
  files = [
      f.split('.')[0] for f in os.listdir(output_path)
      if os.path.isfile(os.path.join(output_path, f)) and f.endswith(ext)
  ]
  return 1 if not files else max(
      int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def generate_trans_sale_detail(output_path, str_date, str_time, str_stime,
                               store, data):
  prefix = 'BISSP_' + store + '_Sales_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".
            format(
                d['store_code'], d['receipt_date'], d['receipt_time'],
                d['transaction_type'], d['pos_id'], d['receipt_no'],
                d['line_number'], d['sku'], d['quantity'],
                d['price_before_override'], d['price_after_override'],
                d['price_override_flag'], d['total_sale'], d['vat_rate'],
                d['vat_amt'], d['discount_code1'], d['discount_amt1'],
                d['discoune_flag1'], d['discount_code2'], d['discount_amt2'],
                d['discoune_flag2'], d['discount_code3'], d['discount_amt3'],
                d['discoune_flag3'], d['discount_code4'], d['discount_amt4'],
                d['discoune_flag4'], d['discount_code5'], d['discount_amt5'],
                d['discoune_flag5'], d['discount_code6'], d['discount_amt6'],
                d['discoune_flag6'], d['discount_code7'], d['discount_amt7'],
                d['discoune_flag7'], d['discount_code8'], d['discount_amt8'],
                d['discoune_flag8'], d['discount_code9'], d['discount_amt9'],
                d['discoune_flag9'], d['discount_code10'], d['discount_amt10'],
                d['discoune_flag10'], d['ref_receipt_no'], d['ref_date'],
                d['return_reason_code'], d['dept_id'], d['subdept_id'],
                d['class_id'], d['subclass_id'], d['vendor_id'], d['brand_id'],
                d['itemized'], d['dtype'], d['member_id'], d['cashier_id'],
                d['sale_id'], d['guide_id'], d['last_modify_date']))
        count = count + 1

      l.write('{}|{}|{}'.format(str_date, str_stime, count))
      print(
          '[AutoPOS] - BI SSP transaction sale detail create files complete..')
    except Exception as e:
      print(
          '[AutoPOS] - BI SSP transaction sale detail create files error: {}: '.
          format(e))


def generate_trans_tendor_detail(output_path, str_date, str_time, str_stime,
                                 store, data):
  prefix = 'BISSP_' + store + '_Tendor_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
            d['store_code'], d['receipt_date'], d['receipt_time'],
            d['transaction_type'], d['pos_id'], d['receipt_no'],
            d['line_number'], d['media_member_code'], d['media_member_desc'],
            d['tendor_amt'], d['credit_cardno'], str_date))
        count = count + 1

      l.write('{}|{}|{}'.format(str_date, str_stime, count))
      print(
          '[AutoPOS] - BI SSP transaction tendor detail create files complete..'
      )
    except Exception as e:
      print(
          '[AutoPOS] - BI SSP transaction tendor detail create files error: {}: '.
          format(e))


def generate_trans_installment(output_path, str_date, str_time, str_stime,
                               store, data):
  prefix = 'BISSP_' + store + '_Installment_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(
            d['store_code'], d['receipt_date'], d['receipt_time'],
            d['transaction_type'], d['pos_id'], d['receipt_no'],
            d['vendor_id'], d['brand_id'], d['dept_id'], d['subdept_id'],
            d['sku'], d['tendor_type'], d['installment_period'],
            d['credit_cardno'], d['interest_rate'], d['tendor_amt'], str_date))
        count = count + 1

      l.write('{}|{}|{}'.format(str_date, str_stime, count))
      print(
          '[AutoPOS] - BI SSP transaction installment create files complete..')
    except Exception as e:
      print(
          '[AutoPOS] - BI SSP transaction installment create files error: {}: '.
          format(e))


def generate_trans_dcpn(output_path, str_date, str_time, str_stime, store,
                        data):
  prefix = 'BISSP_' + store + '_DCPN_' + str_date + str_time + "_"
  seq = get_file_seq(prefix, output_path, '.DAT')
  file_name = prefix + str(seq) + '_.DAT'
  file_fullpath = os.path.join(output_path, file_name)
  log_name = prefix + str(seq) + '_.LOG'
  log_fullpath = os.path.join(output_path, log_name)

  with open(file_fullpath, 'w') as f, open(log_fullpath, 'w') as l:
    try:
      count = 0
      for d in data:
        if count > 0:
          f.write('\n')
        f.write("{}|{}|{}|{}|{}|{}|{}|{}".format(
            d['store_code'], d['receipt_date'], d['receipt_time'],
            d['transaction_type'], d['pos_id'], d['receipt_no'],
            d['coupon_id'], str_date))
        count = count + 1

      l.write('{}|{}|{}'.format(str_date, str_stime, count))
      print('[AutoPOS] - BI SSP transaction dpcn create files complete..')
    except Exception as e:
      print('[AutoPOS] - BI SSP transaction dpcn create files error: {}: '.
            format(e))


def query_transaction_sale_detail(store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_ssp_trans_sale_detail where store_code = %s"
      cursor.execute(sql, (store, ))

      return cursor.fetchall()


def query_transaction_tendor_detail(store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_ssp_trans_tendor_detail where store_code = %s"
      cursor.execute(sql, (store, ))

      return cursor.fetchall()


def query_transaction_installment(store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_ssp_trans_installment where store_code = %s"
      cursor.execute(sql, (store, ))

      return cursor.fetchall()


def query_transaction_dcpn(store):
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      sql = "select * from mv_autopos_bi_ssp_trans_dpcn where store_code = %s"
      cursor.execute(sql, (store, ))

      return cursor.fetchall()


def query_store():
  with connect_psql() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute(
          "select store_code from mv_autopos_bi_ssp_trans_sale_detail group by store_code"
      )

      return cursor.fetchall()


def main():
  now = datetime.now()
  str_date = (now - timedelta(days=1)).strftime('%Y%m%d')
  str_time = (now - timedelta(days=1)).strftime('%H%M')
  str_stime = (now - timedelta(days=1)).strftime('%H%M%S')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_path = os.path.join(parent_path, 'output/autopos/bissp',
                             str_date + str_time)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  try:
    stores = [x['store_code'] for x in query_store()]
    for store in stores:
      generate_trans_sale_detail(target_path, str_date, str_time, str_stime,
                                 store, query_transaction_sale_detail(store))
      generate_trans_tendor_detail(target_path, str_date, str_time,
                                   str_stime, store,
                                   query_transaction_tendor_detail(store))
      generate_trans_installment(target_path, str_date, str_time, str_stime,
                                 store, query_transaction_installment(store))
      generate_trans_dcpn(target_path, str_date, str_time, str_stime, store,
                          query_transaction_dcpn(store))
  except Exception as e:
    print(e)


if __name__ == '__main__':
  main()
