from common import config, connect_psql, insert_transaction, query_all, query_matview, sftp
from datetime import datetime, timedelta
import os, sys, traceback, uuid


def gen_sale_tran_data(data):
  res = []
  res.append(data['line_identifier'])
  res.append(data['source_trans_id'])
  res.append(data['store_code'])
  res.append(data['pos_no'])
  res.append(data['receipt_no'])
  res.append(data['trans_type'])
  res.append(data['trans_sub_type'])
  res.append(data['trans_date'])
  res.append(data['business_date'])
  res.append(data['invoice_date'])
  res.append(data['delivery_date'])
  res.append(data['earn_online_flag'])
  res.append(data['t1c_cardno'])
  res.append(data['mobile_no'])
  res.append(data['pos_user_id'])
  res.append(data['item_seq_no'])
  res.append(data['product_code'])
  res.append(data['product_barcode'])
  res.append(data['quantity'])
  res.append(data['price_unit'])
  res.append(data['price_total'])
  res.append(data['net_price_unit'])
  res.append(data['net_price_total'])
  res.append(data['discount_total'])
  res.append(data['vat_amt'])
  res.append(data['tendor_type'])
  res.append(data['tendor_ref'])
  res.append(data['orginal_receipt_no'])
  res.append(data['orginal_item_seq_no'])
  res.append(data['display_receipt_no'])
  res.append(data['return_all_flag'])
  res.append(data['sbl_cancel_redeem'])
  res.append('' if data['tendor_type'] == 'Coupon' else data['tendor_type'])
  res.append(data['net_amt'])
  res.append(data['redeem_amt'])
  res.append(data['redeem_cash'])
  res.append(data['display_receipt_no'])
  res.append(data['receipt_no'])

  return res


def gen_tender(input):
  values = set(map(lambda x: x[37], input))
  groups = [[y for y in input if y[37] == x] for x in values]
  for g in groups:
    net_amt = 0
    redeem_amt = 0
    order_redeem_cash = 0
    product_index = 0
    for index, sub in enumerate(g):
      if sub[6] == "P":
        product_index = index
      if sub[6] == "C":
        continue
      net_amt = net_amt + sub[33]
      redeem_amt = redeem_amt + sub[34]
      order_redeem_cash = order_redeem_cash + sub[35]

    if redeem_amt == 0:
      g.append(tender(g[product_index][:], net_amt, False))
    else:
      g.append(tender(g[product_index][:], redeem_amt, True))
      if order_redeem_cash > 0:
        g.append(tender(g[product_index][:], order_redeem_cash, False))

    total = g[0][:]
    total[6] = "A"
    total[15:27] = ["1", "", "", "1", "", "", "", str(net_amt), "", "", "", ""]
    g.append(total)

  for g in groups:
    index = 1
    for o in g:
      o[1] = str(uuid.uuid4()).upper()
      o[2] = '{:0>6}'.format(o[2])
      if o[6] != "A" and o[6] != "C":
        o[15] = str(index)
        index = index + 1
        if o[6] == "P":
          o[26] = ''

  out = [item[:32] for sublist in groups for item in sublist]

  return ['|'.join(row) for row in out]


def tender(data, amount, is_t1c):
  t = data
  t[6] = "T"
  t[16:26] = ["", "", "1", "", "", "", str(amount), "", "", t[32]]
  if is_t1c and len(t[12]) > 0:
    t[26] = t[12]
    t[25] = 'T1CP'

  return t


def generate_data_file(output_path, bu, sale_transactions):
  total_row = len(sale_transactions)

  interface_name = 'BCH_{}CTO_T1C_NRTSales'.format(bu)
  now = datetime.now()
  batchdatetime = now.strftime('%d%m%Y_%H:%M:%S:%f')[:-3]
  filedatetime = now.strftime('%d%m%Y_%H%M%S')
  datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, 1)
  filepath = os.path.join(output_path, datfile)
  with open(filepath, 'w') as text_file:
    text_file.write('0|{}\n'.format(total_row))
    for transaction in sale_transactions:
      text_file.write('{}\n'.format(transaction))
    text_file.write('9|END')

  ctrlfile = '{}_{}.ctrl'.format(interface_name, filedatetime)
  filepath = os.path.join(output_path, ctrlfile)
  attribute1 = ""
  attribute2 = ""
  with open(filepath, 'w') as outfile:
    outfile.write('{}|{}|001|1|{}|{}|{}-CTO|{}|{}'.format(
        interface_name, bu, total_row, batchdatetime, bu, attribute1,
        attribute2))
  print(
      '[AUtoPOS] - Siebel[{}] create .DAT & .CTRL file completed..'.format(bu))
  return [datfile, ctrlfile]


def main():
  env = sys.argv[1] if len(sys.argv) > 1 else 'local'
  print("\n===== Start Siebel [{}] =====".format(env))
  cfg = config(env)
  now = datetime.now()
  batch_date = cfg['run_date'] if cfg['run_date'] else (now - timedelta(days=1)).strftime('%Y%m%d')
  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  try:
    # bus = [
        # x['businessunit_code']
        # for x in query_all(cfg['fms'],
            # "select businessunit_code from businessunit where status = 'AT' group by businessunit_code"
        # )
    # ]
    bus = ['CDS', 'MSL']
    for bu in bus:
      target_path = os.path.join(parent_path, 'output/autopos/{}/siebel/{}'.format(env, bu.lower()), now.strftime('%Y%m%d'))
      if not os.path.exists(target_path):
        os.makedirs(target_path)
      refresh_view = "refresh materialized view mv_autopos_siebel"
      sql = "select * from mv_autopos_siebel where bu = '{}' and interface_date = '{}'".format(
          bu, batch_date)
      datas = query_matview(cfg['fms'], refresh_view, sql)
      data_list = [gen_sale_tran_data(data) for data in datas]
      files = generate_data_file(target_path, bu, gen_tender(data_list))

      if cfg['ftp']['is_enable']:
        destination = 'incoming/siebel/{}'.format(bu.lower())
        sftp(cfg['ftp']['host'], cfg['ftp']['user'], target_path, destination, files)
      sql_insert = "insert into transaction_siebel {}".format(sql)
      insert_transaction(cfg['fms'], sql_insert)
  except Exception as e:
    print('[AutoPOS] - Siebel Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)
    sys.exit(1)


if __name__ == '__main__':
  main()
