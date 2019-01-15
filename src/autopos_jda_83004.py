from autopos_jda_common import generate_data_file, prepare_data, query_data
from common import sftp
from datetime import datetime, timedelta
import os, sys, traceback


def main():
  try:
    store = '83004'
    bu = 'ssp'
    str_date = sys.argv[1] if len(sys.argv) > 1 else (
        datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
    target_path = os.path.join(parent_path, 'output/autopos/jda/{}'.format(
        bu.lower()), str_date)
    if not os.path.exists(target_path):
      os.makedirs(target_path)

    generate_data_file(target_path, store, prepare_data(query_data(store, str_date)))
    destination = 'incoming/jda/{}'.format(bu.lower())
    sftp('autopos.cds-uat', target_path, destination)
  except Exception as e:
    print('[AutoPOS] - JDA Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
