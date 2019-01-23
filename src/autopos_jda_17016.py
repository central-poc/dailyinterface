from autopos_jda_common import process
from common import config
from datetime import datetime, timedelta
import os, sys, traceback


def main():
  try:
    store = '17016'
    bu = 'msl'
    env = sys.argv[1] if len(sys.argv) > 1 else 'local'
    print("\n===== Start JDA-{} [{}] =====".format(store, env))
    cfg = config(env)
    run_date = cfg['run_date'] if cfg['run_date'] else datetime.now().strftime('%Y%m%d')
    process(bu, store, cfg, run_date)
  except Exception as e:
    print('[AutoPOS] - JDA Error: %s' % str(e))
    traceback.print_tb(e.__traceback__)


if __name__ == '__main__':
  main()
