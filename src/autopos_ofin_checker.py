from common import notifyLine, notifySlack
from datetime import date, timedelta
import ftplib, os, traceback


def notify(message):
  print(message)
  # notifyLine(message)
  notifySlack(message)


def ofin_gl_rbs():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    gl_path = '/p3/fnp/rbs/epos/data_in'
    ftp = ftplib.FTP('10.0.173.26')
    ftp.login('rbshopos', 'hopos')
    gl_files = ftp.nlst(gl_path)

    zn_dat = '{}/ZN{}RB1.DAT'.format(gl_path, yesterday) in gl_files
    zn_val = '{}/ZN{}RB1.VAL'.format(gl_path, yesterday) in gl_files
    zycd_dat = '{}/ZY{}RB1.DAT'.format(gl_path, yesterday) in gl_files
    zycd_val = '{}/ZY{}RB1.VAL'.format(gl_path, yesterday) in gl_files

    # zycb_dat = '{}/ZY{}RB1.DAT'.format(gl_path, yesterday) in gl_files
    # zycb_val = '{}/ZY{}RB1.VAL'.format(gl_path, yesterday) in gl_files

    notify('[RBS AutoPOS] - ZN RBS Successfully') if zn_dat & zn_val else notify(
        '[RBS AutoPOS] - ZN RBS not found !!')
    notify('[RBS AutoPOS] - ZY RBS Successfully') if zycd_dat & zycd_val else notify(
        '[RBS AutoPOS] - ZY RBS not found !!')

    # notify('[RBS AutoPOS] - ZY RBS9 Successfully') if zycb_dat & zycb_val else notify(
    #     '[RBS AutoPOS] - ZY RBS9 not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[RBS AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


def ofin_ap_rbs():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    ap_path = '/p3/fnp/rbs/invoice/data_in'
    ftp = ftplib.FTP('10.0.173.26')
    ftp.login('rbshoinv', 'rbshoinv')

    ap_files = ftp.nlst(ap_path)

    h = '{}/H{}FMS1.MER'.format(ap_path, yesterday) in ap_files
    hlog = '{}/H{}FMS1.LOG'.format(ap_path, yesterday) in ap_files
    l = '{}/L{}FMS1.MER'.format(ap_path, yesterday) in ap_files
    llog = '{}/L{}FMS1.LOG'.format(ap_path, yesterday) in ap_files

    notify('[RBS AutoPOS] - H Successfully') if h & hlog else notify('[RBS AutoPOS] - H not found !!')
    notify('[RBS AutoPOS] - L Successfully') if l & llog else notify('[RBS AUtoPOS] - L not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[RBS AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


if __name__ == '__main__':
  ofin_gl_rbs()
  ofin_ap_rbs()