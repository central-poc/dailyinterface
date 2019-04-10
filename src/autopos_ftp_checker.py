from common import notifyLine, notifySlack
from datetime import date, timedelta
import ftplib, traceback


def notify(message):
  print(message)
  # notifyLine(message)
  # notifySlack(message)


def ofin_cds_gl():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    gl_path = '/p3/fnp/cds/epos/data_in'
    ftp = ftplib.FTP('10.101.25.21')
    ftp.login('cdshopos', 'hopos')
    gl_files = ftp.nlst(gl_path)

    zn_dat = '{}/ZN{}CD1.DAT'.format(gl_path, yesterday) in gl_files
    zn_val = '{}/ZN{}CD1.VAL'.format(gl_path, yesterday) in gl_files
    zycd_dat = '{}/ZY{}CD1.DAT'.format(gl_path, yesterday) in gl_files
    zycd_val = '{}/ZY{}CD1.VAL'.format(gl_path, yesterday) in gl_files
    zycb_dat = '{}/ZY{}CB1.DAT'.format(gl_path, yesterday) in gl_files
    zycb_val = '{}/ZY{}CB1.VAL'.format(gl_path, yesterday) in gl_files

    notify('[AutoPOS] - ZN CDS Successfully') if zn_dat & zn_val else notify(
        '[AutoPOS] - ZN CDS not found !!')
    notify('[AutoPOS] - ZY CDS Successfully') if zycd_dat & zycd_val else notify(
        '[AutoPOS] - ZY CDS not found !!')
    notify('[AutoPOS] - ZY CBN Successfully') if zycb_dat & zycb_val else notify(
        '[AutoPOS] - ZY CBN not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


def ofin_ssp_gl():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    gl_path = '/p3/fnp/sps/epos/data_in'
    ftp = ftplib.FTP('10.101.25.21')
    ftp.login('spshopos', 'hopos')
    gl_files = ftp.nlst(gl_path)

    zy_dat = '{}/ZY{}SP1.DAT'.format(gl_path, yesterday) in gl_files
    zy_val = '{}/ZY{}SP1.VAL'.format(gl_path, yesterday) in gl_files

    notify('[AutoPOS] - ZY SSP Successfully') if zy_dat & zy_val else notify(
        '[AutoPOS] - ZY SSP not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


def ofin_b2s_gl():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    gl_path = '/p3/fnp/b2s/epos/data_in'
    ftp = ftplib.FTP('10.101.25.21')
    ftp.login('b2shopos', 'hopos')
    gl_files = ftp.nlst(gl_path)

    zy_dat = '{}/ZY{}B21.DAT'.format(gl_path, yesterday) in gl_files
    zy_val = '{}/ZY{}B21.VAL'.format(gl_path, yesterday) in gl_files

    notify('[AutoPOS] - ZY B2S Successfully') if zy_dat & zy_val else notify(
        '[AutoPOS] - ZY B2S not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


def ofin_cds_ap():
  try:
    yesterday = (date.today() - timedelta(days=1)).strftime('%y%m%d')
    ap_path = '/p3/fnp/cds/invoice/data_in'
    ftp = ftplib.FTP('10.101.25.21')
    ftp.login('cdshoinv', 'hoinv')

    ap_files = ftp.nlst(ap_path)

    h = '{}/H{}FMS1.MER'.format(ap_path, yesterday) in ap_files
    hlog = '{}/H{}FMS1.LOG'.format(ap_path, yesterday) in ap_files
    l = '{}/L{}FMS1.MER'.format(ap_path, yesterday) in ap_files
    llog = '{}/L{}FMS1.LOG'.format(ap_path, yesterday) in ap_files

    notify('[AutoPOS] - H Successfully') if h & hlog else notify('[AutoPOS] - H not found !!')
    notify('[AutoPOS] - L Successfully') if l & llog else notify('[AUtoPOS] - L not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


if __name__ == '__main__':
  ofin_cds_gl()
  ofin_ssp_gl()
  ofin_b2s_gl()
  ofin_cds_ap()