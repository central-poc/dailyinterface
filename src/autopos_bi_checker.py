from common import notifyLine, notifySlack
from datetime import date, timedelta
import ftplib, os, traceback


def notify(message):
  print(message)
  # notifyLine(message)
  notifySlack(message)


def cds():
  try:
    today = date.today().strftime('%Y%m%d')
    ftp = ftplib.FTP('10.0.15.66')
    ftp.login('magento', 'magentocd2019')

    path = '/oradata/Informatica_Source_File/POS_Payment'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BIRBS_20174_Payment_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-CDS-Payment successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-CDS-Payment not found !!')

    path = '/oradata/Informatica_Source_File/POS_Promotion'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BICDS_20174_Promotion_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-CDS-Promotion successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-CDS-Promotion not found !!')

    path = '/oradata/Informatica_Source_File/POS_Discount'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BICDS_20174_Discount_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-CDS-Discount successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-CDS-Discount not found !!')

    path = '/oradata/Informatica_Source_File/POS_Payment'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BIMS_20181_Payment_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-MSL-Payment successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-MSL-Payment not found !!')

    path = '/oradata/Informatica_Source_File/POS_Promotion'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BIMS_20181_Promotion_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-MSL-Promotion successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-MSL-Promotion not found !!')

    path = '/oradata/Informatica_Source_File/POS_Discount'
    files = ftp.nlst(path)
    payment = [s for s in files if '{}/BIMS_20181_Discount_{}'.format(path, today) in s]
    notify('[AutoPOS] - BI-MSL-Discount successfully') if len(payment) == 2 else notify('[AutoPOS] - BI-MSL-Discount not found !!')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
  finally:
    ftp.quit()


if __name__ == '__main__':
  cds()