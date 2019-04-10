from common import notifyLine, notifySlack
from datetime import date, timedelta
import pysftp
import os
import traceback
import sys


def notify(message):
  print(message)
  # notifyLine(message)
  notifySlack(message)



def jda():
  try:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
    private_key = parent_path + "/key/cgai-jumpbox-dev"
    print(private_key)
    with pysftp.Connection(host="ai-upload.central.tech", username="admin", private_key=private_key) as sftp:
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/CDS/SD10138.TXT') else notify('[AutoPOS] - JDA_10138 unsuccessfully')
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/CDS/SD15016.TXT') else notify('[AutoPOS] - JDA_15016 unsuccessfully')
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/MSL/SD17016.TXT') else notify('[AutoPOS] - JDA_17016 unsuccessfully')
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/SSP/SD83004.TXT') else notify('[AutoPOS] - JDA_83004 unsuccessfully')
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/B2S/SD57002.TXT') else notify('[AutoPOS] - JDA_57002 unsuccessfully')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[AutoPOS] - FTP Checker error: {}'.format(e))
    sys.exit(1)


if __name__ == '__main__':
  jda()
