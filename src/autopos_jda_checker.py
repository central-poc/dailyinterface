from common import notifyLine, notifySlack
from datetime import date, timedelta
import pysftp
import os
import traceback


def notify(message):
  print(message)
  # notifyLine(message)
  notifySlack(message)



def jda():
  try:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
    private_key = parent_path + "/key/cgai-jumpbox-dev"
    with pysftp.Connection(host="ai-upload.central.tech", username="admin", private_key=private_key) as sftp:
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/RBS/SD20174.TXT') else notify('[RBS AutoPOS] - JDA_20174 unsuccessfully')
      '' if not sftp.exists('/home/jdaprod/incoming/JDA/RBS/SD20181.TXT') else notify('[RBS AutoPOS] - JDA_20181 unsuccessfully')
  except Exception as e:
    traceback.print_tb(e.__traceback__)
    notify('[RBS AutoPOS] - FTP Checker error: {}'.format(e))


if __name__ == '__main__':
  jda()