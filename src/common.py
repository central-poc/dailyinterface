import pysftp
import requests, json
import time
import urllib.parse
import yaml


def chunks(l, n=10000):
  """Yield successive n-sized chunks from l."""
  for i in range(0, len(l), n):
    yield l[i:i + n]


def elapse(tb, start_time):
  elapsed_time = time.time() - start_time
  print("===== Finished {} in {} s".format(tb, elapsed_time))


def config(env):
  with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

  return cfg[env]


def notifyLine(message):
  print(message)
  LINE_ACCESS_TOKEN = "5FJgfpvf4Jgljm8AQ9H6DHFt858TasxjDtf80uYMcMk"
  LINE_HEADERS = {
    'Content-Type': 'application/x-www-form-urlencoded',
    "Authorization": "Bearer " + LINE_ACCESS_TOKEN
  }
  LINE_NOTI_URL = "https://notify-api.line.me/api/notify"
  msg = urllib.parse.urlencode({"message": message})
  response = requests.post(url=LINE_NOTI_URL, headers=LINE_HEADERS, data=msg)
  print('Response HTTP Status Code: {status_code}'.format(status_code=response.status_code))


def sftp(source, destination):
  print('copy: {} to: {}'.format(source, destination))
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None
  with pysftp.Connection('110.164.67.39', username='cgoftptest', password='tsetptfogc', cnopts=cnopts) as sftp:
    sftp.put_d(source, destination)

def sftp_ofm(source, destination):
  print('copy: {} to: {}'.format(source, destination))
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None
  with pysftp.Connection('110.164.67.39', username='ofmftptest', password='tsetptfmfo', cnopts=cnopts) as sftp:
    sftp.put_d(source, destination)

if __name__ == '__main__':
  destination = '/inbound/BCH_SBL_ProductMasterFull/req'
  sftp('/Users/adisorn/Documents/workspace/cng/code/dailyinterface/output/siebel', destination)
