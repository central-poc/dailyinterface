import pysftp
import requests, json
import time
import urllib.parse
import yaml
import paramiko
import os


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

def sftp(owner,source, destination):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = paramiko.rsakey.RSAKey.from_private_key_file('key/' + owner)
        ssh.connect('ai-upload.central.tech',username=owner,pkey=k)
        sftp = ssh.open_sftp()
        for filename in os.listdir(source):
            print(source,filename)
            sftp.put(os.path.join(source,filename), os.path.join(destination,filename))
    except Exception as e:
        print(e)

def cleardir(path):
    filelist = [ f for f in os.listdir(path) ]
    for f in filelist:
        os.remove(os.path.join(path, f))
