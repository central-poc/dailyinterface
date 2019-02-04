import _mssql
import ftplib
import psycopg2
import psycopg2.extras
import pymssql
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
  LINE_ACCESS_TOKEN = "5FJgfpvf4Jgljm8AQ9H6DHFt858TasxjDtf80uYMcMk"
  LINE_HEADERS = {
      'Content-Type': 'application/x-www-form-urlencoded',
      "Authorization": "Bearer " + LINE_ACCESS_TOKEN
  }
  LINE_NOTI_URL = "https://notify-api.line.me/api/notify"
  msg = urllib.parse.urlencode({"message": message})
  response = requests.post(url=LINE_NOTI_URL, headers=LINE_HEADERS, data=msg)
  print(
      'Line Response: {status_code}'.format(status_code=response.status_code))


def notifySlack(message):
  SLACK_URL = "https://hooks.slack.com/services/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"
  payload = {
      "text": message,
  }
  data = json.dumps(payload)
  response = requests.post(SLACK_URL, data=data)
  print(
      'Slakc Response: {status_code}'.format(status_code=response.status_code))


def sftp(host, owner, source, destination, files):
  print('[SFTP] - source: {}, destionation: {}, files: {}'.format(source, destination, files))
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  k = paramiko.rsakey.RSAKey.from_private_key_file('key/' + owner)
  ssh.connect(host, username=owner, pkey=k)
  sftp = ssh.open_sftp()
  for file in files:
    sftp.put(os.path.join(source, file), os.path.join(destination, file))


def cleardir(path):
  filelist = [f for f in os.listdir(path)]
  for f in filelist:
    os.remove(os.path.join(path, f))


def connect_cmos(env):
  return pymssql.connect(env['host'], env['user'], env['password'], env['db'])


def mssql_cmos(env):
  return _mssql.connect(
      server=env['host'],
      user=env['user'],
      password=env['password'],
      database=env['db'])


def connect_psql(env):
  return psycopg2.connect(
      host=env['host'],
      port=env['port'],
      user=env['user'],
      password=env['password'],
      dbname=env['db'])


def replace_pipe(data):
  for key, value in data.items():
    data[key] = str(value).replace('|', '')
  return data


def get_file_seq(prefix, output_path, ext):
  files = [
      f.split('.')[0] for f in os.listdir(output_path)
      if os.path.isfile(os.path.join(output_path, f)) and f.endswith(ext)
  ]
  return 1 if not files else max(
      int(f[len(prefix)]) if f.startswith(prefix) else 0 for f in files) + 1


def query_matview(env, refresh_view, str_query):
  with connect_psql(env) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute(refresh_view)
      cursor.execute(str_query)

      return cursor.fetchall()


def query_all(env, sql):
  with connect_psql(env) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute(sql)

      return cursor.fetchall()


def insert_transaction(env, sql):
  with connect_psql(env) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
      cursor.execute(sql)
      count = cursor.rowcount
      print (count, "Record inserted successfully !!")

def ftp(host, user, pwd, src, dest):
  print('[FTP] - host: {}, user: {}, source: {}, destination: {}'.format(
      host, user, src, dest))
  with ftplib.FTP(host) as ftp:
    try:
      ftp.login(user, pwd)
      files = [f for f in os.listdir(src)]
      for f in files:
        source = '{}/{}'.format(src, f)
        destination = '{}/{}'.format(dest, f)
        with open(source, 'rb') as fp:
          res = ftp.storlines('STOR {}'.format(destination), fp)
          if not res.startswith('226 Transfer complete'):
            print('[FTP] - Upload failed: {}'.format(destination))
    except ftplib.all_errors as e:
      print('[FTP] - error:', e)