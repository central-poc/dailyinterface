import time
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
