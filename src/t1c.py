from datetime import datetime
from common import chunks, config, elapse
import pymssql
import sys
import time


class T1C:
  def setup(self, env):
    self.cfg = config(env)
    self.mssql = pymssql.connect(
        server=self.cfg['cmos']['host'],
        user=self.cfg['cmos']['user'],
        password=self.cfg['cmos']['password'],
        database=self.cfg['cmos']['db'])

    self.curmssql = self.mssql.cursor(as_dict=True)

  def close(self):
    self.mssql.close()

  def dvtruck(self):
    start_time = time.time()
    try:
      sql = """
      select
        createby, createon, driverid, drivername, expiredate,
        licenseno, outsourceid, status, truckcode, truckempid,
        truckempname, truckname, trucktype, updateby, updateon
      from dbo.DvTruck
      """
      self.curmssql.execute(sql)

      for rows in chunks(self.curmssql.fetchall()):
        print (rows

    except Exception as e:
      print('Exception: ', e)

    elapse("DvTruck {} rows".format(self.curmssql.rowcount), start_time)


if __name__ == '__main__':
  env = sys.argv[1] if len(sys.argv) > 1 else 'dev'
  print("===== Start [{}]: {}".format(env, datetime.now()))
  t1c = T1C()
  t1c.setup(env)
  t1c.dvtruck()
  t1c.close()
  print("===== End : {}".format(datetime.now()))
