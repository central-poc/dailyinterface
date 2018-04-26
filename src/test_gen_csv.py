import pymssql
import sys
import csv
import os

def gen_by_table_name(table_name):
    with pymssql.connect("10.17.220.173", "app-cmos", "Zxcv123!", "DBMKP") as conn:
      cursor = conn.cursor()
      query = """
      select top 1 * from {0}
      """
      query = query.format('{}'.format(table_name))
      cursor.execute(query)
      a = [i[0].upper() for i in cursor.description]
      if 'CREATEON' in a :
          print(table_name + ' has create on')
          query = """
          select top 10 * from {0} WHERE CREATEON >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) AND CREATEON < CAST(GETDATE() AS DATE)
          """
      elif 'UPDATEON' in a:
          print(table_name + ' has update on')
          query = """
          select top 10 * from {0} WHERE UPDATEON >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) AND UPDATEON < CAST(GETDATE() AS DATE)
          """
      else:
          query = """
          select top 10 * from {0}
          """

      query = query.format('{}'.format(table_name))
      cursor.execute(query)
      with open('CMOS/20171228_' + table_name + '.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow([i[0] for i in cursor.description]) # write headers
        for row in cursor:
          writer.writerow(row)

if not os.path.exists('CMOS'):
  os.makedirs('CMOS')

with pymssql.connect("10.17.220.173", "app-cmos", "Zxcv123!", "DBMKP") as conn:
  cursor = conn.cursor()
  query = """
  SELECT TABLE_NAME FROM DBMKP.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY Table_name
  """
  cursor.execute(query)
  for row in cursor:
      print('generate ' + row[0])
      try:
          gen_by_table_name(row[0])
      except:
          print('fail ----------------------------- ' + row[0])
      print('generate ' + row[0] + 'complete')
