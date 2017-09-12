import pymssql


def connect_cds_db():
  return pymssql.connect("10.17.220.55", "central", "Cen@tral", "DBCDSContent")


def connect_t1c_db():
  return pymssql.connect("10.17.220.181", "sa", "Asdf123!", "DBSync")


with connect_cds_db() as conn:
  cds_cursor = conn.cursor(as_dict=True)
  query = '''
    SELECT pidnew AS Pid,DocnameEn AS ProductNameEN,Docname AS ProductNameTH,
        d.departmentID AS Local_Cate_ID,d.DisplayNameEN AS LocalNameEN, d.DisplayName AS LocalNameTH,
        tbbrand.Brandid as Brand_ID,tbbrand.DisplaynameEn AS BrandNameEN,
        tbbrand.DisplayName AS BrandNameTH, isnull(tbproduct.Barcode, '') as Barcode
    FROM tbproduct
    JOIN tbbrand
        ON tbproduct.brandid = tbbrand.brandid
    JOIN tbproductGroup pg
        ON tbproduct.productgroupid = pg.productgroupid
    JOIN tbdepartment d
        ON pg.departmentid = d.departmentid
    WHERE tbproduct.isFirstStockGR = 1 AND len(pidnew) > 0
    ORDER BY pidnew
    '''
  cds_cursor.execute(query)
  data = cds_cursor.fetchall()

with connect_t1c_db() as t1c:
  t1c_cursor = t1c.cursor()
  t1c_cursor.execute('''
  TRUNCATE TABLE ProductMappingCDS
  ''')

  sql = """
      INSERT INTO ProductMappingCDS(
        PID,ProductNameEN,ProductNameTH,
        Local_Cate_ID,LocalNameEN,LocalNameTH,
        Brand_ID,BrandNameEN,BrandNameTH,Barcode
      ) values {}
  """
  print("Total Product : ", len(data))
  for i in range(0, len(data), 1000):
    print("Progress : ", i)
    query = ",".join([
        """('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
        (row['Pid'], row['ProductNameEN'].replace("'", "''"),
         row['ProductNameTH'].replace("'", "''"), row['Local_Cate_ID'],
         row['LocalNameEN'].replace("'", "''"),
         row['LocalNameTH'].replace("'", "''"), row['Brand_ID'],
         row['BrandNameEN'].replace("'", "''"),
         row['BrandNameTH'].replace("'", "''"), row["Barcode"])
        for row in data[i:i + 1000]
    ])
    try:
      t1c_cursor.execute(sql.format(query))
      #print (sql.format(query))
    except:
      print(query)
      raise
    t1c.commit()
  print("Finished")
