import pymssql
from common import notifyLine
from datetime import datetime, timedelta


def connect_cds_db():
  return pymssql.connect("10.17.220.55", "central", "Cen@tral", "DBCDSContent")


def connect_rbs_db():
  return pymssql.connect("mssql.production.thecentral.com", "coreapi",
                         "coreapi", "DBMKPOnline")


def connect_t1c_db():
  return pymssql.connect("10.17.220.181", "sa", "Asdf123!", "DBSync")


try:
  start_time = datetime.now()
  with connect_cds_db() as conn:
    cds_cursor = conn.cursor(as_dict=True)
    query = '''
      SELECT
          a.pidnew AS Pid,
          a.DocnameEn AS ProductNameEN,
          a.Docname AS ProductNameTH,
          d.departmentID AS Local_Cate_ID,
          d.DisplayNameEN AS LocalNameEN,
          d.DisplayName AS LocalNameTH,
          c.Brandid as Brand_ID,
          c.DisplaynameEn AS BrandNameEN,
          c.DisplayName AS BrandNameTH,
          CONCAT('000000000', a.PIDNew) as Barcode,
          b.BU AS JDA_BUCODE,
          a.BU_SKU as JDA_SKU
      FROM dbo.tbproduct a
      INNER JOIN dbo.TBbusinessUnit b on b.BusinessUnitId = a.BusinessUnitId
      JOIN dbo.tbbrand c ON c.brandid = a.brandid
      JOIN dbo.tbproductGroup pg ON pg.productgroupid = a.productgroupid
      JOIN dbo.tbdepartment d ON pg.departmentid = d.departmentid
      WHERE a.isFirstStockGR = 1 AND len(a.pidnew) > 0
      ORDER BY a.pidnew
      '''
    cds_cursor.execute(query)
    cds_data = cds_cursor.fetchall()

  with connect_rbs_db() as conn:
    rbs_cursor = conn.cursor(as_dict=True)
    query = '''
      SELECT
        Pid,
        ISNULL(ProductNameEn,'') AS ProductNameEN,
        ISNULL(ProductNameTh,'') AS ProductNameTH,
        ISNULL(Pro.LocalCatId,'') AS Local_Cate_ID,
        ISNULL(LCat.NameEn,'') AS LocalNameEN,
        ISNULL(LCat.NameTh,'') AS LocalNameTH,
        ISNULL(Pro.BrandId,'') AS Brand_ID,
        ISNULL(Ba.BrandNameEn,'') AS BrandNameEN,
        ISNULL(Ba.BrandNameTh,'') AS BrandNameTH,
        CONCAT('000000000',PID) as Barcode,
        'RBS' AS JDA_BUCODE,
        SKU as JDA_SKU
      FROM [DBMKPOnline].[dbo].[Product] Pro
      LEFT JOIN [DBMKPOnline].[dbo].[LocalCategory] LCat ON Pro.LocalCatId = LCat.CategoryId
      LEFT JOIN [DBMKPOnline].[dbo].[Brand] Ba ON Pro.BrandId = Ba.BrandId
      '''
    rbs_cursor.execute(query)
    rbs_data = rbs_cursor.fetchall()

  with connect_t1c_db() as t1c:
    t1c_cursor = t1c.cursor()
    t1c_cursor.execute('TRUNCATE TABLE ProductMapping')

    sql = """
        INSERT INTO ProductMapping (
          PID, ProductNameEN, ProductNameTH,
          Local_Cate_ID, LocalNameEN, LocalNameTH,
          Brand_ID, BrandNameEN, BrandNameTH, Barcode,
          JDA_BUCODE, JDA_SKU
        ) values {}
    """
    data = cds_data + rbs_data
    print("Total Product {:,} records".format(len(data)))
    for i in range(0, len(data), 1000):
      query = ",".join([
          """('%s', '%s', '%s', '%s', '%s', '%s', '%s', 
          '%s', '%s', '%s', '%s', '%s')""" %
          (row['Pid'], row['ProductNameEN'].replace("'", "''"),
           row['ProductNameTH'].replace("'", "''"), row['Local_Cate_ID'],
           row['LocalNameEN'].replace("'", "''"),
           row['LocalNameTH'].replace("'", "''"), row['Brand_ID'],
           row['BrandNameEN'].replace("'", "''"),
           row['BrandNameTH'].replace("'", "''"), row["Barcode"],
           row["JDA_BUCODE"], row["JDA_SKU"]) for row in data[i:i + 1000]
      ])
      try:
        t1c_cursor.execute(sql.format(query))
      except:
        raise
      t1c.commit()

  end_time = datetime.now()
  execution_time = (end_time - start_time).seconds
  notifyLine("[T1C]: Product[{:,}] sync in {:,} s".format(
      len(data), execution_time))
except Exception as e:
  notifyLine("[T1C]: Product master Failure - {}".format(e))
  print(e)
