import pymssql


def connect_cds_db():
  return pymssql.connect("10.17.220.55", "central", "Cen@tral", "DBCDSContent")

def connect_rds_db():
  return pymssql.connect("mssql.production.thecentral.com", "coreapi", "coreapi", "DBMKPOnline")

def connect_t1c_db():
  return pymssql.connect("10.17.220.181", "sa", "Asdf123!", "DBSync")


with connect_cds_db() as conn:
  cds_cursor = conn.cursor(as_dict=True)
  query = '''
    SELECT 
        pidnew AS Pid,
        DocnameEn AS ProductNameEN,
        Docname AS ProductNameTH,
        d.departmentID AS Local_Cate_ID,
        d.DisplayNameEN AS LocalNameEN,
        d.DisplayName AS LocalNameTH,
        tbbrand.Brandid as Brand_ID,
        tbbrand.DisplaynameEn AS BrandNameEN,
        tbbrand.DisplayName AS BrandNameTH,
        CONCAT('000000000',PIDNew) as Barcode,
        CASE WHEN BusinessUnitId = 3 THEN 'CDS'
            WHEN BusinessUnitId = 5 THEN 'B2S'
            WHEN BusinessUnitId = 7 THEN 'SSP'
            WHEN BusinessUnitId = 12 THEN 'MSL'
        END AS JDA_BUCODE,
        BU_SKU as JDA_SKU
    FROM
        tbproduct
    JOIN
        tbbrand
    ON
        tbproduct.brandid = tbbrand.brandid
    JOIN
        tbproductGroup pg
    ON
        tbproduct.productgroupid = pg.productgroupid
    JOIN
        tbdepartment d
    ON
        pg.departmentid = d.departmentid
    WHERE
        tbproduct.isFirstStockGR = 1 AND len(pidnew) > 0
    ORDER BY
        pidnew
    '''
  cds_cursor.execute(query)
  cds_data = cds_cursor.fetchall()


with connect_rds_db() as conn:
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
    ISNULL(Upc,'') AS Barcode,
    'RBS' AS JDA_BUCODE,
    SKU as JDA_SKU
  FROM
    [DBMKPOnline].[dbo].[Product] Pro
  LEFT JOIN
    [DBMKPOnline].[dbo].[LocalCategory] LCat ON Pro.LocalCatId=LCat.CategoryId
  LEFT JOIN
    [DBMKPOnline].[dbo].[Brand] Ba ON Pro.BrandId = Ba.BrandId
    '''
  rbs_cursor.execute(query)
  rbs_data = rbs_cursor.fetchall()


with connect_t1c_db() as t1c:
  t1c_cursor = t1c.cursor()
  t1c_cursor.execute('''
  TRUNCATE TABLE ProductMappingCDS
  ''')

  sql = """
      INSERT INTO ProductMappingCDS(
        PID,ProductNameEN,ProductNameTH,
        Local_Cate_ID,LocalNameEN,LocalNameTH,
        Brand_ID,BrandNameEN,BrandNameTH,Barcode,
        JDA_BUCODE, JDA_SKU
      ) values {}
  """
  print(cds_data)
  print(rbs_data)
  data = cds_data + rbs_data
  print("Total Product : ", len(data))
  for i in range(0, len(data), 1000):
    print("Progress : ", i)
    query = ",".join([
        """('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" %
        (row['Pid'], row['ProductNameEN'].replace("'", "''"),
         row['ProductNameTH'].replace("'", "''"), row['Local_Cate_ID'],
         row['LocalNameEN'].replace("'", "''"),
         row['LocalNameTH'].replace("'", "''"), row['Brand_ID'],
         row['BrandNameEN'].replace("'", "''"),
         row['BrandNameTH'].replace("'", "''"), row["Barcode"],
         row["JDA_BUCODE"], row["JDA_SKU"])
        for row in data[i:i + 1000]
    ])
    try:
      t1c_cursor.execute(sql.format(query))
    except:
      raise
    t1c.commit()
  print("Finished")
