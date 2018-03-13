from common import sftp
import csv
from datetime import datetime
import math
import os
import pymssql
import uuid


dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
target_dir = 'siebel/full'
target_path = os.path.join(parent_path, 'output', target_dir)
if not os.path.exists(target_path):
  os.makedirs(target_path)

interface_name = 'BCH_CGO_T1C_ProductMasterFull'
now = datetime.now()
batchdatetime = now.strftime('%d%m%Y_%H:%M:%S:%f')[:-3]
filedatetime = now.strftime('%d%m%Y_%H%M%S')

per_page = 10000

with pymssql.connect("mssql.production.thecentral.com", "coreapi",
                       "coreapi", "DBMKPOnline") as conn:
  with conn.cursor(as_dict=True) as cursor:
    start_time = datetime.now()
    sql = """
    SELECT
        '1' AS LNIdentifier,
        concat('RBS-', pro.PID, '-', NewID()) AS SourceTransID,
        pro.PID,
        ISNULL(pro.Upc, '') AS Barcode,
        ISNULL([ProductNameEn], '') AS [ProductNameEN],
        ISNULL([ProductNameTh], '') AS [ProductNameTH],
        '' AS DIVCode,
        '' AS DIVNameEN,
        '' AS DIVNameTH,
        CASE WHEN ISNULL(Dept.CategoryId, '') = '' THEN '' ELSE CONCAT('RBS',Dept.CategoryId) END AS DeptID,
        ISNULL(Dept.NameEn, '') AS DeptNameEN,
        ISNULL(Dept.NameTh, '') AS DeptNameTH,
        CASE WHEN ISNULL(SubDept.CategoryId, '') = '' THEN '' ELSE CONCAT('RBS',SubDept.CategoryId) END AS SubDeptID,
        ISNULL(SubDept.NameEn, '') AS SubDeptNameEN,
        ISNULL(SubDept.NameTh, '') AS SubDeptNameTH,
        ISNULL(Class.CategoryId, '') AS ClassID,
        CASE WHEN ISNULL(Class.CategoryId, '') = '' THEN '' ELSE CONCAT('RBS',Class.CategoryId) END AS ClassID,
        ISNULL(Class.NameEn, '') AS ClassNameEN,
        ISNULL(Class.NameTh, '') AS ClassNameTH,
        ISNULL(SubClass.CategoryId, '') AS SubClassID,
        CASE WHEN ISNULL(SubClass.CategoryId, '') = '' THEN '' ELSE CONCAT('RBS',SubClass.CategoryId) END AS SubClassID,
        ISNULL(SubClass.NameEn, '') AS SubClassNameEN,
        ISNULL(SubClass.NameTh, '') AS SubClassNameTH,
        '' AS ProductLine,
        substring(REPLACE(REPLACE(pro.ProdTDNameTh, CHAR(13), ''), CHAR(10), ''), 1, 255) AS PrimaryDesc,
        substring(REPLACE(REPLACE(pro.ProdTDNameEn, CHAR(13), ''), CHAR(10), ''), 1, 100) AS SecondaryDesc,
        pro.Status,
        ISNULL(Pro.[BrandId], '') AS [BrandID],
        ISNULL(Ba.[BrandNameEn], '') AS [BrandNameEN],
        ISNULL(NULLIF(Ba.[BrandNameTh],''), [BrandNameEn]) AS [BrandNameTH],
        pro.vendorid AS VendorID,
        '' AS VendorNameEN,
        '' AS VendorNameTH,
        format(pro.EffectiveDate, 'ddMMyyyy', 'en-us') AS EffectiveStartDate,
        format(pro.ExpireDate, 'ddMMyyyy', 'en-us') AS EffectiveEndDate,
        '01' as CreditConsignmentCode,
        'Credit' as CreditConsignmentDesc,
        'ProductService' AS SourceSystem,
        CASE WHEN pro.TheOneCardEarn = '1' THEN 'Y' ELSE 'N' END AS PointExclusionFlag
    FROM [DBMKPOnline].[dbo].[Product] Pro
    LEFT JOIN [DBMKPOnline].[dbo].[LocalCategory] LCat ON Pro.LocalCatId = LCat.CategoryId
    LEFT JOIN [DBMKPOnline].[dbo].[Brand] Ba ON Pro.BrandId = Ba.BrandId
    LEFT JOIN LocalCategory Dept ON Dept.CategoryId = (
        SELECT CategoryId FROM (
            SELECT
                ROW_NUMBER()
                OVER (
            ORDER BY Lft ) AS RowNum,CategoryId
        FROM LocalCategory
        WHERE ShopID = LCat.ShopId AND Lft <= LCat.Lft AND Rgt >= LCat.Rgt)s WHERE s.RowNum = 1)
    LEFT JOIN LocalCategory SubDept ON SubDept.CategoryId = (
        SELECT CategoryId FROM (
            SELECT
                ROW_NUMBER()
                OVER (
            ORDER BY Lft ) AS RowNum,CategoryId
        FROM LocalCategory
        WHERE ShopID = LCat.ShopId AND Lft <= LCat.Lft AND Rgt >= LCat.Rgt)s WHERE s.RowNum = 2)
    LEFT JOIN LocalCategory Class ON Class.CategoryId = (
        SELECT CategoryId FROM (
            SELECT
                ROW_NUMBER()
                OVER (
            ORDER BY Lft ) AS RowNum,CategoryId
        FROM LocalCategory
        WHERE ShopID = LCat.ShopId AND Lft <= LCat.Lft AND Rgt >= LCat.Rgt)s WHERE s.RowNum = 3)
    LEFT JOIN LocalCategory SubClass ON SubClass.CategoryId = (
        SELECT CategoryId FROM (
            SELECT
                ROW_NUMBER()
                OVER (
            ORDER BY Lft ) AS RowNum,CategoryId
        FROM LocalCategory
        WHERE ShopID = LCat.ShopId AND Lft <= LCat.Lft AND Rgt >= LCat.Rgt)s WHERE s.RowNum = 4)
    WHERE 1 = 1
    AND len(pro.PID) > 0
    ORDER BY pro.Pid
    """
    cursor.execute(sql)
    elapsed_time = (datetime.now() - start_time).seconds
    print("[RBS]:Prepared in {} s.".format(elapsed_time))

    rbs_data = cursor.fetchall()
    rbs_rows = len(rbs_data)
    print("[RBS]:Rows: {}".format(rbs_rows))

with pymssql.connect("10.17.251.160", "central", "Cen@tral", "DBCDSContent") as conn:
  with conn.cursor(as_dict=True) as cursor:
    start_time = datetime.now()
    sql = """
      if object_id('dbo.temp_siebel_product', 'U') is not null
        drop table dbo.temp_siebel_product
    """
    cursor.execute(sql)
    sql = """
      select s.* into dbo.temp_siebel_product from (
        select
          '1' as LNIdentifier,
          concat('CGO-', p.pidnew, '-', NewID()) as SourceTransID,
          p.pidnew as PID,
          isnull(m.sbc, m.ibc) as Barcode,
          substring(REPLACE(REPLACE(p.DocnameEn, CHAR(13), ''), CHAR(10), ''), 1, 100)  as ProductNameEN,
          substring(REPLACE(REPLACE(p.Docname, CHAR(13), ''), CHAR(10), ''), 1, 100) as ProductNameTH,
          '' as DIVCode,
          '' as DIVNameEN,
          '' as DIVNameTH,
          concat(case bu WHEN 'MSL' THEN 'M&S' ELSE bu END,m.IDept) as DeptID,
          substring(REPLACE(REPLACE(d.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameEN,
          substring(REPLACE(REPLACE(d.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameTH,
          concat(case bu WHEN 'MSL' THEN 'M&S' ELSE bu END,m.ISDept) as SubDeptID,
          substring(REPLACE(REPLACE(sd.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameEN,
          substring(REPLACE(REPLACE(sd.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameTH,
          concat(case bu WHEN 'MSL' THEN 'M&S' ELSE bu END,m.IClass) as ClassID,
          substring(REPLACE(REPLACE(c.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameEN,
          substring(REPLACE(REPLACE(c.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameTH,
          concat(case bu WHEN 'MSL' THEN 'M&S' ELSE bu END,m.ISClass) as SubClassID,
          substring(REPLACE(REPLACE(sc.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as SubClassNameEN,
          substring(REPLACE(REPLACE(sc.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as SubClassNameTH,
          '' as ProductLine,
          substring(REPLACE(REPLACE(p.displayname, CHAR(13), ''), CHAR(10), ''), 1, 255) as PrimaryDesc,
          substring(REPLACE(REPLACE(p.displaynameEN, CHAR(13), ''), CHAR(10), ''), 1, 100) as SecondaryDesc,
          'A' as Status,
          b.brandjdaid as BrandID,
          substring(REPLACE(REPLACE(b.brandjdaname, CHAR(13), ''), CHAR(10), ''), 1, 50) AS BrandNameEN,
          substring(REPLACE(REPLACE(b.brandjdaname, CHAR(13), ''), CHAR(10), ''), 1, 50) AS BrandNameTH,
          m.vendorid as VendorID,
          '' as VendorNameEN,
          '' as VendorNameTH,
          format(p.EffectiveDate, 'ddMMyyyy', 'en-us') AS EffectiveStartDate,
          format(p.ExpiredDate, 'ddMMyyyy', 'en-us') AS EffectiveEndDate,
          isnull(m.skutype, '03') as CreditConsignmentCode,
          case
            when m.skutype = '01' then 'Credit'
            when m.skutype = '02' then 'Consigment'
            else 'Non-Merchandise'
          end as CreditConsignmentDesc,
          'ProductService' as SourceSystem,
          'N' as PointExclusionFlag
        from tbproduct p
        inner join TBBusinessUnit bu on p.BusinessUnitId = bu.BusinessUnitId
        inner join tbproductmapping m on m.pidnew = p.pidnew and m.BusinessUnitId = p.BusinessUnitId
        inner join tbjdabrand b on b.brandjdaid = m.brandjdaid and b.businessunitid = m.businessunitid
        inner join tbjdahierarchy d on d.businessunitid = m.businessunitid and d.idept = m.idept and d.isdept = 0 and d.iclass = 0 and d.isclass = 0
        inner join tbjdahierarchy sd on sd.businessunitid = m.businessunitid and sd.idept = m.idept and sd.isdept = m.isdept and sd.iclass = 0 and sd.isclass = 0
        inner join tbjdahierarchy c on c.businessunitid = m.businessunitid and c.idept = m.idept and c.isdept = m.isdept and c.iclass = m.iclass and c.isclass = 0
        inner join tbjdahierarchy sc on sc.businessunitid = m.businessunitid and sc.idept = m.idept and sc.isdept = m.isdept and sc.iclass = m.iclass and sc.isclass = m.isclass
        where 1 = 1
        and len(p.pidnew) > 0
        and p.status in (1, 6, 9)
        and p.isfirststockgr = 1
        and getdate() between p.EffectiveDate and p.ExpiredDate
      ) s
      order by s.pid
    """
    cursor.execute(sql)
    sql = "create index idx_siebel_product_pid ON dbo.temp_siebel_product (pid)"
    cursor.execute(sql)
    elapsed_time = (datetime.now() - start_time).seconds
    print("Prepared in {}    s.".format(elapsed_time))

    sql = "select count(pid) as c from dbo.temp_siebel_product"
    cursor.execute(sql)
    data = cursor.fetchone()
    rows = data['c']
    pages = math.ceil(rows / per_page)
    print("rows: {}, pages: {}".format(rows, pages))
    for page in range(0, pages):
      start_time = datetime.now()
      sql = """
        select * from dbo.temp_siebel_product
        order by pid
        offset {} rows fetch next {} rows only
      """
      cursor.execute(sql.format(per_page*page, per_page))
      data = cursor.fetchall()

      # Fill CDS Last Page Data
      if page == pages-1:
          index = per_page - len(data)
          data = data + rbs_data[:index]
          rbs_data = rbs_data[index:]
      elapsed_time = (datetime.now() - start_time).seconds
      print("Page-{} in {} s.".format(page+1, elapsed_time))

      headers = data[0]
      total_row = len(data)
      datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, page+1)
      filepath = os.path.join(target_path, datfile)
      with open(filepath, 'w') as outfile:
        outfile.write("0|{}\n".format(total_row))
        writer = csv.DictWriter(
            outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
        for d in data:
          writer.writerow(d)
        outfile.write('9|End')

    # rest of rbs data
    if len(rbs_data) > 0 :
      headers = rbs_data[0]
      total_row = len(rbs_data)
      datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, page+2)
      filepath = os.path.join(target_path, datfile)
      with open(filepath, 'w') as outfile:
        outfile.write("0|{}\n".format(total_row))
        writer = csv.DictWriter(
            outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
        for d in rbs_data:
          writer.writerow(d)
        outfile.write('9|End')

ctrlfile = "{}_{}.ctrl".format(interface_name, filedatetime)
filepath = os.path.join(target_path, ctrlfile)
attribute1 = ""
attribute2 = ""
with open(filepath, 'w') as outfile:
  outfile.write("{}|CGO|Online|{}|{}|{}|CGO|{}|{}".format(
    interface_name, pages + 1, rows + rbs_rows, batchdatetime,
    attribute1, attribute2))

start_time = datetime.now()
destination = '/inbound/BCH_SBL_ProductMasterFull/req'
sftp(target_path, destination)
elapsed_time = (datetime.now() - start_time).seconds
print("Success FTP in {} s.".format(elapsed_time))
