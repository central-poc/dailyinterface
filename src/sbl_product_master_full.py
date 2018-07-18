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
        CASE WHEN ISNULL(Dept.IDEPT, '') = '' THEN '' ELSE CONCAT('RBS',Dept.IDEPT) END AS DeptID,
        ISNULL(Dept.DPTNAM, '') AS DeptNameEN,
        ISNULL(Dept.DPTNAM, '') AS DeptNameTH,
        CASE WHEN ISNULL(SubDept.ISDEPT, '') = '' THEN '' ELSE CONCAT('RBS',SubDept.ISDEPT) END AS SubDeptID,
        ISNULL(SubDept.DPTNAM, '') AS SubDeptNameEN,
        ISNULL(SubDept.DPTNAM, '') AS SubDeptNameTH,
        CASE WHEN ISNULL(Class.ICLAS, '') = '' THEN '' ELSE CONCAT('RBS',Class.ICLAS) END AS ClassID,
        ISNULL(Class.DPTNAM, '') AS ClassNameEN,
        ISNULL(Class.DPTNAM, '') AS ClassNameTH,
        CASE WHEN ISNULL(SubClass.ISCLAS, '') = '' THEN '' ELSE CONCAT('RBS',SubClass.ISCLAS) END AS SubClassID,
        ISNULL(SubClass.DPTNAM, '') AS SubClassNameEN,
        ISNULL(SubClass.DPTNAM, '') AS SubClassNameTH,
        '' AS ProductLine,
        substring(REPLACE(REPLACE(pro.ProdTDNameTh, CHAR(13), ''), CHAR(10), ''), 1, 255) AS PrimaryDesc,
        substring(REPLACE(REPLACE(pro.ProdTDNameEn, CHAR(13), ''), CHAR(10), ''), 1, 100) AS SecondaryDesc,
        'A' AS Status,
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
        CASE WHEN pro.TheOneCardEarn = 0 THEN 'Y' ELSE 'N' END AS PointExclusionFlag
    FROM [DBMKPOnline].[dbo].[Product] Pro
    LEFT JOIN [DBMKPOnline].[dbo].[Brand] Ba ON Pro.BrandId = Ba.BrandId
    LEFT JOIN JDARBS_Dept Dept on Dept.IDEPT = pro.JDADept AND Dept.ISDEPT = 0 AND Dept.ICLAS = 0 AND Dept.ISCLAS = 0
    LEFT JOIN JDARBS_Dept SubDept on Dept.IDEPT = pro.JDADept AND Dept.ISDEPT = pro.JDASubDept AND Dept.ICLAS = 0 AND Dept.ISCLAS = 0
    LEFT JOIN JDARBS_Dept Class on Dept.IDEPT = pro.JDADept AND Dept.ISDEPT = pro.JDASubDept AND Dept.ICLAS = pro.ClassCode AND Dept.ISCLAS = 0
    LEFT JOIN JDARBS_Dept SubClass on Dept.IDEPT = pro.JDADept AND Dept.ISDEPT = pro.JDASubDept AND Dept.ICLAS = pro.ClassCode AND Dept.ISCLAS = pro.SubClassCode
    WHERE 1 = 1
    AND len(pro.PID) > 0
    AND len(pro.Upc) > 0
    AND pro.status = 'AP'
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
          isnull(nullif(LTRIM(RTRIM(m.sbc)),''), m.ibc) as Barcode,
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
        inner join tbjdabrand b on b.brandjdaid = m.brandjdaid and b.businessunitid = bu.parentid
        inner join tbjdahierarchy d on d.businessunitid = bu.parentid and d.idept = m.idept and d.isdept = 0 and d.iclass = 0 and d.isclass = 0
        inner join tbjdahierarchy sd on sd.businessunitid = bu.parentid and sd.idept = m.idept and sd.isdept = m.isdept and sd.iclass = 0 and sd.isclass = 0
        inner join tbjdahierarchy c on c.businessunitid = bu.parentid and c.idept = m.idept and c.isdept = m.isdept and c.iclass = m.iclass and c.isclass = 0
        inner join tbjdahierarchy sc on sc.businessunitid = bu.parentid and sc.idept = m.idept and sc.isdept = m.isdept and sc.iclass = m.iclass and sc.isclass = m.isclass
        where 1 = 1
        and len(p.pidnew) > 0
        and p.status in (1, 6, 9)
        and p.isfirststockgr = 1
        and getdate() between p.EffectiveDate and p.ExpiredDate
        and m.sbc is not null
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
# destination = '/inbound/BCH_SBL_ProductMasterFull/req'
# sftp('cgotest',target_path, destination)
elapsed_time = (datetime.now() - start_time).seconds
print("Success FTP in {} s.".format(elapsed_time))
