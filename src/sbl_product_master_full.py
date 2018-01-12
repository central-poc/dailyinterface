from common import sftp
import csv
from datetime import datetime
import math
import os
import pymssql
import uuid


dir_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
target_dir = 'siebel'
target_path = os.path.join(parent_path, 'output', target_dir)
if not os.path.exists(target_path):
  os.makedirs(target_path)

interface_name = 'BCH_CGO_T1C_ProductMasterFull'
filedatetime = datetime.now().strftime('%d%m%Y_%H%M%S')

per_page = 10

with pymssql.connect("10.17.251.160", "central", "Cen@tral", "DBCDSContent") as conn:
  with conn.cursor(as_dict=True) as cursor:
    query = """
      select count(p.pidnew) as c
      from dbo.tbproduct p
      where 1 = 1
      and len(p.pidnew) > 0
      and p.status in (1, 6, 9) 
      and p.isfirststockgr = 1
      and getdate() between p.EffectiveDate and p.ExpiredDate
    """
    cursor.execute(query)
    data = cursor.fetchone()
    rows = data['c']
    pages = math.ceil(rows / per_page)
    print("rows: {}, pages: {}".format(rows, pages))
    for page in range(1, pages + 1):
      start_time = datetime.now()
      query = """
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
          m.IDept as DeptID,
          substring(REPLACE(REPLACE(d.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameEN, 
          substring(REPLACE(REPLACE(d.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameTH,
          m.ISDept as SubDeptID,
          substring(REPLACE(REPLACE(sd.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameEN,
          substring(REPLACE(REPLACE(sd.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameTH,
          m.IClass as ClassID,
          substring(REPLACE(REPLACE(c.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameEN,
          substring(REPLACE(REPLACE(c.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameTH,
          m.ISClass as SubClassID,
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
          p.EffectiveDate as EffectiveStartDate,
          p.ExpiredDate as EffectiveEndDate,
          isnull(m.skutype, '03') as CreditConsignmentCode,
          case
            when m.skutype = '01' then 'Credit'
            when m.skutype = '02' then 'Consigment'
            else 'Non-Merchandise'
          end as CreditConsignmentDesc,
          'ProductService' as SourceSystem,
          'N' as PointExclusionFlag
        from tbproduct p
        inner join tbproductmapping m on m.pidnew = p.pidnew
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
        order by p.pidnew
        offset {} rows fetch next {} rows only
      """
      cursor.execute(query.format(per_page*page, per_page))
      data = cursor.fetchall()

      elapsed_time = (datetime.now() - start_time).seconds
      print("Success query in {} s.".format(elapsed_time))

      headers = data[0]
      total_row = len(data)
      datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, page)
      filepath = os.path.join(target_path, datfile)
      with open(filepath, 'w') as outfile:
        outfile.write("0|{}\n".format(total_row))
        writer = csv.DictWriter(
            outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
        for d in data:
          writer.writerow(d)
        outfile.write('9|End')

start_time = datetime.now()
ctrlfile = "{}_{}.ctrl".format(interface_name, filedatetime)
filepath = os.path.join(target_path, ctrlfile)
with open(filepath, 'w') as outfile:
  outfile.write("{}|CGO|Online|{}|{}|{}|CGO|||".format(interface_name, pages, rows, filedatetime))

destination = '/inbound/BCH_SBL_ProductMasterFull/req'
sftp(target_path, destination)
elapsed_time = (datetime.now() - start_time).seconds
print("Success FTP in {} s.".format(elapsed_time))