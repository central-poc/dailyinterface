import csv
from datetime import datetime
import os
import pymssql
import uuid


with pymssql.connect("10.17.251.160", "central", "Cen@tral", "DBCDSContent") as conn:
  cds_cursor = conn.cursor(as_dict=True)
  query = """
    SELECT
      '1' as LNIdentifier,
      '' as SourceTransID,
      p.pidnew as PID,
      isnull(p.Barcode, '') as Barcode,
      substring(REPLACE(REPLACE(p.DocnameEn, CHAR(13), ''), CHAR(10), ''), 1, 100)  as ProductNameEN,
      substring(REPLACE(REPLACE(p.Docname, CHAR(13), ''), CHAR(10), ''), 1, 100) as ProductNameTH,
      '' as DIVCode,
      '' as DIVNameEN,
      '' as DIVNameTH,
      p.IDept as DeptID,
      substring(REPLACE(REPLACE(p.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameEN, 
      substring(REPLACE(REPLACE(p.DeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as DeptNameTH,
      p.ISDept as SubDeptID,
      substring(REPLACE(REPLACE(p.SubDeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameEN,
      substring(REPLACE(REPLACE(p.SubDeptName, CHAR(13), ''), CHAR(10), ''), 1, 60) as SubDeptNameTH,
      p.IClass as ClassID,
      substring(REPLACE(REPLACE(p.ClassName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameEN,
      substring(REPLACE(REPLACE(p.ClassName, CHAR(13), ''), CHAR(10), ''), 1, 100) as ClassNameTH,
      p.ISClass as SubClassID,
      substring(REPLACE(REPLACE(p.SubClassName, CHAR(13), ''), CHAR(10), ''), 1, 100) as SubClassNameEN,
      substring(REPLACE(REPLACE(p.SubClassName, CHAR(13), ''), CHAR(10), ''), 1, 100) as SubClassNameTH,
      '' as ProductLine,
      substring(REPLACE(REPLACE(p.displayname, CHAR(13), ''), CHAR(10), ''), 1, 255) as PrimaryDesc,
      substring(REPLACE(REPLACE(p.displaynameEN, CHAR(13), ''), CHAR(10), ''), 1, 100) as SecondaryDesc,
      case 
  	    when p.status = 1 and p.isfirststockgr = 1 and getdate() between p.EffectiveDate and p.ExpiredDate
  	    then 'Active' else 'Not Active' 
      end as Status,
      b.Brandid as BrandID,
      substring(REPLACE(REPLACE(b.DisplaynameEN, CHAR(13), ''), CHAR(10), ''), 1, 50) AS BrandNameEN,
      substring(REPLACE(REPLACE(b.DisplayName, CHAR(13), ''), CHAR(10), ''), 1, 50) AS BrandNameTH,
      '' as VendorID,
      '' as VendorNameEN,
      '' as VendorNameTH,
      p.EffectiveDate as EffectiveStartDate,
      p.ExpiredDate as EffectiveEndDate,
      '' as CreditConsignmentCode,
      '' as CreditConsignmentDesc,
      'ProductService' as SourceSystem,
      '' as PointExclusionFlag
    FROM tbproduct p
    INNER JOIN tbbrand b ON b.brandid = p.brandid
    WHERE 1 = 1
    and cast(updateon as date) = cast(getdate() as date)
    ORDER BY p.pidnew
  """
  cds_cursor.execute(query)
  data = cds_cursor.fetchall()
  headers = data[0]
  uid = uuid.uuid4().hex
  total_row = len(data)

  dir_path = os.path.dirname(os.path.realpath(__file__))
  parent_path = os.path.abspath(os.path.join(dir_path, os.pardir))
  target_dir = 't1c'
  target_path = os.path.join(parent_path, 'output', target_dir)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  interface_name = 'BCH_CGO_T1C_ProductMasterIncre'
  filedatetime = datetime.now().strftime('%d%m%Y_%H:%M:%S')
  datfile = '%s_%s.dat.0001' % (interface_name, filedatetime)
  filepath = os.path.join(target_path, datfile)
  with open(filepath, 'w') as outfile:
    outfile.write('0|%d\n' % total_row)
    writer = csv.DictWriter(outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
    for d in data:
      d['SourceTransID'] = 'CGO_%s' %  uid
      writer.writerow(d)
    outfile.write('9|End')

  ctrlfile = '%s_%s.ctrl' % (interface_name, filedatetime)
  filepath = os.path.join(target_path, ctrlfile)
  with open(filepath, 'w') as outfile:
    outfile.write('%s|CGO|Online|1|%d|%s|CGO|||' % (interface_name, total_row, filedatetime))