import csv
from datetime import datetime
import os
import pymssql
import uuid

with pymssql.connect("10.17.251.160", "central", "Cen@tral",
                     "DBCDSContent") as conn:
  cds_cursor = conn.cursor(as_dict=True)
  query = """
    SELECT TOP 100
      '1' as LNIdentifier,
      concat('CGO_', NewID()) as SourceTransID,
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
      case 
        when p.status in (1, 6, 9) and p.isfirststockgr = 1 and getdate() between p.EffectiveDate and p.ExpiredDate
        then 'A' else 'I'
      end as Status,
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
    inner join TBJDABrand b ON b.brandjdaid = m.brandjdaid and b.businessunitid = m.businessunitid
    inner join TBJDAHierarchy d on d.businessunitid = m.businessunitid and d.idept = m.idept and d.isdept = 0 and d.iclass = 0 and d.isclass = 0
    inner join TBJDAHierarchy sd on sd.businessunitid = m.businessunitid and sd.idept = m.idept and sd.isdept = m.isdept and sd.iclass = 0 and sd.isclass = 0
    inner join TBJDAHierarchy c on c.businessunitid = m.businessunitid and c.idept = m.idept and c.isdept = m.isdept and c.iclass = m.iclass and c.isclass = 0
    inner join TBJDAHierarchy sc on sc.businessunitid = m.businessunitid and sc.idept = m.idept and sc.isdept = m.isdept and sc.iclass = m.iclass and sc.isclass = m.isclass
    WHERE 1 = 1
    and len(p.pidnew) > 0
    --and p.status = 1 
    --and p.isfirststockgr = 1 
    --and getdate() between p.EffectiveDate and p.ExpiredDate
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

interface_name = 'BCH_CGO_T1C_ProductMasterFull'
filedatetime = datetime.now().strftime('%d%m%Y_%H:%M:%S')
datfile = '%s_%s.dat.0001' % (interface_name, filedatetime)
filepath = os.path.join(target_path, datfile)
with open(filepath, 'w') as outfile:
  outfile.write('0|%d\n' % total_row)
  writer = csv.DictWriter(
      outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
  for d in data:
    d['SourceTransID'] = 'CGO_%s' % uid
    writer.writerow(d)
  outfile.write('9|End')

ctrlfile = '%s_%s.ctrl' % (interface_name, filedatetime)
filepath = os.path.join(target_path, ctrlfile)
with open(filepath, 'w') as outfile:
  outfile.write('%s|CGO|Online|1|%d|%s|CGO|||' % (interface_name, total_row,
                                                  filedatetime))
