from common import sftp_ofm, chunks
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

interface_name = 'BCH_OFM_T1C_ProductMasterFull'
now = datetime.now()
batchdatetime = now.strftime('%d%m%Y_%H:%M:%S:%f')[:-3]
filedatetime = now.strftime('%d%m%Y_%H%M%S')

per_page = 10000

with pymssql.connect("10.17.1.23", "CTOAI", "CTO@Ai", "DBInterfaceSiebel") as conn:
  with conn.cursor(as_dict=True) as cursor:
    start_time = datetime.now()
    sql = """
        SELECT top 1
        BatchID,
        TotalRecord
        FROM tb_Control_Master
        WHERE MasterType = 'M'
        ORDER BY BatchID DESC
    """
    cursor.execute(sql)
    data = cursor.fetchone()
    batch_id = data["BatchID"]
    print(batch_id)
    sql = """
        SELECT
        '1' AS LNIdentifier,
        'OFM-' + RTLProductCode + '-' + CAST(NEWID() AS NVARCHAR(36)) AS SourceTransID,
        RTLProductCode as PID,
        Barcode AS Barcode,
        ProductNameEN AS [ProductNameEN],
        ProductNameTH AS [ProductNameTH],
        DivCode AS DIVCode,
        DivNameEN AS DIVNameEN,
        DivNameTH AS DIVNameTH,
        DeptID AS DeptID,
        DepNameEN AS DeptNameEN,
        DepNameTH AS DeptNameTH,
        SubDeptID AS SubDeptID,
        SubDeptNameEN AS SubDeptNameEN,
        SubDeptNameTH AS SubDeptNameTH,
        ClassID AS ClassID,
        ClassNameEN AS ClassNameEN,
        ClassNameTH AS ClassNameTH,
        SubClassID AS SubClassID,
        SubClassNameEN AS SubClassNameEN,
        SubDeptNameTH AS SubClassNameTH,
        ProductLine AS ProductLine,
        PrimaryDesc AS PrimaryDesc,
        '' AS SecondaryDesc,
        Status AS Status,
        BrandID AS [BrandID],
        BrandNameEN AS [BrandNameEN],
        BrandNameTH AS [BrandNameTH],
        VendorID AS VendorID,
        VendorNameEN AS VendorNameEN,
        VendorNameTH AS VendorNameTH,
        EffectiveStartDate AS EffectiveStartDate,
        EffectiveEndDate AS EffectiveEndDate,
        CreditConsignmentCode as CreditConsignmentCode,
        CreditConsignmentDesc as CreditConsignmentDesc,
        '' AS SourceSystem,
        PointExcludeFlg AS PointExclusionFlag
    FROM tm_Product_Monthly
    WHERE BatchID=%s
    ORDER BY RTLProductCode
    """
    cursor.execute(sql,batch_id)
    rows = cursor.fetchall()

    elapsed_time = (datetime.now() - start_time).seconds
    print("Prepared in {}    s.".format(elapsed_time))

    chunk = chunks(rows)
    for count, data  in enumerate(chunk):

      headers = data[0]
      total_row = len(data)
      datfile = "{}_{}.dat.{:0>4}".format(interface_name, filedatetime, count+1)
      filepath = os.path.join(target_path, datfile)
      with open(filepath, 'w') as outfile:
        outfile.write("0|{}\n".format(total_row))
        writer = csv.DictWriter(
            outfile, fieldnames=headers, delimiter='|', skipinitialspace=True)
        for d in data:
          writer.writerow(d)
        outfile.write('9|End')

ctrlfile = "{}_{}.ctrl".format(interface_name, filedatetime)
filepath = os.path.join(target_path, ctrlfile)
attribute1 = ""
attribute2 = ""
with open(filepath, 'w') as outfile:
  outfile.write("{}|OFM|Online|{}|{}|{}|OFM|{}|{}".format(
    interface_name, count + 1 , len(rows), batchdatetime,
    attribute1, attribute2))

start_time = datetime.now()
destination = '/inbound/BCH_SBL_ProductMasterFull/req'
sftp_ofm(target_path, destination)
elapsed_time = (datetime.now() - start_time).seconds
print("Success FTP in {} s.".format(elapsed_time))
