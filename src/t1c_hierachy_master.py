import pymssql
from common import notifyLine
from datetime import datetime, timedelta


def connect_cds_db():
  return pymssql.connect("10.17.220.55", "central", "Cen@tral", "")


def connect_rbs_db():
  return pymssql.connect("mssql.production.thecentral.com", "coreapi",
                         "coreapi", "DBMKPOnline")


def connect_t1c_db():
  return pymssql.connect("10.17.220.181", "sa", "Asdf123!", "DBSync")


try:
  start_time = datetime.now()
  with connect_cds_db() as conn:
    cds_cursor = conn.cursor()
    query = '''
      SELECT
          isnull(level0.DepartmentId, 0) AS Level0ID,
          isnull(level0.DisplayName, '') AS CategoryLevel0,
          isnull(level1.DepartmentId, 0) AS Level1ID,
          isnull(level1.DisplayName, '') AS CategoryLevel1,
          isnull(level2.DepartmentId, 0) AS Level2ID,
          isnull(level2.DisplayName, '') AS CategoryLevel2TH,
          isnull(level2.DisplayNameEN, '') AS CategoryLevel2EN,
          isnull(level3.DepartmentId, 0) AS Level3ID,
          isnull(level3.DisplayName, '') AS CategoryLevel3TH,
          isnull(level3.DisplayNameEN, '') AS CategoryLevel3EN,
          isnull(level4.DepartmentId, 0) AS Level4ID,
          isnull(level4.DisplayName, '') AS CategoryLevel4TH,
          isnull(level4.DisplayNameEN, '') AS CategoryLevel4EN,
          'CDS' AS JDA_BUCODE
      FROM DBCDSContent.dbo.TBDepartment level0
      LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level1 ON level0.DepartmentId = level1.ParentId
      LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level2 ON level1.DepartmentId = level2.ParentId
      LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level3 ON level2.DepartmentId = level3.ParentId
      LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level4 ON level3.DepartmentId = level4.ParentId
      ORDER BY Level0ID ASC
      '''
    cds_cursor.execute(query)
    cds_data = cds_cursor.fetchall()

  with connect_rbs_db() as conn:
    rbs_cursor = conn.cursor()
    query = '''
      SELECT
        isnull(t0.CategoryId, 0) AS Level0ID,
        isnull(t0.NameEn, '') AS CategoryLevel0,
        isnull(t1.CategoryId, 0) AS Level1ID,
        isnull(t1.NameEn, '') AS CategoryLevel1,
        isnull(t2.CategoryId, 0) AS Level2ID,
        isnull(t2.NameTh, '') AS CategoryLevel2TH,
        isnull(t2.NameEn, '') AS CategoryLevel2EN,
        isnull(t3.CategoryId, 0) AS Level3ID,
        isnull(t3.NameTh, '') AS CategoryLevel3TH,
        isnull(t3.NameEn, '') AS CategoryLevel3EN,
        isnull(t4.CategoryId, 0) AS Level4ID,
        isnull(t4.NameTh, '') AS CategoryLevel4TH,
        isnull(t4.NameEn, '') AS CategoryLevel4EN,
        'RBS' AS JDA_BUCODE
      FROM [DBMKPOnline].[dbo].[GlobalCategory] t0
      LEFT JOIN [DBMKPOnline].[dbo].[GlobalCategory] t1 ON t0.Lft < t1.Lft AND t1.Rgt < t0.Rgt
      LEFT JOIN [DBMKPOnline].[dbo].[GlobalCategory] t2 ON t1.Lft < t2.Lft AND t2.Rgt < t1.Rgt
      LEFT JOIN [DBMKPOnline].[dbo].[GlobalCategory] t3 ON t2.Lft < t3.Lft AND t3.Rgt < t2.Rgt
      LEFT JOIN [DBMKPOnline].[dbo].[GlobalCategory] t4 ON t3.Lft < t4.Lft AND t4.Rgt < t3.Rgt
      ORDER BY Level0ID ASC
      '''
    rbs_cursor.execute(query)
    rbs_data = rbs_cursor.fetchall()

  with connect_t1c_db() as t1c:
    t1c_cursor = t1c.cursor()
    t1c_cursor.execute("TRUNCATE TABLE DBSync.dbo.CategoryHierarchy")

    insert_sql = '''
      INSERT INTO DBSync.dbo.CategoryHierarchy (
        Level0ID, CategoryLevel0, Level1ID, CategoryLevel1, 
        Level2ID, CategoryLevel2TH, CategoryLevel2EN, 
        Level3ID, CategoryLevel3TH, CategoryLevel3EN, 
        Level4ID, CategoryLevel4TH, CategoryLevel4EN, JDA_BUCODE)
      VALUES (%d, %s, %d, %s, %d, %s, %s, %d, %s, %s, %d, %s, %s, %s)
    '''

    data = cds_data + rbs_data
    print("Total Hierarchy {:,} records".format(len(data)))
    t1c_cursor.executemany(insert_sql, data)
    t1c.commit()

  end_time = datetime.now()
  execution_time = (end_time - start_time).seconds
  notifyLine("[T1C]: Hierarchy[{:,}] sync in {:,} s".format(
      len(data), execution_time))
except Exception as e:
  notifyLine("[T1C]: Hierarchy Failure - {}".format(e))
  print(e)
