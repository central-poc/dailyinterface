import pymssql
from datetime import datetime, timedelta


def connect_cds_db():
    return pymssql.connect("10.17.220.55", "central", "Cen@tral")


def connect_t1c_db():
    return pymssql.connect("10.17.220.181", "sa", "Asdf123!")


start_time = datetime.now()

with connect_cds_db() as conn:
    cds_cursor = conn.cursor()
    query = '''
    select isnull(level0.DepartmentId, 0) as Level0ID,
      isnull(level0.DisplayName, '') as CategoryLevel0,
      isnull(level1.DepartmentId, 0) as Level1ID,
      isnull(level1.DisplayName, '') as CategoryLevel1,
      isnull(level2.DepartmentId, 0) as Level2ID,
      isnull(level2.DisplayName, '') as CategoryLevel2TH,
      isnull(level2.DisplayNameEN, '') as CategoryLevel2EN,
      isnull(level3.DepartmentId, 0) as Level3ID,
      isnull(level3.DisplayName, '') as CategoryLevel3TH,
      isnull(level3.DisplayNameEN, '') as CategoryLevel3EN,
      isnull(level4.DepartmentId, 0) as Level4ID,
      isnull(level4.DisplayName, '') as CategoryLevel4TH,
      isnull(level4.DisplayNameEN, '') as CategoryLevel4EN
    from DBCDSContent.dbo.TBDepartment level0
    LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level1
    ON level0.DepartmentId = level1.ParentId
    LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level2
    ON level1.DepartmentId = level2.ParentId
    LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level3
    ON level2.DepartmentId = level3.ParentId
    LEFT OUTER JOIN DBCDSContent.dbo.TBDepartment level4
    ON level3.DepartmentId = level4.ParentId
    ORDER BY Level0ID ASC;
    '''
    cds_cursor.execute(query)
    data = cds_cursor.fetchall()
    print("Total %d records" % len(data))

with connect_t1c_db() as t1c:
  t1c_cursor = t1c.cursor()
  t1c_cursor.execute("TRUNCATE TABLE DBSync.dbo.CategoryHierarchyCDS;")

  insert_sql = '''
    INSERT INTO DBSync.dbo.CategoryHierarchyCDS 
    (Level0ID, CategoryLevel0, Level1ID, CategoryLevel1, Level2ID, CategoryLevel2TH, CategoryLevel2EN, Level3ID, 
    CategoryLevel3TH, CategoryLevel3EN, Level4ID, CategoryLevel4TH, CategoryLevel4EN)
    VALUES (%d, %s, %d, %s, %d, %s, %s, %d, %s, %s, %d, %s, %s)
  '''

  t1c_cursor.executemany(insert_sql, data)
  t1c.commit()


end_time = datetime.now()
execution_time = (end_time - start_time).seconds
print("Update CDS category hierachy in %ds" % execution_time)