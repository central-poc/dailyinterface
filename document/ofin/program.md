# PROGRAM

## ZL
##### Get Data ZL From Stored Procedure
1. Execute [SPC_GenARTRNDTL_3PL](./sp/SPC_GenARTRNDTL_3PL.sql)
2. Execute [spc_OFIN_SaleAndSaleReturn](./sp/spc_OFIN_SaleAndSaleReturn.sql)

##### GenerateTextFiles (ZL.DAT & ZL.VAL)
1. ดึงข้อมูลจาก DB `Select * From TBOFINSaleAndSaleReturnUFMT_Temp`
2. เช็คเงื่อนไขของข้อมูล
 * sum(debit) != sum(credit) from TBOFINSaleAndSaleReturnUFMT_Temp
 * find Data[CPCID] is null
    * return SoIvHead.InvDate in lastday
 * ถ้าผิดเงื่อนไขจะไม่เขียนไฟล์
3. เขียนไฟล์ .DAT & .VAL
4. Move .Dat .Val ไปที่ folder อื่น

##### Insert Data To TBOFINSaleAndSaleReturnFMT_Temp(ZL)
1. Generate Mapping Template
 * เขียนหัว column
2. Create Data ZL
 * `TRUNCATE TABLE TBOFINSaleAndSaleReturnFMT_Temp`
 * Insert Data from .DAT
---
## ZN
##### GetSPC to DataTable(ZN)
1. ดึง order id `Get orderid from tborderhead` / ordertype normal
2. เอาแต่ละ orderid ไป execute [spc_GenARTranMST](./sp/spc_GenARTranMST.sql) / isprepaid = no
3. ถ้า result != 0 ส่งเมล์ไปที่ autodevmkp@gmail.com
4. `Get order id from TBSubOrderHead` / status = Canceled / paymenttype != COD
5. เอาแต่ละ orderid ไป execute [SPC_GENTBReturnAgent](./sp/SPC_GENTBReturnAgent.sql)
6. `Get order id from TBSubOrderHead` / status = ReadyToShip, shipping, delivery
7. เอาแต่ละ orderid ไป execute [SPC_GENTBReturnAgent](./sp/SPC_GENTBReturnAgent.sql)
8. Run Command [spc_OFIN_CustomerReceiptAndAdjust](./sp/spc_OFIN_CustomerReceiptAndAdjust.sql)

##### GenerateTextFiles (ZN.DAT & ZN.VAL)
1. `Select * From TBOFINCustomerReceiptAndAdjust_Temp`
2. Check Data Rule
 * query check sum(debit) != sum(credit) from TBOFINCustomerReceiptAndAdjust_Temp
 * find Data[CPCID] is null
    * return SoIvHead.InvDate in lastday
 * ถ้าผิดเงื่อนไขจะไม่เขียนไฟล์
3. เขียนไฟล์ .DAT & .VAL
4. Move .Dat .Val ไปที่ folder BACKUP_OFIN

##### Insert Data To TBOFINReceiptAndAdjustFMT_Temp(ZL)
1. Generate Mapping Template
 * func TranferDataToOFINMappingZN
2. Create Data ZL
 * `TRUNCATE TABLE TBOFINReceiptAndAdjustFMT_Temp`
 * Insert Data from .DAT
---
## ZO
#### GetSPC to DataTable(ZO)
1. Run Command [spc_OFIN_GoodsReceiveAndRTV](./sp/spc_OFIN_GoodsReceiveAndRTV.sql)

##### GenerateTextFiles (ZO.DAT & ZO.VAL)
1. `Select * From TBOFINGoodReceiveAndRTVUFMT_Temp`
2. Check Data Rule
 * query check sum(debit) != sum(credit) from TBOFINGoodReceiveAndRTVUFMT_Temp
 * find Data[CPCID] is null
    * return SoIvHead.InvDate in lastday
 * ถ้าผิดเงื่อนไขจะไม่เขียนไฟล์
3. เขียนไฟล์ .DAT & .VAL
4. Move .Dat .Val ไปที่ folder BACKUP_OFIN

##### Insert Data To TBOFINGoodReceiveAndRTVFMT_Temp(ZO)
1. Generate Mapping Template
 * func TBOFINGoodReceiveAndRTVFMT_Temp
2. Create Data ZL
 * `TRUNCATE TABLE TBOFINGoodReceiveAndRTVFMT_Temp`
 * Insert Data from .DAT

##### ZO Return
1. Run Command `spc_OFIN_GoodsReceiveAndRTV_DocNO`

---
## RTV MERLine
#### GetSPC to DataTable(MERLine)
1. Run Command [spc_OFIN_APTransLine](./sp/spc_OFIN_APTransLine.sql)

##### GenerateTextFiles (.MER & .LOG)
1. `Select * From TBOFINAPTransUFMT_Temp`
2. Check Data Rule
 * find Data[Vendor_num] is 000000 || 00000
    * return TBOFINAPTransUFMT_Temp.Vendor_num = '000000'
 * ถ้าผิดเงื่อนไขจะไม่เขียนไฟล์
3. เขียนไฟล์ .MER & .LOG
4. Move .MER .LOG ไปที่ folder BACKUP_OFIN

##### Insert Data To TBOFINAPTransFMT_Temp(MER)
1. Generate Mapping Template
 * func TranferDataToOFINMappingMERHeader
2. Create Data MERHeader
 * `TRUNCATE TABLE TBOFINAPTransFMT_Temp`
 * Insert Data from .MER

---
## RTV MERHeader
#### GetSPC to DataTable(MERHeader)
1. Run Command [spc_OFIN_APTranHead](./sp/spc_OFIN_APTranHead.sql)

##### GenerateTextFiles (.MER & .LOG)
1. `Select * From TBOFINAPTransLineUFMT_Temp`
2. Check Data Rule
 * find Data[Vendor_num] is 000000 || 00000
    * return TBOFINAPTransUFMT_Temp.Vendor_num = '000000'
 * ถ้าผิดเงื่อนไขจะไม่เขียนไฟล์
3. เขียนไฟล์ .MER & .LOG
4. Move .MER .LOG ไปที่ folder BACKUP_OFIN

##### Insert Data To TBOFINAPTransLineFMT_Temp(MER)
1. Generate Mapping Template
 * func TBOFINAPTransLineFMT_Temp
2. Create Data MERHeader
 * `TRUNCATE TABLE TBOFINAPTransLineFMT_Temp`
 * Insert Data from .MER

---
## Vendor
#### GetSPC to DataTable(Vendor)
1. Run Command [spc_OFIN_Vendor](./sp/spc_OFIN_Vendor.sql)

##### GenerateTextFiles (ZO.DAT & ZO.VAL)
1. `Select * From TBOFINVendor_Temp`
2. เขียนไฟล์ .DAT & .VAL
3. Move .Dat .Val ไปที่ folder BACKUP_OFIN

---
