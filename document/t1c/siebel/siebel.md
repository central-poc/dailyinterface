### T1C - CDS - 29.08.2017

- Product Master -> T1C เพราะฝั่ง T1C ไม่มี PID ไป map
- ต้องเช็คเรื่องยอด sale ว่าใช้ที่ text หรือ POS
- text file layout จะต้องเปลี่ยนด้วย

### T1C - New Format File - 30.08.2017

- รวมเป็น file เดียว
- ส่ง paymenttype
- check จังหวะการคืนมัดจำ
- Product Master ส่ง active มาทุกเดือน, incremental ทุกวัน
	- nameTH ถ้าไม่มีใส่ nameEN เข้าไปแทน
	- Product Line : คล้ายๆ cate, hierachy
	- primary desc = product.descriptionTH
	- second desc = product.descriptionEN
	- PointExclusionFlag : สินค้าที่ห้ามให้แต้ม

### Timeline

- SIT : 02.10.2017