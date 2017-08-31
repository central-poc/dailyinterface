import unittest
import GoodsReceivingRTV_MERLine as gr


class GoodsReceivingRTVMERLineIntegrationTest(unittest.TestCase):
  def test_checkrule_goodsreceiving_rtv_mer_line(self):
    dt_merl = gr.checkrule_goodsreceiving_rtv_mer_line()
    self.assertTrue(len(dt_merl) > 0)

    merl = dt_merl[0]
    for k in [
        'Source', 'Invoice_num', 'Invoice_type', 'Vendor_number', 'Line_type',
        'Item_description', 'Amount', 'Item_qty', 'Item_cost'
    ]:
      self.assertTrue(k in merl)


if __name__ == '__main__':
  unittest.main()
