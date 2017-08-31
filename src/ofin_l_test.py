import unittest
import GoodsReceivingRTV_MERLine as gr


class GoodsReceivingRTVMERLineTest(unittest.TestCase):
  def test_main_checkrule_merl_found_000000_or_00000(self):
    testdata = [{
        'Vendor_number': '000000'
    }, {
        'Vendor_number': '00000'
    }, {
        'Vendor_number': '700003'
    }]
    self.assertTrue(gr.main_checkrule_merl(testdata))

  def test_main_checkrule_merl_not_found_00000_or_00000(self):
    testdata = [{'Vendor_number': '700003'}]
    self.assertFalse(gr.main_checkrule_merl(testdata))

  def test_gen_seq_number_return_fromdate_plus_1_when_found_in_files_name(
      self):
    fromdate = 'L170721'
    seq = gr.gen_seq_number(['L170721.MER', 'L170620.MER'], fromdate,
                            len(fromdate))
    self.assertEqual(seq, 170722)

  def test_gen_seq_number_return_1_when_not_found_fromdate_in_files_name(self):
    fromdate = 'L170722'
    seq = gr.gen_seq_number(['L170721.MER', 'L170620.MER'], fromdate,
                            len(fromdate))
    self.assertEqual(seq, 1)

  def test_get_invoice_type_dash_or_zero(self):
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('4'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('5'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('6'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('7'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('c'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('C'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('d'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('D'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('e'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('E'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('f'))
    self.assertEqual('-', gr.get_invoice_type_dash_or_zero('F'))

    self.assertEqual('0', gr.get_invoice_type_dash_or_zero('A'))
    self.assertEqual('0', gr.get_invoice_type_dash_or_zero('CC'))
    self.assertEqual('0', gr.get_invoice_type_dash_or_zero('1'))


if __name__ == '__main__':
  unittest.main()
