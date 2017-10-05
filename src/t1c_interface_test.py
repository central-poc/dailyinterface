import unittest
from unittest.mock import patch
import datetime
import t1c_program


class UnderTest(unittest.TestCase):
  def test_gen_option_string_value(self):
    res = t1c_program.get_option_str(None)
    self.assertEqual('', res)

    res = t1c_program.get_option_str('text')
    self.assertEqual('text', res)

    res = t1c_program.get_option_str(1)
    self.assertEqual('1', res)

  def test_gen_option_number_value(self):
    res = t1c_program.get_option_number(None)
    self.assertEqual(0, res)

    res = t1c_program.get_option_number(1)
    self.assertEqual(1, res)

    res = t1c_program.get_option_number('1')
    self.assertEqual(1, res)

    res = t1c_program.get_option_number('eiei')
    self.assertEqual(0, res)

  def test_split_price(self):
    res = t1c_program.split_price('9.0')
    self.assertEqual('000000000900', res)

    res = t1c_program.split_price('10.00')
    self.assertEqual('000000001000', res)

  def test_get_tracking_number(self):
    res = t1c_program.get_tracking_number('CDS00012345678')
    self.assertEqual('01234567', res)

    res = t1c_program.get_tracking_number('SSP001')
    self.assertEqual('55555555', res)

  def test_gen_sale_tran_data(self):
    data = {
        'InvDate': '2017-12-15',
        'CreateOn': datetime.time(6, 30, 0),
        'Status': 'AT',
        'SaleType': '02',
        'ShopID': '1234',
        'InvNo': 'SSP123123123',
        'PID': '12345678',
        'Quantity': 10,
        'UnitPrice': 10.01,
        'UnitSalesPrice': 9.99,
        'NetAmt': 900.0,
        'VatAmt': 58.88,
        'ItemDiscountAmount': 0.02,
        'TransactionDiscountAmount': 0.01,
        'DEPT': '1',
        'SUB_DPT': '12',
        'T1CNoEarn': '0123456',
        'CreateBy': '9999',
        'ShopGroup': 'BU'
    }
    store_number = '12321'
    expected = """123212017121506300212340012312300000000123456780000000012345678+001000000000001001000000000999N000000090000000000005888    000000000002    000000000001                              20171215000000000010120000000000000000                 0123456              000000000000000000009999"""
    actual = """123212017121506300212340012312300000000123456780000000012345678+001000000000001001000000000999N000000090000000000005888    000000000002    000000000001                              20171215000000000010120000000000000000                 0123456              000000000000000000009999"""
    self.maxDiff = None
    self.assertEqual(expected,
                     t1c_program.gen_sale_tran_data(data, 0, store_number))

  def test_gen_tender(self):
    data = {
        'InvDate': '2017-12-15',
        'CreateOn': datetime.time(6, 30),
        'Status': 'AT',
        'SaleType': '02',
        'ShopID': '1234',
        'InvNo': 'SSP123123123',
        'TenderType': 'CASH',
        'NetAmt': 1000.00,
        'TotalNetAmt': 2000.00,
        'redeemamt': 100.00,
        'TransactionDiscountAmount': 0.01,
        'T1CNoEarn': '0123456',
        'ShopGroup': 'BU'
    }
    store_number = '99999'
    res = t1c_program.gen_tender(data, 0, store_number)
    expected = '9999920171215063002123400123123CASH+000000095000              000000000001                                           0123456              00000000        201707251000'
    self.maxDiff = None
    self.assertEqual(expected, res)


if __name__ == '__main__':
  unittest.main()
