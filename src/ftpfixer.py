#!/usr/bin/env python3

import ftplib
import os


def ftp(host, user, pwd, src, dest):
  print('[FTP] - host: {}, user: {}, source: {}, destination: {}'.format(host, user, src, dest))
  with ftplib.FTP(host) as ftp:
    try:
      ftp.login(user, pwd)
      files = [f for f in os.listdir(src)]
      for f in files:
        source = '{}/{}'.format(src, f)
        destination = '{}/{}'.format(dest, f)
        with open(source, 'rb') as fp:
          res = ftp.storlines('STOR {}'.format(destination), fp)
          if not res.startswith('226 Transfer complete'):
            print('[FTP] - Upload failed: {}'.format(destination))
    except ftplib.all_errors as e:
      print('[FTP] - error:', e)


if __name__ == '__main__':
  ftp('10.101.59.21', 'magento', 'ssPmagentop0s', '/home/autopos.cds-uat/incoming/bissp/tendor', '/ssporadata/Informatica_Source_File')
  ftp('10.101.59.21', 'magento', 'ssPmagentop0s', '/home/autopos.cds-uat/incoming/bissp/sale', '/ssporadata/Informatica_Source_File/POS_Sales')
  ftp('10.101.59.21', 'magento', 'ssPmagentop0s', '/home/autopos.cds-uat/incoming/bissp/installment', '/ssporadata/Informatica_Source_File/POS_Installment')
  ftp('10.101.59.21', 'magento', 'ssPmagentop0s', '/home/autopos.cds-uat/incoming/bissp/dcpn', '/ssporadata/Informatica_Source_File/POS_DCPN')
  ftp('10.101.59.21', 'magento', 'ssPmagentop0s', '/home/autopos.cds-uat/incoming/bissp/master', '/ssporadata/Informatica_Source_File/POS_Master')
  ftp('10.0.15.154', 'magento', 'Cdsmagentop0s', '/home/autopos.cds-uat/incoming/bicds/payment', '/oradata/Informatica_Source_File/POS_Payment')
  ftp('10.0.15.154', 'magento', 'Cdsmagentop0s', '/home/autopos.cds-uat/incoming/bicds/promotion', '/oradata/Informatica_Source_File/POS_Promotion')
  ftp('10.0.15.154', 'magento', 'Cdsmagentop0s', '/home/autopos.cds-uat/incoming/bicds/discount', '/oradata/Informatica_Source_File/POS_Discount')
  ftp('10.0.15.154', 'magento', 'Cdsmagentop0s', '/home/autopos.cds-uat/incoming/bicds/master', '/oradata/Informatica_Source_File/POS_Master')
  ftp('10.0.173.24', 'cdshopos', 'hopos', '/home/autopos.cds-uat/incoming/ofin/gl', '/p3/fnp/cds/epos/data_in')
  ftp('10.0.173.24', 'cdshoinv', 'hoinv', '/home/autopos.cds-uat/incoming/ofin/ap', '/p3/fnp/cds/invoice/data_in')
  ftp('10.0.173.24', 'cdsarinv', 'arinv', '/home/autopos.cds-uat/incoming/ofin/ar', '/p3/fnp/cds/arinv/data_in')
  ftp('10.0.173.24', 'cdshoven', 'hoven', '/home/autopos.cds-uat/incoming/ofin/vendor', '/p3/fnp/cds/vendor/data_in')
  ftp('10.0.173.24', 'cdshopos', 'hopos', '/home/autopos.cds-uat/incoming/ofin/zy/cds', '/p3/fnp/cds/epos/data_in')
  ftp('10.0.173.24', 'cdshopos', 'hopos', '/home/autopos.cds-uat/incoming/ofin/zy/cbn', '/p3/fnp/cds/epos/data_in')
  ftp('10.0.173.24', 'spshopos', 'hopos', '/home/autopos.cds-uat/incoming/ofin/zy/spb', '/p3/fnp/sps/epos/data_in')
  ftp('10.0.173.24', 'b2shopos', 'hopos', '/home/autopos.cds-uat/incoming/ofin/zy/b2n', '/p3/fnp/b2s/epos/data_in')