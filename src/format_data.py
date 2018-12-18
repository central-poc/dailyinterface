#!/usr/bin/env python3

import csv
import argparse
import datetime

text_format = {
    'ofin': {
        'Segment 4 - Store':'1-6',
        'Segment 5 - CPC':'7-11',
        'Segment 6 - Account':'12-19',
        'Segment 7 - SubAccount':'20-25',
        'Accounting_Date':'26-34',
        'Debit':'35-46',
        'Credit':'47-58',
        'Short name':'59-62',
        'Journal Category Name * FAST':'63-87',
        'Journal Source Name *FAST':'88-112',
        'Reference1 (Batch Name)':'113-138',
        'Seq':'139-139',
        'CFS Flag *FAST':'140-149',
        'Description':'150-390',
    }
}


def split_text(text, spec):
    spec = spec.split('-')
    return eval('text[%s:%s]' % (spec[0], int(spec[1])+1))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source')
    parser.add_argument('-f', '--format')
    args = parser.parse_args()

    input_params = {
        'source': args.source,
        'format': args.format,
    }

    output_file_name = '%s - %s.csv' % (input_params['format'], datetime.datetime.now().strftime("%Y-%m-%d"))

    with open(output_file_name, 'w') as output_file:
        fieldnames = text_format[input_params['format']].keys()
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        file = open(input_params['source'], 'r')
        for row in file:
            row = ' ' + row
            row_data = {k:split_text(row, v) for (k,v) in text_format[input_params['format']].items()}
            writer.writerow(row_data)


