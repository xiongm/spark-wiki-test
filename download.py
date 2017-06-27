from __future__ import print_function

import os
import sys
import urllib
from datetime import timedelta, date

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <year> <from_month> <to_month>", file=sys.stderr)
        exit(-1)

    print("WARN: This is a simple analysis of wiki page view data\n", file=sys.stderr)

    year = sys.argv[1]
    from_month = sys.argv[2]
    to_month = sys.argv[3]

    start_date = date(int(year), int(from_month), 1)
    end_date = date(int(year), int(end_month), 1)
    delta = timedelta(days=1)

    base_url = 'https://dumps.wikimedia.org/other/pagecounts-raw/'
    curr_date = start_date

    dest_path = os.path.join('.', 'datasets')
    while curr_date < end_date:
        print("Downloading ",curr_date.strftime("%Y-%m-%d"))
        url = base_url + year + '/' + year + '-' + curr_date.strftime("%m") + '/'
        for i in range(0,24):
            file_name = 'pagecounts-' + curr_date.strftime("%Y%m%d") + '-%(number)02d' % {"number": i} + '0000.gz'
            f = urllib.urlretrieve(url + file_name, os.path.join(dest_path, file_name))
        curr_date += delta


