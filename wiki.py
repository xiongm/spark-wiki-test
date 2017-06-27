# experimental spark scripts to analyse wiki page view data
# based on
# https://www.percona.com/blog/2015/10/07/using-apache-spark-mysql-data-analysis/
# analyse wiki history page view data
# to run:
# ~/spark/bin/spark-submit ./wiki.py 2016 8 8

from __future__ import print_function

import re
import sys
import os
from operator import add

from pyspark.sql import SparkSession, Row
from datetime import timedelta, date

import urllib

def load_day(spark, file_name, my_date):
    sc = spark.sparkContext
    # lines = spark.read.text(file_name)
    lines = sc.textFile(file_name)

    parts = lines.map(lambda l : l.split(" ")).filter(lambda l : l[0] == 'en').cache()
    print(parts)
    wiki = parts.map(lambda p : Row(project = p[0], url = urllib.unquote(p[1]).lower(), requests = int(p[2]), size = int(p[3])))
    df_wiki = spark.createDataFrame(wiki)
    df_wiki.createOrReplaceTempView('wikistats')

    df_group = spark.sql("select '" + my_date + "' as my_date, url, sum(requests) as clicks from wikistats group by url ")
    df_group.write.mode('overwrite').parquet('parquet/my_date=' + my_date)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: wiki.py <year> <from_month> <to_month>", file=sys.stderr)
        exit(-1)

    print("WARN: This is a simple analysis of wiki page view data\n", file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("WikiPageViews")\
        .getOrCreate()
    year = int(sys.argv[1])
    from_month = int(sys.argv[2])
    to_month = int(sys.argv[3])

    start_date = date(year, from_month, 1)
    end_date = date(year, to_month, 2)
    delta = timedelta(days=1)

    base_url = 'https://dumps.wikimedia.org/other/pagecounts-raw/'
    curr_date = start_date

    dest_path = os.path.join('.', 'datasets')
    while curr_date < end_date:
        print("Processing ",curr_date.strftime("%Y-%m-%d"))
        file_name = dest_path + '/pagecounts-' + curr_date.strftime("%Y%m%d") + '-*.gz'
        print(file_name)
        load_day(spark, file_name, curr_date.strftime("%Y-%m-%d"))
        curr_date += delta




