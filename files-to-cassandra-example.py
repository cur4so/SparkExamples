from __future__ import print_function
import sys
from pyspark.sql import SparkSession

"""
 The following script reads specified dir 
 and writes files' content to cassandra db
 Parameters: 
 <cassandra-keyspace> <table> - destination where to write obtained information, the keyspace and the table name are required 
 <content_dir> - dir, where information to be transferred is stored
 <header> - columns where to data should be inserted, format "col1,col2"

 Run:
    `$ <path_to_spark_bin>/spark-submit 
      --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.6
      files-to-cassandra-example.py <cassandra-keyspace> <table> <data_dir> [<header>]`
"""

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: files-to-cassandra-example.py <cassandra-keyspace> <table> <data_dir> [<header>]", file=sys.stderr)
        exit(-1)

    header = None
    if len(sys.argv) == 4:
        keyspace, table, data_dir = sys.argv[1:]
    elif len(sys.argv) == 5:
        keyspace, table, data_dir, header = sys.argv[1:]
    else:
        print("Unknown args. Usage: files-to-cassandra-example.py <cassandra-keyspace> <table> <data_dir> [<header>]", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Files2Cassandra") \
        .getOrCreate()

    if header is not None:
        header_list = header.split(',')
        files = spark.read.option("header","false").csv(data_dir+"/part-*.csv").toDF(*header_list)
    else:
        files = spark.read.option("header","true").option("inferSchema","true").csv(data_dir+"/part-*.csv")

    files\
        .write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table, keyspace=keyspace)\
        .save()


