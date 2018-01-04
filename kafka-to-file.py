from __future__ import print_function
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, IntegerType
from pyspark.sql.functions import col, get_json_object

"""
 The following script reads Kafka <topic> json structured stream from specified broker(s) 
 and writes selected columns and rows into files 
 Selection: the data should obey the schema: have ("field1", "field2" and "field3"),
 only messages with "field2" content started with '#' go to the output.
 Parameters: 
 <broker_list> - kafka brokers servers
 <topic> - topic the script subscribes to
 <files_path> - directory where output will be stored
 <checkpoints_dir> - dir, where checkpoint information will be stored (required by the producer) 

 Requires spark 2.2.0 or higher 
 Run:
    `$ <path_to_spark_bin>/spark-submit 
      --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
      kafka-to-file.py <server:port> <topic> <saved_files_dir> <checkpoints_dir>`
"""

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: kafka-to-file.py <broker_list> <topic> <output_dir> <checkpoints_dir>", file=sys.stderr)
        exit(-1)

    brokers, topic, save2file, path2Checkpoint = sys.argv[1:]

    spark = SparkSession \
        .builder \
        .appName("Kafka2Files") \
        .getOrCreate()

    schema = StructType()\
        .add("field1", IntegerType())\
        .add("field2", StringType())\
        .add("field3", StringType())

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic) \
        .load()\
        .select(get_json_object(col("value").cast("string"),"$.field1").alias('user_id'),\
                get_json_object(col("value").cast("string"),"$.field2").alias('type'))\
        .where(col("type").startswith("#"))

    query = lines \
     .writeStream \
     .format("csv") \
     .option("format", "append") \
     .option("path", save2file) \
     .option("checkpointLocation", path2Checkpoint) \
     .outputMode("append") \
     .start()

    query.awaitTermination()

