from __future__ import print_function
import sys,re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType

"""
 The following script reads Kafka <topic1> unstructured stream from specified broker(s) 
 and writes filtered and modified unstructured stream to <topic2>.
 Filtering: only messages started with '#' go to the output.
 Modification: if a message has '"' symbols, they are backslashed in the output.  
 Parameters: 
 <broker_list> - kafka brokers servers
 <topic1> - topic the script subscribes to
 <topic2> - topic the script acts as a producer
 <checkpoints_dir> - dir, where checkpoint information will be stored (required by the producer) 

 Requires spark 2.2.0 or higher 
 Run:
    `$ <path_to_spark_bin>/spark-submit 
      --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
      kafka-in-out-example.py <server:port> <topic1> <topic2> <checkpoints_dir>
"""

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: kafka-in-out-example.py <broker_list> <topic1> <topic2> <checkpoints_dir>", file=sys.stderr)
        exit(-1)

    brokers, topic1, topic2, path2Checkpoint = sys.argv[1:]

    spark = SparkSession \
        .builder \
        .appName("InAndOut") \
        .getOrCreate()

    udf = UserDefinedFunction(lambda x: re.sub('"','\\"',str(x)), StringType())

    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", topic1) \
        .load()\
        .select(udf(col("value")).alias("value"))\
        .where(col("value").startswith("#"))

    query = lines\
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", topic2) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", path2Checkpoint) \
        .start()

    query.awaitTermination()

