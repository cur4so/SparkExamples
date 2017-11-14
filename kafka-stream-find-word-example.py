from __future__ import print_function

import sys
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

"""
 The following script reads Kafka <topic> stream from specified broker(s) every 5 seconds. 
 If a message contains a word of interest, it is copied to a file with a timestamp when it
 has been received.
 The output shows how many messages with the word have been received in a given 30 sec window, 
 with 30 sec sliding.
 Usage: kafka-stream-find-word-example.py <broker_list> <topic> <word_of_interest> 
 Run:
    `$ <path_to_spark_bin>/spark-submit 
      --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1
      kafka-stream-find-word-example.py <server:port> <topic> <word_of_interest>
"""

def check_for_word(line, word, file_path):
    if word in line:
        open(file_path, "a").write(str(datetime.now())+" "+line+"\n")
        return 1
    else:
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: kafka-stream-find-word-example.py <broker_list> <topic> <word>", file=sys.stderr)
        exit(-1)

    file_path = "/path/to/output_file/file_name"
    checkpointDirectoryLocation = "/path/to/checkpoint/"

    sc = SparkContext(appName="StreamingKafkaExample")

    ssc = StreamingContext(sc, 5) # read every 5 sec
    ssc.checkpoint(checkpointDirectoryLocation) # required for Window function

    brokers, topic, word = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])

    ct = lines.map(lambda line: check_for_word(line, word, file_path))\
        .reduceByWindow(lambda a, b: a+b, lambda a, b: a-b, 30, 30)
    ct.pprint()

    ssc.start()
    ssc.awaitTermination()

