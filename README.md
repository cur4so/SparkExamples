*top_bottom_from_apache_log.py* - the script parses a standard apache log file and returns top/bottom <selected fields> with the highest/lowest successful/failure request ratio, with at least one failed request.
The output: <selected fields>, the ratio, the request count.

To run:

spark-submit top_bottom_from_apache_log.py <your_access.log> top|bottom ['{"limit":<int>, "select":["<valid_field1>","<valid_field2>"]}']

*kafka-stream-find-word-example.py* - this script reads Kafka <topic> stream from specified broker(s) every 5 seconds. If a message contains a word of interest, it is copied to a file with a timestamp when it has been received.
The output shows how many messages with the word have been received in a given 30 sec window, with 30 sec sliding, plus the file with massages, containing the word.

To run:

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 kafka-stream-find-word-example.py <server:port> <topic> <word_of_interest>

*kafka-in-out-example.py* - this script reads Kafka <topic1> unstructured stream from specified broker(s) and writes filtered and modified unstructured stream to <topic2>.
 Filtering: only messages started with '#' go to the output.
 Modification: if a message has '"' symbols, they are backslashed in the output.

To run:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 kafka-in-out-example.py <server:port> <topic1> <topic2> <checkpoints_dir>  

