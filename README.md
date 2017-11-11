*top_bottom_from_apache_log.py* - the script parses a standard apache log file and returns top/bottom <selected fields> with the highest/lowest successful/failure request ratio, with at least one failed request.
The output: <selected fields>, the ratio, the request count.

To run:

spark-submit top_bottom_from_apache_log.py <your_access.log> top|bottom ['{"limit":<int>, "select":["<valid_field1>","<valid_field2>"]}']

