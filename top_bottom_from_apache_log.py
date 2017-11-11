from __future__ import print_function
import apache_log_parser as ap
import sys, os
import json

from pyspark.sql import SparkSession, SQLContext
#from pyspark.sql import SQLContext

"""
The script parses a standard apache log file and returns top/bottom <selected fields>
with the highest/lowest successful/failure request ratio, with at least one failed request.
The output: <selected fields> the ratio, the request count.
"""

modes = ['top', 'bottom']
allowed = ['status',
           'request_first_line',
           'request_url_query',
           'request_url_username',
           'time_received_utc_datetimeobj',
           'request_url_hostname',
           'response_bytes_clf',
           'request_url',
           'request_http_ver',
           'remote_user',
           'request_url_port',
           'time_received_tz_isoformat',
           'time_received_utc_isoformat',
           'remote_logname',
           'request_url_path',
           'time_received_datetimeobj',
           'request_method',
           'remote_host',
           'time_received']

select = []

def selected_fields(l):
    try:
        out=[l['status']]
        for e in select:
            out.append(l[e])
        return out
    except:
        return None                  

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: <$PATH>top_bottom_from_apache_log.py <input_file> <mode> <parameters {key:value}>", file=sys.stderr)
        exit(-1)

    if os.stat(sys.argv[1]).st_size == 0:
        print("file %s is empty" % (sys.argv[1]), file=sys.stderr)
        exit(-1)

    if sys.argv[2] not in modes: 
        print("%s is not in known modes: %s" % (sys.argv[2], modes), file=sys.stderr)
        exit(-1)
    else:
        mode = sys.argv[2]

    limit = 10
    if len(sys.argv) > 3:
        try:
            parameters=json.loads(str(sys.argv[3]))
            if 'limit' in parameters:
               try:
                   limit = int(parameters['limit'])
               except:
                   print("limit should be integer", file=sys.stderr)            
                   exit(-1)
            if 'select' in parameters:
                select = parameters['select']
                for i in select:
                    if i not in allowed:
                        print("unknown field %s. Known:%s" % (i, allowed), file=sys.stderr)
                        exit(-1)
        except:
            print("parameters should be in json format. Example: '{\"limit\":5, \"select\":[\"request_url_path\",\"request_method\"]}'", file=sys.stderr)
            exit(-1)

    if len(select) == 0:
        select.append('request_url')

    spark = SparkSession\
        .builder\
        .appName("TopBottomRank")\
        .getOrCreate()

    sqlCtx = SQLContext(spark)

    line_parser = ap.make_parser("%h %l %u %t \"%r\" %>s %b")
#    some logs start from pid
#    if it's your case replace line_parser with the line below
#    line_parser = ap.make_parser("%v %h %u %t \"%r\" %>s %b")

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    pages = lines.map(lambda l: line_parser(l))\
                 .map(lambda l: selected_fields(l))

    select_as_line = ",".join(select)
    select_ext = ["status"]
    select_ext.extend(select)

    if mode == 'top':
        direction = 'desc'
    else:
        direction = 'asc'

    pages = pages.filter(lambda p: p is not None)
    if pages.count() == 0:
        print("something wrong with the provided file, cannot extract requested info", file=sys.stderr)
        spark.stop()
        exit(-1)

    table = pages.toDF(select_ext)
    table.registerTempTable("pages")

    query = "select "+select_as_line+", r as rank, c as count from \
        (select "+select_as_line+", case when count(*)=sum(s) then -1 else sum(s)/(count(*)-sum(s)) end as r, \
        count(*) as c from (select "+select_as_line+", case when status < 399 then 1 else 0 end \
        as s from pages) group by "+select_as_line+" having r > -1) order by r "+direction+" limit {}"

    result = sqlCtx.sql(query.format(limit))

    result.show()
    spark.stop()

