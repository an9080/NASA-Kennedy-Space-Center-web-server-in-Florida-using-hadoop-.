import os
import sys
import re

os.environ["SPARK_HOME"] = "/usr/spark2.4.3"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession


PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)'

#Example line
line = 'slppp6.intermind.net - - [01/Aug/1995:00:00:39 -0400] "GET /history/skylab/skylab-logo.gif HTTP/1.0" 200 3274'
#line = 'slppp6.intermind.net - - [01/Aug/1995:00:00:39 -0400] "GET /history/skylab/skylab-logo.gif HTTP/1.0" XXX 3274'

def parseLogLine(log):
    m = re.match(PATTERN, log)
    if m:
        return [Row(host=m.group(1), timeStamp=m.group(4),url=m.group(6), httpCode=int(m.group(8)))]
    else:
        return []

#Test if it is working
parseLogLine(line)


spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
sc = spark.sparkContext

logFile = sc.textFile("/user/razanalbaqami3351/access_log_Jul95.txt")

accessLog = logFile.flatMap(parseLogLine)
accessDf = spark.createDataFrame(accessLog)
accessDf.printSchema()
accessDf.createOrReplaceTempView("nasalog")
output = spark.sql("select * from nasalog")
output.createOrReplaceTempView("nasa_log")

# Uncomment this for optimizations only
#spark.sql("cache TABLE nasa_log")

spark.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show()

spark.sql("select host,count(*) as req_cnt from nasa_log group by host order by req_cnt desc LIMIT 5").show()

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show()

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show()

spark.sql("select httpCode,count(*) as req_cnt from nasa_log group by httpCode").show()