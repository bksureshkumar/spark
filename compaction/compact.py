from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

input_path = '/Users/sbalakrishnan/sothanai/python/compaction/sample_20.json'


# used for: compact by the date format
def convert_receive_time(receive_time):
    date = str(receive_time)
    date = date.replace('-', '/').replace(' ', '').replace(':', '/')
    return date


# used for: compact by day
def strip_time(receive_time):
    date = str(receive_time)
    date = date.split(' ')[0]
    return date


convert_date_udf = udf(lambda date_time: convert_receive_time(date_time), StringType())
compaction_by_day_udf = udf(lambda date_time: strip_time(date_time), StringType())


spark = SparkSession.builder \
.appName('Compaction job') \
.getOrCreate()


dataframe = spark.read.json(input_path)
dataframe.show(5)

# Convert the date time to '/' format
dataframeConverted = dataframe.withColumn("converted_receive_time", convert_date_udf(dataframe.receive_time))
dataframeConverted = dataframeConverted.repartition(1)
dataframeConverted.show(5)

dataframeConverted.write.partitionBy("converted_receive_time").parquet("/Users/sbalakrishnan/sothanai/python/compaction/out_parquet")
dataframeConverted.write.partitionBy("converted_receive_time").orc("/Users/sbalakrishnan/sothanai/python/compaction/out_orc")
dataframeConverted.write.partitionBy("converted_receive_time").csv("/Users/sbalakrishnan/sothanai/python/compaction/out_csv")

#Compaction by day
dataframeCompByDay = dataframe.withColumn("comp_receive_date", compaction_by_day_udf(dataframe.receive_time))
dataframeCompByDay = dataframeCompByDay.repartition(1)
dataframeCompByDay.show(5)

dataframeCompByDay.write.partitionBy("comp_receive_date").parquet("/Users/sbalakrishnan/sothanai/python/compaction/out_parquet_day")
dataframeCompByDay.write.partitionBy("comp_receive_date").orc("/Users/sbalakrishnan/sothanai/python/compaction/out_orc_day")
dataframeCompByDay.write.partitionBy("comp_receive_date").csv("/Users/sbalakrishnan/sothanai/python/compaction/out_csv_day")

