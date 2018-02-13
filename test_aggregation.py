import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import udf

spark = SparkSession \
    .builder \
    .appName("Test_Aggregation") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

# df = spark.sql(" \
#     SELECT 20180201 AS LoadDate, 1 AS KeyColInt, 'A' AS KeyColChar, 100 AS Value, 20180101 AS LastUpdatedDate UNION ALL\
#     SELECT 20180201 AS LoadDate, 1 AS KeyColInt, 'B' AS KeyColChar, 200 AS Value, 20180102 AS LastUpdatedDate UNION ALL\
#     SELECT 20180201 AS LoadDate, 2 AS KeyColInt, 'A' AS KeyColChar, 300 AS Value, 20180110 AS LastUpdatedDate UNION ALL\
#     SELECT 20180202 AS LoadDate, 1 AS KeyColInt, 'A' AS KeyColChar, 150 AS Value, 20180201 AS LastUpdatedDate UNION ALL\
#     SELECT 20180202 AS LoadDate, 2 AS KeyColInt, 'B' AS KeyColChar, 600 AS Value, 20180201 AS LastUpdatedDate UNION ALL\
#     SELECT 20180203 AS LoadDate, 1 AS KeyColInt, 'B' AS KeyColChar, 250 AS Value, 20180202 AS LastUpdatedDate \
# ")

# df.write.save("c:\\temp\\hdfs\\output\\test_aggregate\\", format="parquet", mode="overwrite", partitionBy="LoadDate")

df = spark.read.load("c:\\temp\\hdfs\\output\\test_aggregate\\", format="parquet")

mapped = df.rdd.map(lambda row: (",".join((str(row.KeyColInt), str(row.KeyColChar))), row.LastUpdatedDate))

groupped = mapped.combineByKey(lambda x: x, lambda x, y: x if x >= y else y, lambda x, y: x if x >= y else y).collect()

groupped.collect()

pass
