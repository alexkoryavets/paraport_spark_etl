import datetime
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql.functions import udf
from subprocess import Popen, PIPE
import sys
import traceback

spark = SparkSession \
    .builder \
    .appName("Test_Aggregation") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

if __name__ == "__main__":

    database_name = None
    source_view_name = None
    partitioning_column_name = None
    destination_table_name = None

    workflowStartTime = datetime.datetime.now()
    if len(sys.argv) > 1:
        database_name = sys.argv[1]
    if len(sys.argv) > 2:
        source_view_name = sys.argv[2]
    if len(sys.argv) > 3:
        partitioning_column_name = sys.argv[3]
    else:
        partitioning_column_name = ""
    if len(sys.argv) > 4:
        destination_table_name = sys.argv[4]
    else:
        destination_table_name = "m_" + source_view_name
    
    destination_table_name = translate(destination_table_name, "`", "")

    if ((database_name is None) or (source_view_name is None)):
        print("ERROR: either database or source view name is not provided.\nUsage: materialize_view.py <database_name> <source_view_name> [<partitioning_column>] [<destination_view_name>]")
        exit
    
    df = spark.sql("select * from `%s`.`%s`" % (database_name, source_view_name))
    sql = "drop table if exists `%s`.`%s`;\n\n" % (database_name, destination_table_name)
    sql = sql + "CREATE TABLE `%s`.`%s` (\n" % (database_name, destination_table_name)
    firstRow = True

    for curCol in df.schema:
        if (df.dataType == "DateType"):
            dataType = "DATE"
        elif (df.dataType == "IntegerType"):
            dataType = "INT"
        elif (df.dataType == "DoubleType"):
            dataType = "DOUBLE"
        else:
            dataType = "STRING"
        
        if (curCol.name != partitioning_column_name):
            sql = sql + "" if firstRow else "," + "\t" + curCol.name + " " + dataType
            firstRow = False
        else:
            partitioning_column_name_and_type = curCol.name + " " + dataType

    sql = sql + ")\n"

    if (partitioning_column_name != ""):
        sql = sql + "PARTITIONED BY (" + partitioning_column_name_and_type + ");"
    else
        sql = sql + ";"
    
    sql = sql + "insert into `%s`.`%s`" % (database_name, destination_table_name)
    
    if (partitioning_column_name != ""):
        sql = sql + " partition (%s)" % partitioning_column_name
    
    
    