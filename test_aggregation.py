
import datetime
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql.functions import udf
import sys

spark = SparkSession \
    .builder \
    .appName("Test_Aggregation") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

#   Input parameters
# main_config_file = "/user/RashiR/Metadata/Metadata_sqoop.txt"
# columns_config_file = "/user/RashiR/Metadata/Table_schema_metadata.csv"
main_config_file = "c:\\temp\\hdfs\\config\\Metadata_sqoop.txt"
columns_config_file = "c:\\temp\\hdfs\\config\\Table_schema_metadata.csv"
#-----------------------------------------------------------------------------
def convertInt(text):
    if text is not None:
        text2 = text.replace('#', '-1')
        if text2.isdigit():
            return int(text2)
    return 0

#-----------------------------------------------------------------------------
def convertDouble(text2):
    if text2 is not None:
        try:
            f = float(text2)
            return f
        except ValueError:
            #print("text %s is not convertable to float" % (text2))
            return 0.0
    return 0.0

#-----------------------------------------------------------------------------
def convertDatetime(text2):
    if text2 is not None:
        try:
            f = to_date(text2)
            return f
        except ValueError:
            #print("text %s is not convertable to float" % (text2))
            return to_date('1900-01-01')
    return to_date('1900-01-01')

#----------------------------------------------------------------------------
## Main functionality
if __name__ == "__main__":

    workflowStartTime = datetime.datetime.now()
    if len(sys.argv) > 2:
        main_config_file = sys.argv[1]
        columns_config_file = sys.argv[2]

    spark.udf.register('udfConvertInt', convertInt, IntegerType())
    spark.udf.register('udfConvertDouble', convertDouble, DoubleType())
    spark.udf.register('udfConvertDatetime', convertDatetime, TimestampType())

    mainConfig = spark.read.load(main_config_file, format="csv", delimiter="|", header=True)
    columnsConfig = spark.read.load(columns_config_file, format="csv", delimiter=",", header=True)
    
    for row in mainConfig.collect():
        tableName = row.Table
        keyColumns = [] # ['_c0', '_c2']
        lastUpdatedColumn = None # '_c4'
        partitionColumn = row.PartitionColumn # 'partition_column'
        pathToRaw = row.TargetLocationRaw # "/user/RashiR/data/ccep_constituents_hdfs/Raw"
        pathToCooked = row.TargetLocationCooked # "/user/RashiR/data/ccep_constituents_hdfs/Cooked"
        sql = "CREATE EXTERNAL TABLE default." + tableName + "(\n"
        sqlCols = []
        outputSchema = []
        selectSqlTokens = []
        
        # Mapping column names from table schema metadata to column numbers in CSV file
        for curCol in columnsConfig.filter(columnsConfig.Table == tableName).collect():
            srcColName = '_c' + str(int(curCol.Position) - 1)

            if row.UniqueIdentifiers and row.DeltaColumn:
                if curCol.Column.lower() in row.UniqueIdentifiers.lower().split(','):
                    keyColumns.append(srcColName)
                if curCol.Column.lower() in row.DeltaColumn.lower().split(','):
                    lastUpdatedColumn = srcColName
            
            # !!!TEMP!!! Need to create proper mapping of source column types to destination ones
            if (curCol.Datatype in ("datetime", "datetime2")):
                dataframeDatatype = TimestampType()
                hiveDatatype = "TIMESTAMP"
                selectSqlTokens.append("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`%s`, 'yyyy-MM-dd hh:mm:ss'))) AS `%s`" % (srcColName, curCol.Column))
            elif (curCol.Datatype == "int"):
                dataframeDatatype = IntegerType()
                hiveDatatype = "INT"
                selectSqlTokens.append("udfConvertInt(`%s`) AS `%s`" % (srcColName, curCol.Column))
            elif (curCol.Datatype == "double"):
                dataframeDatatype = DoubleType()
                hiveDatatype = "DOUBLE"
                selectSqlTokens.append("udfConvertDouble(`%s`) AS `%s`" % (srcColName, curCol.Column))
            else:
                dataframeDatatype = StringType()
                hiveDatatype = "STRING"
                selectSqlTokens.append("`%s` AS `%s`" % (srcColName, curCol.Column))

            outputSchema.append(StructField(curCol.Column, dataframeDatatype, curCol.Nullable))
            sqlCols.append("\t%s %s" % (str(curCol.Column), hiveDatatype))
        
        sql = sql + ",\n".join(sqlCols) + ")\n STORED AS PARQUET LOCATION '%s'" % pathToCooked
        
        print('Started processing %s at: %s' % (pathToRaw, datetime.datetime.now()))
            
        # df = spark.read.load("C:\\Temp\\hdfs\\Raw", format="csv", delimiter="|")
        df = spark.read.load(pathToRaw, format="csv", delimiter="|")

        if (len(keyColumns) > 0 and lastUpdatedColumn):
            
            # Creating mapping group with key = all PK columns concatenated ('pk_column1, ..., pk_columnN'), value = last_updated_date_column
            #mappedGroup = df.rdd.map(lambda row: (",".join((str(row._c0), str(row._c2))), row._c4))
            mappedGroup = df.rdd.map(lambda row: (",".join(str(row[kc]) for kc in keyColumns), row[lastUpdatedColumn]))
            
            # Creating mapping group with key = all PK columns plus last_updated ('pk_column1, ..., pk_columnN, last_updated_date_column'), value = whole_row
            #mappedAll = df.rdd.map(lambda row: (",".join((str(row._c0), str(row._c2), str(row._c4))) , [row]))
            mappedAll = df.rdd.map(lambda row: (",".join(str(row[kc]) for kc in keyColumns) + "," + str(row[lastUpdatedColumn]) , [row]))
            
            # Extracting maximum last_updated_column_value per PK columns combination
            grouppedFilter = mappedGroup.combineByKey(lambda x: x, lambda x, y: x if x >= y else y, lambda x, y: x if x >= y else y)
            # Converting ("pk_column1, ..., pk_columnN", last_updated_date_value) => ("pk_column1, ..., pk_columnN, last_updated_date_value", None)
            grouppedFilterCombined = grouppedFilter.map(lambda row: (",".join(str(c) for c in row), None))
            
            # Joining aggregated values with the unaggregated dataset and removing columns belonging to grouppedFilterCombined from the results
            df = grouppedFilterCombined.join(mappedAll).values().map(lambda row: row[1][0]) #.collect()
        
            df = spark.createDataFrame(df)

        df.createOrReplaceTempView("resTempDF")
        
        sqlCmd = "SELECT %s FROM resTempDF" % ",".join(selectSqlTokens)
        resDF = spark.sql(sqlCmd)
        
        resDF.write.save(pathToCooked, format="parquet", mode="overwrite", partitionBy=partitionColumn)
        #resDF.coalesce(1).write.save("C:\\Temp\\hdfs\\Cooked", format="csv", delimiter="|", mode="overwrite")
        
        # resDF.write.save("c:\\temp\\hdfs\\output\\test_aggregate_filtered\\", format="parquet", mode="overwrite", partitionBy="LoadDate")
        
        print('Finished processing %s at: %s' % (pathToCooked, datetime.datetime.now()))
        print('SQL to create table (will be replaced when connectivity to Hive metastore is fixed): \n %s' % sql)
        
    workflowEndTime = datetime.datetime.now()    
    message = 'Processing of %i file(s) is done. Started at: %s, ended at %s, total time:%s\n\t' % (mainConfig.count(), workflowStartTime, workflowEndTime, workflowEndTime-workflowStartTime)
    print(message)
