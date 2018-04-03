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

spark = SparkSession \
    .builder \
    .appName("Test_Aggregation") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

#   Input parameters
# main_config_file = "/user/RashiR/Metadata/Metadata_sqoop.txt"
# columns_config_file = "/user/RashiR/Metadata/Table_schema_metadata.csv"
# main_config_file = "c:\\temp\\hdfs\\config\\Metadata_sqoop.txt"
# columns_config_file = "c:\\temp\\hdfs\\config\\Table_schema_metadata.csv"
# main_config_file = "/user/RashiR/Metadata/Metadata_sqoop.txt"
#-----------------------------------------------------------------------------
def isNull(value, replacement):
    return (replacement if not value else value)

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

    main_config_file_filter = None

    workflowStartTime = datetime.datetime.now()
    if len(sys.argv) > 1:
        main_config_file = sys.argv[1]
    if len(sys.argv) > 2:
        main_config_file_filter = sys.argv[2]

    spark.udf.register('udfConvertInt', convertInt, IntegerType())
    spark.udf.register('udfConvertDouble', convertDouble, DoubleType())
    spark.udf.register('udfConvertDatetime', convertDatetime, TimestampType())

    mainConfig = spark.read.load(main_config_file, format="csv", delimiter="|", header=True)
    
    #Opretaion|LoadType|threads|Server|Database|Table|WhereClause|DeltaColumn|UniqueIdentifiers|PartitionColumn|TargetLocationRaw|TargetLocationCooked|TargetLocationTableSchema|HiveDatabase|HiveTable|Comments

    if (main_config_file_filter is not None):
        mainConfig = mainConfig.filter(main_config_file_filter)
    
    for row in mainConfig.collect():
        #try:
        tableName = row.Table
        keyColumns = [] # ['_c0', '_c2']
        lastUpdatedColumn = None # '_c4'
        partitionColumn = row.PartitionColumn # 'partition_column'
        # For "snapshot" load type we can use SnapshotPartition to parse column name from
        if ((row.LoadType == "snapshot") and (partitionColumn is None)):
            partitionColumn = row.SnapshotPartition.split("=")[0]
        pathToRaw = row.TargetLocationRaw # "/user/RashiR/data/ccep_constituents_hdfs/Raw"
        pathToCooked = row.TargetLocationCooked # "/user/RashiR/data/ccep_constituents_hdfs/Cooked"
        # if (row.TargetLocationRaw != None):
        #     pathToCooked = pathToCooked + "/" + row.TargetLocationRaw
        sourceDatabase = row.Database
        pathToColumnsConfig = row.TargetLocationTableSchema
        hiveDatabaseName = row.HiveDatabase
        hiveTableName = row.HiveTable
        sql = "DROP TABLE IF EXISTS %s.%s;\nCREATE EXTERNAL TABLE IF NOT EXISTS %s.%s(\n" % (hiveDatabaseName, hiveTableName, hiveDatabaseName, hiveTableName)
        sqlCols = {}
        outputSchema = []
        selectSqlTokens = []
        colDelimiter = "|"
        hasHeader = False

        # Setting table name for flatfiles
        if (tableName is None):
            tableName = hiveTableName

        # Setting delimiter and header parameters
        if (row.FieldSeparator is None):
            colDelimiter = "|"
        else:
            colDelimiter = row.FieldSeparator
        if (row.Header == "TRUE"):
            hasHeader = True
        else:
            hasHeader = False
        
        # Mapping column names from table schema metadata to column numbers in CSV file
        columnsConfig = spark.read.load(pathToColumnsConfig, format="csv", delimiter="|", header=False)
        for curCol in columnsConfig.filter(" \
            UPPER(_c0) == UPPER('" + isNull(tableName, "") + "') OR \
            UPPER(_c0) == UPPER('" + isNull(sourceDatabase, "") + "_" + isNull(tableName, "") + "') OR \
            UPPER(_c0) == UPPER('" + isNull(hiveTableName, "") + "') \
        ").collect():
            if (hasHeader):
                srcColName = curCol._c1
            else:
                srcColName = '_c' + str(int(curCol._c2) - 1)
            
            if row.UniqueIdentifiers and row.DeltaColumn:
                if curCol._c1.lower() in [x.strip() for x in row.UniqueIdentifiers.lower().split(',')]:
                    keyColumns.append(srcColName)
                if curCol._c1.lower() in row.DeltaColumn.lower().split(','):
                    lastUpdatedColumn = srcColName
            
            # !!!TEMP!!! Need to create proper mapping of source column types to destination ones
            bAddColumn = False
            if ("OutputColumns" in row.__fields__):
                if (row.OutputColumns == None):
                    bAddColumn = True
                else:
                    if (curCol._c1.lower() in row.OutputColumns.lower().split(',')):
                        bAddColumn = True
            else:
                bAddColumn = True
            
            if (bAddColumn):
                if (curCol._c4 in ("datetime", "datetime2")):
                    dataframeDatatype = TimestampType()
                    hiveDatatype = "DATE"
                    dateFormat = "yyyy-MM-dd hh:mm:ss"
                    # Temporary workaround for a specific column in APX_HOLDINGS
                    if (hiveTableName == "APX_HOLDINGS"):
                        dateFormat = "MM-dd-yyyy"
                    selectSqlTokens.append(["TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(`",str(srcColName),"`, '",dateFormat,"'))) AS `", str(curCol._c1),"`"])
                elif (curCol._c4 == "int"):
                    dataframeDatatype = IntegerType()
                    hiveDatatype = "INT"
                    selectSqlTokens.append(["udfConvertInt(`",str(srcColName),"`) AS `",str(curCol._c1),"`"])
                elif (curCol._c4 == "double"):
                    dataframeDatatype = DoubleType()
                    hiveDatatype = "DOUBLE"
                    selectSqlTokens.append(["udfConvertDouble(`",str(srcColName),"`) AS `",str(curCol._c1),"`"])
                else:
                    dataframeDatatype = StringType()
                    hiveDatatype = "STRING"
                    selectSqlTokens.append(["`",str(srcColName),"` AS `",str(curCol._c1),"`"])
                
                outputSchema.append(StructField(curCol._c1, dataframeDatatype, curCol._c3))
                sqlCols[curCol._c1.lower()] = ("\t`%s` %s" % (str(curCol._c1), hiveDatatype))
        
        if (len(sqlCols) > 0):
            print('Started processing %s at: %s' % (pathToRaw, datetime.datetime.now()))
            
            # df = spark.read.load("C:\\Temp\\hdfs\\Raw", format="csv", delimiter="|")
            
            # In order to allow processing snapshots we compare source and destination folders
            # print("Parsing folders")
            foldersToProcess = {}
            destinationFolders = []
            if (row.LoadType == "snapshot"):
                for curFolder in Popen(["hdfs", "dfs", "-ls", pathToCooked], stdout=PIPE).stdout.read().split("\n"):
                    buf = curFolder.split()
                    if (len(buf) > 0):
                        if (buf[0][0] == "d"):
                            destinationFolders.append(buf[-1].split("/")[-1])
                for curFolder in Popen(["hdfs", "dfs", "-ls", pathToRaw], stdout=PIPE).stdout.read().split("\n"):
                    buf = curFolder.split()
                    if (len(buf) > 0):
                        if (buf[0][0] == "d"):
                            if ((buf[-1].split("/")[-1] not in destinationFolders) or (buf[-1].split("/")[-1] == row.SnapshotPartition)):
                                foldersToProcess[buf[-1]] = pathToCooked + "/" + buf[-1].split("/")[-1]
            else:
                foldersToProcess[pathToRaw] = pathToCooked
            
            # Processing every folder or file in the bottom level of the RAW folder
            for curPath in foldersToProcess:
                print("Processing folder: %s" % curPath)
                df = spark.read.load(curPath, format="csv", delimiter=colDelimiter, header=hasHeader)
                
                # If we have UniqueKey columns and LastUpdated - we're doing deduplication
                if (len(keyColumns) > 0 and lastUpdatedColumn):
                    
                    # Creating mapping group with key = all PK columns concatenated ('pk_column1, ..., pk_columnN'), value = last_updated_date_column
                    #mappedGroup = df.rdd.map(lambda row: (",".join((str(row._c0), str(row._c2))), row._c4))
                    mappedGroup = df.rdd.map(lambda row: (u",".join((u"" if row[kc] is None else unicode(row[kc])) for kc in keyColumns), unicode(row[lastUpdatedColumn])))
                    
                    # Creating mapping group with key = all PK columns plus last_updated ('pk_column1, ..., pk_columnN, last_updated_date_column'), value = whole_row
                    #mappedAll = df.rdd.map(lambda row: (",".join((str(row._c0), str(row._c2), str(row._c4))) , [row]))
                    mappedAll = df.rdd.map(lambda row: (u",".join((u"" if row[kc] is None else unicode(row[kc])) for kc in keyColumns) + u"," + unicode(row[lastUpdatedColumn]) , [row]))
                    
                    # Extracting maximum last_updated_column_value per PK columns combination
                    grouppedFilter = mappedGroup.combineByKey(lambda x: x, lambda x, y: x if x >= y else y, lambda x, y: x if x >= y else y)
                    # Converting ("pk_column1, ..., pk_columnN", last_updated_date_value) => ("pk_column1, ..., pk_columnN, last_updated_date_value", None)
                    grouppedFilterCombined = grouppedFilter.map(lambda row: (u",".join((u"" if c is None else unicode(c)) for c in row), None))
                    # Converting the recordset to persisted to prevent OutOfMemory errors
                    grouppedFilterCombined.persist(pyspark.StorageLevel(True, True, False, False, 1))
                    
                    # Joining aggregated values with the unaggregated dataset and removing columns belonging to grouppedFilterCombined from the results
                    df = grouppedFilterCombined.join(mappedAll).values().map(lambda row: row[1][0]) #.collect()
                    
                    df = spark.createDataFrame(df)
                
                df.createOrReplaceTempView("resTempDF")
                
                # If the table has headers, mapping those columns to destination by their positions
                i = 0
                for curCol in df.schema:
                    if (i < len(selectSqlTokens)):
                        selectSqlTokens[i] = "".join(curCol.name if (j == 1 and hasHeader) else selectSqlTokens[i][j] for j in range(len(selectSqlTokens[i])))
                        i = i + 1
                    else:
                        break
                
                sqlCmd = "SELECT %s FROM resTempDF" % ",".join(selectSqlTokens)
                resDF = spark.sql(sqlCmd)
                
                if (row.LoadType == "snapshot"):
                    resDF.write.save(foldersToProcess[curPath], format="parquet", mode="overwrite")
                else:
                    resDF.write.save(foldersToProcess[curPath], format="parquet", mode="overwrite", partitionBy=partitionColumn)

                realDF = spark.read.load(pathToCooked, format="parquet")
                
                # reading real schema in saved document cause saving can move columns when partitioning resulting dataset
                outputSQLColumns = []
                partitionBySql = ""
                for realCol in realDF.schema:
                    if (realCol.name == partitionColumn):
                        if (realCol.name.lower() in sqlCols):
                            partitionBySql = "PARTITIONED BY (%s)" % sqlCols[realCol.name.lower()]
                        else:
                            partitionBySql = "PARTITIONED BY (%s STRING)" % partitionColumn
                    else:
                        outputSQLColumns.append(sqlCols[realCol.name.lower()])
                
                # Generating Hive SQL to create a table on top of the saved file
                sql = sql + ",\n".join(outputSQLColumns) + ")\n%s\nSTORED AS PARQUET LOCATION '%s';" % (partitionBySql, pathToCooked)
                
                # Adding manual static partitioning cause dynamic one doesn't seem to work
                if partitionColumn:
                    for curPartition in realDF.select(partitionColumn).distinct().sort(partitionColumn).collect():
                        #ALTER TABLE default.T_PPA_MASTER_PRICE ADD PARTITION(PRICE_EFFECTIVE_DATE='2018-02-12') location '/user/AlexK/data/T_PPA_MASTER_PRICE/Cooked/PRICE_EFFECTIVE_DATE=2018-02-12';
                        curPartition = str(curPartition[0])
                        sql = sql + "\n ALTER TABLE `%s`.`%s` ADD PARTITION(%s='%s') location '%s/%s=%s';" % (hiveDatabaseName, hiveTableName, partitionColumn, curPartition, pathToCooked, partitionColumn, curPartition)
                
                #resDF.coalesce(1).write.save("C:\\Temp\\hdfs\\Cooked", format="csv", delimiter="|", mode="overwrite")
                
                # resDF.write.save("c:\\temp\\hdfs\\output\\test_aggregate_filtered\\", format="parquet", mode="overwrite", partitionBy="LoadDate")
                
                print('Finished processing %s at: %s' % (foldersToProcess[curPath], datetime.datetime.now()))
                #print('SQL to create table (will be replaced when connectivity to Hive metastore is fixed): \n %s' % sql)
                for curSql in sql.split(";"):
                    # print(curSql)
                    if (len(curSql) > 0):
                        spark.sql(curSql)
        else:
            print("No columns found for %s" % (tableName))
        # except:
        #     #print('Errors when processing %s' % (tableName))
        #     print(sys.exc_info())
        # #     print('SQL to create table (will be replaced when connectivity to Hive metastore is fixed): \n %s' % sql)
        
    workflowEndTime = datetime.datetime.now()    
    message = 'Processing of %i file(s) is done. Started at: %s, ended at %s, total time:%s\n\t' % (mainConfig.count(), workflowStartTime, workflowEndTime, workflowEndTime-workflowStartTime)
    print(message)
