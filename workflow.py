## Imports
import pyspark
import json
import yaml
import time
import subprocess
import unicodedata
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
import datetime
import sys
import multiprocessing
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import udf
# custom libraries
#from loadWorkflow import loadWorkflow

## CONSTANTS

APP_NAME = "Spark_Metadata_ETL"
WORKFLOW_FOLDER = "\\temp\\hdfs\\config\\test_merge\\"
#WORKFLOW_FOLDER = "hdfs://thegreatlakes/spark/metadata_etl/config/test_merge/"
FOLDER_NAME_DICT={}
DATA_PIPELINE_NAME = ''
WORKFLOW_ROOT_FOLDER_NAME = ''
global CURRENT_ACTION
global ERROR_MESSAGE
# SNS_ARN_PREFIX="arn:aws:sns:us-west-2:088921935615:Vortex-"
NO_MATCH = "noMatch"

##OTHER FUNCTIONS/CLASSES

#-----------------------------------------------------------------------------
def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
    return (output, errors)

#-----------------------------------------------------------------------------
def execSQL(sqlCmd):
    msg = "Executing SQL: " + sqlCmd
    LOGGER.info(msg)
    print(msg)
    return spark.sql(sqlCmd)

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
def getRootFolderFromWorkflowPath(workflowPath):
    tokens =  workflowPath.split("/")
    if len(tokens) <= 3:
        LOGGER.error("Path %s is not valid " % workflowPath)
        return None, None
    else:
        rootFolderName = tokens[2]
        prefix =  "/".join(tokens[3:])
        return rootFolderName, prefix

#-----------------------------------------------------------------------------
def changeRootFolderName(folder, rootFolderName, FOLDER_NAME_DICT):
    tokens =  folder.replace("\\\\", '\\').replace('\\', '/').split("/")
    # if tokens[0] != 's3:' and len(tokens) <= 3:
    #     LOGGER.error("Path %s is not a valid s3 path" % s3Key)
    #     return None
    if FOLDER_NAME_DICT.has_key(tokens[2]):
        tokens[2] = FOLDER_NAME_DICT[tokens[2]]
    else:
        #change the bucket name to the default bucket name
        tokens[2] = rootFolderName
    res =  "/".join(tokens)
    return res

#-----------------------------------------------------------------------------
def loadWorkflow(folder, completeWorkFlow, wfList):
    #find the bucket location from folder
    rootFolderName, prefix = getRootFolderFromWorkflowPath(folder)
    workflowFileName = folder + "masterConfig.csv"
    calculationFileName = folder + "calculations.csv"
    joinTableFileName = folder + "joinTableConfig.csv"
    loadFileName = folder + "loadColumns.csv"
    lookupFilterFileName = folder + "lookupFilter.csv"
    filterDataFileName = folder + "filterData.csv"
    selectTransformFileName = folder + "selectTransform.csv"    
    workflowSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("stepName", StringType(), False),
        StructField("type", StringType(), False),
        StructField("srcData", StringType(), True),
        StructField("metaData", StringType(), True),
        StructField("tgtData", StringType(), True),
        StructField("tgtPath", StringType(), True),
        StructField("outputFormat", StringType(), True),
        StructField("disabled", StringType(), True)])
    loadColSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("columnNo", IntegerType(), False),
        StructField("columnName", StringType(), False),
        StructField("dataType", StringType(), False)])
    calculationSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("calcNo", IntegerType(), False),
        StructField("columnName", StringType(), False),
        StructField("srcColumn", StringType(), False),
        StructField("plusMinus", StringType(), False)])
    #stepNo	srcData	joinColumn	addColumn        
    joinTableSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("srcData", StringType(), False),
        StructField("baseColumn", StringType(), False),        
        StructField("joinColumn", StringType(), False),
        StructField("addColumn", StringType(), False),
        StructField("addColumnAlias", StringType(), False),
        StructField("defaultValue", StringType(), False)]) 
    #### Lookup Filter schema
    lookupFilterSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("trgtMetric", StringType(), False),
        StructField("srcColName", StringType(), True),        
        StructField("dimFilter", StringType(), True),
        StructField("includeExclude", StringType(), True),
        StructField("matchingValue", StringType(), True)])       
    #### Filter Data schema
    filterDataSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("dimFilter", StringType(), True),
        StructField("includeExclude", StringType(), True),
        StructField("matchingValue", StringType(), True)])       
    #### selecct transform schema
    selectTransformSchema = StructType([
        StructField("stepNo", IntegerType(), False),
        StructField("columnNo", IntegerType(), False),
        StructField("tgtColumn", StringType(), False),    
        StructField("exp", StringType(), False),
        StructField("aggregation", StringType(), True)])
    spark.udf.register('udfCleanup', lambda x: x.strip() if not (x is None) else None)
    
    workflowDf = spark.read.csv(workflowFileName, header=True, schema = workflowSchema)
    workflowDf.createOrReplaceTempView('master')
    completeWorkFlow['master'] = spark.sql("SELECT stepNo, udfCleanup(stepName) as stepName, udfCleanup(type) AS type, udfCleanup(srcData) AS srcData, udfCleanup(metaData) AS metaData, udfCleanup(tgtData) AS tgtData, udfCleanup(tgtPath) AS tgtPath, udfCleanup(outputFormat) AS outputFormat FROM master WHERE disabled is NULL or udfCleanup(disabled)='' ORDER by stepNo")    
    #### loadConfig
    loadDf = spark.read.csv(loadFileName, header=True, schema = loadColSchema)
    loadDf.createOrReplaceTempView('loadCols')   
    completeWorkFlow['load'] = spark.sql("SELECT stepNo, columnNo, udfCleanup(columnName) AS columnName, udfCleanup(dataType) as dataType FROM loadCols")
    completeWorkFlow['load'].cache()
    completeWorkFlow['load'].createOrReplaceTempView('loadCols')
    #### calculation Config
    calcDf = spark.read.csv(calculationFileName, header=True, schema = calculationSchema)
    calcDf.createOrReplaceTempView('calcCols')
    completeWorkFlow['calculation'] = spark.sql("SELECT stepNo, calcNo, udfCleanup(columnName) AS columnName, udfCleanup(srcColumn) AS srcColumn, udfCleanup(plusMinus) AS plusMinus FROM calcCols")
    completeWorkFlow['calculation'].cache()
    completeWorkFlow['calculation'].createOrReplaceTempView('calcCols')
    #### joinTable Config
    joinTblDf = spark.read.csv(joinTableFileName, header=True, schema = joinTableSchema)
    joinTblDf.createOrReplaceTempView('joinTable')
    completeWorkFlow['joinTable'] = spark.sql("SELECT stepNo, udfCleanup(srcData) as srcData, udfCleanup(baseColumn) AS baseColumn, udfCleanup(joinColumn) AS joinColumn, udfCleanup(addColumn) AS addColumn, udfCleanup(addColumnAlias) AS addColumnAlias, udfCleanup(defaultValue) AS defaultValue FROM joinTable")
    completeWorkFlow['joinTable'].cache()
    completeWorkFlow['joinTable'].createOrReplaceTempView('joinTable')
    #### lookupFilter Config
    lookupfilterDf = spark.read.csv(lookupFilterFileName, header=True, schema = lookupFilterSchema)
    lookupfilterDf.createOrReplaceTempView('lookupFilter')
    completeWorkFlow['lookupFilter'] = spark.sql("SELECT stepNo, udfCleanup(trgtMetric) as trgtMetric, udfCleanup(srcColName) AS srcColName, udfCleanup(dimFilter) AS dimFilter, udfCleanup(includeExclude) AS includeExclude, udfCleanup(matchingValue) AS matchingValue FROM lookupFilter")
    completeWorkFlow['lookupFilter'].cache()
    completeWorkFlow['lookupFilter'].createOrReplaceTempView('lookupFilter') 
    #### filterData Config
    filterDataDf = spark.read.csv(filterDataFileName, header=True, schema = filterDataSchema)
    filterDataDf.createOrReplaceTempView('filterData')
    completeWorkFlow['filterData'] = spark.sql("SELECT stepNo, udfCleanup(dimFilter) AS dimFilter, udfCleanup(includeExclude) AS includeExclude, udfCleanup(matchingValue) AS matchingValue FROM filterData")
    completeWorkFlow['filterData'].cache()
    completeWorkFlow['filterData'].createOrReplaceTempView('filterData') 
    #### selectTransform Config
    selectTransformDf = spark.read.csv(selectTransformFileName, header=True, schema = selectTransformSchema)
    selectTransformDf.createOrReplaceTempView('selectTransform')
    completeWorkFlow['selectTransform'] = spark.sql("SELECT stepNo, columnNo, udfCleanup(tgtColumn) as tgtColumn, udfCleanup(exp) as exp, udfCleanup(aggregation) AS aggregation FROM selectTransform")
    completeWorkFlow['selectTransform'].cache()
    completeWorkFlow['selectTransform'].createOrReplaceTempView('selectTransform')     
    for row in completeWorkFlow['master'].sort(workflowDf.stepNo.asc()).collect():
        print("step found: %d: %s" % (row.stepNo, row.stepName))
        d = {}
        d['stepNo']= int(row.stepNo)
        d['stepName'] = row.stepName
        d['transFormType'] = row.type
        if d['transFormType'] == 'Load':
            d['srcData'] = changeRootFolderName(row.srcData, rootFolderName, FOLDER_NAME_DICT)
        else:
            d['srcData'] = row.srcData
        if d['transFormType'] == 'MapLeft' or d['transFormType'] == 'MapLeftSilent' or d['transFormType'] == 'MapInner':        
            d['metaData'] = changeRootFolderName(row.metaData, rootFolderName, FOLDER_NAME_DICT)
        elif  d['transFormType'] == 'CompareValues':
            d['metaData'] = changeRootFolderName(row.metaData, rootFolderName, FOLDER_NAME_DICT)
            if d['metaData'] is None:
                d['metaData'] = row.metaData
        else:    
            d['metaData'] = row.metaData
        d['tgtData'] = row.tgtData
        if row.tgtPath is None:
            d['tgtPath'] = row.tgtPath
        else:
            d['tgtPath'] = row.tgtPath
            # d['tgtPath'] = changeRootFolderName(row.tgtPath, rootFolderName, FOLDER_NAME_DICT)
        d['outputFormat'] = row.outputFormat
        d['Saved'] = False
        wfList.append(d)

#-----------------------------------------------------------------------------
def calculationTransform(wfDict):
    baseTable = wfDict['srcData']
    calculatedMeasures = spark.sql("SELECT DISTINCT calcNo, columnName, srcColumn, plusMinus FROM calcCols WHERE stepNo = %d ORDER BY calcNo" % wfDict['stepNo']).collect()
    #build a dctionary with key being the new columns
    calcDict = buildCalculatedMeasureDict(calculatedMeasures)
    requiredColumnDict = {}
    
    for item in calculatedMeasures:
        newColName = item.columnName.strip()
        if requiredColumnDict.has_key(newColName) == False:
            requiredColumnDict[newColName] = []
        srcColName = item.srcColumn.strip()
        requiredColumnDict[newColName].append(srcColName)
    #if checkFieldExists(allDF, baseTable, combinedRequiredColumnList) == False:
    #    return False
    #initially, the available columns are only consist of the base table's columns
    availableColumnSet = set(allDF[baseTable].columns)
    sqlCmdLines = []
    dfList = []
    progressMade = True
    completed = False
    while progressMade == True and completed == False:
        combinedFormulaStr = ''
        newlyAddedColumns = []
        requiredColumnDict2 = {}
        # for each new column, check to see if all its required columns are selected 
        for newColName, requiredColumnList in requiredColumnDict.items():
            if set(requiredColumnList) <= availableColumnSet:
                #all the required columns for this new column has been satisfied, now we'll build the sql command
                if combinedFormulaStr != '':
                    combinedFormulaStr += ', '
                combinedFormulaStr += buildSQLColumnPlusMinusToken(newColName, calcDict[newColName])
                newlyAddedColumns.append(newColName)
            else:
                # it the new column cannot be done at this time, we save it later
                requiredColumnDict2[newColName] = requiredColumnList
        if combinedFormulaStr != '':
            #adding what's remaining of the original columns in the select statement
            formularStr = ''
            for existingColName in list(availableColumnSet - set(newlyAddedColumns)):
                formularStr += ', `%s`' % (existingColName)
            #the subsequent command will select from a table with the tgtData as the name, not the baseTable 
            if len(sqlCmdLines) > 0:
                baseTable = wfDict['tgtData']
            combinedFormulaStr = "SELECT %s %s FROM %s" % (combinedFormulaStr, formularStr, baseTable)
            sqlCmdLines.append(combinedFormulaStr)
            #adding the new columns to the originalColumns
            availableColumnSet = availableColumnSet | set(newlyAddedColumns)
            if len(requiredColumnDict2) > 0:
                #pop up the saved newColumns and their required columns
                requiredColumnDict = requiredColumnDict2
                #print("-----------Still requires:")
                #print(requiredColumnDict2)
            else:
                requiredColumnDict = {}
                completed = True
        else:
            #no more new sql Statements can be added
            progressMade = False
    if len(requiredColumnDict) > 0:
        msg = "required column unresolved for new columns."
        LOGGER.error(msg)
    else:
        for i in range(0, len(sqlCmdLines)):
            wfDict['tgtDF'] = execSQL(sqlCmdLines[i])
            wfDict['tgtDF'].createOrReplaceTempView(wfDict['tgtData'])

#-----------------------------------------------------------------------------
def loadQuotedFile(wfDict):
    predefinedFields = loadDf.filter(loadDf.stepNo == wfDict['stepNo']).sort(loadDf.columnNo).collect()
    path = wfDict["srcData"]
    options = {}
    if wfDict['metaData'] is not None:
        try:
            options ={k:v.strip('"') for k,v in [i.split("=",1) for i in wfDict['metaData'].lower().split('" ')]}
        except ValueError:
            options = {}
    headerExists = True
    delimiter = '|'
    if len(options) > 0:
        delimiter = options['delimiter'][0] if (options.has_key('delimiter') and len(options['delimiter'])>= 0) else '|'
        headerExists = False if (options.has_key('noheader') and options['noheader'] == 'true') else True
    #    load the files and replace the quotes immediately
    df = sc.textFile(path).map( lambda x: x.replace('\"','').replace(delimiter + '#', delimiter + '-1').replace('#' + delimiter, '-1' + delimiter))
    if headerExists:
        #    getting rid of all the header rows from all the files in the folder
        header = df.first()
        headerDf = df.filter(lambda l: header in l).distinct()
        dfNoHeader = df.subtract(headerDf)
    else:
        print("Source files contain no header")
        dfNoHeader = df
    #    split the lines into columns
    temp = dfNoHeader.map(lambda k: k.split(delimiter))
    fields = []
    for i in range(0, len(predefinedFields)):
        dataType = StringType()
        fields.append(StructField(predefinedFields[i].columnName.strip(), StringType(), True))
        #fields.append(StructField(predefinedFields[i].columnName.strip().replace(' ', '_').replace('(', '[').replace(')', ']'), StringType(), True))
    schema = StructType(fields)
    newDF = spark.createDataFrame(temp, schema)
    tempDataName = wfDict["tgtData"] + "_temp"
    newDF.createOrReplaceTempView(tempDataName)
    selectSqlTokens = []
    for i in range(0, len(predefinedFields)):
        colName = predefinedFields[i].columnName.strip()
        if (predefinedFields[i].dataType == 'Int#' or predefinedFields[i].dataType == 'Int'):
            selectSqlTokens.append("udfConvertInt(`%s`) AS `%s`" % (newDF.schema[i].name, colName))
        elif (predefinedFields[i].dataType == 'Double'):
            selectSqlTokens.append("udfConvertDouble(`%s`) AS `%s`" % (newDF.schema[i].name, colName))
        else:
            selectSqlTokens.append("`%s`" % (newDF.schema[i].name))
    selectSql = ', '.join(selectSqlTokens)
    sqlCmd = 'SELECT %s FROM %s' % (selectSql, tempDataName)
    wfDict['tgtDF'] = execSQL(sqlCmd)
    spark.catalog.dropTempView(tempDataName)
    return True

#---------------------------------------------------------------------------------------------------------
def loadOrc(wfDict):
    predefinedFields = loadDf.filter(loadDf.stepNo == wfDict['stepNo']).sort(loadDf.columnNo).collect()
    path = wfDict["srcData"]
    
    df = spark.read.load(path, format='orc')
    wfDict['tgtDF'] = df
    
    return True

#-----------------------------------------------------------------------------

def buildCalculatedMeasureDict(list):
    dct = {}
    for item in list:
        name = item.columnName.strip()
        if dct.has_key(name) == False:
            dct[name] = []
        dct[name].append(item)
    return dct

def buildRequiredColumnDict(list):
    dct = {}
    for item in list:
        name = item.columnName.strip()
        if dct.has_key(name) == False:
            dct[name] = []
        dct[name].append(item.srcColumn.strip())
    return dct

def buildRequiredColumnList(dict):
    list = []
    for key, value in dict.items():
        list += value
    return list

def buildSQLColumnPlusMinusToken(columnName, colList):
    formularStr = ''
    for calcItem in colList:
        if formularStr == '':
            formularStr = '0.0 '
        colName = calcItem.srcColumn.strip()
        formularStr += '%s COALESCE(`%s`, 0) ' % ( calcItem.plusMinus, colName)
    s = "%s AS `%s`" % (formularStr, columnName)
    #print("buildSQLColumnPlusMinusToken: " + s)
    return s

#-----------------------------------------------------------------------------

def getColumnDataType(srcDF, columnName):
    asciiColumnName = unicodedata.normalize('NFKD', columnName).encode('ascii','ignore')
    return srcDF.schema[asciiColumnName].dataType

#-----------------------------------------------------------------------------

def joinTransform(wfDict, allDF):
    # get all the calculation parts for the step
    baseTable = wfDict['srcData']
    calculatedMeasures = spark.sql("SELECT DISTINCT stepNo, srcData, baseColumn, joinColumn, addColumn, addColumnAlias, defaultValue FROM joinTable WHERE stepNo = %d ORDER BY srcData" % wfDict['stepNo']).collect()
    sqlCmd = ''
    allRequiredColumns = {}
    strAddedColumns = ""
    #adding the added columns from joined table
    for col in calculatedMeasures:
        if strAddedColumns != '':
            strAddedColumns += ', '
        strAddedColumns += "COALESCE(%s.`%s`,'%s') AS `%s`" % (col.srcData, col.addColumn, col.defaultValue, col.addColumnAlias)
        #first time reference a joined table
        if allRequiredColumns.has_key(col.srcData) == False:
            allRequiredColumns[col.srcData] = []
        allRequiredColumns[col.srcData].append(col.joinColumn)
        allRequiredColumns[col.srcData].append(col.addColumn)
        if allRequiredColumns.has_key(baseTable) == False:
            allRequiredColumns[baseTable] = []        
        allRequiredColumns[baseTable].append(col.baseColumn)
    for table, requiredCols in allRequiredColumns.items():
        if checkFieldExists(allDF, table, requiredCols) == False:
            return False
    addedColumns = set([d.addColumn for d in calculatedMeasures]) | set([d.baseColumn for d in calculatedMeasures])
    
    strOtherColumnTokens = []
    if allDF.has_key(baseTable):
        for col in allDF[baseTable].columns:
            if col not in addedColumns:
                strOtherColumnTokens.append('A.`%s`' % (col))
    strOtherColumns = ', '.join(strOtherColumnTokens)
    #
    sqlCmd = 'SELECT %s, %s\n FROM %s A' % (strOtherColumns, strAddedColumns, baseTable)
    joinExp= {}
    joinedTables = set([d.srcData for d in calculatedMeasures])
    #for each unique joined table, create a fragment of left join on... query text
    for joinedTable in joinedTables:
        joinExp[joinedTable] = '%s' %(joinedTable)
    #get the unique list of srcData, baseColumn and joinedColumn combination
    joinList = dict((v.srcData+'|'+v.baseColumn + '|'+v.joinColumn, v) for v in calculatedMeasures).values()
    for i in range(0, len(joinList)):
        str = joinExp[joinList[i].srcData].strip()
        if ' ON ' not in str:
            str += '\n ON '
        else:
            str += '\n\t AND '
        if getColumnDataType(allDF[baseTable], joinList[i].baseColumn) == StringType():
            str += "IF(A.`%s` IS NULL OR A.`%s`='', '-2', A.`%s`)= %s.`%s`" %(joinList[i].baseColumn, joinList[i].baseColumn, joinList[i].baseColumn, joinExp[joinList[i].srcData].strip(), joinList[i].joinColumn)
        else:
            str += "IF(A.`%s` IS NULL, -2, A.`%s`)= %s.`%s`" %(joinList[i].baseColumn, joinList[i].baseColumn, joinExp[joinList[i].srcData].strip(), joinList[i].joinColumn)
        joinExp[joinList[i].srcData] = str
    for key, value in joinExp.iteritems():
        sqlCmd += '\n LEFT OUTER JOIN ' + value
    wfDict['tgtDF'] = execSQL(sqlCmd)
    return True
#-----------------------------------------------------------------------------

def processStep(wfList, allDF, startAtStepNum=0, endAtStepNum=-1):
    global CURRENT_ACTION
    global ERROR_MESSAGE
    allStepStartTime = datetime.datetime.now()
    if endAtStepNum == -1:
        endAtStepNum = len(wfList) - 1
    result = True
    
    try:
        for i in range(startAtStepNum, endAtStepNum+1):
            
            step = wfList[i]
            step['count'] = 0
            print("Executing: %d: %s" % (step["stepNo"], step["stepName"]))
            CURRENT_ACTION = "%d - %s" % (step["stepNo"], step["stepName"])
            startTime = datetime.datetime.now()
            step['startTime'] = startTime
            if step['transFormType'] == 'Load':
                result = loadQuotedFile(step)
            elif step['transFormType'] == 'LoadOrc':
                result = loadOrc(step)
            elif step['transFormType'] == 'Calculate':
                result = calculationTransform(step)
            elif step['transFormType'] == 'Join':
                result = joinTransform(step, allDF)
            # elif step['transFormType'] == 'CheckKeyUnique':
            #     result = checkKeyUnique(step, allDF)
            elif step['transFormType'] == 'FilterData':
                result = filterData(step, allDF)
            # elif step['transFormType'] == 'LookupFilter':
            #     result = lookupFilter(step, allDF)
            # elif step['transFormType'] == 'MapLeft':
            #     result = mappingColumns(step, allDF, 'LEFT', False)
            # elif step['transFormType'] == 'MapLeftSilent':
            #     result = mappingColumns(step, allDF, 'LEFT', True)
            # elif step['transFormType'] == 'MapInner':
            #     result = mappingColumns(step, allDF, 'INNER', True)
            elif step['transFormType'] == 'SelectTransform':
                result = selectTransform(step, allDF)
            elif step['transFormType'] == 'CompareValues':
                result = compareValues(step, allDF)
            elif step['transFormType'] == 'OutputQuery':
                result = outputQueryTransform(step, allDF)
            elif step['transFormType'] == 'OutputMerge':
                result = outputMergeTransform(step, allDF)     
            elif step['transFormType'] == 'OutputParquet':
                result = outputParquetTransform(step, allDF)
            elif step['transFormType'] == 'OutputHive':
                result = outputHiveTransform(step, allDF)
            elif step['transFormType'] == 'LogProcessDetails':
                result = logProcessDetails(step, wfList)
            else:
                ERROR_MESSAGE = "Unrecognized step type %s" % step['transFormType']
                result = False
            if result == False:
                ERROR_MESSAGE = ("Step %d: %s failed - " % (step["stepNo"], step["stepName"])) + ERROR_MESSAGE
                LOGGER.error(ERROR_MESSAGE)
                return False
            if step.has_key('tgtDF'):
                step['tgtDF'].persist()            
                if step['tgtData'] is None or len(step['tgtData']) == 0:
                    print("DataFrame is not persisted.")
                else:
                    step['tgtDF'].createOrReplaceTempView(step['tgtData'])
                    allDF[step['tgtData']] = step['tgtDF']
            # write to output path if provided
            if step['Saved'] == False and step.has_key('tgtPath') and step['tgtPath'] is not None and len(step['tgtPath']) > 0:
                if step.has_key('tgtDF') == False or step['tgtDF'] is None:
                    ERROR_MESSAGE = "Step %d: %s - needs to output file but does not have a DataFrame" % (step['stepNo'], step['stepName'])
                    LOGGER.error(ERROR_MESSAGE)
                    return False
                elif step.has_key('outputFormat') == False or (step['outputFormat'] != 'csv' and step['outputFormat'] != 'parquet'):
                    ERROR_MESSAGE = "Step %d: %s - does not have either csv or parquet as output format.  The format is [%s]" % (step['stepNo'], step['stepName'], step['outputFormat'])
                    LOGGER.error(ERROR_MESSAGE)
                    return False
                else:
                    msg = "\n**StepNo %d - %s saving files to %s." % (step['stepNo'], step['stepName'], step['tgtPath'])
                    LOGGER.info(msg)
                    print(msg)
                    if step['outputFormat'] == 'parquet':
                        step['tgtDF'].write.parquet(step['tgtPath'],  mode='overwrite')
                    else:
                        step['tgtDF'].write.save(step['tgtPath'], format=step['outputFormat'], header=True, mode='overwrite', sep='|',quote ='"')
                    #try:
                    #    step['tgtDF'].write.saveAsTable('tbl_' + step['tgtData'], format='parquet', mode='overwrite')
                    #except Error:
                    #    print('tbl_' + step['tgtData'] + " cannot be save as Hive table.")
            endTime = datetime.datetime.now()
            step['endTime'] = endTime
            step['duration'] = endTime-startTime
            print('Step %d: %s finished at %s: total time:%s' % (step["stepNo"], step["stepName"], endTime, step['duration']))
            ERROR_MESSAGE = ""
        CURRENT_ACTION = 'processStep completed'
    except:
        ERROR_MESSAGE = "Step %d: %s - Unexpected error:%s" % (step["stepNo"], step["stepName"], sys.exc_info()[0])
        LOGGER.error(ERROR_MESSAGE)
        raise
    return True

#-----------------------------------------------------------------------------

def checkKeyUnique(wfDict, allDF):
    global ERROR_MESSAGE
    tableName = wfDict['srcData']
    #keyCol = wfDict['metaData'].strip().replace(' ', '_').replace('(', '[').replace(')', ']').replace('-', '_').replace('.', '?')
    keyCol = wfDict['metaData'].strip()
    if allDF.has_key(tableName) == False:
        ERROR_MESSAGE = 'table %s does not exist' %(tableName)
        return False
    df = allDF[tableName]
    wfDict['tgtDF'] = df
    wfDict["saved"] = True
    requireColumns = [keyCol]
    if checkFieldExists(allDF, tableName, requireColumns) == False:
        return False
    count = df.count()
    #count distict keys
    distinctCount = df.select("`" + keyCol + "`").distinct().count()
    if count != distinctCount:
        print("!!!Table %s Contains duplicate keys %s: total count %d, distinct key count %d" % (tableName, wfDict['metaData'], count, distinctCount))
        #sqlContext.sql("SELECT * FROM( SELECT *, ROW_NUMBER()OVER(PARTITION BY `%s`) rn FROM x) y WHERE rn = 1", keyCol)
        formularTokens = []
        for calcItem in df.columns:
            if calcItem != keyCol:
                formularTokens.append('max(`' + calcItem + '`) AS `' + calcItem +'`')
            else:
                formularTokens.append('`' + calcItem + '`')
        formularStr = ', '.join(formularTokens)
        wfDict['tgtDF'] = execSQL("SELECT %s FROM %s GROUP BY `%s`" % (formularStr, tableName, keyCol))
        print('Updated table: %s - count=%d' % (tableName, allDF[tableName].count()))
        wfDict['tgtDF'].unpersist()
        spark.catalog.dropTempView(tableName)
        wfDict["saved"] = False
    return True

def checkFieldExists(allDF, srcDataName, requiredColumns):
    global ERROR_MESSAGE
    if allDF.has_key(srcDataName) == False:
        ERROR_MESSAGE = "SourceData: %s does not exists" % (srcDataName)
        return False
    colSet = set(allDF[srcDataName].columns)
    reqSet = set(requiredColumns)
    if set(requiredColumns) <= colSet:
        return True
    else:
        ERROR_MESSAGE = "These columns are not included in the columns from the source table of %s:" % (srcDataName)
        ERROR_MESSAGE += "\n\t%s" % (reqSet - colSet)
        ERROR_MESSAGE += '\n\tSource columns are: %s' % (colSet)
        return False

def buildColumnSql(columns, aggregation=''):
    otherColumnsSql = ''
    for col in columns:
        if otherColumnsSql != '':
            otherColumnsSql += ', '
        if aggregation == '':
            otherColumnsSql += '`' + col + '`'
        else:
            otherColumnsSql += aggregation + '(`' + col + '`)'
    return otherColumnsSql

def lookupFilter(wfDict, allDF):
    baseTable = wfDict['srcData']
    srcDF = allDF[baseTable]
    configList = spark.sql("SELECT DISTINCT trim(trgtMetric) AS trgtMetric, trim(srcColName) AS srcColName, trim(dimFilter) AS dimFilter, trim(includeExclude) AS includeExclude, trim(matchingValue) AS matchingValue FROM lookupFilter WHERE stepNo =%d ORDER BY trgtMetric" % (wfDict['stepNo'])).collect()
    #tgtMetrics is a dictionary containg filter dictionary, columnValuePair dictionary and srcMetricName
    #filter dictory has includeExclude symbol as key and the value is a list of condition string sich as  "a=b", "c=d"
    #columnValuePair is a dictionary of columnName and columnValue 
    tgtMetrics = {}
    requireColumns = []
    otherColumns = []
    #metricFilterDit looks like this:
    #{ "=": {   srcColName1: ['srcColName1=a OR srcColName1=b'],
    #           srcColName2: ['srcColName2=d OR srcColName2=f' OR srcColName2=g']}
    #  "<>" {   srcColName3: ['srcColName3<>a AND srcColName3=k'],
    #           srcColName4: ['srcColName4<>x AND srcColName4=y']
    for row in configList:
        # if there is no include/exclude expression, treat it as the dimension field
        if row['includeExclude'] is None or row['includeExclude'] == '':
            otherColumns.append(row['trgtMetric'])
            requireColumns.append(row['trgtMetric'])
        else:
            if tgtMetrics.has_key(row['trgtMetric']) == False:
                columnExpr = '`%s` AS `%s` ' % (row['srcColName'], row['trgtMetric'])
                tgtMetrics[row['trgtMetric']] = {'filter':{}, 'srcMetric':row['srcColName']}
            metricFilterDict = tgtMetrics[row['trgtMetric']]['filter']
            str = ''
            if metricFilterDict.has_key(row['includeExclude']) == False:
                metricFilterDict[row['includeExclude']] = {row['dimFilter']:''}
            if metricFilterDict[row['includeExclude']].has_key(row['dimFilter']) == False:
                metricFilterDict[row['includeExclude']][row['dimFilter']] = []
            else:
                str = metricFilterDict[row['includeExclude']][row['dimFilter']]
                #print("\nprevious results for :" +row['includeExclude'] + ":" + row['dimFilter'] + ":" + str)
            if row['includeExclude'] == '=' and str != '':
                str += ' OR '
            elif str != '':
                str += ' AND '
            subStr = "`%s` %s '%s'" % (row['dimFilter'], row['includeExclude'], row['matchingValue'])
            if row['includeExclude'] == '<>':
                subStr = "(%s OR `%s` IS NULL)" % (subStr, row['dimFilter'])                
            #if the data type is string, then add quotes to the compared value, otherwisse don't add quotes
            #if getColumnDataType(srcDF, row['dimFilter']) == StringType():
            #    subStr = "`%s` %s '%s'" % (row['dimFilter'], row['includeExclude'], row['matchingValue'])
            #else:
            #    subStr = "`%s` %s %s" % (row['dimFilter'], row['includeExclude'], row['matchingValue'])
            #if row['includeExclude'] == '<>':
            #    subStr = "(%s AND `%s` IS NOT NULL)" % (subStr, row['dimFilter'])
            str += subStr
            metricFilterDict[row['includeExclude']][row['dimFilter']] = str
            #print("updated results for :" +row['includeExclude'] + ":" + row['dimFilter'] + ":" + str)
            requireColumns.append(row['srcColName'])
            requireColumns.append(row['dimFilter'])
    if checkFieldExists(allDF, baseTable, requireColumns) == False:
        return False
    #now, build the filtering querystring
    metricFilters = {}
    newMetricsTokens = []
    for metric, metricDict in tgtMetrics.items():
        filterDict = tgtMetrics[metric]['filter']
        sqlFilterTokens = []
        for includeExclude, dimFilterDict in filterDict.items():
            # loop through all the dimFilters and build a subFilterStr
            for colName, subStr in dimFilterDict.items():
                sqlFilterTokens.append('(' + subStr + ')')
        sqlFilterStr = ' AND '.join(sqlFilterTokens)
        newMetricsTokens.append('\nSUM(CASE WHEN %s THEN `%s` ELSE 0 END) AS `%s`' % (sqlFilterStr, metricDict['srcMetric'], metric))
    otherColumnsSql = buildColumnSql(otherColumns)
    newMetricsSQL = ', '.join(newMetricsTokens)
    sqlCmd = 'SELECT %s, %s FROM %s GROUP BY %s' % (otherColumnsSql, newMetricsSQL, baseTable, otherColumnsSql)
    df = execSQL(sqlCmd)
    intrimTable = baseTable + "_intrim"
    df.cache()
    df.createOrReplaceTempView(intrimTable)
    #wfDict['tgtDF'] = None
    writeMode = 'overwrite'
    deleteFolder(wfDict['tgtData'])
    tempFolder = WORKFLOW_FOLDER + '/' + wfDict['tgtData'] + '/temp/'
    for metric in tgtMetrics.keys():
        msg = "Writing BEX Logic result for metric: %s" % metric
        print(msg)
        LOGGER.info(msg)
        sqlCmd = "SELECT %s, '%s' AS Metric, `%s` AS Value FROM %s WHERE `%s` <> 0" % (otherColumnsSql, metric, metric, intrimTable, metric)
        df = spark.sql(sqlCmd).coalesce(1)
        #df.write.save(tempFolder, format="csv", header=True, mode=writeMode, sep=',',quote ='')
        df.write.parquet(tempFolder, mode=writeMode)
        writeMode = 'append'
    #wfDict['tgtDF'] = spark.read.csv(tempFolder, header=True, schema=df.schema)
    wfDict['tgtDF'] = spark.read.parquet(tempFolder)
    df.unpersist()
    spark.catalog.dropTempView(intrimTable)
    return True   

def mappingColumns(wfDict, allDF, joinType, silent):
    global ERROR_MESSAGE
    baseTable = wfDict["srcData"]
    configFilePath =  wfDict["metaData"]
    #    load the file
    configDf = spark.read.csv(configFilePath, header=True)
    configDf.createOrReplaceTempView('configTable')
    sourceColumns = allDF[baseTable].columns
    #get config columns
    joinColSqlCols = []
    newColSqlCols = []
    noMatchSelectCols = []
    noMatchFilterCols = []
    selectedColumns = []
    for col in configDf.columns:
        columnName = col.strip()
        #if the column name starts with a !, then it is a new value on top of the existing value to be added
        if columnName.startswith('!'):
            realColName = columnName[1:len(columnName)]
            if realColName in sourceColumns:
                newColSqlCols.append('CASE WHEN a.`%s` IS NULL THEN b.`%s` ELSE a.`%s` END AS `%s`' % (realColName, columnName, realColName, realColName))
                noMatchFilterCols.append('b.`%s` IS NULL' % (columnName))
                selectedColumns.append(realColName)
            else:
                #the column name starts with '!' but the sourceColumns don't contain the realColName
                newColSqlCols.append('b.`%s` AS `%s`' % (columnName, realColName))
        elif columnName in sourceColumns:
            #joining existing column
            joinColSqlCols.append('a.`%s` = b.`%s`' % (columnName, columnName))
            noMatchSelectCols.append('a.`%s`' % (columnName))
        else:
            newColSqlCols.append('b.`%s`' % (columnName))
            noMatchFilterCols.append('b.`%s` IS NULL' % (columnName))
    #don't run the sql query if there is no join columns or there's no new or updated column
    if len(joinColSqlCols) == 0 or len(newColSqlCols) == 0 :
        ERROR_MESSAGE = "Error: either there's no joining column or no new/update column found." % (wfDict['stepName'])
        return False            
    newColSqlStr = ', '.join(newColSqlCols)
    #add the rest of the columns from the source DF that're not in the selected columns in the select statement
    for col in sourceColumns:
        if col not in selectedColumns:
            newColSqlStr += ', a.`%s`' % col
    joinColSqlStr = ' AND '.join(joinColSqlCols)
    sqlCmd = 'SELECT %s FROM %s a %s JOIN configTable b ON %s' % (newColSqlStr, baseTable, joinType, joinColSqlStr)
    wfDict['tgtDF'] = execSQL(sqlCmd)
    #Build the query to check for no match and dump the combination of the joining columns into a flat file for trouble shooting
    if (joinType.upper() == 'LEFT' or joinType.upper() == 'FULL') and len(noMatchSelectCols) > 0 and silent != True:
        noMatchSelectSqlStr = ', '.join(noMatchSelectCols)
        noMatchSqlCmd = "SELECT count(*) AS RowCount, %s FROM %s a LEFT JOIN configTable b ON %s WHERE %s GROUP BY %s" % (noMatchSelectSqlStr, baseTable, joinColSqlStr, ' OR '.join(noMatchFilterCols), noMatchSelectSqlStr) 
        df = execSQL(noMatchSqlCmd)
        if df.count() > 0:
            nomatchfile = WORKFLOW_FOLDER + NO_MATCH + "/" + wfDict['stepName']
            df.coalesce(1).write.save(nomatchfile, format='csv', header=True, mode='overwrite', sep='|',quote ='"')
            #s = "%s" % df.show()
            # noMatchFileURL = nomatchfile.replace('s3://', 'https://console.aws.amazon.com/s3/buckets/').replace(' ', '%20')
            # response = sns.publish(
            #     TargetArn=SNS_ARN_PREFIX + WORKFLOW_ROOT_FOLDER_NAME,
            #     Subject="Non-matching record found in ETL step: %s" % (wfDict['stepName']),
            #     Message="Non-matching record found when joining with mapping table during step %s.\n\tmasterConfig location: %s\n\tMapping table location is: %s\n\tPlease check: %s for details.\nNo match:\n\t%s" % (wfDict['stepName'], WORKFLOW_FOLDER, configFilePath, noMatchFileURL, s)
            # )
    return True

def selectTransform(wfDict, allDF):
    SELECT_TYPE_EXHAUSIVE = 0
    SELECT_TYPE_ADD = 1
    baseTable = wfDict['srcData']
    if wfDict["metaData"] != None and wfDict["metaData"].upper() == 'ADD':
        selectType = SELECT_TYPE_ADD
    else:
        selectType = SELECT_TYPE_EXHAUSIVE
    transformList = spark.sql("SELECT DISTINCT columnNo, trim(tgtColumn) AS tgtColumn, trim(exp) AS exp, trim(aggregation) AS aggregation FROM selectTransform WHERE stepNo = %d ORDER BY columnNo" % wfDict['stepNo']).collect()
    innerSqlStr = ''
    outterSqlStr = ''
    groupByStr = ''
    for row in transformList:
        if innerSqlStr != '':
            innerSqlStr += ', '
            outterSqlStr += ', '
        if row.exp == row.tgtColumn:
            innerSqlStr += '%s' % (row.exp)
        else:
            innerSqlStr += '%s AS %s' % (row.exp, row.tgtColumn)
        if row.aggregation is not None and row.aggregation != '':
            outterSqlStr += '%s(%s) AS %s' % (row.aggregation, row.tgtColumn, row.tgtColumn)
        else:
            outterSqlStr += '%s' % row.tgtColumn
            if groupByStr != '':
                groupByStr += ', '
            groupByStr += row.tgtColumn
    
    if selectType == SELECT_TYPE_EXHAUSIVE:
        innerSqlStr = 'SELECT %s FROM %s' % (innerSqlStr, baseTable)
        outterSqlStr = 'SELECT %s FROM (%s) subQ GROUP BY %s ORDER BY %s' % (outterSqlStr, innerSqlStr, groupByStr, groupByStr)
    else:
        #getting other columns
        allColumns = set(allDF[baseTable].columns)
        addedColumns = set(allDF[baseTable].columns) - set([d.tgtColumn for d in transformList])        
        addedColumnSelectSql = buildColumnSql(addedColumns)
        outterSqlStr = 'SELECT %s, %s FROM %s' % (innerSqlStr, addedColumnSelectSql, baseTable)
    wfDict['tgtDF'] = execSQL(outterSqlStr)
    return True

#----------------------------------------------------------------------------
def outputQueryTransform(wfDict, allDF):
    sqlCmd = wfDict["metaData"]
    if sqlCmd is None or sqlCmd == "":
        LOGGER.error("Query transform requires a query, none was provided for %s" % wfDict['stepName'])
        return False
    outputPath = wfDict['tgtPath']
    df = execSQL(sqlCmd)
    if wfDict['tgtData'] is not None:
        wfDict['tgtDF'] = df
    if outputPath != None and outputPath != "":
        print("Saving output to %s" % outputPath)
        df.coalesce(1).write.save(outputPath, format=wfDict['outputFormat'], header=True, mode='overwrite', sep='|',quote ='"')
        wfDict['Saved'] = True
    return True

#----------------------------------------------------------------------------
def outputMergeTransform(wfDict, allDF):
    global ERROR_MESSAGE
    baseTable = wfDict['srcData']
    if baseTable is None or baseTable == "":
        ERROR_MESSAGE = "OutputMerge transform requires a srcData, none was provided for %s" % wfDict['stepName']
        return False
    outputPath = wfDict['tgtPath']
    if outputPath is None or outputPath == "":
        ERROR_MESSAGE = "OutputMerge transform requires a target path, none was provided for %s" % wfDict['tgtPath']
        return False
    if allDF.has_key(baseTable) == False:
        ERROR_MESSAGE = "OutputMerge reference a non-existing srcData %s" % baseTable
        return False
    print("Saving output to {0}".format(wfDict['tgtPath']))
    allDF[baseTable].coalesce(1).write.save(outputPath, format=wfDict['outputFormat'], header=True, mode='overwrite', sep='|',quote ='"')
    wfDict['Saved'] = True
    # if moveFile(outputPath) == False:
    #     ERROR_MESSAGE = ('Failed to move the output file "%s"\n' % (outputPath)) + ERROR_MESSAGE
    #     return False
    return True

#----------------------------------------------------------------------------
def outputParquetTransform(wfDict, allDF):
    global ERROR_MESSAGE
    baseTable = wfDict['srcData']
    if baseTable is None or baseTable == "":
        ERROR_MESSAGE = "OutputParquet transform requires a srcData, none was provided for %s" % wfDict['stepName']
        return False
    if allDF.has_key(baseTable) == False:
        ERROR_MESSAGE =  "OutputParquet reference a non-existing srcData %s" % baseTable
        return False
    print("Saving output to %s" % wfDict['tgtPath'])
    newNames = []
    df = allDF[baseTable]
    for n in df.schema.names:
        newNames.append(n.lower())
    df2 = reduce(lambda df, idx: df.withColumnRenamed(df.schema.names[idx], newNames[idx]), xrange(len(newNames)), df)
    df2.write.parquet(wfDict['tgtPath'], mode='overwrite')
    wfDict['Saved'] = True
    return True

#----------------------------------------------------------------------------
def outputHiveTransform(wfDict, allDF):
    global ERROR_MESSAGE
    baseTable = wfDict['srcData']
    if baseTable is None or baseTable == "":
        ERROR_MESSAGE = "OutputHive transform requires a srcData, none was provided for %s" % wfDict['stepName']
        return False
    if allDF.has_key(baseTable) == False:
        ERROR_MESSAGE =  "OutputHive reference a non-existing srcData %s" % baseTable
        return False
    print("Creating output table %s" % wfDict['tgtPath'])
    newNames = []
    df = allDF[baseTable]
    for n in df.schema:
        newNames.append(n.lower())

    outputSql = "CREATE TABLE %s" % wfDict['tgtPath']
    wfDict['Saved'] = True
    return True

#----------------------------------------------------------------------------
def compareValues(wfDict, allDF):
    global ERROR_MESSAGE
    baseTable = wfDict["srcData"]
    configFilePath =  wfDict["metaData"]
    if configFilePath.startswith("s3://"):
        #load the file
        configDf = spark.read.csv(configFilePath, header=True, sep='|')
    else:
        #refer to an existing data frame
        configDf = allDF[configFilePath]
    newSchema =  StructType([])
    compareColStr = ''
    compareColPhrases = []
    aggOriginalPhrases = []
    otherColumns = []
    innerQueryOtherColumns = []
    joinColPhrases = []
    firstSumPhases = []
    groupByCols = []
    for t in configDf.schema:
        #if t.name == 'stepNo':
        #    newSchema.add(t.name, IntegerType(), True)
        #elif t.name.startswith('#'):
        if t.name.startswith('#'):
            realColName = t.name[1:]
            newSchema.add(realColName, DoubleType(), True)
            firstSumPhases.append("SUM(`%s`) as `%s`" % (realColName, realColName))
            compareColPhrases.append("a.`%s` as %s_result, b.`%s` as %s_ref, coalesce(a.`%s`, 0) - coalesce(b.`%s`, 0) AS %s_diff" % (realColName, realColName, realColName, realColName, realColName, realColName, realColName))
        else:
            newSchema.add(t.name, t.dataType, True)
            otherColumns.append('coalesce(a.`%s`, b.`%s`) AS `%s`' % (t.name, t.name, t.name))
            innerQueryOtherColumns.append('a.`%s`' % (t.name))
            joinColPhrases.append('a.`%s` = b.`%s`' % (t.name, t.name))
            groupByCols.append('a.`%s`' % (t.name))
            #print(t.name + " - %s" % t.dataType.typeName
    innerQuerySelectSegment = []
    if len(innerQueryOtherColumns) > 0:
        innerQuerySelectSegment.append(', '.join(innerQueryOtherColumns))
    if len(firstSumPhases) > 0:
        innerQuerySelectSegment.append(', '.join(firstSumPhases))
    selectColSegment = []
    if len(otherColumns) > 0:
        selectColSegment.append(', '.join(otherColumns))
    if len(compareColPhrases) > 0:
        selectColSegment.append(', '.join(compareColPhrases))
    joinColSqlStr = ' AND '.join(joinColPhrases)
    if configFilePath.startswith("hdfs://"):
        configDf = spark.read.csv(configFilePath, header=True, schema = newSchema, sep='|')#.filter('stepNo=%d' % wfDict['stepNo'])
    configDf.createOrReplaceTempView('compareTable')
    subQuery = "SELECT %s FROM %s a GROUP BY %s" % (', '.join(innerQuerySelectSegment), baseTable, ', '.join(groupByCols))
    sqlCmd = 'SELECT %s FROM (%s) a FULL JOIN compareTable b ON %s' % (', '.join(selectColSegment), subQuery, joinColSqlStr)
    wfDict['tgtDF'] = execSQL(sqlCmd).coalesce(1)
    return True

#----------------------------------------------------------------------------
# ???
def moveFile(path):
    global ERROR_MESSAGE
    rootFolderName, prefix = getRootFolderFromWorkflowPath(path)
    # if rootFolderName is None:
    #     ERROR_MESSAGE = "Cannot get s3 bucket name from %s" % path
    #     return False
    # bucket = s3.Bucket(rootFolderName)
    #
    successFound = False
    numberOfTries = 0
    #test to see if the _SUCCESS file exists in the folder.  If not, wait for 30 seconds and test again.
    #this is because the saving of the folder files may be performed by other nodes than the current one
    if path.split('/')[0] == "hdfs":
        ls_cmd = ['hdfs', 'dfs', '-ls', path]
        cp_cmd = ['hdfs', 'dfs', '-cp', path]
        rm_cmd = ['hdfs', 'dfs', '-rm', '-R', '-skipTrash', path]
        os = "linux"
    else:
        win_path = path.replace('/', '\\')
        ls_cmd = 'dir "{0}"'.format(path)
        cp_cmd = 'copy "{0}" "{1}.csv"'
        rm_cmd = 'rd /S /Q "{0}"'.format(win_path)
        ren_cmd = 'ren "{0}.csv" "{1}"'.format(win_path, path.split("/")[-1])
        os = "win"
    while successFound == False and numberOfTries < 300:
        collection = run_cmd(ls_cmd)[0].splitlines()
        for object in collection:
            if (successFound == False):
                #   No RE so replacing sequences of spaces in simple manner
                for word in object.replace("    ", " ").replace("   ", " ").replace("  ", " ").split(" "):
                    tokens = word.split("/")
                    
                    if tokens[-1] == '_SUCCESS':
                        successFound = True
                        break;
            else:
                break
        if successFound == False:
            LOGGER.warn("File not ready to move, wait 1 seconds")
            time.sleep(1)
            numberOfTries += 1
    fileCopied = False
    collection = run_cmd(ls_cmd)[0].splitlines()
    for object in collection:
        if (fileCopied == False):
            #   No RE so replacing sequences of spaces in simple manner
            for word in object.replace("    ", " ").replace("   ", " ").replace("  ", " ").split(" "):
                tokens = word.split("/")
                if fileCopied == False and tokens[-1].startswith('part-') and tokens[-1] != (prefix + '.csv'):
                    print("Copying file from %s to %s" % (tokens[-1], prefix))
                    if os == "win":
                        cp_cmd = cp_cmd.format(win_path + '\\' + tokens[-1], win_path)
                    else:
                        cp_cmd = cp_cmd.format(path + '/' + tokens[-1], path)
                    run_cmd(cp_cmd)
    #                    s3.Object(rootFolderName, prefix + '.csv').copy_from(CopySource=rootFolderName + "/" + tokens[-1])
                    fileCopied = True        
                    break
        else:
            break
    if fileCopied == False:
        ERROR_MESSAGE = "There's no file(s) under %s to be copied." % prefix
        return False
    else:
        #delete the folder
        run_cmd(rm_cmd)
        #rename the file copied
        run_cmd(ren_cmd)
        # if prefix.endswith("/") == False:
        #     prefix += '/'
        # for obj in bucket.objects.filter(Prefix=prefix):
        #     s3.Object(bucket.name, obj.key).delete()
        return True

def filterData(wfDict, wfList):
    baseTable = wfDict['srcData']
    srcDF = allDF[baseTable]
    configList = spark.sql("SELECT DISTINCT trim(dimFilter) AS dimFilter, trim(includeExclude) AS includeExclude, trim(matchingValue) AS matchingValue FROM filterData WHERE stepNo =%d" % (wfDict['stepNo'])).collect()
    #tgtMetrics is a dictionary containg filter dictionary, columnValuePair dictionary and srcMetricName
    #filter dictory has includeExclude symbol as key and the value is a list of condition string sich as  "a=b", "c=d"
    #columnValuePair is a dictionary of columnName and columnValue 
    tgtMetrics = {}
    requireColumns = []
    otherColumns = []
    filterDict = {}
    #metricFilterDit looks like this:
    #{ "=": {   srcColName1: ['srcColName1=a OR srcColName1=b'],
    #           srcColName2: ['srcColName2=d OR srcColName2=f' OR srcColName2=g']}
    #  "<>" {   srcColName3: ['srcColName3<>a AND srcColName3=k'],
    #           srcColName4: ['srcColName4<>x AND srcColName4=y']
    for row in configList:
        # if there is no include/exclude expression, treat it as the dimension field
        if row['includeExclude'] is None or row['includeExclude'] == '':
            pass
            #otherColumns.append(row['trgtMetric'])
            #requireColumns.append(row['trgtMetric'])
        else:
            #if tgtMetrics.has_key(row['trgtMetric']) == False:
                #columnExpr = '`%s` AS `%s` ' % (row['srcColName'], row['trgtMetric'])
                #tgtMetrics[row['trgtMetric']] = {'filter':{}, 'srcMetric':row['srcColName']}
            #metricFilterDict = tgtMetrics[row['trgtMetric']]['filter']
            str = ''
            if filterDict.has_key(row['includeExclude']) == False:
                filterDict[row['includeExclude']] = {row['dimFilter']:''}
            if filterDict[row['includeExclude']].has_key(row['dimFilter']) == False:
                filterDict[row['includeExclude']][row['dimFilter']] = []
            else:
                str = filterDict[row['includeExclude']][row['dimFilter']]
                #print("\nprevious results for :" +row['includeExclude'] + ":" + row['dimFilter'] + ":" + str)
            if row['includeExclude'] == '=' and str != '':
                str += ' OR '
            elif str != '':
                str += ' AND '
            subStr = "`%s` %s %s" % (row['dimFilter'], row['includeExclude'], '' if row['matchingValue'] is None else row['matchingValue'])
            # if row['includeExclude'] == '<>':
            #     subStr = "(%s OR `%s` IS NULL)" % (subStr, row['dimFilter'])                
            #if the data type is string, then add quotes to the compared value, otherwisse don't add quotes
            #if getColumnDataType(srcDF, row['dimFilter']) == StringType():
            #    subStr = "`%s` %s '%s'" % (row['dimFilter'], row['includeExclude'], row['matchingValue'])
            #else:
            #    subStr = "`%s` %s %s" % (row['dimFilter'], row['includeExclude'], row['matchingValue'])
            #if row['includeExclude'] == '<>':
            #    subStr = "(%s AND `%s` IS NOT NULL)" % (subStr, row['dimFilter'])
            str += subStr
            filterDict[row['includeExclude']][row['dimFilter']] = str
            #print("updated results for :" +row['includeExclude'] + ":" + row['dimFilter'] + ":" + str)
            #requireColumns.append(row['srcColName'])
            requireColumns.append(row['dimFilter'])
    
    if checkFieldExists(allDF, baseTable, requireColumns) == False:
        return False
    
    #now, build the filtering querystring
    # metricFilters = {}
    # newMetricsTokens = []
    # for metric, metricDict in tgtMetrics.items():
    #     filterDict = tgtMetrics[metric]['filter']
    sqlFilterTokens = []
    for includeExclude, dimFilterDict in filterDict.items():
        # loop through all the dimFilters and build a subFilterStr
        for colName, subStr in dimFilterDict.items():
            sqlFilterTokens.append('(' + subStr + ')')
        sqlFilterStr = ' AND '.join(sqlFilterTokens)
        #newMetricsTokens.append('\nSUM(CASE WHEN %s THEN `%s` ELSE 0 END) AS `%s`' % (sqlFilterStr, metricDict['srcMetric'], metric))
    # otherColumnsSql = buildColumnSql(otherColumns)
    # newMetricsSQL = ', '.join(newMetricsTokens)
    # sqlCmd = 'SELECT %s, %s FROM %s GROUP BY %s' % (otherColumnsSql, newMetricsSQL, baseTable, otherColumnsSql)
    
    sqlCmd = 'SELECT * FROM %s WHERE %s' % (baseTable, sqlFilterStr)
    print(sqlCmd)
    wfDict['tgtDF']  = execSQL(sqlCmd)

#----------------------------------------------------------------------------
def logProcessDetails(wfDict, wfList):
    stats = []
    begin_time = datetime.datetime.now()
    for step in wfList:
        if step.has_key('tgtDF'):
            log = {}
            stats.append(log)
            if step['startTime'] < begin_time:
                begin_time = step['startTime']
            log['run_id'] = "%s-%s" % (DATA_PIPELINE_NAME, begin_time.strftime("%Y%m%d-%H:%M"))
            log['data_pipeline'] = DATA_PIPELINE_NAME
            log['step_no'] = step['stepNo']
            log['step_name'] = step['stepName']
            log['type'] = step['transFormType']
            log['start_time'] = step['startTime'].strftime("%Y-%m-%d %H:%M:%S")
            log['end_time'] = step['endTime'].strftime("%Y-%m-%d %H:%M:%S")
            log['duration'] = int(step['duration'].total_seconds() * 1000) #milliseconds
            log['tgt_data'] = step['tgtData'] 
            log['row_count'] = step['tgtDF'].count()
            log['root_folder'] = WORKFLOW_ROOT_FOLDER_NAME
            log['src_data'] = step['srcData']
            log['meta_data'] = step['metaData']
    #rdd = sc.parallelize(stats)
    
    logSchema = StructType([
        StructField("run_id", StringType(), False),
        StructField("data_pipeline", StringType(), False),
        StructField("step_no", IntegerType(), False),
        StructField("step_name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("start_time", StringType(), False),
        StructField("end_time", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("tgt_data", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("bucket", StringType(), True),
        StructField("src_data", StringType(), True),
        StructField("meta_data", StringType(), True)])
    df = spark.createDataFrame(sc.parallelize(stats), logSchema)
    df.coalesce(1).write.save(wfDict['tgtPath'], format=wfDict['outputFormat'], header=True, mode='append', sep=',',quote ='"')
    print("Processing stats written to %s" %  wfDict['tgtPath'])
    wfDict['Saved'] = True
    return True

#-------------------------------
def deleteFolder(subFolderName):
    deleteFolder = WORKFLOW_FOLDER + subFolderName
    rootFolderName, prefix = getRootFolderFromWorkflowPath(deleteFolder)
    try:
        if (WORKFLOW_FOLDER.split("/")[0] == "hdfs"):
            run_cmd(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', prefix])
        else:
            run_cmd('rd /S /Q "{0}"'.format(deleteFolder.replace('/', '\\')))
    except:
        pass

#----------------------------------------------------------------------------
## Main functionality
if __name__ == "__main__":
    statusCode = "failed"
    CURRENT_ACTION = "loadWorkflow"
    ERROR_MESSAGE = ''

    workflowStartTime = datetime.datetime.now()
    if len(sys.argv) > 1:
        WORKFLOW_FOLDER = sys.argv[1]
    if len(sys.argv) > 2:
        #the seccond argument is the folder name list
        s = sys.argv[2]
        s = s.replace(' ', '').replace(':', ': ')
        FOLDER_NAME_DICT = yaml.load('{' + s + '}')
    #parsing workflow folder name to get the data pipeline name
    WORKFLOW_FOLDER =  WORKFLOW_FOLDER.replace('\\', '/').replace("//", '/')
    tokens =  WORKFLOW_FOLDER.split("/")
    if tokens[-1] == '':
        WORKFLOW_ROOT_FOLDER_NAME = tokens[2]
        DATA_PIPELINE_NAME = tokens[-2]
    else:
        WORKFLOW_ROOT_FOLDER_NAME = tokens[2]    
        DATA_PIPELINE_NAME = tokens[-1]  
    print("Workflow folder: %s\nrootFolderName replacement: %s" % (WORKFLOW_FOLDER, FOLDER_NAME_DICT ))
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext

    # s3 = boto3.session.Session().resource('s3')
    # sns =  boto3.client('sns')
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    spark.udf.register('udfConvertInt', convertInt, IntegerType())
    spark.udf.register('udfConvertDouble', convertDouble, DoubleType())
    allDF = {}
    completeWorkFlow = {}
    wfList = []
try:
    loadWorkflow(WORKFLOW_FOLDER, completeWorkFlow, wfList)
    #clean up noMatch folder from the previous run
    #deleteFolder(NO_MATCH)
    workflowDf = completeWorkFlow['master']
    loadDf = completeWorkFlow['load']
    calcDf = completeWorkFlow['calculation']
    joinDf = completeWorkFlow['joinTable']
    lookupFilterDf = completeWorkFlow['lookupFilter']
    filterDataDf = completeWorkFlow['filterData']
    selectTransformDf = completeWorkFlow['selectTransform']
    CURRENT_ACTION = "processStep"
    status = processStep(wfList, allDF)
    workflowEndTime = datetime.datetime.now()    
    message = 'Started at: %s, ended at %s, total time:%s\n\tData pipeline is: %s' % (workflowStartTime, workflowEndTime, workflowEndTime-workflowStartTime, WORKFLOW_FOLDER)

    if status == True:
        statusCode = "succeeded"
        message = 'Vortex ETL process %s.\n\t%s\n\tAll %d steps finished.' % (statusCode, message, len(wfList))
    else:
        statusCode = "failed"
        message = 'Vortex ETL process %s.\n\tFailed at: %s\n\t%s\n\t%s' % (statusCode, CURRENT_ACTION, message, ERROR_MESSAGE)
        LOGGER.error(message)

    # response = sns.publish(
    #             TargetArn=SNS_ARN_PREFIX + WORKFLOW_ROOT_FOLDER_NAME,
    #             Subject="Processed Complete - %s - %s, bucket=%s" % (statusCode, DATA_PIPELINE_NAME, WORKFLOW_ROOT_FOLDER_NAME),
    #             Message=message
    #         )     

except:
    workflowEndTime = datetime.datetime.now()
    message = 'Vortex ETL process failed.\n\tAll %d steps finished.\n\tStarted at: %s, ended at %s, total time:%s\n\tFailed at : %s' % (len(wfList), workflowStartTime, workflowEndTime, workflowEndTime-workflowStartTime, CURRENT_ACTION)
    message = "%s\n\t%s" % (message, ERROR_MESSAGE)
    LOGGER.error(message)
    # response = sns.publish(
    #             TargetArn=SNS_ARN_PREFIX + WORKFLOW_ROOT_FOLDER_NAME,
    #             Subject="Processed Complete - Failed - %s, bucket=%s" % (DATA_PIPELINE_NAME, WORKFLOW_ROOT_FOLDER_NAME),
    #             Message=message
    #         )
    raise
    






