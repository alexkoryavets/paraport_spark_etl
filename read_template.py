#--------------------------------------------------------------------------------------------------
##	Imports
import sys

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import \
	StructType, StructField, DoubleType, IntegerType, StringType, DecimalType


#--------------------------------------------------------------------------------------------------
##	Constants
CONFIG_FILE = 'C:\\Users\\alexk\\Documents\\Code\\the_engine\\template.yml'
APP_NAME	= 'Spark_Metadata_ETL'

#--------------------------------------------------------------------------------------------------
##	Classes
class pipeline:
	def __init__(self, dict):
		self.steps			= {}

		bufSteps = readArray(dict, 'steps')
		for curStp in bufSteps:
			for curStpItem in bufSteps[curStp]:
				objStep = pipelinestep(curStpItem)
				self.steps[curStp] = objStep

class pipelinestep:
	def __init__(self, dict):
		self.sources		= {}
		self.destinations	= {}
		self.transformations= {}

		self.name			= dict.get('name')
		self.description	= dict.get('description')
		self.action			= dict.get('action')

		bufDatasets = readArray(dict, 'sources')
		for curDS in bufDatasets:
			for curDSItem in bufDatasets[curDS]:
				objSource = dataset(curDSItem)
				self.sources[curDS] = objSource

		bufDatasets = readArray(dict, 'destinations')
		for curDS in bufDatasets:
			for curDSItem in bufDatasets[curDS]:
				objDest = dataset(curDSItem)
				self.destinations[curDS] = objDest

		bufTransformations = readArray(dict, 'transformations')
		for curTf in bufTransformations:
			for curTfItem in bufTransformations[curTf]:
				objTf = transformation(curTfItem)
				self.transformations[curTf] = objTf

class dataset:
	def __init__(self, dict):
		self.columns			= {}
		self.path				= dict.get('path')
		self.format				= dict.get('format')

		self.hasHeaderRow		= dict.get('hasHeaderRow', False)
		self.isPersisted		= dict.get('isPersisted', False)
		self.delimiter			= dict.get('delimiter', ',')
		self.mode				= dict.get('mode', 'read')

		bufColumns = readArray(dict, 'columns')
		for curCol in bufColumns:
			for curColItem in bufColumns[curCol]:
				objColumn = column(curColItem)
				if (curCol not in self.columns):
					self.columns[curCol] = []
				self.columns[curCol].append(objColumn)


class transformation:
	def __init__(self, dict):
		self.action			= dict.get('action')
		self.source_1		= dict.get('source 1')
		self.source_2		= dict.get('source 2')
		self.destination_1	= dict.get('destination 1')
		self.destination_2	= dict.get('destination 2')

		self.query			= dict.get('query')

class column:
	def __init__(self, dict):
		self.name			= dict.get('name')
		self.position		= dict.get('position')
		self.datatype		= dict.get('datatype')

#--------------------------------------------------------------------------------------------------
##	Service functions
#------------------------------------------------
def readArray(input, collection):
	res = {}

	if collection in input:
		for item in input[collection]:
			key = next(iter(item))
			if (key not in res):
				res[key] = []
			res[key].append(item[key])

	return res

#------------------------------------------------
def readConfig(inputFile):

	# TEMP: Hardcoding configuration per Mei's request
	y_file = {}

	curPipeline = {}
	#	Step 1
	curPipelineStep = {'name': 'Load Some Table', 'description': 'Loading some table from HDFS into Hive', 'action': 'load'}

	curPipelineStepSource = {'path': 'C:\\Temp\\hdfs\\masterConfig_Completed.csv', 'format': 'csv', 'delimiter': ',', 'hasHeaderRow': True}
	curPipelineStepSourceColumns = [ \
		{'all_columns': {}}, \
		{'column_1': {'position': 1, 'name': None, 'datatype': 'int'}}, \
		{'column_2': {'position': None, 'name': 'stepName', 'datatype': 'string'}}, \
		{'column_desc': {'position': None, 'name': 'Entity Description', 'datatype': 'string'}}, \
		{'column value': {'position': None, 'name': 'stepNo', 'datatype': 'money'}}
	]
	curPipelineStepSource['columns'] = curPipelineStepSourceColumns
	curPipelineStep['sources'] = [{'source_1': curPipelineStepSource}]

	curPipelineStepTransformation = {'action': 'map', 'source 1': 'source_1.*', 'source 2': None, 'destination 1': 'destination_1.*', 'destination 2': None}
	curPipelineStep['transformations'] = [{'transformation_1': curPipelineStepTransformation}]

	curPipelineStepDestination = {}
	curPipelineStepDestinationColumns = [ \
		{'column_1': {'position': 1, 'name': 'Row ID', 'datatype': 'int'}}, \
		{'column_2': {'position': 2, 'name': 'Entity Name', 'datatype': 'string'}}, \
		{'column_desc': {'position': 3, 'name': 'Entity Description', 'datatype': 'string'}}, \
		{'column value': {'position': 4, 'name': 'Sum Qty', 'datatype': 'money'}}, \
		{'column value': {'position': 5, 'name': 'Sum Qty 2', 'datatype': 'money'}}
	]
	curPipelineStepDestination['columns'] = curPipelineStepDestinationColumns
	curPipelineStep['destinations'] = [{'destination_1': curPipelineStepDestination}]

	curPipeline['steps'] = [{'step 1': curPipelineStep}]

	#	Step 2
	curPipelineStep = {'name': 'Run SQL on Some Table', 'description': 'Running some simple aggregate', 'action': 'SQL'}

	curPipelineStepTransformation = {'action': 'sql', 'query': 'SELECT SUM(SumQty), AVG(SumQty2) FROM destination_1'}
	curPipelineStep['transformations'] = [{'transformation_1': curPipelineStepTransformation}]

	curPipelineStepDestination = {'path': 'c:\\temp\\hdfs\\output.csv', 'format': 'csv', 'delimiter': '|', 'hasHeaderRow': True, 'isPersisted': False, 'mode': 'overwrite'}
	curPipelineStep['destinations'] = [{'destination_2': curPipelineStepDestination}]

	curPipeline['steps'].append({'step 2': curPipelineStep})
	y_file['pipelines'] = []
	y_file['pipelines'] = [{'pipeline 1': curPipeline}]
	# /TEMP: 

	#	Result
	f = open(inputFile, 'r')

	res = {}

	bufDict = readArray(y_file, 'pipelines')

	for curPpl in bufDict:
		for curPplItem in bufDict[curPpl]:
			objPipeline = pipeline(curPplItem)
			res[curPpl] = objPipeline

	return res;

# #------------------------------------------------
# def createSchema(inputDataset, inputDataset):

# 	res = StructType()

# 	if (inputDataset.type in ('csv', 'txt') and inputDataset.hasHeaderRow == True):
# 		headerRow = inputDataset.first()

# 	for curColId in inputDataset.columns:
# 		curCol = inputDataset.columns[curColId]

# 		# If given position, schema is initialized from inputDataset
# 		if (curCol.position):
# 			if 'schema' in dir(inputDataset):
# 				curField = inputDataset.schema[curCol.position-1]
# 			else:
# 				curField = StructField('_' + str(curCol.position), StringType())
# #			curField.metadata
# 		else:
# 			fieldName = curCol.name
# 			if (curCol.datatype == 'int'):
# 				fieldDataType = IntegerType()
# 			elif (curCol.datatype == 'money'):
# 				fieldDataType = DecimalType(19, 4)
# 			elif (curCol.datatype == 'double'):
# 				fieldDataType = DoubleType()
# 			else:
# 				fieldDataType = StringType()

# 			curField = StructField(fieldName, fieldDataType)

# 		res.add(curField)

# 	return res;

#--------------------------------------------------------------------------------------------------
##	Base metadata to schema functions
#------------------------------------------------
def getDataschema(inputDataset, inputDataobject):

	res = StructType()

	if (inputDataset.format in ('csv', 'txt') and inputDataset.hasHeaderRow == True):
		headerRow = inputDataobject.first()

		for curCol in headerRow:
			curField = StructField(curCol, StringType())
			res.add(curField)
	else:
		for curCol in inputDataobject:
			curField = curCol
			res.add(curField)

	return res;

#------------------------------------------------
def getDataFrame(inputDataset):

	if (inputDataset.format in ('csv', 'txt')):
		txtRDDRows = sc.textFile(inputDataset.path).map( lambda x: x.replace('\"',''))
		txtRDDColumns = txtRDDRows.map(lambda row: row.split(inputDataset.delimiter))
		dataschema = getDataschema(inputDataset, txtRDDColumns)

		#	Removing all header row entires from the RDD
		if (inputDataset.hasHeaderRow):
			header = txtRDDRows.first()
			headerDf = txtRDDRows.filter(lambda l: header in l).distinct()
			txtRDDNoHeaderRows = txtRDDRows.subtract(headerDf)
		else:
			txtRDDNoHeaderRows = txtRDDRows

		txtRDDNoHeaderColumns = txtRDDNoHeaderRows.map(lambda row: row.split(inputDataset.delimiter))
		res = spark.createDataFrame(txtRDDNoHeaderColumns, dataschema)
	else:
		res = spark.read.load(inputDataset.path, format=inputDataset.format)
		dataschema = getDataschema(inputDataset, res)

	return res, dataschema;

#--------------------------------------------------------------------------------------------------
##	Transformation functions
#------------------------------------------------
## Will do mapping of input table columns to output table columns
def mapDataFields(inputDataset, inputDataschema, outputDataset, transformations):

	res = ''

	inputColumns = {}
	#	Placing source columns into dict: position/name->columnId to locate all inputs to be returned
	for curInputColumnKey in inputDataset.columns:
		for curInputColumn in inputDataset.columns[curInputColumnKey]:
			if (not (curInputColumn.position is None)):
				key = curInputColumn.position
			else:
				key = curInputColumn.name

			if (key in inputColumns):
				inputColumns[key].append(curInputColumnKey)
			else:
				inputColumns[key] = [curInputColumnKey]
	
	outputColumns = {}
	#	Placing destination columns into dict columnId->dict(position,name,datatype) to do mapping from source
	for curOutputColumnKey in outputDataset.columns:
		for curOutputColumn in outputDataset.columns[curOutputColumnKey]:
			if (curOutputColumnKey in inputDataset.columns):
				curOutputColumnObject = { \
					'position': curOutputColumn.position, \
					'name': curOutputColumn.name \
						.replace(' ', '') \
						.replace(',', '') \
						.replace(';', '') \
						.replace('{', '') \
						.replace('}', '') \
						.replace('(', '') \
						.replace(')', '') \
						.replace('\n', '') \
						.replace('\t', '') \
						.replace('=', ''), \
					'datatype': curOutputColumn.datatype \
				}
				if (curOutputColumnKey not in outputColumns):
					outputColumns[curOutputColumnKey] = []
				outputColumns[curOutputColumnKey].append(curOutputColumnObject)

	resultingColumns = {}
	#	Navigating through every column in source recordset to do proper mapping
	for curInputCol in inputDataschema:
		columnKeys = []
		if (curInputCol.name in inputColumns.keys()):
			columnKeys += inputColumns[curInputCol.name]
		if (inputDataschema.names.index(curInputCol.name)+1 in inputColumns.keys()):
			columnKeys += inputColumns[inputDataschema.names.index(curInputCol.name)+1]

		for curResColKey in columnKeys:
			for curOutputColumn in outputColumns[curResColKey]:
				resultingColumns[curOutputColumn['position']] = "`%s` AS `%s`" % (curInputCol.name, curOutputColumn['name'])

	#	Assemblying resulting recordset in a row with all fields
	res = ", ".join([k[1] for k in sorted(resultingColumns.items())])


	return res;

#------------------------------------------------
def loadData(pipelineName, stepName, sourceName, destinationName, transformationsName):

	sourceObj = dctPipelines[pipelineName].steps[stepName].sources[sourceName]
	destinationObj = dctPipelines[pipelineName].steps[stepName].destinations[destinationName]
	transformationsObj = dctPipelines[pipelineName].steps[stepName].transformations[transformationsName]

	sourceDF, sourceDataschema = getDataFrame(sourceObj)
	sourceDF.createOrReplaceTempView(destinationName)

	dataFieldsMapping = mapDataFields(sourceObj, sourceDataschema, destinationObj, transformationsObj)

	res = spark.sql("SELECT %s FROM %s" % (dataFieldsMapping, destinationName))

#	destinationDF = getDataFrame(destination, 'Write')

	return res;

#------------------------------------------------
def runSQL(pipelineName, stepName, sourceName, destinationName, transformationsName):

	# sourceObj = dctPipelines[pipelineName].steps[stepName].sources[sourceName]
	# destinationObj = dctPipelines[pipelineName].steps[stepName].destinations[destinationName]
	transformationsObj = dctPipelines[pipelineName].steps[stepName].transformations[transformationsName]

	# sourceDF, sourceDataschema = getDataFrame(sourceObj)
	# sourceDF.createOrReplaceTempView(destinationName)

	# dataFieldsMapping = mapDataFields(sourceObj, sourceDataschema, destinationObj, transformationsObj)

	res = spark.sql(transformationsObj.query)

#	destinationDF = getDataFrame(destination, 'Write')

	return res;

#--------------------------------------------------------------------------------------------------
##	Spark specific functions
#------------------------------------------------
def initializeSpark():
	spark = SparkSession \
		.builder \
		.appName(APP_NAME) \
		.enableHiveSupport() \
		.getOrCreate()

	return spark;

#--------------------------------------------------------------------------------------------------
##	Main
if __name__ == "__main__":

	#	Locals
	dctPipelines	= {}

	if len(sys.argv) > 1:
		CONFIG_FILE = sys.argv[1]

	try:
		dctPipelines = readConfig(CONFIG_FILE)

		spark = initializeSpark()
		sc = spark.sparkContext

		#	TESTING. Temporary hardcoded call for one specific transformation
		sourceData = loadData( \
			'pipeline 1',
			'step 1',
			'source_1',
			'destination_1',
			'transformation_1'
			# dctPipelines['pipeline 1'].steps['step 1'].sources['source_1'], \
			# dctPipelines['pipeline 1'].steps['step 1'].destinations['destination_1'], \
			# dctPipelines['pipeline 1'].steps['step 1'].transformations['transformation_1'] \
		)

		#	TESTING. Temporary hardcoded call to create in-memory dataframe
		sourceData.createOrReplaceTempView('destination_1')

		#	TESTING. Temporary hardcoded call for one specific transformation
		sqlData = runSQL( \
			'pipeline 1',
			'step 2',
			'source_1',
			'destination_1',
			'transformation_1'
			# dctPipelines['pipeline 1'].steps['step 1'].sources['source_1'], \
			# dctPipelines['pipeline 1'].steps['step 1'].destinations['destination_1'], \
			# dctPipelines['pipeline 1'].steps['step 1'].transformations['transformation_1'] \
		)

		#	TESTING. Temporary hardcoded call to define results saving destination
		destination = dctPipelines['pipeline 1'].steps['step 2'].destinations['destination_2']

		#	TESTING. Temporary hardcoded call to materialize results
		sqlData.coalesce(1).write \
			.option("header", destination.hasHeaderRow) \
			.option("delimiter", destination.delimiter) \
			.save(destination.path, format=destination.format, mode=destination.mode)


	except:
		raise