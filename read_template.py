#--------------------------------------------------------------------------------------------------
##	Imports
import re
import sys
import yaml

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
	def __init__(self):
		self.steps			= {}

class pipelinestep:
	def __init__(self, dict):
		self.sources		= {}
		self.destinations	= {}
		self.transformations= {}

		self.name			= dict['name']
		self.description	= dict['description']
		self.action			= dict['action']

class dataset:
	def __init__(self, dict):
		self.columns			= {}
		self.path				= dict['path']
		self.format				= dict['format']

		self.hasHeaderRow		= dict.get('hasHeaderRow', False)
		self.isPersisted		= dict.get('isPersisted', False)
		self.delimiter			= dict.get('delimiter', ',')
		self.mode				= dict.get('mode', 'read')

class transformation:
	def __init__(self, dict):
		self.action			= dict['action']
		self.source_1		= dict['source 1']
		self.source_2		= dict['source 2']
		self.destination_1	= dict['destination 1']
		self.destination_2	= dict['destination 2']

class column:
	def __init__(self, dict):
		self.name			= dict['name']
		self.position		= dict['position']
		self.datatype		= dict['datatype']

#--------------------------------------------------------------------------------------------------
##	Service functions
#------------------------------------------------
def readArray(input, collection):
	res = {}

	if collection in input:
		for item in input[collection]:
			key = next(iter(item))
			res[key] = item[key]

	return res

#------------------------------------------------
def readConfig(inputFile):

	#	Result
	res = {}

	#	Locals
	bufPipelines	= {}
	bufSteps		= {}
	bufDatasets	= {}
	bufColumns		= {}

	f = open(inputFile, 'r')

	y_file = yaml.load(f)

	bufDict = readArray(y_file, 'pipelines')

	for curPpl in bufDict:
		objPipeline = pipeline()
		bufSteps = readArray(bufDict[curPpl], 'steps')

		for curStp in bufSteps:
			objStep = pipelinestep(bufSteps[curStp])

			bufDatasets = readArray(bufSteps[curStp], 'sources')
			for curDS in bufDatasets:
				objSource = dataset(bufDatasets[curDS])

				bufColumns = readArray(bufDatasets[curDS], 'columns')
				for curCol in bufColumns:
					objColumn = column(bufColumns[curCol])
					objSource.columns[curCol] = objColumn

				objStep.sources[curDS] = objSource

			bufDatasets = readArray(bufSteps[curStp], 'destinations')
			for curDS in bufDatasets:
				objDest = dataset(bufDatasets[curDS])

				bufColumns = readArray(bufDatasets[curDS], 'columns')
				for curCol in bufColumns:
					objColumn = column(bufColumns[curCol])
					objDest.columns[curCol] = objColumn

				objStep.destinations[curDS] = objDest

			bufTransformations = readArray(bufSteps[curStp], 'transformations')
			for curTf in bufTransformations:
				objTf = transformation(bufTransformations[curTf])
				objStep.transformations[curTf] = objTf

			objPipeline.steps[curStp] = objStep

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
		curInputColumn = inputDataset.columns[curInputColumnKey]
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
	for curInputColumn in inputDataset.columns.keys():
		if (curInputColumn in outputDataset.columns):
			outputColumns[curInputColumn] = { \
				'position': outputDataset.columns[curInputColumn].position, \
				'name': re.sub('[ ,;{}()\\n\\t=]', '', outputDataset.columns[curInputColumn].name), \
				'datatype': outputDataset.columns[curInputColumn].datatype \
			}	

	resultingColumns = {}
	#	Navigating through every column in source recordset to do proper mapping
	for curInputCol in inputDataschema:
		columnKeys = []
		if (curInputCol.name in inputColumns.keys()):
			columnKeys += inputColumns[curInputCol.name]
		if (inputDataschema.names.index(curInputCol.name)+1 in inputColumns.keys()):
			columnKeys += inputColumns[inputDataschema.names.index(curInputCol.name)+1]

		for curResColKey in columnKeys:
			if (curResColKey in outputColumns):
				resultingColumns[outputColumns[curResColKey]['position']] = "`%s` AS `%s`" % (curInputCol.name, outputColumns[curResColKey]['name'])

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

		#	TESTING. Temporary hardcoded call to save results
		destination = dctPipelines['pipeline 1'].steps['step 1'].destinations['destination_1']

		sourceData.coalesce(1).write \
			.option("header", destination.hasHeaderRow) \
			.option("delimiter", destination.delimiter) \
			.save(destination.path, format=destination.format, mode=destination.mode)


	except:
		raise