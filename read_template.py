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

class dataobject:
	def __init__(self, dict):
		self.columns		= {}
		self.path			= dict['path']
		self.format			= dict['format']
		if 'mode' in dict:
			self.mode		= dict['mode']
		else:
			self.mode		= 'read'

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
##	Service methods
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
	bufDataobjects	= {}
	bufColumns		= {}

	f = open(inputFile, 'r')

	y_file = yaml.load(f)

	bufDict = readArray(y_file, 'pipelines')

	for curPpl in bufDict:
		objPipeline = pipeline()
		bufSteps = readArray(bufDict[curPpl], 'steps')

		for curStp in bufSteps:
			objStep = pipelinestep(bufSteps[curStp])

			bufDataobjects = readArray(bufSteps[curStp], 'sources')
			for curDS in bufDataobjects:
				objSource = dataobject(bufDataobjects[curDS])
				
				bufColumns = readArray(bufDataobjects[curDS], 'columns')
				for curCol in bufColumns:
					objColumn = column(bufColumns[curCol])
					objSource.columns[curCol] = objColumn

				objStep.sources[curDS] = objSource
			
			bufDataobjects = readArray(bufSteps[curStp], 'destinations')
			for curDS in bufDataobjects:
				objDest = dataobject(bufDataobjects[curDS])
				
				bufColumns = readArray(bufDataobjects[curDS], 'columns')
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

#------------------------------------------------
def createSchema(inputDataobject, inputDataset):

	res = StructType()

	for curColId in inputDataobject.columns:
		curCol = inputDataobject.columns[curColId]

		# If given position, schema is initialized from inputDataset
		if (curCol.position):
			if 'schema' in dir(inputDataset):
				curField = inputDataset.schema[curCol.position-1]
			else:
				curField = StructField(curColId, StringType())
#			curField.metadata
		else:
			fieldName = curCol.name
			if (curCol.datatype == 'int'):
				fieldDataType = IntegerType()
			elif (curCol.datatype == 'money'):
				fieldDataType = DecimalType(19, 4)
			elif (curCol.datatype == 'double'):
				fieldDataType = DoubleType()
			else:
				fieldDataType = StringType()
			
			curField = StructField(fieldName, fieldDataType)
		
		res.add(curField)

	return res;

#------------------------------------------------
def getDataFrame(inputDataobject):

	if (inputDataobject.format in ('csv', 'txt')):
		dataset = sc.textFile(inputDataobject.path).map( lambda x: x.replace('\"',''))
		dataschema = createSchema(inputDataobject, dataset)
		res = spark.createDataFrame(dataset, dataschema)
	else:
		res = spark.read.load(inputDataobject.path, format=inputDataobject.format)
		dataschema = createSchema(inputDataobject, res)
		
	return res, dataschema;

#------------------------------------------------
## Will do mapping of input table columns to output table columns
def mapDataFields(inputDataobject, inputDataschema, outputDataobject, transformations):

	res = ''
	for curSrcCol in inputDataschema:
		#	For now no mapping, to be modified soon
		sourceCol = curSrcCol.name
		destCol = re.sub('[ ,;{}()\\n\\t=]', '', curSrcCol.name)
		res += (",", "")[res==''] + "`%s` AS `%s`" % (sourceCol, destCol)
		
	return res;

#------------------------------------------------
def loadData(source, destination, transformations):

	sourceDF, sourceDataschema = getDataFrame(source)
	sourceDF.createOrReplaceTempView('sourceData')

	sourceDataFields = mapDataFields(source, sourceDataschema, destination, transformations)

	sourceData = spark.sql("SELECT %s FROM sourceData" % sourceDataFields)

	sourceData.coalesce(1).write.save(destination.path, format=destination.format, mode=destination.mode)

#	destinationDF = getDataFrame(destination, 'Write')

	return;

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

		loadData( \
			dctPipelines['pipeline 1'].steps['step 1'].sources['source_1'], \
			dctPipelines['pipeline 1'].steps['step 1'].destinations['destination_1'], \
			dctPipelines['pipeline 1'].steps['step 1'].transformations['transformation_1'] \
		)

	except:
		raise