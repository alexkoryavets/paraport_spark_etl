#--------------------------------------------------------------------------------------------------
##	Imports
import sys
import yaml

#import pyspark
#from pyspark import SparkConf, SparkContext
# from pyspark.sql import SQLContext, SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType


#--------------------------------------------------------------------------------------------------
##	Constants
CONFIG_FILE = 'C:\\Users\\alexk\\Documents\\Code\\the_engine\\template.yml'
APP_NAME	= 'Spark_Metadata_ETL'

#--------------------------------------------------------------------------------------------------
##	Classes
class pipeline:
	def __init__(self):
		self.steps			= {}
	pass

class pipelinestep:
	def __init__(self, dct):
		self.sources		= {}
		self.destinations	= {}
		self.transformations= {}

		self.name			= dict['name']
		self.description	= dict['description']
		self.action			= dict['action']
	pass

class dataobject:
	def __init__(self, dict):
		self.columns		= {}
		self.path			= dict['path']
		self.types			= dict['type']
	pass

class transformation:
	def __init__(self):
		
	pass

#--------------------------------------------------------------------------------------------------
##	Service methods
#------------------------------------------------
def readArray(input, collection):
	res = {}

	if collection in input:
		for item in input[collection]:
			key = next(iter(item))
			res[key] = item[key]
	
	return res;

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
				objSource.columns = readArray(bufDataobjects[curDS], 'columns')
				objStep.sources[curDS] = objSource
			
			bufDataobjects = readArray(bufSteps[curStp], 'destinations')
			for curDS in bufDataobjects:
				objDest = dataobject(bufDataobjects[curDS])
				objDest.columns = readArray(bufDataobjects[curDS], 'columns')
				objStep.destinations[curDS] = objDest

			objPipeline.steps[curStp] = objStep
			
		res[curPpl] = objPipeline
	
	return res;

#------------------------------------------------
def getDataFrame(source):

#	source.

	return res;

#------------------------------------------------
def loadData(source, destination):

	sourceDF = getDataFrame(source)

	return;

#------------------------------------------------
def initializeSpark():
	spark = SparkSession \
		.builder \
		.appName(APP_NAME) \
		.enableHiveSupport() \
		.getOrCreate()
	sc = spark.sparkContext

	return sc;

#--------------------------------------------------------------------------------------------------
##	Main
if __name__ == "__main__":

	#	Locals
	dctPipelines	= {}

	if len(sys.argv) > 1:
		CONFIG_FILE = sys.argv[1]

	try:
		dctPipelines = readConfig(CONFIG_FILE)
		print(len(dctPipelines))

		sc = initializeSpark()

	except:
		raise