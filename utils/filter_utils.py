from pyspark.sql import SparkSession
from constants import *
import json


def is_not_empty(line):
	return len(line.strip()) != 0

def isEmpty(rdd):
	return rdd.count() == 0

def to_json(record):
	return json.loads(record)

def isDeleteOperation(rdd):

	return rdd["type"] == DELETE_OPERATION

def isInsertOperation(rdd):
 	return rdd["type"] == INSERT_OPERATION

def isUpdateOperation(rdd):
 	return rdd["type"] == UPDATE_OPERATION