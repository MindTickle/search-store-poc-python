from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.sql import SparkSession
import json
from utils.filter_utils import *
from schemas.event_schema import *
import time


def process_insert_rdd(rdds):
	basic_start_time = time.time()
	try:
		event_df = SparkSession(rdds.context).createDataFrame(rdds.filter(is_not_empty).map(to_json).map(lambda x : x['data']).map(lambda x : x['document']))
		print(event_df.show())

        # db writer logic is put here that is application dependent
        # writer logic to db here
	except Exception as e:
		print("Error in parsing of event schema")
		print(e)

	time_taken = int(time.time() - basic_start_time)
	print("Time taken for insert rdd is: " + str(time_taken))