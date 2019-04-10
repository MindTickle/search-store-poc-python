from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.sql import SparkSession
from datastore.kafka_stream import *

def process_rdd(rdd, time_val=None):
	if isEmpty(rdd):
		print("Empty RDD: No operation required")
		return
	if time_val is not None:
		print("streaming time is : " + str(time_val))
	start_time = int(time.time())
	delete_rdd = rdd.map(lambda x : to_json(x)).filter(lambda x : isDeleteOperation(to_json(x)))
	update_rdd = rdd.map(lambda x : to_json(x)).filter(lambda x : isUpdateOperation(to_json(x)))
	insert_rdd = rdd.map(lambda x : to_json(x)).filter(lambda x : isInsertOperation(to_json(x)))

	if delete_rdd.count()!=0:
		print("Procesing delete rdds")
	if update_rdd.count() != 0:
		print("Processing update rdds")
        process_update_rdd(update_rdd)
	if insert_rdd.count() != 0:
		print("Procesing insert rdds")
		process_insert_rdd(insert_rdd)

	time_taken = int(time.time() - start_time)
	print("Time taken is: " + str(time_taken))

offsetRanges = []
def transformWithOffsetRanges(rdd):
	for o in rdd.offsetRanges():
		offsetRanges.append(o)
	return rdd

def getSparkSession():
	return SparkSession.builder.appName(APP_NAME).getOrCreate()

# In[56]:
def getStreamingContext():
	sc = SparkContext.getOrCreate()
	sc.setLogLevel("WARN")
	ssc = StreamingContext(sc, BATCH_DURATION)
	return ssc;

def main():
	ssc = getStreamingContext()
	kafka_stream = get_kafka_stream(ssc)
	kafka_stream.transform(transformWithOffsetRanges).map(lambda v: v[1]).foreachRDD(lambda time_val, rdd: process_rdd(rdd, time_val))
	ssc.start()
	ssc.awaitTermination()
	ssc.stop(stopGraceFully=True)



if __name__ == "__main__":
	main()

