from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.sql import SparkSession
from processors.insert_rdd_processor import *
from processors.update_rdd_processor import *
from utils.constants import *


def get_kafka_stream(spark_streaming_context):
    topicPartion = TopicAndPartition(TOPIC,PARTITION)
    fromOffset = {topicPartion: long(START)}

    kafkaParams = {"metadata.broker.list": KAFKA_BROKER,'auto.offset.reset':'smallest',"group.id":"group_id_1"}

    kafka_stream = KafkaUtils.createDirectStream(spark_streaming_context, [TOPIC],kafkaParams = kafkaParams, fromOffsets= fromOffset)
    return kafka_stream