from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

group_series_schema = StructType([
	StructField("type", StringType(), False),
	StructField("series_id", IntegerType(), False),
	StructField("group_id", IntegerType(), False),
])


def read_from_mysql(spark_session, table):
	mysql_connection = "jdbc:mysql://127.0.0.1:3306/search?user=test&password=test@123"
	return spark_session.read.jdbc(mysql_connection, table=table)


def write_to_mysql(df, table, mode):
	mysql_connection = "jdbc:mysql://127.0.0.1:3306/search?user=test&password=test@123"
	print("Writing to db")
	df.write.jdbc(mysql_connection, table=table, mode=mode)
	print("Write to db done.")


def is_not_empty(line):
	return len(line.strip()) != 0


def process_group_series(record):
	return json.loads(record)


def group_user_df(spark_session):
	return read_from_mysql(spark_session, "group_user").drop("id")


def series_entities_df(spark_session):
	return read_from_mysql(spark_session, "series_modules").drop("id")


def process_rdd(spark_session, rdd):
	gs_df = SparkSession(rdd.context).createDataFrame(rdd.filter(is_not_empty).map(process_group_series), group_series_schema)

	print(gs_df.head())
	se_df = series_entities_df(spark_session)
	gu_df = group_user_df(spark_session)

	df = gs_df.join(gu_df, ["group_id"]).join(se_df, ["series_id"]).select("user_id", "series_id", "module_id")
	write_to_mysql(df, "acl", "append")


def main():
	sc = SparkContext.getOrCreate()
	spark_session = SparkSession.builder.appName('abc').getOrCreate()
	sc.setLogLevel("WARN")
	ssc = StreamingContext(sc, 1)

	# kafka_stream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'test1': 1})
	# kafka_stream.map(lambda v: v[1]).foreachRDD(
	# 	lambda time_val, rdd: process_rdd(spark_session, rdd)
	# )

	# ssc.start()
	# ssc.awaitTermination()
	ssc.stop(stopGraceFully=True)


if __name__ == "__main__":
	main()