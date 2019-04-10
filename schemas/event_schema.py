from include import *

event_schema = StructType([
	StructField("database", StringType(), False),
	StructField("table", StringType(), False),
	StructField("type", StringType(), False),
	StructField("ts", IntegerType(), False),
	StructField("xid", IntegerType(), False),
	StructField("commit", BooleanType(), False),
	StructField("data", StructType(), False),
])