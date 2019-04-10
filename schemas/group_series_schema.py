from include import *

group_series_schema = StructType([
	StructField("type", StringType(), False),
	StructField("series_id", IntegerType(), False),
	StructField("group_id", IntegerType(), False),
])
