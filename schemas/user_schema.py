from include import *

user_schema = StructType([
	StructField("id", IdType(), False),
	StructField("name", StringType(), False),
	StructField("state", IntegerType(), False),
])
