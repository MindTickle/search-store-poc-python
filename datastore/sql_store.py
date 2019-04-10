from pyspark.sql import SparkSession
import json

def read_from_mysql(spark_session, table):
	mysql_connection = "jdbc:mysql://127.0.0.1:3306/search?user=test&password=test@123"
	return spark_session.read.jdbc(mysql_connection, table=table)


def write_to_mysql(df, table, mode):
	mysql_connection = "jdbc:mysql://127.0.0.1:3306/search?user=test&password=test@123"
	print("Writing to db")
	df.write.jdbc(mysql_connection, table=table, mode=mode)
	print("Write to db done.")

def group_user_df(spark_session):
	return read_from_mysql(spark_session, "group_user").drop("id")

def series_entities_df(spark_session):
	return read_from_mysql(spark_session, "series_modules").drop("id")

def updateTableSql(df, name, index_columns=[]):
	columns = df.columns

	collist = ', '.join(['"{}"'.format(x) for x in columns])

	conflict = ', '.join(['"{}"'.format(x) for x in index_columns])

	excluded = ', '.join('"{colname}" = excluded."{colname}"'.format(colname=x) for x in columns)

	where = ''

	sql = """ INSERT INTO {name} ({collist})
        VALUES %s
        ON DUPLICATE KEY UPDATE
        {excluded}
        WHERE {where}
          """.format(
		name=name,
		collist=collist,
		conflict=conflict,
		excluded=excluded,
		where=where
	)

	return sql

