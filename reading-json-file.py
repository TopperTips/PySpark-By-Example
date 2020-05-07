spark.read.json("file:///tmp/tips/data/flight-data.json")
'''
DataFrame[DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string, count: bigint]
'''
tips_df = spark.read.json("file:///tmp/tips/data/flight-data.json")
tips_df.printSchema()
'''
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)
'''
tips_df.show()
'''
+--------------------+-------------------+-----+
|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+--------------------+-------------------+-----+
|       United States|            Romania|   15|
|       United States|            Croatia|    1|
|       United States|            Ireland|  344|
|               Egypt|      United States|   15|
|       United States|              India|   62|
|       United States|          Singapore|    1|
|       United States|            Grenada|   62|
|          Costa Rica|      United States|  588|
|             Senegal|      United States|   40|
|             Moldova|      United States|    1|
|       United States|       Sint Maarten|  325|
|       United States|   Marshall Islands|   39|
|              Guyana|      United States|   64|
|               Malta|      United States|    1|
|            Anguilla|      United States|   41|
|             Bolivia|      United States|   30|
|       United States|           Paraguay|    6|
|             Algeria|      United States|    4|
|Turks and Caicos ...|      United States|  230|
|       United States|          Gibraltar|    1|
+--------------------+-------------------+-----+
only showing top 20 rows
'''
tips_df = spark.read.format("json").option("mode","FAILFAST").option("inferSchema","true").load("file:///tmp/tips/data/flight-data.json")
tips_df.printSchema()
'''
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)
'''
tips_df.createOrReplaceTempVeiw("tips_table")
tips_df.createOrReplaceTempView("tips_table")
spark.sql("select count from tips_table").show(5)
'''
20/05/07 10:19:55 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
20/05/07 10:19:55 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
20/05/07 10:19:56 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
+-----+
|count|
+-----+
|   15|
|    1|
|  344|
|   15|
|   62|
+-----+
only showing top 5 rows
'''
