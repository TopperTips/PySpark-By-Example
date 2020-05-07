#toppertips.com
#http://toppertips.com/pyspark-data-frame-rearrange-columns

#import necessary files
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


tips_list = [[1,"item 1" , 2],	[2 ,"item 2", 3],	[1 ,"item 3" ,2],	[3 ,"item 4" , 5]]
tips_schema = StructType(	[StructField("Item_ID",IntegerType(), True),StructField("Item_Name",StringType(), True ),	StructField("Quantity",IntegerType(), True)])

#create dataframe using spark session
tips_df = spark.createDataFrame(tips_list, tips_schema)

#the schema
tips_df.printSchema()
''' 
root
 |-- Item_ID: integer (nullable = true)
 |-- Item_Name: string (nullable = true)
 |-- Quantity: integer (nullable = true)
'''

#register as table
tips_df.createOrReplaceTempView("tips_table")

tips_new_df = spark.sql("select Item_Name, Item_ID, Quantity from tips_table")

tips_new_df.printSchema()

'''
root
 |-- Item_Name: string (nullable = true)
 |-- Item_ID: integer (nullable = true)
 |-- Quantity: integer (nullable = true)
'''
#using the dataframe approach

#create dataframe using spark session
tips_df = spark.createDataFrame(tips_list, tips_schema)

#use the select function from the dataframe
tips_new_df = tips_df.select("Item_Name","Item_ID","Quantity")


tips_new_df.printSchema()

