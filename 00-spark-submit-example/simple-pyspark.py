
#import necessary functions at the begining of the program
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#standard varialbes
APP_NAME = "My-Simple-PySpark-Program"


def main(spark_session):
    # Have you processing here
    # Create a datafram using range function which start with 1 and ends with 100 with 2 as step (1,3, 5 ...) 
    df =spark_session.range(1,100,2)
    df.printSchema()
    df.show()

if __name__ == '__main__':
    print("The program started... ")
    #Create The spark session by giving app name
    
    #https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    main(spark)
    print("Program is over")
