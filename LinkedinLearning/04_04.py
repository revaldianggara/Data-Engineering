##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "E:/Course/Data Engineer/Data_Engineering_Foundations/postgresql-42.7.4.jar") \
   .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
   .option("dbtable", "movies") \
   .option("user", "postgres") \
   .option("password", "12345678") \
   .option("driver", "org.postgresql.Driver") \
   .load()
   
##add code below
user_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
   .option("dbtable", "users") \
   .option("user", "postgres") \
   .option("password", "12345678") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the users dataframe
print(user_df.show())




