# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("codingTest").getOrCreate()
# read csv with proper schema
df=spark.read.csv("/datasets/customers.csv", inferSchema=True, header=True)
#Apply filters
df_result=df.filter((col("purchase_amount")>100) & (col("age")>30))
# select required columns
df_result=df_result.select("customer_id","name","purchase_amount")
#display result
display(df_result)
