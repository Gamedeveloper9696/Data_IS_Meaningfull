# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("null").getOrCreate()
df=spark.read.csv("/datasets/customers_raw.csv", inferSchema=True, header=True)
df=df.dropna(subset=["customer_id", "email"])
display(df)

#Copy the starter code or load the file path available in the problem statement 

# Display the final DataFrame using the display() function.
#display(df_result)


