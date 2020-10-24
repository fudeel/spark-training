from  pyspark.sql import SparkSession


spark  = SparkSession.builder.appName('basics').getOrCreate()
df = spark.read.json('people.json')

df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people WHERE age>30 ")


results.show()