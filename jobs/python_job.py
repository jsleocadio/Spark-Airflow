# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonJob") \
    .getOrCreate()
#    .config("spark.executor.heartbeatInterval", "600s") \
#    .config("spark.network.timeout", "600s") \
    

text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Jefferson"

print(text)

words = spark.sparkContext().parallelize(text.split(" "))

print(words.collect())

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()