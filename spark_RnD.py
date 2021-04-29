# -*- coding: utf-8 -*-
#pip install pyspark
#https://dataplatform.cloud.ibm.com/exchange/public/entry/view/5ad1c820f57809ddec9a040e37b2bd55


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()

df = spark.read.csv("/Users/baotran/Desktop/SalesJan2009.csv")
df.createOrReplaceTempView("temp")
df.show()

count_df = spark.sql("""
SELECT count(*) row_count
FROM temp
                     """)

count_df.show()