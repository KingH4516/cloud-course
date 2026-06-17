"""
wordcount.py - PySpark 入门示例作业
用于验证 Spark Operator 环境部署是否成功
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# 读取示例文本（OBS 路径由教师提供）
# 如果无法访问 OBS，可以使用本地文件或内置示例
lines = spark.sparkContext.textFile("s3a://<BUCKET>/sample.txt")

word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)  # type: ignore[arg-type]
         .sortBy(lambda x: x[1], ascending=False)  # type: ignore[arg-type]
)

print("Top 10 words:", word_counts.take(10))
spark.stop()
