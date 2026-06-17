"""
豆瓣电影评分数据集 - 数据清洗与Spark SQL统计分析
=================================================
包含：A-1 数据清洗 + A-2 Spark SQL 统计分析

使用方法：
  1. 在 CCE Spark Operator 上运行（推荐）：
     kubectl apply -f sparkapplication-analysis.yaml

  2. 在本地运行（需要安装 pyspark）：
     pip install pyspark pandas
     python analysis.py

  3. 本地 Pandas 测试版（不需要 pyspark）：
     python local_test.py

注意：CSV 文件中 summary 字段包含换行符和引号，
      Spark 读取时需要设置 multiLine=True 和 quote 参数
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, avg, stddev, min as spark_min, max as spark_max, lit, trim
import os
import sys

# ============================================================
# 初始化 SparkSession
# ============================================================
spark = SparkSession.builder \
    .appName("DoubanMovieAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 设置日志级别，减少干扰信息
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("豆瓣电影评分数据集 - 数据清洗与统计分析")
print("=" * 70)

# ============================================================
# A-1: 数据清洗
# ============================================================
print("\n" + "=" * 70)
print("【A-1】数据清洗")
print("=" * 70)

# 1. 加载数据
# 自动检测运行环境，选择合适的路径
script_dir = os.path.dirname(os.path.abspath(__file__))
local_csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "douban_movies.csv")

# 如果本地文件不存在，尝试当前目录
if not os.path.exists(local_csv_path):
    local_csv_path = os.path.join(os.getcwd(), "douban_movies.csv")
if not os.path.exists(local_csv_path):
    local_csv_path = os.path.join(script_dir, "douban_movies.csv")

print(f"\n[数据加载] 尝试加载数据...")
print(f"  本地路径: {local_csv_path}")
print(f"  文件存在: {os.path.exists(local_csv_path)}")

if os.path.exists(local_csv_path):
    # 本地模式：使用 file:// 协议
    # 注意：CSV 中 summary 字段包含换行符，需要使用 multiLine=True
    local_path = "file:///" + local_csv_path.replace("\\", "/")
    print(f"  使用本地路径: {local_path}")
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(local_path)
else:
    # CCE 集群模式：尝试 OBS 路径
    data_path = "s3a://<YOUR_BUCKET>/douban_movies.csv"
    print(f"  使用 OBS 路径: {data_path}")
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(data_path)

total_rows = df.count()
print(f"\n数据集行数: {total_rows}")
print(f"数据集列数: {len(df.columns)}")

# 打印 Schema
print("\n[Schema]")
df.printSchema()

# 打印前5行（只显示关键字段，避免summary过长）
print("\n[前5行数据]")
df.select("movie_id", "title", "year", "rating_score", "rating_count", "genres", "countries", "directors").show(5, truncate=False)

# 2. 统计各字段缺失值比例
# 注意：isnan 只能用于数值列，这里用 isNull 替代
print("\n[缺失值统计]")
print(f"{'字段名':<25} {'缺失数':<10} {'缺失比例':<10}")
print("-" * 45)
for c in df.columns:
    # 统计 NULL 和空字符串
    null_count = df.filter(col(c).isNull() | (trim(col(c)) == "")).count()
    ratio = null_count / total_rows * 100
    print(f"{c:<25} {null_count:<10} {ratio:.2f}%")

# 3. 缺失值处理策略
print("\n[缺失值处理]")

# 策略1: year 字段 - 使用中位数填充
year_median = df.filter(col("year").isNotNull() & (trim(col("year")) != "")) \
    .agg(avg(col("year").cast("double"))).collect()[0][0]
print(f"  策略1 - 年份中位数填充: {year_median:.0f}")

# 策略2: rating_score 字段 - 使用平均值填充
avg_rating = df.filter(col("rating_score").isNotNull() & (trim(col("rating_score")) != "")) \
    .agg(avg(col("rating_score").cast("double"))).collect()[0][0]
print(f"  策略2 - 评分平均值填充: {avg_rating:.2f}")

# 策略3: genres/countries/directors - 填充为"未知"
# 策略4: summary - 填充为"暂无简介"

# 执行清洗
df_clean = df \
    .fillna({"year": str(int(year_median))}) \
    .fillna({"rating_score": str(round(avg_rating, 1))}) \
    .fillna({"genres": "未知"}) \
    .fillna({"countries": "未知"}) \
    .fillna({"directors": "未知"}) \
    .fillna({"summary": "暂无简介"}) \
    .fillna({"original_title": ""}) \
    .fillna({"rating_count": "0"}) \
    .fillna({"collect_count": "0"})

# 删除 rating_score 为 0 的记录（0分表示无评分，非有效数据）
before_drop = df_clean.count()
df_clean = df_clean.filter(col("rating_score").cast("float") > 0)
after_drop = df_clean.count()
print(f"\n  删除评分=0的记录: {before_drop} -> {after_drop} (删除 {before_drop - after_drop} 条)")

# 转换数据类型
# 注意：year 字段是 "1994.0" 格式，需要先转 float 再转 int
# rating_count 和 collect_count 可能包含空字符串，用 try_cast 处理
df_clean = df_clean \
    .withColumn("year_int", col("year").cast("float").cast("int")) \
    .withColumn("rating_score_float", col("rating_score").cast("float")) \
    .withColumn("rating_count_int", col("rating_count").cast("int")) \
    .withColumn("collect_count_int", col("collect_count").cast("int"))


clean_count = df_clean.count()
print(f"\n清洗后总行数: {clean_count}")

# 4. 清洗前后对比
print("\n[清洗前后对比]")
print(f"  清洗前行数: {total_rows}")
print(f"  清洗后行数: {clean_count}")
print(f"  减少行数: {total_rows - clean_count}")

# 清洗后基本统计信息
print("\n[清洗后各字段基本统计信息]")
df_clean.select(
    avg("rating_score_float").alias("平均评分"),
    stddev("rating_score_float").alias("评分标准差"),
    spark_min("rating_score_float").alias("最低评分"),
    spark_max("rating_score_float").alias("最高评分"),
    avg("year_int").alias("平均年份"),
    spark_min("year_int").alias("最早年份"),
    spark_max("year_int").alias("最晚年份"),
    avg("rating_count_int").alias("平均评分人数"),
    spark_min("rating_count_int").alias("最少评分人数"),
    spark_max("rating_count_int").alias("最多评分人数"),
    avg("collect_count_int").alias("平均收藏数"),
    spark_min("collect_count_int").alias("最少收藏数"),
    spark_max("collect_count_int").alias("最多收藏数")
).show()

# ============================================================
# A-2: Spark SQL 统计分析
# ============================================================
print("\n" + "=" * 70)
print("【A-2】Spark SQL 统计分析")
print("=" * 70)

# 注册为临时视图
df_clean.createOrReplaceTempView("movies")

# ----------------------------------------------------------
# 查询1: GROUP BY 聚合 - 按电影类型统计平均评分和数量
# ----------------------------------------------------------
print("\n" + "-" * 70)
print("【查询1】GROUP BY 聚合：各电影类型的平均评分与数量")
print("-" * 70)

query1 = """
SELECT 
    genres,
    COUNT(*) AS movie_count,
    ROUND(AVG(rating_score_float), 2) AS avg_rating,
    ROUND(AVG(rating_count_int), 0) AS avg_rating_count,
    ROUND(AVG(collect_count_int), 0) AS avg_collect_count
FROM movies
GROUP BY genres
ORDER BY movie_count DESC
LIMIT 20
"""
result1 = spark.sql(query1)
result1.show(20, truncate=False)
print(f"查询1结果行数: {result1.count()}")

# ----------------------------------------------------------
# 查询2: ORDER BY Top-N - 评分最高的Top-20电影
# ----------------------------------------------------------
print("\n" + "-" * 70)
print("【查询2】ORDER BY Top-N：评分最高的Top-20电影（评分人数>10000）")
print("-" * 70)

query2 = """
SELECT 
    title,
    year_int,
    rating_score_float,
    rating_count_int,
    genres,
    countries,
    directors
FROM movies
WHERE rating_count_int > 10000
ORDER BY rating_score_float DESC, rating_count_int DESC
LIMIT 20
"""
result2 = spark.sql(query2)
result2.show(20, truncate=False)

# ----------------------------------------------------------
# 查询3: 时间维度趋势分析 - 每年电影数量、平均评分趋势
# ----------------------------------------------------------
print("\n" + "-" * 70)
print("【查询3】时间维度趋势分析：每年电影数量与平均评分变化趋势")
print("-" * 70)

query3 = """
SELECT 
    year_int,
    COUNT(*) AS movie_count,
    ROUND(AVG(rating_score_float), 2) AS avg_rating,
    ROUND(AVG(rating_count_int), 0) AS avg_rating_count,
    ROUND(AVG(collect_count_int), 0) AS avg_collect_count,
    ROUND(STDDEV(rating_score_float), 2) AS rating_stddev
FROM movies
WHERE year_int >= 1900 AND year_int <= 2025
GROUP BY year_int
ORDER BY year_int
"""
result3 = spark.sql(query3)
result3.show(50, truncate=False)
print(f"查询3结果行数: {result3.count()}")

# ----------------------------------------------------------
# 查询4: 窗口函数 - 每年评分最高的Top-5电影
# ----------------------------------------------------------
print("\n" + "-" * 70)
print("【查询4】窗口函数：每年评分最高的Top-5电影（评分人数>5000）")
print("-" * 70)

query4 = """
WITH ranked_movies AS (
    SELECT 
        title,
        year_int,
        rating_score_float,
        rating_count_int,
        genres,
        directors,
        ROW_NUMBER() OVER (PARTITION BY year_int ORDER BY rating_score_float DESC, rating_count_int DESC) AS rank
    FROM movies
    WHERE year_int >= 2000 AND year_int <= 2023 AND rating_count_int > 5000
)
SELECT *
FROM ranked_movies
WHERE rank <= 5
ORDER BY year_int DESC, rank
"""
result4 = spark.sql(query4)
result4.show(50, truncate=False)
print(f"查询4结果行数: {result4.count()}")

# ----------------------------------------------------------
# 查询5: JOIN 操作 - 自关联分析（高分电影与平均水平的对比）
# ----------------------------------------------------------
print("\n" + "-" * 70)
print("【查询5】JOIN操作：高分电影（评分≥9.0）与各类型平均水平的对比")
print("-" * 70)

query5 = """
WITH genre_avg AS (
    SELECT 
        genres,
        AVG(rating_score_float) AS genre_avg_rating,
        AVG(rating_count_int) AS genre_avg_count
    FROM movies
    GROUP BY genres
)
SELECT 
    m.title,
    m.genres,
    m.rating_score_float,
    ROUND(ga.genre_avg_rating, 2) AS genre_avg_rating,
    ROUND(m.rating_score_float - ga.genre_avg_rating, 2) AS rating_diff,
    m.rating_count_int,
    ROUND(ga.genre_avg_count, 0) AS genre_avg_count,
    m.year_int
FROM movies m
JOIN genre_avg ga ON m.genres = ga.genres
WHERE m.rating_score_float >= 9.0 AND m.rating_count_int > 10000
ORDER BY m.rating_score_float DESC
LIMIT 30
"""
result5 = spark.sql(query5)
result5.show(30, truncate=False)

print("\n" + "=" * 70)
print("分析完成！")
print("=" * 70)

spark.stop()
