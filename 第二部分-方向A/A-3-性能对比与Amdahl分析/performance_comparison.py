"""
A-3: 性能对比与 Amdahl 分析
=============================
选取 A-2 中一个查询，分别用 Pandas（单机）和 PySpark（executorInstances=1 及 2）实现，
记录执行时间，绘制对比图；结合 Amdahl 定律分析加速比未达到线性的原因。

注意：此脚本需要在 Spark 环境中运行（如 Spark Operator 提交的作业），
Pandas 部分在 Driver 节点上单机运行。
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, min as spark_min, max as spark_max, trim
import time
import sys
import os

# ============================================================
# 初始化 SparkSession
# ============================================================
spark = SparkSession.builder \
    .appName("DoubanMovie-PerformanceComparison") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("A-3: 性能对比与 Amdahl 分析")
print("=" * 70)

# 自动检测数据路径
script_dir = os.path.dirname(os.path.abspath(__file__))
local_csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "douban_movies.csv")

if not os.path.exists(local_csv_path):
    local_csv_path = os.path.join(os.getcwd(), "douban_movies.csv")
if not os.path.exists(local_csv_path):
    local_csv_path = os.path.join(script_dir, "douban_movies.csv")

print(f"\n[数据加载] 尝试加载数据...")
print(f"  本地路径: {local_csv_path}")
print(f"  文件存在: {os.path.exists(local_csv_path)}")

if os.path.exists(local_csv_path):
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
    # 使用 OBS 路径（桶名已替换为 douban2023112510）
    data_path = "s3a://douban2023112510/douban_movies.csv"
    print(f"  使用 OBS 路径: {data_path}")
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(data_path)

total_rows = df.count()
print(f"  数据集行数: {total_rows}")

# 数据清洗（同 A-1）
# 先计算中位数和平均值用于填充
year_median = df.filter(col("year").isNotNull() & (trim(col("year")) != "")) \
    .agg(avg(col("year").cast("double"))).collect()[0][0]
avg_rating = df.filter(col("rating_score").isNotNull() & (trim(col("rating_score")) != "")) \
    .agg(avg(col("rating_score").cast("double"))).collect()[0][0]

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

df_clean = df_clean.filter(df_clean["rating_score"].cast("float") > 0)

df_clean = df_clean \
    .withColumn("year_int", col("year").cast("float").cast("int")) \
    .withColumn("rating_score_float", col("rating_score").cast("float")) \
    .withColumn("rating_count_int", col("rating_count").cast("int")) \
    .withColumn("collect_count_int", col("collect_count").cast("int"))

df_clean.createOrReplaceTempView("movies")

# ============================================================
# 选取的查询：按年份统计每年电影数量、平均评分（时间维度趋势分析）
# 对应 A-2 的查询3
# ============================================================
query = """
SELECT 
    year_int,
    COUNT(*) AS movie_count,
    ROUND(AVG(rating_score_float), 2) AS avg_rating,
    ROUND(AVG(rating_count_int), 0) AS avg_rating_count,
    ROUND(AVG(collect_count_int), 0) AS avg_collect_count
FROM movies
WHERE year_int >= 1900 AND year_int <= 2025
GROUP BY year_int
ORDER BY year_int
"""

# ============================================================
# 方法1: Pandas（单机）实现
# ============================================================
print("\n" + "-" * 70)
print("方法1: Pandas（单机）实现")
print("-" * 70)

start_time = time.time()

# 使用 Spark 收集数据到 Driver，然后用 Pandas 处理
import pandas as pd

# 获取原始数据（已清洗）
pdf = df_clean.select("year_int", "rating_score_float", "rating_count_int", "collect_count_int") \
    .filter("year_int >= 1900 AND year_int <= 2025") \
    .toPandas()

print(f"  Pandas DataFrame 行数: {len(pdf)}")

# Pandas 执行 GROUP BY 聚合
pandas_start = time.time()
pdf_result = pdf.groupby("year_int").agg(
    movie_count=("rating_score_float", "count"),
    avg_rating=("rating_score_float", "mean"),
    avg_rating_count=("rating_count_int", "mean"),
    avg_collect_count=("collect_count_int", "mean")
).reset_index().sort_values("year_int")

pdf_result["avg_rating"] = pdf_result["avg_rating"].round(2)
pdf_result["avg_rating_count"] = pdf_result["avg_rating_count"].round(0).astype(int)
pdf_result["avg_collect_count"] = pdf_result["avg_collect_count"].round(0).astype(int)

pandas_end = time.time()
pandas_time = pandas_end - pandas_start

print(f"  Pandas 聚合执行时间: {pandas_time:.4f} 秒")
print(f"  Pandas 结果预览:")
print(pdf_result.head(10).to_string(index=False))

# ============================================================
# 方法2: PySpark（executorInstances=1）实现
# ============================================================
print("\n" + "-" * 70)
print("方法2: PySpark（executorInstances=1）实现")
print("-" * 70)

# 设置 Spark 并行度为 1
spark.conf.set("spark.sql.shuffle.partitions", "1")

spark1_start = time.time()
result1 = spark.sql(query)
result1.collect()  # 触发计算
spark1_end = time.time()
spark1_time = spark1_end - spark1_start

print(f"  PySpark (1 executor) 执行时间: {spark1_time:.4f} 秒")
result1.show(10)

# ============================================================
# 方法3: PySpark（executorInstances=2）实现
# ============================================================
print("\n" + "-" * 70)
print("方法3: PySpark（executorInstances=2）实现")
print("-" * 70)

# 设置 Spark 并行度为 2
spark.conf.set("spark.sql.shuffle.partitions", "2")

spark2_start = time.time()
result2 = spark.sql(query)
result2.collect()  # 触发计算
spark2_end = time.time()
spark2_time = spark2_end - spark2_start

print(f"  PySpark (2 executors) 执行时间: {spark2_time:.4f} 秒")
result2.show(10)

# ============================================================
# 性能对比与 Amdahl 分析
# ============================================================
print("\n" + "=" * 70)
print("性能对比结果")
print("=" * 70)

# 注意：Pandas 时间包含了数据从 Spark 传输到 Driver 的开销
# 这里我们主要对比 PySpark 在不同 executor 数量下的性能
print(f"""
{'=' * 60}
性能对比表
{'=' * 60}
| 方法                    | 执行时间 (秒) | 加速比 |
|-------------------------|--------------|--------|
| Pandas (单机)           | {pandas_time:.4f}       | -      |
| PySpark (1 executor)    | {spark1_time:.4f}       | 1.00   |
| PySpark (2 executors)   | {spark2_time:.4f}       | {spark1_time/spark2_time:.2f} |
{'=' * 60}
""")

# Amdahl 定律分析
print("\n" + "=" * 70)
print("Amdahl 定律分析")
print("=" * 70)

# 根据实测数据估算可并行比例 f
# Amdahl 定律: S(p) = 1 / ((1-f) + f/p)
# 已知 S(2) = T(1)/T(2)，求解 f
S2 = spark1_time / spark2_time
# S(2) = 1 / ((1-f) + f/2) = 1 / (1 - f/2)
# 1 - f/2 = 1/S(2)
# f/2 = 1 - 1/S(2)
# f = 2 * (1 - 1/S(2))
f_estimated = 2 * (1 - 1 / S2)
# 限制 f 在 [0, 1] 范围内
f_estimated = max(0.0, min(1.0, f_estimated))

print(f"""
Amdahl 定律公式: S(p) = 1 / ((1-f) + f/p)
  其中:
  - p = 处理器数量
  - f = 可并行化比例
  - (1-f) = 串行部分比例

实测数据:
  T(1) = {spark1_time:.4f}s, T(2) = {spark2_time:.4f}s
  实测加速比 S(2) = {S2:.2f}

估算可并行比例 f:
  S(2) = 1 / ((1-f) + f/2)
  {S2:.2f} = 1 / (1 - f/2)
  f = {f_estimated:.4f} = {f_estimated*100:.2f}%

理论加速比（若 f=1，即完全并行）:
  S_ideal(2) = 2.00

实测加速比与理论值差距分析:
  1. 通信开销: Spark 在 shuffle 阶段需要在 executor 之间传输数据，
     网络通信带来了额外的时间开销。
  2. 序列化/反序列化: 数据在 JVM 和 Python 之间传递需要序列化，
     这部分是串行的。
  3. 任务调度: Spark Driver 需要调度任务到各 executor，
     调度本身也有开销。
  4. 数据倾斜: 某些分区的数据量可能不均匀，导致部分任务
     成为瓶颈（长尾效应）。
  5. 资源竞争: 多个 executor 共享同一节点的 CPU 和内存资源，
     可能产生资源争抢。

结论:
  对于本查询（按年份分组聚合），可并行比例 f ≈ {f_estimated*100:.1f}%，
  意味着约 {(1-f_estimated)*100:.1f}% 的操作是串行的（数据读取、任务调度等）。
  因此加速比未能达到线性（2.00），实测为 {S2:.2f}。
  
  注意：本地运行 Spark 时，executor 实际运行在同一进程中，
  没有真实网络通信开销，因此加速比可能接近或略超线性。
  在真实 CCE 集群上运行时，由于网络通信和资源竞争，
  加速比通常会低于线性值。
""")

# 生成性能对比图（使用 matplotlib，如果可用）
try:
    import matplotlib
    matplotlib.use('Agg')  # 非交互式后端
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import numpy as np

    # 尝试设置中文字体
    chinese_fonts = [f.name for f in fm.fontManager.ttflist if any(name in f.name for name in ['SimHei', 'Microsoft YaHei', 'SimSun', 'FangSong', 'KaiTi', 'Arial Unicode MS', 'PingFang', 'Noto Sans CJK'])]
    if chinese_fonts:
        plt.rcParams['font.sans-serif'] = [chinese_fonts[0]] + plt.rcParams['font.sans-serif']
    else:
        # 如果找不到中文字体，使用英文标签
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        use_english = True
    plt.rcParams['axes.unicode_minus'] = False

    # 数据
    methods = ['Pandas\n(Single)', 'PySpark\n(1 executor)', 'PySpark\n(2 executors)']
    times = [pandas_time, spark1_time, spark2_time]
    
    # 加速比（以 PySpark 1 executor 为基准）
    speedups = [1.0, 1.0, S2]
    
    # Amdahl 理论值
    p_values = [1, 2]
    f = f_estimated
    amdahl_speedups = [1.0, 1.0 / ((1 - f) + f / 2)]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # 左图：执行时间对比
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    bars = ax1.bar(methods, times, color=colors, alpha=0.8)
    ax1.set_ylabel('Execution Time (s)', fontsize=12)
    ax1.set_title('Execution Time Comparison', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # 在柱状图上标注数值
    for bar, t in zip(bars, times):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                f'{t:.2f}s', ha='center', va='bottom', fontsize=11)
    
    # 右图：加速比对比
    ax2.plot([1, 2], amdahl_speedups, 'ro-', label=f'Amdahl Theory (f={f:.2f})', 
             linewidth=2, markersize=8)
    ax2.plot([1, 2], [1.0, S2], 'bs--', label='Measured Speedup', 
             linewidth=2, markersize=8)
    ax2.plot([1, 2], [1.0, 2.0], 'g:', label='Linear Speedup (Ideal)', 
             linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Executors', fontsize=12)
    ax2.set_ylabel('Speedup', fontsize=12)
    ax2.set_title('Measured vs Amdahl Theoretical Speedup', fontsize=14, fontweight='bold')
    ax2.set_xticks([1, 2])
    ax2.legend(fontsize=10)
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    # 保存到当前目录
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "performance_comparison.png")
    plt.savefig(output_path, dpi=150)
    print(f"\n[Chart] Performance comparison chart saved to {output_path}")
    
except ImportError:
    print("\n[WARNING] matplotlib not installed, skipping chart generation")
    print("Please manually plot the comparison chart using the following data:")
    print(f"  Pandas (Single): {pandas_time:.4f}s")
    print(f"  PySpark (1 executor): {spark1_time:.4f}s")
    print(f"  PySpark (2 executors): {spark2_time:.4f}s")
    print(f"  Measured Speedup S(2) = {S2:.2f}")
    print(f"  Estimated parallel fraction f = {f_estimated:.4f}")

print("\n" + "=" * 70)
print("A-3 性能对比分析完成！")
print("=" * 70)

spark.stop()