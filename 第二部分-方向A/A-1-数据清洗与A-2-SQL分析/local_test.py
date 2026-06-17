"""
本地测试版本 - 使用 Pandas 模拟 Spark 分析逻辑
用于在没有 Spark 环境的本地验证代码正确性
"""
import pandas as pd
import numpy as np

print("=" * 70)
print("豆瓣电影评分数据集 - 本地测试版 (Pandas)")
print("=" * 70)

# ============================================================
# 加载数据
# ============================================================
df = pd.read_csv("douban_movies.csv", encoding="utf-8")
print(f"\n数据集行数: {len(df)}")
print(f"数据集列数: {len(df.columns)}")
print(f"列名: {list(df.columns)}")

# 打印前5行
print("\n前5行数据:")
print(df.head())

# ============================================================
# A-1: 数据清洗
# ============================================================
print("\n" + "=" * 70)
print("【A-1】数据清洗")
print("=" * 70)

# 1. 统计各字段缺失值
print("\n各字段缺失值统计:")
for col in df.columns:
    null_count = df[col].isnull().sum() + (df[col] == "").sum()
    ratio = null_count / len(df) * 100
    print(f"  {col}: {null_count} 条缺失 ({ratio:.2f}%)")

# 2. 缺失值处理
print("\n缺失值处理策略:")

# 策略1: year - 使用中位数填充
year_median = df['year'].dropna().astype(float).median()
print(f"  年份中位数: {year_median:.0f}")

# 策略2: rating_score - 使用平均值填充
avg_rating = df['rating_score'].dropna().astype(float).mean()
print(f"  评分平均值: {avg_rating:.2f}")

# 策略3: genres/countries/directors - 填充为"未知"
# 策略4: summary - 填充为"暂无简介"

# 执行清洗
df_clean = df.copy()
df_clean['year'] = df_clean['year'].fillna(str(int(year_median)))
df_clean['rating_score'] = df_clean['rating_score'].fillna(str(round(avg_rating, 1)))
df_clean['genres'] = df_clean['genres'].fillna("未知")
df_clean['countries'] = df_clean['countries'].fillna("未知")
df_clean['directors'] = df_clean['directors'].fillna("未知")
df_clean['summary'] = df_clean['summary'].fillna("暂无简介")
df_clean['original_title'] = df_clean['original_title'].fillna("")
df_clean['rating_count'] = df_clean['rating_count'].fillna("0")
df_clean['collect_count'] = df_clean['collect_count'].fillna("0")

# 删除评分=0的记录
before_drop = len(df_clean)
df_clean = df_clean[df_clean['rating_score'].astype(float) > 0]
after_drop = len(df_clean)
print(f"\n  删除评分=0的记录: {before_drop} -> {after_drop} (删除 {before_drop - after_drop} 条)")

# 转换数据类型
df_clean['year_int'] = df_clean['year'].astype(int)
df_clean['rating_score_float'] = df_clean['rating_score'].astype(float)
df_clean['rating_count_int'] = df_clean['rating_count'].astype(int)
df_clean['collect_count_int'] = df_clean['collect_count'].astype(int)

print(f"\n清洗后总行数: {len(df_clean)}")

# 3. 清洗前后对比
print("\n清洗前后对比:")
print(f"  清洗前行数: {len(df)}")
print(f"  清洗后行数: {len(df_clean)}")
print(f"  减少行数: {len(df) - len(df_clean)}")

# 清洗后基本统计信息
print("\n清洗后各字段基本统计信息:")
print(df_clean[['rating_score_float', 'year_int', 'rating_count_int', 'collect_count_int']].describe())

# ============================================================
# A-2: Spark SQL 统计分析 (使用 Pandas 模拟)
# ============================================================
print("\n" + "=" * 70)
print("【A-2】Spark SQL 统计分析")
print("=" * 70)

# 查询1: GROUP BY 聚合 - 各电影类型的平均评分与数量
print("\n" + "-" * 70)
print("【查询1】GROUP BY 聚合：各电影类型的平均评分与数量")
print("-" * 70)

# 由于 genres 是复合类型（如"剧情/爱情"），这里按原始类型分组
genre_stats = df_clean.groupby('genres').agg(
    movie_count=('rating_score_float', 'count'),
    avg_rating=('rating_score_float', 'mean'),
    avg_rating_count=('rating_count_int', 'mean'),
    avg_collect_count=('collect_count_int', 'mean')
).round(2).sort_values('movie_count', ascending=False)

print(genre_stats.head(20))

# 查询2: ORDER BY Top-N - 评分最高的Top-20电影
print("\n" + "-" * 70)
print("【查询2】ORDER BY Top-N：评分最高的Top-20电影（评分人数>10000）")
print("-" * 70)

top_movies = df_clean[df_clean['rating_count_int'] > 10000] \
    .sort_values(['rating_score_float', 'rating_count_int'], ascending=[False, False]) \
    .head(20)

print(top_movies[['title', 'year_int', 'rating_score_float', 'rating_count_int', 'genres', 'countries', 'directors']].to_string(index=False))

# 查询3: 时间维度趋势分析 - 每年电影数量、平均评分
print("\n" + "-" * 70)
print("【查询3】时间维度趋势分析：每年电影数量与平均评分变化趋势")
print("-" * 70)

yearly_stats = df_clean[(df_clean['year_int'] >= 1900) & (df_clean['year_int'] <= 2025)] \
    .groupby('year_int').agg(
        movie_count=('rating_score_float', 'count'),
        avg_rating=('rating_score_float', 'mean'),
        avg_rating_count=('rating_count_int', 'mean'),
        avg_collect_count=('collect_count_int', 'mean'),
        rating_stddev=('rating_score_float', 'std')
    ).round(2)

print(yearly_stats.head(50))
print(f"\n总年份数: {len(yearly_stats)}")

# 查询4: 窗口函数 - 每年评分最高的Top-5电影
print("\n" + "-" * 70)
print("【查询4】窗口函数：每年评分最高的Top-5电影（评分人数>5000）")
print("-" * 70)

# 筛选数据
window_data = df_clean[(df_clean['year_int'] >= 2000) & (df_clean['year_int'] <= 2023) & (df_clean['rating_count_int'] > 5000)].copy()

# 按年份分组，取评分最高的前5
top_per_year = window_data.groupby('year_int').apply(
    lambda x: x.nlargest(5, ['rating_score_float', 'rating_count_int'])
).reset_index(drop=True)

top_per_year = top_per_year.sort_values(['year_int', 'rating_score_float'], ascending=[False, False])
print(top_per_year[['title', 'year_int', 'rating_score_float', 'rating_count_int', 'genres', 'directors']].head(50).to_string(index=False))

# 查询5: JOIN 操作 - 高分电影与各类型平均水平的对比
print("\n" + "-" * 70)
print("【查询5】JOIN操作：高分电影（评分≥9.0）与各类型平均水平的对比")
print("-" * 70)

# 计算各类型的平均评分和评分人数
genre_avg = df_clean.groupby('genres').agg(
    genre_avg_rating=('rating_score_float', 'mean'),
    genre_avg_count=('rating_count_int', 'mean')
).round(2)

# 筛选高分电影并合并类型平均数据
high_rated = df_clean[(df_clean['rating_score_float'] >= 9.0) & (df_clean['rating_count_int'] > 10000)].copy()
high_rated = high_rated.merge(genre_avg, on='genres', how='left')
high_rated['rating_diff'] = (high_rated['rating_score_float'] - high_rated['genre_avg_rating']).round(2)
high_rated = high_rated.sort_values('rating_score_float', ascending=False)

print(high_rated[['title', 'genres', 'rating_score_float', 'genre_avg_rating', 'rating_diff', 'rating_count_int', 'genre_avg_count', 'year_int']].head(30).to_string(index=False))

print("\n" + "=" * 70)
print("本地测试完成！")
print("=" * 70)
