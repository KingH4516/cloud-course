# 第二部分 方向A：Spark 大数据分析

## 项目结构

```
第二部分-方向A/
├── README.md                                    # 本说明文件
├── 部署运行指南.md                               # 详细部署指南
├── A-0-环境部署/
│   ├── sparkapplication.yaml                    # SparkApplication CR（wordcount 示例）
│   └── wordcount.py                             # PySpark 入门示例（WordCount）
├── A-1-数据清洗与A-2-SQL分析/
│   ├── analysis.py                              # 主分析脚本（A-1 数据清洗 + A-2 SQL 分析）
│   ├── local_test.py                            # 本地 Pandas 测试版（无需 Spark）
│   └── sparkapplication-analysis.yaml           # 用于提交到 CCE 的 SparkApplication CR
├── A-3-性能对比与Amdahl分析/
│   ├── performance_comparison.py                # 性能对比与 Amdahl 分析脚本
│   ├── sparkapplication-perf.yaml               # 用于提交到 CCE 的 SparkApplication CR
│   └── performance_comparison.png               # 生成的性能对比图
└── douban_movies.csv                            # 豆瓣电影评分数据集（约 200MB）
```

## 任务完成情况

### A-0: 环境部署（10分）
- ✅ `sparkapplication.yaml` - SparkApplication CR，配置了 executorInstances=2, executorMemory="1g"
- ✅ `wordcount.py` - PySpark 入门示例，用于验证 Spark Operator 部署

### A-1: 数据清洗（10分）
- ✅ 加载数据并打印 Schema 和前5行
- ✅ 统计各字段缺失值比例（year: 3%, rating_score: 5%, genres: 8%, directors: 13%, summary: 29%）
- ✅ 2种缺失值处理策略：
  - **策略1**: year 字段用中位数（1998）填充
  - **策略2**: rating_score 字段用平均值（2.41）填充
  - 其他字段：genres/countries/directors 填充为"未知"，summary 填充为"暂无简介"
- ✅ 删除评分=0的记录（67,132 → 26,813 条）
- ✅ 输出清洗前后行数对比及各字段基本统计信息

### A-2: Spark SQL 统计分析（15分）
- ✅ **查询1 - GROUP BY 聚合**: 各电影类型的平均评分与数量
- ✅ **查询2 - ORDER BY Top-N**: 评分最高的Top-20电影（评分人数>10000）
- ✅ **查询3 - 时间维度趋势分析**: 每年电影数量与平均评分变化趋势
- ✅ **查询4 - 窗口函数**: 每年评分最高的Top-5电影
- ✅ **查询5 - JOIN 操作**: 高分电影与各类型平均水平的对比

### A-3: 性能对比与 Amdahl 分析（5分）
- ✅ Pandas（单机）实现
- ✅ PySpark（executorInstances=1）实现
- ✅ PySpark（executorInstances=2）实现
- ✅ 性能对比表与双折线图
- ✅ Amdahl 定律分析（估算可并行比例 f ≈ 81.4%）

## 运行方式

### 本地测试（无需 Spark）
```bash
python "A-1-数据清洗与A-2-SQL分析/local_test.py"
```

### 本地运行（需要 PySpark）
```bash
pip install pyspark pandas matplotlib
python "A-1-数据清洗与A-2-SQL分析/analysis.py"
python "A-3-性能对比与Amdahl分析/performance_comparison.py"
```

### 在 CCE 集群上运行
```bash
# 1. 安装 Spark Operator
helm install spark-op ./spark-operator-chart/ -n spark-operator --create-namespace

# 2. 提交 WordCount 示例
kubectl apply -f A-0-环境部署/sparkapplication.yaml

# 3. 提交数据分析作业
kubectl apply -f A-1-数据清洗与A-2-SQL分析/sparkapplication-analysis.yaml

# 4. 提交性能对比作业
kubectl apply -f A-3-性能对比与Amdahl分析/sparkapplication-perf.yaml
```

## 注意事项
1. 在 CCE 上运行时，需要将 `analysis.py` 和 `performance_comparison.py` 中的 OBS 路径替换为实际的 Bucket 路径
2. 数据集文件 `douban_movies.csv` 需要上传到 OBS 或挂载到 Spark Pod 中
3. CSV 文件中 summary 字段包含换行符，读取时需要设置 `multiLine=True`
