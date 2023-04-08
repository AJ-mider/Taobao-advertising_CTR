## 二 根据用户行为数据创建ALS模型并召回商品

### 2.0 用户行为数据拆分

- 方便练习可以对数据做拆分处理

  - pandas的数据分批读取  chunk 厚厚的一块 相当大的数量或部分

  ``` python
  import pandas as pd
  reader = pd.read_csv('behavior_log.csv',chunksize=100,iterator=True)
  count = 0;
  for chunk in reader:
      count += 1
      if count ==1:
          chunk.to_csv('test4.csv',index = False)
      elif count>1 and count<1000:
          chunk.to_csv('test4.csv',index = False, mode = 'a',header = False)
      else:
          break
  pd.read_csv('test4.csv')
  ```


### 2.1 预处理behavior_log数据集

- 创建spark session

```python
import os
# 配置spark driver和pyspark运行时，所使用的python解释器路径
PYSPARK_PYTHON = "/miniconda2/envs/py365/bin/python"
JAVA_HOME='/root/bigdata/jdk'
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
os.environ['JAVA_HOME']=JAVA_HOME
# spark配置信息
from pyspark import SparkConf
from pyspark.sql import SparkSession

SPARK_APP_NAME = "preprocessingBehaviorLog"
SPARK_URL = "spark://192.168.19.137:7077"

conf = SparkConf()    # 创建spark config对象
config = (
	("spark.app.name", SPARK_APP_NAME),    # 设置启动的spark的app名称，没有提供，将随机产生一个名称
	("spark.executor.memory", "6g"),    # 设置该app启动时占用的内存用量，默认1g
	("spark.master", SPARK_URL),    # spark master的地址
    ("spark.executor.cores", "4"),    # 设置spark executor使用的CPU核心数
    # 以下三项配置，可以控制执行器数量
#     ("spark.dynamicAllocation.enabled", True),
#     ("spark.dynamicAllocation.initialExecutors", 1),    # 1个执行器
#     ("spark.shuffle.service.enabled", True)
# 	('spark.sql.pivotMaxValues', '99999'),  # 当需要pivot DF，且值很多时，需要修改，默认是10000
)
# 查看更详细配置及说明：https://spark.apache.org/docs/latest/configuration.html

conf.setAll(config)

# 利用config对象，创建spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

- 从hdfs中加载csv文件为DataFrame

```python
# 从hdfs加载CSV文件为DataFrame
df = spark.read.csv("/datasets/behavior_log.csv", header=True)
df.show()    # 查看dataframe，默认显示前20条
# 大致查看一下数据类型
df.printSchema()    # 打印当前dataframe的结构
```

显示结果:

```shell
+------+----------+----+-----+------+
|  user|time_stamp|btag| cate| brand|
+------+----------+----+-----+------+
|558157|1493741625|  pv| 6250| 91286|
|558157|1493741626|  pv| 6250| 91286|
|558157|1493741627|  pv| 6250| 91286|
|728690|1493776998|  pv|11800| 62353|
|332634|1493809895|  pv| 1101|365477|
|857237|1493816945|  pv| 1043|110616|
|619381|1493774638|  pv|  385|428950|
|467042|1493772641|  pv| 8237|301299|
|467042|1493772644|  pv| 8237|301299|
|991528|1493780710|  pv| 7270|274795|
|991528|1493780712|  pv| 7270|274795|
|991528|1493780712|  pv| 7270|274795|
|991528|1493780712|  pv| 7270|274795|
|991528|1493780714|  pv| 7270|274795|
|991528|1493780765|  pv| 7270|274795|
|991528|1493780714|  pv| 7270|274795|
|991528|1493780765|  pv| 7270|274795|
|991528|1493780764|  pv| 7270|274795|
|991528|1493780633|  pv| 7270|274795|
|991528|1493780764|  pv| 7270|274795|
+------+----------+----+-----+------+
only showing top 20 rows

root
 |-- user: string (nullable = true)
 |-- time_stamp: string (nullable = true)
 |-- btag: string (nullable = true)
 |-- cate: string (nullable = true)
 |-- brand: string (nullable = true)
```

- 从hdfs加载数据为dataframe，并设置结构

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# 构建结构对象
schema = StructType([
    StructField("userId", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("btag", StringType()),
    StructField("cateId", IntegerType()),
    StructField("brandId", IntegerType())
])
# 从hdfs加载数据为dataframe，并设置结构
behavior_log_df = spark.read.csv("hdfs://localhost:8020/datasets/behavior_log.csv", header=True, schema=schema)
behavior_log_df.show()
behavior_log_df.count() 
```

显示结果:

```shell
+------+----------+----+------+-------+
|userId| timestamp|btag|cateId|brandId|
+------+----------+----+------+-------+
|558157|1493741625|  pv|  6250|  91286|
|558157|1493741626|  pv|  6250|  91286|
|558157|1493741627|  pv|  6250|  91286|
|728690|1493776998|  pv| 11800|  62353|
|332634|1493809895|  pv|  1101| 365477|
|857237|1493816945|  pv|  1043| 110616|
|619381|1493774638|  pv|   385| 428950|
|467042|1493772641|  pv|  8237| 301299|
|467042|1493772644|  pv|  8237| 301299|
|991528|1493780710|  pv|  7270| 274795|
|991528|1493780712|  pv|  7270| 274795|
|991528|1493780712|  pv|  7270| 274795|
|991528|1493780712|  pv|  7270| 274795|
|991528|1493780714|  pv|  7270| 274795|
|991528|1493780765|  pv|  7270| 274795|
|991528|1493780714|  pv|  7270| 274795|
|991528|1493780765|  pv|  7270| 274795|
|991528|1493780764|  pv|  7270| 274795|
|991528|1493780633|  pv|  7270| 274795|
|991528|1493780764|  pv|  7270| 274795|
+------+----------+----+------+-------+
only showing top 20 rows

root
 |-- userId: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- btag: string (nullable = true)
 |-- cateId: integer (nullable = true)
 |-- brandId: integer (nullable = true)
```

- 分析数据集字段的类型和格式
  - 查看是否有空值
  - 查看每列数据的类型
  - 查看每列数据的类别情况

```python
print("查看userId的数据情况：", behavior_log_df.groupBy("userId").count().count())
# 约113w用户
#注意：behavior_log_df.groupBy("userId").count()  返回的是一个dataframe，这里的count计算的是每一个分组的个数，但当前还没有进行计算
# 当调用df.count()时才开始进行计算，这里的count计算的是dataframe的条目数，也就是共有多少个分组
```

```shell
查看user的数据情况： 1136340
```

```python
print("查看btag的数据情况：", behavior_log_df.groupBy("btag").count().collect())    # collect会把计算结果全部加载到内存，谨慎使用
# 只有四种类型数据：pv、fav、cart、buy
# 这里由于类型只有四个，所以直接使用collect，把数据全部加载出来
```

```shell
查看btag的数据情况： [Row(btag='buy', count=9115919), Row(btag='fav', count=9301837), Row(btag='cart', count=15946033), Row(btag='pv', count=688904345)]
```

```python
print("查看cateId的数据情况：", behavior_log_df.groupBy("cateId").count().count())
# 约12968类别id
```

```shell
查看cateId的数据情况： 12968
```

```python
print("查看brandId的数据情况：", behavior_log_df.groupBy("brandId").count().count())
# 约460561品牌id
```

```shell
查看brandId的数据情况： 460561
```

```python
print("判断数据是否有空值：", behavior_log_df.count(), behavior_log_df.dropna().count())
# 约7亿条目723268134 723268134
# 本数据集无空值条目，可放心处理
```

```shell
判断数据是否有空值： 723268134 723268134
```

- pivot透视操作，把某列里的字段值转换成行并进行聚合运算(pyspark.sql.GroupedData.pivot)
  - 如果透视的字段中的不同属性值超过10000个，则需要设置spark.sql.pivotMaxValues，否则计算过程中会出现错误。[文档介绍](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=pivot#pyspark.sql.GroupedData.pivot)。

```python
# 统计每个用户对各类商品的pv、fav、cart、buy数量
cate_count_df = behavior_log_df.groupBy(behavior_log_df.userId, behavior_log_df.cateId).pivot("btag",["pv","fav","cart","buy"]).count()
cate_count_df.printSchema()    # 此时还没有开始计算
```

显示效果:

```shell
root
 |-- userId: integer (nullable = true)
 |-- cateId: integer (nullable = true)
 |-- pv: long (nullable = true)
 |-- fav: long (nullable = true)
 |-- cart: long (nullable = true)
 |-- buy: long (nullable = true)
```

- 统计每个用户对各个品牌的pv、fav、cart、buy数量并保存结果

```python
# 统计每个用户对各个品牌的pv、fav、cart、buy数量
brand_count_df = behavior_log_df.groupBy(behavior_log_df.userId, behavior_log_df.brandId).pivot("btag",["pv","fav","cart","buy"]).count()
# brand_count_df.show()    # 同上
# 113w * 46w
# 由于运算时间比较长，所以这里先将结果存储起来，供后续其他操作使用
# 写入数据时才开始计算
cate_count_df.write.csv("hdfs://localhost:9000/preprocessing_dataset/cate_count.csv", header=True)
brand_count_df.write.csv("hdfs://localhost:9000/preprocessing_dataset/brand_count.csv", header=True)
```

### 2.2 根据用户对类目偏好打分训练ALS(最小二乘)模型

- 根据您统计的次数 + 打分规则 ==> 偏好打分数据集 ==> ALS模型

```python
# spark ml的模型训练是基于内存的，如果数据过大，内存空间小，迭代次数过多的化，可能会造成内存溢出，报错
# 设置Checkpoint的话，会把所有数据落盘，这样如果异常退出，下次重启后，可以接着上次的训练节点继续运行
# 但该方法其实指标不治本，因为无法防止内存溢出，所以还是会报错
# 如果数据量大，应考虑的是增加内存、或限制迭代次数和训练数据量级等
spark.sparkContext.setCheckpointDir("/checkPoint/")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# 构建结构对象
schema = StructType([
    StructField("userId", IntegerType()),
    StructField("cateId", IntegerType()),
    StructField("pv", IntegerType()),
    StructField("fav", IntegerType()),
    StructField("cart", IntegerType()),
    StructField("buy", IntegerType())
])

# 从hdfs加载CSV文件
cate_count_df = spark.read.csv("hdfs://localhost:9000/preprocessing_dataset/cate_count.csv", header=True, schema=schema)
cate_count_df.printSchema()
cate_count_df.first()    # 第一行数据
```

显示结果:

```shell
root
 |-- userId: integer (nullable = true)
 |-- cateId: integer (nullable = true)
 |-- pv: integer (nullable = true)
 |-- fav: integer (nullable = true)
 |-- cart: integer (nullable = true)
 |-- buy: integer (nullable = true)

Row(userId=1061650, cateId=4520, pv=2326, fav=None, cart=53, buy=None)
```

- 处理每一行数据：r表示row对象

```python
def process_row(r):
    # 处理每一行数据：r表示row对象
    
    # 偏好评分规则：
	#     m: 用户对应的行为次数
    #     该偏好权重比例，次数上限仅供参考，具体数值应根据产品业务场景权衡
	#     pv: if m<=20: score=0.2*m; else score=4
	#     fav: if m<=20: score=0.4*m; else score=8
	#     cart: if m<=20: score=0.6*m; else score=12
	#     buy: if m<=20: score=1*m; else score=20
    
    # 注意这里要全部设为浮点数，spark运算时对类型比较敏感，要保持数据类型都一致
	pv_count = r.pv if r.pv else 0.0
	fav_count = r.fav if r.fav else 0.0
	cart_count = r.cart if r.cart else 0.0
	buy_count = r.buy if r.buy else 0.0

	pv_score = 0.2*pv_count if pv_count<=20 else 4.0
	fav_score = 0.4*fav_count if fav_count<=20 else 8.0
	cart_score = 0.6*cart_count if cart_count<=20 else 12.0
	buy_score = 1.0*buy_count if buy_count<=20 else 20.0

	rating = pv_score + fav_score + cart_score + buy_score
	# 返回用户ID、分类ID、用户对分类的偏好打分
	return r.userId, r.cateId, rating
```

- 返回一个PythonRDD类型

```python
# 返回一个PythonRDD类型，此时还没开始计算
cate_count_df.rdd.map(process_row).toDF(["userId", "cateId", "rating"])
```

显示结果:

```shell
DataFrame[userId: bigint, cateId: bigint, rating: double]
```

- 用户对商品类别的打分数据

```python
# 用户对商品类别的打分数据
# map返回的结果是rdd类型，需要调用toDF方法转换为Dataframe
cate_rating_df = cate_count_df.rdd.map(process_row).toDF(["userId", "cateId", "rating"])
# 注意：toDF不是每个rdd都有的方法，仅局限于此处的rdd
# 可通过该方法获得 user-cate-matrix
# 但由于cateId字段过多，这里运算量比很大，机器内存要求很高才能执行，否则无法完成任务
# 请谨慎使用

# 但好在我们训练ALS模型时，不需要转换为user-cate-matrix，所以这里可以不用运行
# cate_rating_df.groupBy("userId").povit("cateId").min("rating")
# 用户对类别的偏好打分数据
cate_rating_df
```

显示结果:

```
DataFrame[userId: bigint, cateId: bigint, rating: double]
```

- 通常如果USER-ITEM打分数据应该是通过一下方式进行处理转换为USER-ITEM-MATRIX

![](./img/CF%E4%BB%8B%E7%BB%8D.png)

但这里我们将使用的Spark的ALS模型进行CF推荐，因此注意这里数据输入不需要提前转换为矩阵，直接是 USER-ITEM-RATE的数据

- 基于Spark的ALS隐因子模型进行CF评分预测

  - ALS的意思是交替最小二乘法（Alternating Least Squares），是Spark2.*中加入的进行基于模型的协同过滤（model-based CF）的推荐系统算法。

    同SVD，它也是一种矩阵分解技术，对数据进行降维处理。

  - 详细使用方法：[pyspark.ml.recommendation.ALS](https://spark.apache.org/docs/2.2.2/api/python/pyspark.ml.html?highlight=vectors#module-pyspark.ml.recommendation)

  - 注意：由于数据量巨大，因此这里也不考虑基于内存的CF算法

    参考：[为什么Spark中只有ALS](https://www.cnblogs.com/mooba/p/6539142.html)

```python
# 使用pyspark中的ALS矩阵分解方法实现CF评分预测
# 文档地址：https://spark.apache.org/docs/2.2.2/api/python/pyspark.ml.html?highlight=vectors#module-pyspark.ml.recommendation
from pyspark.ml.recommendation import ALS   # ml：dataframe， mllib：rdd

# 利用打分数据，训练ALS模型
als = ALS(userCol='userId', itemCol='cateId', ratingCol='rating', checkpointInterval=5)

# 此处训练时间较长
model = als.fit(cate_rating_df)
```

- 模型训练好后，调用方法进行使用，[具体API查看](https://spark.apache.org/docs/2.2.2/api/python/pyspark.ml.html?highlight=alsmodel#pyspark.ml.recommendation.ALSModel)

```python
# model.recommendForAllUsers(N) 给所有用户推荐TOP-N个物品
ret = model.recommendForAllUsers(3)
# 由于是给所有用户进行推荐，此处运算时间也较长
ret.show()
# 推荐结果存放在recommendations列中，
ret.select("recommendations").show()
```

显示结果:

```shell
+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|   148|[[3347, 12.547271...|
|   463|[[1610, 9.250818]...|
|   471|[[1610, 10.246621...|
|   496|[[1610, 5.162216]...|
|   833|[[5607, 9.065482]...|
|  1088|[[104, 6.886987],...|
|  1238|[[5631, 14.51981]...|
|  1342|[[5720, 10.89842]...|
|  1580|[[5731, 8.466453]...|
|  1591|[[1610, 12.835257...|
|  1645|[[1610, 11.968531...|
|  1829|[[1610, 17.576496...|
|  1959|[[1610, 8.353473]...|
|  2122|[[1610, 12.652732...|
|  2142|[[1610, 12.48068]...|
|  2366|[[1610, 11.904813...|
|  2659|[[5607, 11.699315...|
|  2866|[[1610, 7.752719]...|
|  3175|[[3347, 2.3429515...|
|  3749|[[1610, 3.641833]...|
+------+--------------------+
only showing top 20 rows

+--------------------+
|     recommendations|
+--------------------+
|[[3347, 12.547271...|
|[[1610, 9.250818]...|
|[[1610, 10.246621...|
|[[1610, 5.162216]...|
|[[5607, 9.065482]...|
|[[104, 6.886987],...|
|[[5631, 14.51981]...|
|[[5720, 10.89842]...|
|[[5731, 8.466453]...|
|[[1610, 12.835257...|
|[[1610, 11.968531...|
|[[1610, 17.576496...|
|[[1610, 8.353473]...|
|[[1610, 12.652732...|
|[[1610, 12.48068]...|
|[[1610, 11.904813...|
|[[5607, 11.699315...|
|[[1610, 7.752719]...|
|[[3347, 2.3429515...|
|[[1610, 3.641833]...|
+--------------------+
only showing top 20 rows
```

- model.recommendForUserSubset 给部分用户推荐TOP-N个物品

```python
# 注意：recommendForUserSubset API，2.2.2版本中无法使用
dataset = spark.createDataFrame([[1],[2],[3]])
dataset = dataset.withColumnRenamed("_1", "userId")
ret = model.recommendForUserSubset(dataset, 3)

# 只给部分用推荐，运算时间短
ret.show()
ret.collect()    # 注意： collect会将所有数据加载到内存，慎用
```

显示结果:

```shell
+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|     1|[[1610, 25.4989],...|
|     3|[[5607, 13.665942...|
|     2|[[5579, 5.9051886...|
+------+--------------------+

[Row(userId=1, recommendations=[Row(cateId=1610, rating=25.498899459838867), Row(cateId=5737, rating=24.901548385620117), Row(cateId=3347, rating=20.736785888671875)]),
 Row(userId=3, recommendations=[Row(cateId=5607, rating=13.665942192077637), Row(cateId=1610, rating=11.770171165466309), Row(cateId=3347, rating=10.35690689086914)]),
 Row(userId=2, recommendations=[Row(cateId=5579, rating=5.90518856048584), Row(cateId=2447, rating=5.624575138092041), Row(cateId=5690, rating=5.2555742263793945)])]
```

- transform中提供userId和cateId可以对打分进行预测，利用打分结果排序后

```python
# transform中提供userId和cateId可以对打分进行预测，利用打分结果排序后，同样可以实现TOP-N的推荐
model.transform
# 将模型进行存储
model.save("hdfs://localhost:8020/models/userCateRatingALSModel.obj")
# 测试存储的模型
from pyspark.ml.recommendation import ALSModel
# 从hdfs加载之前存储的模型
als_model = ALSModel.load("hdfs://localhost:8020/models/userCateRatingALSModel.obj")
# model.recommendForAllUsers(N) 给用户推荐TOP-N个物品
result = als_model.recommendForAllUsers(3)
result.show()
```

显示结果:

```shell
+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|   148|[[3347, 12.547271...|
|   463|[[1610, 9.250818]...|
|   471|[[1610, 10.246621...|
|   496|[[1610, 5.162216]...|
|   833|[[5607, 9.065482]...|
|  1088|[[104, 6.886987],...|
|  1238|[[5631, 14.51981]...|
|  1342|[[5720, 10.89842]...|
|  1580|[[5731, 8.466453]...|
|  1591|[[1610, 12.835257...|
|  1645|[[1610, 11.968531...|
|  1829|[[1610, 17.576496...|
|  1959|[[1610, 8.353473]...|
|  2122|[[1610, 12.652732...|
|  2142|[[1610, 12.48068]...|
|  2366|[[1610, 11.904813...|
|  2659|[[5607, 11.699315...|
|  2866|[[1610, 7.752719]...|
|  3175|[[3347, 2.3429515...|
|  3749|[[1610, 3.641833]...|
+------+--------------------+
only showing top 20 rows
```

- 召回到redis（现在没必要召回，因为我们是用户对类别的评分，不是对物品的评分，下面跳过）

```python
import redis
host = "192.168.19.8"
port = 6379    
# 召回到redis
def recall_cate_by_cf(partition):
    # 建立redis 连接池
    pool = redis.ConnectionPool(host=host, port=port)
    # 建立redis客户端
    client = redis.Redis(connection_pool=pool)
    for row in partition:
        client.hset("recall_cate", row.userId, [i.cateId for i in row.recommendations])
# 对每个分片的数据进行处理 #mapPartition Transformation   map
# foreachPartition Action操作             foreachRDD
result.foreachPartition(recall_cate_by_cf)

# 注意：这里这是召回的是用户最感兴趣的n个类别
# 总的条目数，查看redis中总的条目数是否一致
result.count()
```

显示结果:

```shell
1136340
```

### 2.3 根据用户对品牌偏好打分训练ALS模型

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("userId", IntegerType()),
    StructField("brandId", IntegerType()),
    StructField("pv", IntegerType()),
    StructField("fav", IntegerType()),
    StructField("cart", IntegerType()),
    StructField("buy", IntegerType())
])
# 从hdfs加载预处理好的品牌的统计数据
brand_count_df = spark.read.csv("hdfs://localhost:8020/preprocessing_dataset/brand_count.csv", header=True, schema=schema)
# brand_count_df.show()
def process_row(r):
    # 处理每一行数据：r表示row对象
    
    # 偏好评分规则：
	#     m: 用户对应的行为次数
    #     该偏好权重比例，次数上限仅供参考，具体数值应根据产品业务场景权衡
	#     pv: if m<=20: score=0.2*m; else score=4
	#     fav: if m<=20: score=0.4*m; else score=8
	#     cart: if m<=20: score=0.6*m; else score=12
	#     buy: if m<=20: score=1*m; else score=20
    
    # 注意这里要全部设为浮点数，spark运算时对类型比较敏感，要保持数据类型都一致
	pv_count = r.pv if r.pv else 0.0
	fav_count = r.fav if r.fav else 0.0
	cart_count = r.cart if r.cart else 0.0
	buy_count = r.buy if r.buy else 0.0

	pv_score = 0.2*pv_count if pv_count<=20 else 4.0
	fav_score = 0.4*fav_count if fav_count<=20 else 8.0
	cart_score = 0.6*cart_count if cart_count<=20 else 12.0
	buy_score = 1.0*buy_count if buy_count<=20 else 20.0

	rating = pv_score + fav_score + cart_score + buy_score
	# 返回用户ID、品牌ID、用户对品牌的偏好打分
	return r.userId, r.brandId, rating
# 用户对品牌的打分数据
brand_rating_df = brand_count_df.rdd.map(process_row).toDF(["userId", "brandId", "rating"])
# brand_rating_df.show()
```

- 基于Spark的ALS隐因子模型进行CF评分预测

  - ALS的意思是交替最小二乘法（Alternating Least Squares），是Spark中进行基于模型的协同过滤（model-based CF）的推荐系统算法，也是目前Spark内唯一一个推荐算法。

    同SVD，它也是一种矩阵分解技术，但理论上，ALS在海量数据的处理上要优于SVD。

    更多了解：[pyspark.ml.recommendation.ALS](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectors#module-pyspark.ml.recommendation)

    注意：由于数据量巨大，因此这里不考虑基于内存的CF算法

    参考：[为什么Spark中只有ALS](https://www.cnblogs.com/mooba/p/6539142.html)

- 使用pyspark中的ALS矩阵分解方法实现CF评分预测

```python
# 使用pyspark中的ALS矩阵分解方法实现CF评分预测
# 文档地址：https://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectors#module-pyspark.ml.recommendation
from pyspark.ml.recommendation import ALS

als = ALS(userCol='userId', itemCol='brandId', ratingCol='rating', checkpointInterval=2)
# 利用打分数据，训练ALS模型
# 此处训练时间较长
model = als.fit(brand_rating_df)
# model.recommendForAllUsers(N) 给用户推荐TOP-N个物品
model.recommendForAllUsers(3).show()
# 将模型进行存储
model.save("hdfs://localhost:9000/models/userBrandRatingModel.obj")
# 测试存储的模型
from pyspark.ml.recommendation import ALSModel
# 从hdfs加载模型
my_model = ALSModel.load("hdfs://localhost:9000/models/userBrandRatingModel.obj")
my_model
# model.recommendForAllUsers(N) 给用户推荐TOP-N个物品
my_model.recommendForAllUsers(3).first()
```

