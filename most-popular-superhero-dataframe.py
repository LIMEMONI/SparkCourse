from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# SparkSession을 생성하여 spark 변수에 할당
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# 스키마 정의
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Marvel-names.txt 파일을 읽어서 names 변수에 할당
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

# Marvel-graph.txt 파일을 읽어서 lines 변수에 할당
lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# lines 데이터프레임에 id 컬럼과 connections 컬럼을 추가
# withColumn : , 뒤의 작업으로 새로운 컬럼을 만든다.
# value 컬럼을 공백을 기준으로 분할하여 첫 번째 요소를 id 컬럼으로 추가
# value 컬럼을 공백을 기준으로 분할하여 요소의 개수에서 1을 뺀 값을 connections 컬럼으로 추가
# id 컬럼을 기준으로 그룹화하고 connections 컬럼의 합을 계산하여 connections 컬럼으로 집계
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))  

# connections 데이터프레임을 connections 컬럼을 기준으로 내림차순 정렬하고 가장 첫 번째 행을 mostPopular 변수에 할당
mostPopular = connections.sort(func.col("connections").desc()).first()

# names 데이터프레임에서 id 컬럼이 mostPopular[0]과 일치하는 행을 필터링하고 name 컬럼을 선택하여 mostPopularName 변수에 할당
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# 가장 인기 있는 슈퍼히어로의 이름과 공동 등장 횟수를 출력
print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
