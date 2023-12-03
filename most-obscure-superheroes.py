from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# SparkSession을 생성하여 spark 변수에 할당
spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

# 스키마 정의
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Marvel-names.txt 파일을 읽어서 names 변수에 할당
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

# Marvel-graph.txt 파일을 읽어서 lines 변수에 할당
lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# lines 데이터프레임에 id 컬럼과 connections 컬럼을 추가
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# connections 데이터프레임에서 connections 컬럼의 최소값을 구함
minConnectionCount = connections.agg(func.min("connections")).first()[0]

# connections 데이터프레임에서 connections 컬럼이 최소값인 행들을 필터링하여 minConnections 변수에 할당
minConnections = connections.filter(func.col("connections") == minConnectionCount)

# minConnections와 names 데이터프레임을 id 컬럼을 기준으로 조인하여 minConnectionsWithNames 변수에 할당
minConnectionsWithNames = minConnections.join(names, "id")

# 최소 연결 수가 몇 개인지 출력
print("The following characters have only " + str(minConnectionCount) + " connection(s):")

# minConnectionsWithNames 데이터프레임에서 name 컬럼만 선택하여 출력
minConnectionsWithNames.select("name").show()