from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

# SparkSession을 생성한다.
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

# CSV 파일을 읽어들인다.
lines = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/fakefriends-header.csv")

# "age"와 "friends" 컬럼만 선택한다.
friendsByAge = lines.select("age", "friends")

# friendsByAge를 "age"로 그룹화하고 평균을 계산한다.
friendsByAge.groupBy("age").avg("friends").show()

# 정렬된 결과를 출력한다.
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# 더 예쁘게 포맷팅한다.
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# 컬럼 이름을 사용자 정의로 변경한다.
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

# SparkSession을 종료한다.
spark.stop()
