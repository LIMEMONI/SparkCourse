from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# SparkSession을 생성한다.
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# u.data 파일의 스키마를 정의한다.
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# movie 데이터를 DataFrame으로 로드한다.
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

# 영화별로 그룹화하여 각 영화의 인기도를 계산한다. (count의 내림차순)
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# 상위 10개의 영화를 출력한다.
topMovieIDs.show(10)

# SparkSession을 종료한다.
spark.stop()
