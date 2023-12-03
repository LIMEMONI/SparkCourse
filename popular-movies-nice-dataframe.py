# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020

이 코드는 Spark를 사용하여 인기있는 영화를 찾는 예제입니다.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    # u.ITEM 파일의 경로를 여기에 입력하세요.
    with codecs.open("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# loadMovieNames 함수를 호출하여 반환된 결과를 nameDict에 저장하고, 
# sparkContext.broadcast를 사용하여 브로드캐스트 변수로 만듭니다.
nameDict = spark.sparkContext.broadcast(loadMovieNames())  

# u.data를 읽을 때 스키마 생성
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# 영화 데이터를 데이터프레임으로 로드
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

# 영화별로 카운트
movieCounts = moviesDF.groupBy("movieID").count()

# 브로드캐스트된 딕셔너리에서 영화 이름을 찾기 위한 사용자 정의 함수 생성
def lookupName(movieID):
    return nameDict.value[movieID]

# lookupName 함수를 UDF(User Defined Function)로 등록
lookupNameUDF = func.udf(lookupName)  

# 새로운 UDF를 사용하여 movieTitle 열 추가
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# 결과 정렬
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# 상위 10개 출력
sortedMoviesWithNames.show(10, False)

# 세션 종료
spark.stop()
