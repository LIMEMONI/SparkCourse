from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data):
    # SparkSession 객체 생성
    # SparkSession은 Spark 기능을 사용하기 위한 진입점 역할을 한다.
    spark = SparkSession.builder.getOrCreate()
    
    # xx, xy, yy 열 계산
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # 분자, 분모, numPairs 열 계산
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # score 계산 및 필요한 열만 선택 (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# 주어진 영화 ID에 해당하는 영화 이름을 가져오는 함수
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

# 영화 이름 스키마 정의
movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
    
# 영화 데이터 스키마 정의
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# movieNames 데이터셋을 생성하고 movieID와 movieTitle을 broadcast로 만듭니다.
# ISO-885901 문자셋을 적용합니다.
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("file:///SparkCourse/ml-100k/u.item")

# Load up movie data as dataset
# movies 데이터셋을 생성합니다.
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("file:///SparkCourse/ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating")

# 동일한 사용자가 함께 평가한 모든 영화를 추출합니다.
# 자체 조인을 사용하여 모든 조합을 찾습니다.
# 영화 쌍과 평점 쌍을 선택합니다.
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    # scoreThreshold와 coOccurrenceThreshold를 충족하는 유사한 영화를 필터링합니다.
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    # 품질 점수로 정렬합니다.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # 우리가 찾고 있는 영화가 아닌 유사성 결과를 표시합니다.
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
