from pyspark.sql import SparkSession  # SparkSession 모듈을 임포트합니다.
from pyspark.sql.types import StructType, StructField, IntegerType, LongType  # StructType, StructField, IntegerType, LongType 모듈을 임포트합니다.
from pyspark.ml.recommendation import ALS  # ALS 모듈을 임포트합니다.
import sys  # sys 모듈을 임포트합니다.
import codecs  # codecs 모듈을 임포트합니다.
import os

os.environ['PYSPARK_PYTHON'] = sys.executable

# 영화 이름을 로드하는 함수
def loadMovieNames():
    movieNames = {}
    # u.ITEM 파일의 경로를 변경하세요.
    with codecs.open("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# SparkSession 생성
spark = SparkSession.builder.appName("ALSExample").getOrCreate()

# 영화 데이터의 스키마 정의
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# 영화 이름을 로드
names = loadMovieNames()

# 평점 데이터를 읽어옴
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///SparkCourse/ml-100k/u.data")

print("Training recommendation model...")

# ALS 모델 생성 및 학습
# ALS 모델 객체 생성
als = ALS()
# ALS 모델의 최대 반복 횟수 설정
als.setMaxIter(5)
# ALS 모델의 정규화 파라미터 설정
als.setRegParam(0.01)
# 사용자 ID 컬럼 설정
als.setUserCol("userID")
# 영화 ID 컬럼 설정
als.setItemCol("movieID")
# 평점 컬럼 설정
als.setRatingCol("rating")

model = als.fit(ratings)

# 추천을 받을 사용자 ID를 수동으로 데이터프레임으로 생성
userID = int(sys.argv[1])  # 사용자 ID를 명령행 인수로 입력받아 정수로 변환하여 변수 userID에 저장한다.
userSchema = StructType([StructField("userID", IntegerType(), True)])  # userSchema 변수에 StructType 객체를 생성하여 할당한다. 객체는 "userID"라는 필드명과 IntegerType 타입을 가지며, null 값을 허용한다.
users = spark.createDataFrame([[userID,]], userSchema)

# 사용자에게 영화 추천
recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userID))

# 추천 결과 출력
for userRecs in recommendations:
    myRecs = userRecs[1]  # userRecs는 (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: # myRecs는 사용자에게 추천된 영화들의 열(column)만을 가지고 있음
        movie = rec[0] # 각 추천 결과에서 영화 ID와 평점을 추출
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))

