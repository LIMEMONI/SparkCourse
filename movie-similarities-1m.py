import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

# SparkConf 및 SparkContext를 가져옴
# SparkConf: 스파크 애플리케이션의 설정을 구성하기 위한 클래스
# SparkContext: 스파크 클러스터와의 연결을 나타내는 클래스
# pyspark 모듈에서 가져옴

# EMR에서 성공적으로 실행하고 Star Wars에 대한 결과를 출력하기 위한 주석
# # s3 버킷에서 MovieSimilarities1M.py 파일을 현재 디렉토리로 복사한다.
# aws s3 cp s3://sundog-spark/MovieSimilarities1M.py 
# # s3 버킷에서 movies.dat 파일을 현재 디렉토리로 복사한다.
# aws s3 sp c3://sundog-spark/ml-1m/movies.dat
# # spark-submit 명령어를 사용하여 MovieSimilarities1M.py 파일을 실행하고, executor 메모리를 1g로 설정한 뒤에 260을 인자로 전달한다.
# spark-submit --executor-memory 1g MovieSimilarities1M.py 260  

def loadMovieNames():
    # 영화 이름을 로드하는 함수
    movieNames = {}
    with open("movies.dat", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Python 3에서는 튜플을 언패킹하여 전달할 수 없으므로,
# 여기서는 평점을 명시적으로 추출함
def makePairs( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf()
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

# ratings.dat 파일을 읽어들여서 RDD 생성
data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat")  

# 평점을 키/값 쌍으로 매핑: 사용자 ID => 영화 ID, 평점
ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# 동일한 사용자가 함께 평가한 모든 영화를 발행
# 모든 조합을 찾기 위해 자체 조인
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

# 이 시점에서 RDD는 userID => ((movieID, rating), (movieID, rating))로 구성됨

# 중복된 쌍 필터링
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# 이제 (movie1, movie2) 쌍으로 키를 지정
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

# 이제 (movie1, movie2) => (rating1, rating2)를 가지게 됨
# 이제 각 영화 쌍에 대한 모든 평점을 수집하고 유사도를 계산할 수 있음
moviePairRatings = moviePairs.groupByKey()

# 이제 (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...를 가지게 됨
# 이제 유사도를 계산할 수 있음
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()

# 원하는 경우 결과 저장
moviePairSimilarities.sortByKey()
moviePairSimilarities.saveAsTextFile("movie-sims")

# "좋은" 것으로 정의된 이 sim을 가진 영화를 필터링하여 관심 있는 영화의 유사성 추출
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 1000

    movieID = int(sys.argv[1])

    # 위의 품질 임계값에 따라 "좋은" 영화로 필터링
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # 품질 점수로 정렬
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # 찾고 있는 영화가 아닌 유사성 결과를 표시
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
