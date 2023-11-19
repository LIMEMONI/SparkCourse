from pyspark import SparkConf, SparkContext

# SparkConf 객체 생성
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
# SparkContext 객체 생성
sc = SparkContext(conf = conf)

# 입력된 라인을 파싱하는 함수 정의
def parseLine(line):
    # 쉼표로 구분된 필드로 분리
    fields = line.split(',')
    # 나이 필드를 정수로 변환
    age = int(fields[2])
    # 친구 수 필드를 정수로 변환
    numFriends = int(fields[3])
    # 나이와 친구 수를 튜플로 반환
    return (age, numFriends)

# 파일에서 라인을 읽어 RDD 생성
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# RDD의 각 요소에 대해 parseLine 함수를 적용하여 RDD 생성
rdd = lines.map(parseLine)

# 나이를 키로 하고 (친구 수, 1) 튜플을 값으로 하는 RDD 생성
## (33,385) -> (33,(385,1))  : value값만 바꾼다
## (33,2) -> (33,(2,1))
## (33,(385+2,1+1)) : 같은 키를 가진 RDD를 기준으로 reduce한다 
## -> 33살이 등장한 숫자와 33살의 친구수의 합을 구할 수 있음
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 나이를 키로 하고 평균 친구 수를 값으로 하는 RDD 생성
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# 결과를 수집하여 리스트로 반환
results = averagesByAge.collect()
# 결과를 출력
for result in results:
    print(result)
