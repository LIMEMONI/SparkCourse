from pyspark import SparkConf, SparkContext

# SparkConf 객체를 생성하고 로컬 모드로 실행하며 애플리케이션 이름을 "MaxTemperatures"로 설정한다.
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")

# SparkContext 객체를 생성하고 위에서 생성한 SparkConf 객체를 전달한다.
sc = SparkContext(conf = conf)

# 각 줄을 파싱하는 함수를 정의한다.
def parseLine(line):
    # 쉼표로 구분된 필드를 분리한다.
    fields = line.split(',')
    # 첫 번째 필드는 stationID로 저장한다.
    stationID = fields[0]
    # 세 번째 필드는 entryType으로 저장한다.
    entryType = fields[2]
    # 네 번째 필드는 온도를 나타낸다.
    temperature = float(fields[3])
    # stationID, entryType, temperature을 튜플로 반환한다.
    return (stationID, entryType, temperature)

# 파일을 읽어 RDD를 생성한다.
lines = sc.textFile("file:///SparkCourse/1800.csv")

# 각 줄을 파싱하여 RDD를 생성한다.
parsedLines = lines.map(parseLine)

# TMAX을 포함하는 entryType을 필터링하여 RDD를 생성한다.
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# stationID와 temperature만을 추출하여 RDD를 생성한다.
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# stationID를 기준으로 최고 온도를 계산한다.
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
## -> 같은 키 기준으로 가장 최고 온도를 구하는 과정

# 결과를 수집한다.
results = maxTemps.collect();

# 결과를 출력한다.
for result in results:
    # stationID와 온도를 포맷에 맞게 출력한다.
    print(result[0] + "\t{:.2f}C".format(result[1]))
