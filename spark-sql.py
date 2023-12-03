from pyspark.sql import SparkSession
from pyspark.sql import Row

# SparkSession 생성
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    # 쉼표로 구분된 각 필드를 분리하여 리스트로 반환
    fields = line.split(',')
    # Row 객체 생성하여 필드 값을 지정
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# fakefriends.csv 파일을 읽어 RDD 생성
lines = spark.sparkContext.textFile("fakefriends.csv")
# mapper 함수를 적용하여 RDD를 변환하여 생성된 Row 객체들로 이루어진 RDD 생성
people = lines.map(mapper)

# 스키마를 추론하고 DataFrame을 생성한 후 캐시
schemaPeople = spark.createDataFrame(people).cache()
# DataFrame을 임시 테이블로 등록
schemaPeople.createOrReplaceTempView("people")

# SQL 쿼리를 사용하여 DataFrame에서 실행
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# SQL 쿼리의 결과는 RDD이며 일반적인 RDD 연산을 지원
for teen in teenagers.collect():
    # 각 teenager의 정보 출력
    print(teen)

# SQL 쿼리 대신 함수를 사용할 수도 있음
# age를 기준으로 그룹화하고 count를 계산한 뒤 age를 기준으로 정렬하여 출력
schemaPeople.groupBy("age").count().orderBy("age").show()

# SparkSession 종료
spark.stop()
