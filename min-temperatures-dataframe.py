from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# SparkSession을 생성한다.
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# 스키마를 정의한다. 마지막 인자는 nullable
schema = StructType([ \
                     StructField("stationID", StringType(), True), 
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# 파일을 DataFrame으로 읽어온다.
df = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
df.printSchema()  # DataFrame의 스키마를 출력하는 함수 호출

# TMIN인 데이터만 필터링한다.
minTemps = df.filter(df.measure_type == "TMIN")

# stationID와 temperature만 선택한다.
stationTemps = minTemps.select("stationID", "temperature")

# 각 stationID별 최소 온도를 구한다.
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# 온도를 화씨로 변환하고 데이터를 정렬한다. 온도 컬럼에 숫자계산을 한 다음 반올림처리
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")
                                                  
# 결과를 수집하고 포맷하여 출력한다.
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
# SparkSession을 종료한다.
spark.stop()