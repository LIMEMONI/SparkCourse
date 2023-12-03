from pyspark.sql import SparkSession

# SparkSession을 생성한다.
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# CSV 파일을 읽어서 DataFrame을 생성한다.
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")
    
# DataFrame의 스키마를 출력한다.
print("Here is our inferred schema:")
people.printSchema()

# name 컬럼을 선택하여 출력한다.
print("Let's display the name column:")
people.select("name").show()

# 21세 미만인 사람들을 필터링하여 출력한다.
print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

# age 컬럼을 기준으로 그룹화하여 개수를 센다.
print("Group by age")
people.groupBy("age").count().show()

# 모든 사람들의 나이를 10살씩 증가시킨다.
print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

# SparkSession을 종료한다.
spark.stop()
