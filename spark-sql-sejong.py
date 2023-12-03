from pyspark.sql import SparkSession

# SparkSession을 생성한다.
spark = SparkSession.builder.appName('SparkSQL_sejong').getOrCreate()

# 'fakefriends-header.csv' 파일을 읽어서 DataFrame을 생성한다.
friends = spark.read.option('header', 'true').option('inferSchema', 'true')\
    .csv('file:///SparkCourse/fakefriends-header.csv')
    
# DataFrame의 스키마를 출력한다.
print("Here is our inferred schema:")
friends.printSchema()



# 나이별 평균 친구 수를 계산하여 출력한다.
print('나이별 평균 친구 수')
friends.groupBy('age').avg('friends').show()

# FRIENDS라는 임시 뷰를 생성한다.
friends.createOrReplaceTempView("FRIENDS")

# FRIENDS 뷰에서 나이가 40 이상이고 친구 수가 300 미만인 데이터를 조회한다.
results = spark.sql('''
          SELECT * FROM FRIENDS
          WHERE AGE >= 40 AND FRIENDS < 300;
          ''')
print('='*30)

# 결과를 출력한다.
for each in results.collect():
    print(each)

# SparkSession을 종료한다.
spark.stop()