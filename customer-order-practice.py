# SparkSession을 생성한다.
spark = SparkSession.builder.appName("CusOrderPractice").getOrCreate()

# 스키마를 정의한다.
schema = StructType([
    StructField('cus_id', StringType(), True),
    StructField('unnamed', StringType(), True),
    StructField('amount_spend', FloatType(), True)
])

# csv 파일을 읽어서 DataFrame으로 생성한다.
df = spark.read.schema(schema).csv('file:///SparkCourse/customer-orders.csv')

# DataFrame의 스키마를 출력한다.
df.printSchema()

# cus_id와 amount_spend 열만 선택한다.
spends = df.select('cus_id','amount_spend')

# cus_id 별로 amount_spend를 합산하여 total_spend로 그룹화한다.
# 합산된 값을 소수점 둘째 자리까지 반올림한다.
# total_spend로 정렬한다.
spends.groupBy('cus_id').agg(func.round(func.sum('amount_spend'),2)\
    .alias('total_spend')).sort('total_spend').show()

# SparkSession을 종료한다.
spark.stop()
