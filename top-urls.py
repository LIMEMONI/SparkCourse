from pyspark.sql import SparkSession

import pyspark.sql.functions as func

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs")


# 정규 표현식을 사용하여 공통 로그 형식을 DataFrame으로 파싱합니다.
contentSizeExp = r'\s(\d+)$'  # content size를 추출하기 위한 정규 표현식
statusExp = r'\s(\d{3})\s'  # 상태 코드를 추출하기 위한 정규 표현식
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'  # 메소드, 엔드포인트, 프로토콜을 추출하기 위한 정규 표현식
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'  # 타임스탬프를 추출하기 위한 정규 표현식
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'  # 호스트를 추출하기 위한 정규 표현식

logsDF = accessLines.select(func.regexp_extract('value', hostExp, 1).alias('host'),  # 호스트 추출
                         func.regexp_extract('value', timeExp, 1).alias('timestamp'),  # 타임스탬프 추출
                         func.regexp_extract('value', generalExp, 1).alias('method'),  # 메소드 추출
                         func.regexp_extract('value', generalExp, 2).alias('endpoint'),  # 엔드포인트 추출
                         func.regexp_extract('value', generalExp, 3).alias('protocol'),  # 프로토콜 추출
                         func.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),  # 상태 코드 추출
                         func.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))  # content size 추출

logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())  # 현재 타임스탬프를 eventTime 컬럼으로 추가

# Keep a running count of endpoints

# 엔드포인트별로 실행 횟수를 계산합니다.
endpointCounts = logsDF2.groupBy(func.window(func.col("eventTime"), \
      "30 seconds", "10 seconds"), func.col("endpoint")).count()

sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())  # 실행 횟수를 기준으로 내림차순으로 정렬

# Display the stream to the console

# 스트림을 콘솔에 출력합니다.
query = sortedEndpointCounts.writeStream.outputMode("complete").format("console") \
      .queryName("counts").start()

# Wait until we terminate the scripts
query.awaitTermination()  # 스크립트가 종료될 때까지 대기합니다.

# Stop the session
spark.stop()  # SparkSession을 종료합니다.
