# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

# SparkContext 및 StreamingContext 가져오기
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

# 정규 표현식을 사용하기 위해 regexp_extract 가져오기
from pyspark.sql.functions import regexp_extract

# SparkSession 생성 (Windows 용 설정)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StructuredStreaming").getOrCreate()

# 새로운 로그 데이터를 모니터링하고, raw lines을 accessLines로 읽기
accessLines = spark.readStream.text("logs")

# 공통 로그 형식을 DataFrame으로 파싱
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),  # value에서 host 추출
                         regexp_extract('value', timeExp, 1).alias('timestamp'),  # value에서 timestamp 추출
                         regexp_extract('value', generalExp, 1).alias('method'),  # value에서 method 추출
                         regexp_extract('value', generalExp, 2).alias('endpoint'),  # value에서 endpoint 추출
                         regexp_extract('value', generalExp, 3).alias('protocol'),  # value에서 protocol 추출
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),  # value에서 status 추출 후 integer로 변환
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))  # value에서 content_size 추출 후 integer로 변환

# 상태 코드별로 액세스 수를 계속해서 유지
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# 쿼리를 시작하여 결과를 콘솔에 출력
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# 종료될 때까지 계속 실행
query.awaitTermination()

# 세션 정리
spark.stop()
