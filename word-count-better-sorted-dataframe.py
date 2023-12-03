from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# SparkSession을 생성한다.
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# 책의 각 줄을 데이터프레임으로 읽어온다.
inputDF = spark.read.text("file:///SparkCourse/book.txt")

# 정규식을 사용하여 단어를 추출하기 위해 split 함수와 explode 함수를 사용한다. 기본적으로 컬럼명은 value로 지정됨
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
# "\\W+" \는 이스케이프 문자로 뒤에 정규식을 표현하기 위해 넣어줘야 한다.
'''
func.split(inputDF.value, "\\W+")는 
비문자(숫자, 기호 등)를 구분자로 사용하여 각 줄을 단어로 분리합니다.
func.explode 함수는 각 줄의 단어들을 개별 행으로 변환하여, 
모든 단어가 별도의 행을 가지게 합니다. 
이 결과는 "word"라는 이름의 컬럼으로 words 데이터프레임에 저장됩니다.
'''
words.filter(words.word != "")

# 모든 단어를 소문자로 통일한다.
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# 각 단어의 출현 빈도를 계산한다.
wordCounts = lowercaseWords.groupBy("word").count()

# 빈도수에 따라 정렬한다.
wordCountsSorted = wordCounts.sort("count")

# 결과를 출력한다.
wordCountsSorted.show(wordCountsSorted.count())
