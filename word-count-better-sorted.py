import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")  # 입력 파일을 읽어들임
words = input.flatMap(normalizeWords)  # 단어로 분리하여 RDD 생성

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)  # 단어별로 카운트하여 RDD 생성
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()  # 카운트를 기준으로 정렬하여 RDD 생성
results = wordCountsSorted.collect()  # 결과를 수집하여 리스트로 반환

for result in results:  # 결과 출력
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
