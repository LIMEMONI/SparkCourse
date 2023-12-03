from pyspark import SparkConf, SparkContext

# SparkConf 객체 생성
conf = SparkConf().setMaster("local").setAppName("WordCount")
# SparkContext 객체 생성
sc = SparkContext(conf = conf)

# 텍스트 파일을 RDD로 로드
input = sc.textFile("file:///sparkcourse/book.txt")
# 각 줄을 단어로 분할하여 RDD 생성
words = input.flatMap(lambda x: x.split())
# 단어의 개수를 세어 RDD 생성
wordCounts = words.countByValue()

# 단어와 개수를 출력
for word, count in wordCounts.items():
    # 단어를 ASCII로 인코딩하여 정제
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        # 정제된 단어와 개수를 출력
        print(cleanWord.decode() + " " + str(count))
        