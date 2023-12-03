import re # 정규식으로 표현하기
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # 정규표현식을 사용하여 텍스트에서 특수문자를 제거하고 소문자로 변환하는 함수입니다.
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# 입력 파일을 읽어들임
input = sc.textFile("file:///sparkcourse/book.txt")  
# 단어로 분리하여 RDD 생성
words = input.flatMap(normalizeWords)  
# 단어의 개수를 세어 RDD 생성
wordCounts = words.countByValue()  

# 단어와 개수를 출력
for word, count in wordCounts.items():  
    # ASCII로 인코딩하여 정제된 단어 생성
    cleanWord = word.encode('ascii', 'ignore')  
    if (cleanWord):
        # 정제된 단어와 개수 출력
        print(cleanWord.decode() + " " + str(count))  
