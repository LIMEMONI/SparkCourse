from pyspark import SparkConf, SparkContext

# SparkConf 객체 생성
conf = SparkConf().setMaster("local").setAppName("Sejong")
# SparkContext 객체 생성
sc = SparkContext(conf = conf)

def line_to_data(line):
    # 각 줄을 쉼표로 분할하여 리스트로 변환
    listed_data = line.split(',')
    # 리스트에서 첫 번째 요소를 정수로 변환하여 키 데이터로 설정
    key_data = int(listed_data[0])
    # 리스트에서 세 번째 요소를 실수로 변환하여 값 데이터로 설정
    value_data = float(listed_data[2])
    # 키와 값 데이터를 튜플로 반환
    return (key_data, value_data)

# 텍스트 파일을 RDD로 로드
lines = sc.textFile("file:///sparkcourse/customer-orders.csv")
# 각 줄을 단어로 분할하여 RDD 생성
friends = lines.map(line_to_data).reduceByKey(lambda x, y: (x+ y))

# 키와 값을 뒤집어서 새로운 RDD 생성
flipped = friends.map(lambda x: (x[1], x[0])).sortByKey()
# 다시 키와 값을 뒤집어서 원래 형태로 복원
friends = flipped.map(lambda x: (x[1], x[0]))

# 결과를 수집
results = friends.collect()

# 결과를 출력
for k,v in results:
    print(f'key : {k} / value : {v}')