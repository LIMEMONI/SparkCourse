#Boilerplate stuff:
# SparkConf 및 SparkContext를 가져온다.
from pyspark import SparkConf, SparkContext

# SparkConf 객체를 생성하고 로컬 모드에서 실행하며, 애플리케이션 이름을 "DegreesOfSeparation"으로 설정한다.
conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")

# SparkContext 객체를 생성하고 위에서 생성한 SparkConf를 사용하여 설정한다.
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
# 시작 캐릭터 ID를 5306으로 설정한다. (SpiderMan)
startCharacterID = 5306

# 목표 캐릭터 ID를 14로 설정한다. (ADAM 3,031)
targetCharacterID = 14

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
# hitCounter라는 이름의 accumulator를 생성한다. 이 accumulator는 BFS 탐색 중 목표 캐릭터를 찾았을 때 신호를 보내기 위해 사용된다.
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    # 입력된 line을 공백을 기준으로 분리하여 fields에 저장
    fields = line.split()
    # 첫 번째 필드를 heroID로 저장
    heroID = int(fields[0])
    # connections 리스트 초기화
    connections = []
    # fields[1:]부터 반복하여 connections 리스트에 연결된 노드들을 저장
    for connection in fields[1:]:
        connections.append(int(connection))

    # 초기값 설정
    color = 'WHITE'
    distance = 9999

    # 만약 heroID가 startCharacterID와 같다면
    if (heroID == startCharacterID):
        # color를 'GRAY'로 설정하고 distance를 0으로 설정
        color = 'GRAY'
        distance = 0

    # (heroID, (connections, distance, color)) 형태로 반환
    return (heroID, (connections, distance, color))


def createStartingRdd():
    # "file:///sparkcourse/marvel-graph.txt" 파일을 읽어서 RDD 생성
    inputFile = sc.textFile("file:///sparkcourse/marvel-graph.txt")
    # convertToBFS 함수를 적용하여 RDD 변환
    return inputFile.map(convertToBFS)

def bfsMap(node):
    # node에서 characterID와 data를 추출
    characterID = node[0]
    data = node[1]
    # data에서 connections, distance, color를 추출
    connections = data[0]
    distance = data[1]
    color = data[2]

    # 결과를 저장할 리스트 초기화
    results = []

    # 만약 이 노드를 확장해야 한다면
    if (color == 'GRAY'):
        # connections에 있는 각 노드에 대해 반복
        for connection in connections:
            # 새로운 characterID와 distance, color를 설정
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            # targetCharacterID와 connection이 같다면 hitCounter를 1 증가
            if (targetCharacterID == connection):
                hitCounter.add(1)

            # (newCharacterID, ([], newDistance, newColor)) 형태로 newEntry 생성 후 results에 추가
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        # 이 노드를 처리했으므로 color를 'BLACK'으로 설정
        color = 'BLACK'

    # 입력된 노드를 결과 리스트에 추가하여 손실되지 않도록 함
    results.append( (characterID, (connections, distance, color)) )
    return results

def bfsReduce(data1, data2):
    # data1과 data2에서 edges, distance, color 추출
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    # 최소 거리와 가장 어두운 색상을 저장할 변수 초기화
    distance = 9999
    color = color1
    edges = []

    # edges1이 비어있지 않다면 edges에 추가
    if (len(edges1) > 0):
        edges.extend(edges1)
    # edges2가 비어있지 않다면 edges에 추가
    if (len(edges2) > 0):
        edges.extend(edges2)

    # 최소 거리를 유지
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # 가장 어두운 색상을 유지
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    # (edges, distance, color) 형태로 반환
    return (edges, distance, color)


#Main program here:
# 메인 프로그램 시작

iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))
    # BFS 반복 횟수 출력

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)
    # 현재 RDD에 대해 bfsMap 함수를 적용하여 새로운 정점 생성

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")
    # mapped RDD의 원소 개수 출력

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break
        # hitCounter 값이 0보다 크면 타겟 캐릭터를 찾은 것이므로 반복문 종료

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
    # mapped RDD를 bfsReduce 함수를 사용하여 각 character ID에 대해 데이터를 결합하여 새로운 RDD 생성
