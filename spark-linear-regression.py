from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # SparkSession 생성 (참고: config 섹션은 Windows 용입니다!)
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()
    
    # 데이터를 로드하고 MLLib이 예상하는 형식으로 변환합니다.
    inputLines = spark.sparkContext.textFile("regression.txt")

    # inputLines를 콤마로 분리하여 각 요소를 리스트로 변환하는 map 함수를 적용합니다.
    data = inputLines.map(lambda x: x.split(","))

    # 리스트의 첫 번째 요소를 실수로 변환하고, 두 번째 요소를 실수로 변환하여 dense 벡터로 변환하는 map 함수를 적용합니다.
    data = data.map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))


    # 이 RDD를 DataFrame으로 변환합니다.
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # 참고: RDD에서 DataFrame으로 가는 것을 피할 수 있는 경우가 많습니다.
    # 실제 데이터베이스에서 데이터를 가져오는 경우일 수도 있습니다. 또는 구조화된 스트리밍을 사용하여 데이터를 가져오는 경우일 수도 있습니다.

    # 데이터를 훈련 데이터와 테스트 데이터로 분할합니다.
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # 이제 선형 회귀 모델을 생성합니다.
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # 훈련 데이터를 사용하여 모델을 훈련합니다.
    model = lir.fit(trainingDF)

    # 이제 테스트 데이터에서 값을 예측해 봅니다.
    # 선형 회귀 모델을 사용하여 테스트 데이터의 모든 특성에 대한 예측을 생성합니다.
    fullPredictions = model.transform(testDF).cache()

    # 예측과 "알려진" 올바른 레이블을 추출합니다.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # 예측과 실제 값을 묶어서 출력합니다.
    predictionAndLabel = predictions.zip(labels).collect()

    # 각 점에 대한 예측된 값과 실제 값 출력
    for prediction in predictionAndLabel:
      print(prediction)

    # 세션 종료
    spark.stop()
