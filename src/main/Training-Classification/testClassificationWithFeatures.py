import numpy
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, RegexTokenizer, CountVectorizer
import pandas as pd
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.classification import LinearSVC, LinearSVCModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import re

def evaluation(predictions):
    evaluator = BinaryClassificationEvaluator(metricName="areaUnderPR",rawPredictionCol="rawPrediction")
    accuracy = evaluator.evaluate(predictions)
    print("Model Accuracy: ", accuracy)

def modify_regex(x):
    x = re.sub(r'[\d-]+', 'num', x)
    x = re.sub(r'[^A-Za-z0-9]+',' ',x)

    return x.lower()

spark = SparkSession.builder.appName('imbalanced_binary_classification').master('local[*]').config('job.local.dir','/home/ritu/testSpark/').getOrCreate()

data = pd.read_csv('/home/bhim/MetaDataClass/data/VerticalMetaData.csv', error_bad_lines=False, engine="python")
data[['data', 'num_of_cells', 'row_above_exists', 'row_below_exists', 'no_of_cells_above', 'no_of_cells_below',
      'label','row_number','is_row_num_2','is_row_num_3','is_row_num_4','is_row_num_5','row_greater_than_5']] = data[['data', 'num_of_cells', 'row_above_exists', 'row_below_exists', 'no_of_cells_above', 'no_of_cells_below',
      'label','row_number','is_row_num_2','is_row_num_3','is_row_num_4','is_row_num_5','row_greater_than_5']].astype(str)
print(data.head())
data.dropna()
df = spark.createDataFrame(data)
df = df.selectExpr("data as data", "cast(num_of_cells as int) as num_of_cells", "cast(row_above_exists as int) row_above_exists", "cast(row_below_exists as int) row_below_exists","cast(no_of_cells_above as int) no_of_cells_above","cast(no_of_cells_below as int) no_of_cells_below","cast(label as int) label",
                   "cast(row_number as int) as row_number","cast(is_row_num_2 as int) as is_row_num_2","cast(is_row_num_3 as int) as is_row_num_3","cast(is_row_num_3 as int) as is_row_num_3","cast(is_row_num_5 as int) as is_row_num_5","cast(row_greater_than_5 as int) as row_greater_than_5")

df.printSchema()

# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="data", outputCol="words")

#regexTokenizer.transform(df.na.drop(Array("words")))

# bag of words count
countVectors = CountVectorizer(inputCol="words", outputCol="wordfeatures", vocabSize=500000, minDF=5)



pipelineModel = PipelineModel.load("/home/bhim/MetaDataClass/pipeLineModel/MetaDataClassifierSVMEMDED")
dataset = pipelineModel.transform(df)

assembler = VectorAssembler(inputCols=['wordfeatures'],outputCol="features")
dataset = assembler.transform(dataset)
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
#print("=======================================")
#dataset.show()
#print("=======================================")


#loadedModel = LogisticRegressionModel.load("/home/bhim/MetaDataClass/Model/MetaDataClassifierLR1MD1D");
loadedModel = LinearSVCModel.load("/home/bhim/MetaDataClass/Model/MetaDataClassifierSVMEMDED")
#loadedModel = NaiveBayesModel.load("/home/bhim/MetaDataClass/Model/MetaDataClassifierNB1MDAD")
#loadedModel = LinearSVCModel.load("./Models/newExpModels/verticalExp/SVMModelSchoolWithoutMD_DD")
#loadedModel.numfeatures
#lr = LogisticRegression(maxIter=15)
#lrModel = lr.fit(trainingData)
predictions = loadedModel.transform(dataset)

#predictions.filter(predictions['prediction'] == 1) \
#    .select("id","features","prediction") \
#    .orderBy("probability", ascending=False) \
#    .show(n = 30, truncate = 30)

predictions.select("label","prediction","data").toPandas().to_csv('/home/bhim/MetaDataClass/data/ClassifiedVerticalMetaData1.csv', encoding='utf-8')
#predictions.select("label","prediction","attr","name").toPandas().to_csv('./classified/t2dv2_cols/LRCoName_NoMDDD.csv', encoding='utf-8')
predictions.printSchema()
evaluation(predictions)
