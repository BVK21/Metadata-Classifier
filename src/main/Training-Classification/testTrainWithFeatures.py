import numpy
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, RegexTokenizer, CountVectorizer, Word2Vec, Word2VecModel
import pandas as pd
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes,NaiveBayesModel
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator

def naiveBayes(dataset):
    nb = NaiveBayes(smoothing=1.0)
    nbModel = nb.fit(dataset)
    #nbModel.write().overwrite().save('./Models/newExpModels/NBModelSongsWithMD11')
    nbModel.save('/home/bhim/MetaDataClass/Model/MetaDataClassifierNBEMDED')
    return nbModel;

def logisticRegression(dataset):
    lr = LogisticRegression(maxIter=50)
    lrModel = lr.fit(trainingData)
    #output_dir = './LRModel2'
    lrModel.save('/home/bhim/MetaDataClass/Model/MetaDataClassifierLREMDED')
    return lrModel;

def SVM(dataset):
    svm = LinearSVC(maxIter=10, regParam=0.1)
    lsvcModel = svm.fit(dataset)
    lsvcModel.save('/home/bhim/MetaDataClass/Model/MetaDataClassifierSVMWTEMDED1')
    return lsvcModel;

def evaluation(predictions):
    evaluator = BinaryClassificationEvaluator(metricName="areaUnderPR",rawPredictionCol="rawPrediction")
    accuracy = evaluator.evaluate(predictions)
    print("Model Accuracy: ", accuracy)
    #predictionRDD = predictions.select(['label', 'prediction'])
    #                        .rdd.map(lambda line: (line[1], line[0]))
    #metrics = MulticlassMetrics(predictionRDD)
    #print "%s" % metrics.precision()
    #print "%s" % metrics.weightedRecall

# initiate our session and read the main CSV file, then we print the #dataframe schema
spark = SparkSession.builder.appName('imbalanced_binary_classification').master('local[*]').config('job.local.dir','/home/ritu/testSpark/').getOrCreate()
#word2VecModel = Word2VecModel.load('word2vecAttrh_300.model')
#print(word2VecModel.getVectors())

#data = spark.read.csv('./trainSet/engSongsAsTuplesTR.csv', header=True, inferSchema=True)
#data.cache()
data = pd.read_csv('/home/bhim/MetaDataClass/data/WTTrainData3.csv',error_bad_lines=False, engine="python")
print(data.head())
data[['data', 'num_of_cells', 'row_above_exists', 'row_below_exists', 'no_of_cells_above', 'no_of_cells_below',
      'label','row_number','is_row_num_2','is_row_num_3','is_row_num_4','is_row_num_5','row_greater_than_5']] = data[['data', 'num_of_cells', 'row_above_exists', 'row_below_exists', 'no_of_cells_above', 'no_of_cells_below',
      'label','row_number','is_row_num_2','is_row_num_3','is_row_num_4','is_row_num_5','row_greater_than_5']].astype(str)

print(data.head())
#df = data.selectExpr("f1 as attr","f2 as name","cast(label as int) label")
#df = df.filter("name != ''")
#df = df.na.drop()
data.dropna()
#data.printSchema()
print(data)
df = spark.createDataFrame(data)
df = df.selectExpr("data as data", "cast(num_of_cells as int) as num_of_cells", "cast(row_above_exists as int) row_above_exists", "cast(row_below_exists as int) row_below_exists","cast(no_of_cells_above as int) no_of_cells_above","cast(no_of_cells_below as int) no_of_cells_below","cast(label as int) label",
                   "cast(row_number as int) as row_number","cast(is_row_num_2 as int) as is_row_num_2","cast(is_row_num_3 as int) as is_row_num_3","cast(is_row_num_3 as int) as is_row_num_3","cast(is_row_num_5 as int) as is_row_num_5","cast(row_greater_than_5 as int) as row_greater_than_5")
df.printSchema()

#word2VecModel = Word2VecModel.load('word2vecAttrh_300.model')
#print(word2VecModel.getVectors())
#print df.groupby('label').count()

# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="data", outputCol="words")
#regexTokenizer1 = RegexTokenizer(inputCol="attr", outputCol="attrWords")
#regexTokenizer.transform(df.na.drop(Array("words")))

# bag of words count
countVectors = CountVectorizer(inputCol="words", outputCol="wordfeatures", vocabSize=500000, minDF=5)
#countVectors1 = CountVectorizer(inputCol="attrWords", outputCol="attrFeatures", vocabSize=500000, minDF=5)

#(trainingData, testData) = df.randomSplit([0.7, 0.3], seed = 100)
#trainingData.cache()
#testData.cache()

nb = NaiveBayes(smoothing=1.0, modelType="bernoulli")
pipeline = Pipeline(stages=[regexTokenizer,countVectors])
#pipeline = Pipeline(stages=[regexTokenizer,countVectors])
model = pipeline.fit(df)
model.save('/home/bhim/MetaDataClass/pipeLineModel/MetaDataClassiifierSVMWTEMDED1')
dataset = model.transform(df)

assembler = VectorAssembler(inputCols=['wordfeatures'],outputCol="features")
#assembler = VectorAssembler(inputCols=['wordfeatures'],outputCol="features")
dataset = assembler.transform(dataset)
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
#print("=======================================")
#dataset.show()
#print("=======================================")

trainingData.cache()
testData.cache()

#crossValidation(pipeline,dataset)
#print trainingData.count()
#print testData.count()

trainModel = SVM(dataset)
#trainModel = naiveBayes(dataset)
#trainModel = logisticRegression(dataset)

predictions = trainModel.transform(testData)

predictions.filter(predictions['prediction'] == 1) \
    .select("features","label","prediction") \
    .orderBy("prediction", ascending=False) \
    .show(n = 10, truncate = 30)

# Make predictions on testData so we can measure the accuracy of our model on new data
#predictions = model.transform(testData)

# Display what results we can view
predictions.printSchema()

# Compute raw scores on the test set
#predictionAndLabels = testData.map(lambda lp: predictions.select("prediction"), testData.label)

#metrics = BinaryClassificationMetrics(predictions.select("prediction"))
# Area under precision-recall curve
#print("Precision = %s" % metrics.precision)

evaluation(predictions)
