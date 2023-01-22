import numpy
import pandas as pd
import re

#spark = SparkSession.builder.appName('imbalanced_binary_classification').master('local[*]').config('job.local.dir','/home/ritu/testSpark/').getOrCreate()
#data = pd.read_csv('/home/ritu/testFiles/testSetCSVs/newExpTS/classifiedArtist_dummy.csv')
#SOPHIE CHANGE
#data = pd.read_csv('./testSetCSVs/classified500tuples/songs_MDCD_dummy.csv')
#data = pd.read_csv('./testSetCSVs/newExpTS/classifiedCoNameNNNoMD_CD.csv')
#data = pd.read_csv('./dummy.csv')
#print(data.head())
#data.rename(columns={'f1':'attr','f2':'relation'}, inplace=True)
data = pd.read_csv('/home/bhim/MetaDataClass/data/ClassifiedVerticalMetaData1.csv')
TP = 0
FP = 0
FN = 0

for index, row in data.iterrows():
    if(row['label'] == 1 and row['prediction'] == 1):
      TP = TP+1
    elif(row['label'] == 0 and row['prediction'] == 1):
      FP = FP+1
    elif(row['label'] == 1 and row['prediction'] == 0):
      FN = FN+1

print('TP value is ',TP,' FP value is ',FP,' FN value is ',FN)
print(TP+FP)
Precision = float(float(TP)/float(TP+FP))
Recall = float(float(TP)/float(TP+FN))
FMeasure = (2*Precision*Recall)/(Precision+Recall)

print('Precision ',Precision,' Recall ',Recall,' Fmeasure ',FMeasure)
