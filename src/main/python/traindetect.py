import xgboost as xgb

from pyspark.sql import SparkSession

from numpy import loadtxt
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

#import pyarrow as pa

ss = SparkSession.builder.appName("Fraud-model-builder").getOrCreate()

fin_data = ss.read.csv('hdfs://user/root/fin_data.csv')
fin_data.show()

#fs = pa.hdfs.connect('anarasimham-ds0.field.hortonworks.com', 8020, 'root')


#with fs.open('/user/root/fin_data.csv', 'rb') as f:
#   dtrain = xgb.DMatrix(f)
