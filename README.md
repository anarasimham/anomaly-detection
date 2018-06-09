# anomaly-detection

This is a machine learning model based on the following Kaggle competition:
https://www.kaggle.com/arjunjoshua/predicting-fraud-in-financial-payment-services

I've taken the approach and converted it into a Scala-based productionalized model which allows both training of the model as well as evaluation of test data

### Pre-requisites

A few pre-requisites that you'll need to setup before being able to execute this application:
- Compile the XGBoost library and install it locally. Compilation instructions can be found here: http://xgboost.readthedocs.io/en/latest/build.html
  - Once you download the repository, you'll need to `git checkout tags/v0.71` to have the version of the XGBoost code that I used
- Install CMake's latest version to build with. On CentOS, I had to manually install using `wget https://cmake.org/files/v3.6/cmake-3.6.2.tar.gz`
- Copy the transaction data from the Kaggle source site into HDFS
  - First download the data from the above Kaggle link
  - Then `hdfs dfs -put` the file
- Install Maven. On CentOS I was able to `yum install maven`
- Install SBT: https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Linux.html
- A YARN cluster on which you can run Spark jobs

### Building 

1. Update the `create_jni.py` file in the `jvm_packages` folder of the XGBoost source before building. Add the following lines to the `CONFIG = ` section
    1. `"USE_HDFS": "ON"` (already present, change OFF to ON)
    1. `"HDFS_INCLUDE_DIR": "/usr/hdp/<HDP-VERSION>/usr/include/"`
    1. `"HDFS_LIB": "/usr/hdp/<HDP-VERSION>/usr/lib/libhdfs.so"`
1. `cd` into the `jvm-packages` folder of the XGBoost Git repository and `mvn install`. This should install the XGBoost package locally (`~/.m2`)
1. Go into the `anomaly-detection` Git repo that you've cloned and run `sbt assembly`

### Executing

Train the model by executing the following:
`SPARK_MAJOR_VERSION=2 spark-submit --class train.TrainModel --deploy-mode cluster --master yarn --executor-memory 2G --driver-memory 2G target/scala-2.11/Anomaly\ Trainer-assembly-1.0.jar /path/to/save/FraudModel`

The argument at the end of the above command is where you would like to save the trained model.

Test the trained model by executing the following:
`SPARK_MAJOR_VERSION=2 spark-submit --class test.BatchTestModel --deploy-mode cluster --master yarn --executor-memory 2G --driver-memory 2G target/scala-2.11/Anomaly\ Trainer-assembly-1.0.jar /path/to/saved/FraudModel /path/to/testData`
The above are the same except for the class being executed

You will need the test dataset, which is in the `resources` folder, on HDFS. As you can see in the above test CLI, there are two arguments.
- First is the location of the saved model (same as what you specified in the training step)
- Second is the location of the test data
