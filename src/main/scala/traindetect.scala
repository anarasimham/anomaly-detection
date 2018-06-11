package train

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit,udf}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkContext

import ml.dmlc.xgboost4j.scala.spark.XGBoost

import dataprep.DataPreparer

object TrainModel {
   def main(args: Array[String]) {
      if (args.length == 0) {
        println("Need argument for model destination dir")
	System.exit(1)
      }
      implicit val sc = new SparkContext()
      val prep = new DataPreparer("fin_data.csv")
      var fraudData = prep.prepData()
      fraudData.show
     
      val Array(train, test) = fraudData.randomSplit(Array[Double](0.7,0.3), 18)
      val train_full = prep.vectorize(train)
      val test_full = prep.vectorize(test)

      val numRound = 10
      val numWorkers = 2

      val paramMap = List(
        //"eta"-> 0.023f,
        //"max_depth" -> 10,
        //"min_child_weight" -> 3.0,
        //"subsample" -> 1.0,
        //"colsample_bytree" -> 0.82,
        //"colsample_bylevel" -> 0.9,
        //"base_score" -> 0.005,
        //"eval_metric" -> "auc",
        //"seed" -> 49,
        //"silent" -> 1,
        "objective" -> "binary:logistic"
      ).toMap
      println("Starting xgboost")     
      val xgBoostModel = XGBoost.trainWithDataFrame(train_full, paramMap, round=numRound, nWorkers=numWorkers, useExternalMemory=true)
      val predictions = xgBoostModel.setExternalMemory(true).transform(test_full).select("label", "probabilities")
      predictions.show(10)
      
      xgBoostModel.saveModelAsHadoopFile(args(0))
   }
}
