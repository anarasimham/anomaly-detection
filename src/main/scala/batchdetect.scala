package test

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit,udf}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkContext

import ml.dmlc.xgboost4j.scala.spark.XGBoost

import dataprep.DataPreparer

object BatchTestModel {
   def main(args: Array[String]) {
      if (args.length == 0) {
        println("Need argument for model source dir")
	System.exit(1)
      }
      implicit val sc = new SparkContext()
      val prep = new DataPreparer("fin_test.csv")
      var fraudData = prep.prepData()
      fraudData.show
      fraudData = prep.vectorize(fraudData)

      val fraudModel = XGBoost.loadModelFromHadoopFile("hdfs://anarasimham-hdp2-19-master-0.field.hortonworks.com:8020"+args(0))
      System.out.println(fraudModel)
      //val predictions = fraudModel.predict(fraudData.select("features","label"))
      val predictions = fraudModel.setExternalMemory(true).transform(fraudData).select("label", "probabilities")
      predictions.show(100)
   }
}
