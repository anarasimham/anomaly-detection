package dataprep

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit,udf}
import org.apache.spark.ml.feature.VectorAssembler

import ml.dmlc.xgboost4j.scala.spark.XGBoost

class DataPreparer(val filename: String) {
  def prepData() : DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("SparkSessionTrainer")
      .getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)

    val data = sparkSession.read
      .option("inferSchema", "true")
      .option("header", true)
      .csv(filename)

    data.printSchema
    System.out.println(data.take(5).foreach(println))
    System.out.println("OK");
    
    val Y = data.select("isFraud")
    System.out.println("Labels:")
    Y.take(10).foreach(println)
    var fraudData = data.filter("type in ('TRANSFER','CASH_OUT')")
      .drop("nameOrig")
      .drop("nameDest")
      .drop("isFlaggedFraud")

    fraudData.take(50).foreach(println)
    
    var transfers = fraudData.filter("type in ('TRANSFER')")
    var cashouts = fraudData.filter("type in ('CASH_OUT')")

    transfers = transfers.withColumn("type", lit(0))
    cashouts = cashouts.withColumn("type", lit(1))
    fraudData = transfers.union(cashouts)


    def calcDelta(
      startBal: Column, 
      endBal: Column, 
      txnAmt: Column
    ): Column = startBal-txnAmt-endBal

    fraudData = fraudData.withColumn("origDelta", 
      calcDelta(
       fraudData.col("oldBalanceOrig"), 
       fraudData.col("newBalanceOrig"),
       fraudData.col("amount")
      )
    )

    fraudData = fraudData.withColumn("destDelta", 
      calcDelta(
       fraudData.col("oldBalanceDest"), 
       fraudData.col("newBalanceDest"),
       fraudData.col("amount")
      )
    )

    fraudData.take(50).foreach(println)
    return fraudData

  }

  def vectorize(data : DataFrame) : DataFrame = {
      val assembler = new VectorAssembler()
        .setInputCols(data.columns)
        .setOutputCol("features")

      val data_xfm = assembler.transform(data)
      val data_full = data_xfm.withColumn("label", data_xfm.col("isFraud")).select("label", "features")
      return data_full
    
  }  
}
