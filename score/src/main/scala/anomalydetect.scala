import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object DetectAnomalies {
   def main(args: Array[String]) {

    val sc = new SparkContext()
      val input = sc.textFile("README.md")             
      val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)       

      System.out.println("OK");
      System.out.println(count.foreach(println))
   }
}

