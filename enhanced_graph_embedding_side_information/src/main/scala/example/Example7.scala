package example

import breeze.linalg.{DenseVector, norm}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sparkapplication.BaseSparkLocal

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Example7 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
     val spark = this.basicSpark
     import spark.implicits._

    val list = List(("y", "2019"), ("q", "2018"))
    val data = spark.createDataFrame(list).toDF("member_id", "visit_time")
      .withColumn("index", row_number().over(Window.partitionBy("member_id").orderBy(desc("visit_time"))))
      .select("index")
      .rdd.map(k => k.getAs[Int]("index"))


    data.foreach(println)






















  }
}
