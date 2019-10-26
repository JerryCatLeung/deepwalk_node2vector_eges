package example

import java.util.Date
import java.util.Random
import breeze.linalg.DenseVector
import utils.miscellaneous.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.util.random.XORShiftRandom
import sparkapplication.BaseSparkLocal
import breeze.linalg.DenseVector
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Example6 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
    val spark = this.basicSpark
    import spark.implicits._


    val a = "-0.30614856@0.16460776@0.19909532@-0.48152933@-0.7925998"
    val aVector = new DenseVector[Double](a.split("@").map(_.toDouble))
    val b = "-0.7356615@0.6408239@-0.4380877@-1.920213@-2.628376"
    val bVector = new DenseVector[Double](b.split("@").map(_.toDouble))
    val similar = aVector.t * bVector
    val eHx = math.exp(math.max(math.min(-similar, 35.0), -35.0)).toFloat
    val probability = 1.toFloat / (1.toFloat + eHx)
    println(probability)












































  }
}
