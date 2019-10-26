package example

import java.util.Random
import eges.embedding.WeightedSkipGram
import org.apache.spark.sql.functions._
import sparkapplication.BaseSparkLocal
import breeze.linalg.{norm, DenseVector}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Example4 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
    val spark = this.basicSpark
    import spark.implicits._

    val dataList = List("a@s1,b@s2,c@s3,d@s4,e@s5,f@s6,g@s7", "e@s5,f@s6,e@s5,a@s1,c@s3,h@s2,i@s6,j@s7,a@s2")
    val dataRDD = spark.sparkContext.parallelize(dataList)
      .map(k => k.split(",").map(v => v.split("@")))
    val weightedSkipGram = new WeightedSkipGram()
      .setVectorSize(4)
      .setWindowSize(1)
      .setNegativeSampleNum(1)
      .setPerSampleMaxNum(100)
      // .setSubSample(0.1)
      .setSubSample(0.0)
      .setLearningRate(0.25)
      .setIterationNum(6)
      .setIsNoShowLoss(true)
      .setNumPartitions(2)
      .setSampleTableNum(200)
    val nodeEmbedding = weightedSkipGram.fit(dataRDD).map(k => (k._1, new DenseVector[Double](k._2.split("@").map(_.toDouble)))).collectAsMap()
    println("nodeEmbedding结果如下:")
    nodeEmbedding.map(k => (k._1, k._2.toArray.mkString("@"))).foreach(println)

    val node = Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    // val nodeWeight = Array("a#Weight", "b#Weight", "c#Weight", "d#Weight", "e#Weight", "f#Weight", "g#Weight", "h#Weight", "i#Weight", "j#Weight")
    val nodeAgg = Array("a#s1", "b#s2", "c#s3", "d#s4", "e#s5", "f#s6", "g#s7", "h#s2", "i#s6", "j#s7", "a#s2")
    val i2iSimilar = ArrayBuffer[(String, String, Double)]()
    for(mainNodeInfo <- nodeAgg){
      val mainNodeInfoVector = nodeEmbedding(mainNodeInfo)
      for(slaveNodeInfo <- nodeAgg){
        val slaveNodeInfoVector = nodeEmbedding(slaveNodeInfo)
        if(!mainNodeInfo.equals(slaveNodeInfo)) {
          val cosineSimilar = mainNodeInfoVector.t * slaveNodeInfoVector / ( norm(mainNodeInfoVector, 2) * norm(slaveNodeInfoVector, 2) )
          i2iSimilar.append((mainNodeInfo, slaveNodeInfo, cosineSimilar))
        }
      }
    }
    println("i2iSimilarHv结果如下:")
    i2iSimilar.foreach(println)

    val i2iSimilarSequence = ArrayBuffer[(String, String, Double)]()
    for(mainNodeInfo <- nodeAgg){
      val mainNodeInfoVector = nodeEmbedding(mainNodeInfo)
      for(slaveNode <- node){
        val slaveNodeInfoVector = nodeEmbedding(slaveNode+"#Zu")
        if(!mainNodeInfo.split("#").head.equals(slaveNode)) {
          val similar = mainNodeInfoVector.t * slaveNodeInfoVector
          val eHx = math.exp(math.max(math.min(-similar, 20.0), -20.0))
          val probability = 1.0 / (1.0 + eHx)
          i2iSimilarSequence.append((mainNodeInfo, slaveNode, probability))
        }
      }
    }
    println("i2iSimilarSequence结果如下:")
    i2iSimilarSequence.foreach(println)

  }
}