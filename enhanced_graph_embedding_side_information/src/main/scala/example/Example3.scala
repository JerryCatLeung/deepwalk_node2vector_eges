package example

import eges.random.walk.RandomWalk
import sparkapplication.BaseSparkLocal

object Example3 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
    val spark = this.basicSpark
    import spark.implicits._

    val data = List(("y", "a", 0.8), ("y", "b", 0.6),  ("y", "c", 0.4), ("q", "b", 0.8), ("q", "c", 0.6), ("q", "d", 0.4),
      ("s", "a", 0.8), ("s", "c", 0.6), ("s", "d", 0.4), ("a", "e", 0.5), ("b", "f", 0.5), ("c", "e", 0.5), ("c", "f", 0.5))
    val dataRDD = spark.sparkContext.parallelize(data)
    val randomWalk = new RandomWalk()
      .setNodeIterationNum(5)
      .setNodeWalkLength(6)
      .setReturnParameter(1.0)
      .setInOutParameter(1.0)
      .setNodeMaxDegree(200)
      .setNumPartitions(2)

    val paths = randomWalk.fit(dataRDD)
    paths.cache()
    println("总路径数: ", paths.count())
    println(paths.map(k => k._2).collect().mkString("#"))
    paths.unpersist()

  }
}
