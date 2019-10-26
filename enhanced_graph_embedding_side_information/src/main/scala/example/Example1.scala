package example

import scala.collection.mutable
import eges.random.walk.RandomWalk
import sparkapplication.BaseSparkLocal

object Example1 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
    val spark = this.basicSpark
    import spark.implicits._

    val list1 = List((("gds1", "group1"), ("gds2", 0.1)), (("gds1", "group1"), ("gds3", 0.2)), (("gds1", "group1"), ("gds4", 0.3)),
      (("gds2", "group2"), ("gds3", 0.1)), (("gds2", "group2"), ("gds4", 0.3)), (("gds2", "group2"), ("gds5", 0.6)),
      (("gds3", "group4"), ("gds2", 0.1)), (("gds3", "group4"), ("gds5", 0.3)), (("gds3", "group4"), ("gds6", 0.5)))
    val list2 = spark.sparkContext.parallelize(list1)
    val list3 = list2.combineByKey(gds2WithSimilarity=>{
      implicit object ord extends Ordering[(String,Double)] {
        override def compare(p1: (String,Double), p2: (String,Double)): Int = {
          p2._2.compareTo(p1._2)
        }
      }

      val priorityQueue = new mutable.PriorityQueue[(String,Double)]()
      priorityQueue.enqueue(gds2WithSimilarity)
      priorityQueue
    },(priorityQueue:mutable.PriorityQueue[(String,Double)],gds2WithSimilarity)=>{

      if(priorityQueue.size < 2){
        priorityQueue.enqueue(gds2WithSimilarity)
      }else{
        if(priorityQueue.head._2 < gds2WithSimilarity._2){
          priorityQueue.dequeue()
          priorityQueue.enqueue(gds2WithSimilarity)
        }
      }

      priorityQueue
    },(priorityQueuePre:mutable.PriorityQueue[(String,Double)],
       priorityQueueLast:mutable.PriorityQueue[(String,Double)])=>{
      while(!priorityQueueLast.isEmpty){

        val gds2WithSimilarity = priorityQueueLast.dequeue()
        if(priorityQueuePre.size < 2){
          priorityQueuePre.enqueue(gds2WithSimilarity)
        }else{
          if(priorityQueuePre.head._2 < gds2WithSimilarity._2){
            priorityQueuePre.dequeue()
            priorityQueuePre.enqueue(gds2WithSimilarity)
          }
        }
      }

      priorityQueuePre}).flatMap({case ((gdsCd1, l4GroupCd), gdsCd2AndSimilarityPriorityQueue) => {

      gdsCd2AndSimilarityPriorityQueue.toList.map({case(gdsCd2, similarity) => (gdsCd1, gdsCd2, l4GroupCd, similarity)})

    }})

    list3.foreach(println)













  }

}
