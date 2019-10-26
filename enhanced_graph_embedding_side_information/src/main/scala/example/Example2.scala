package example

import breeze.linalg.operators
import breeze.linalg.{DenseVector, SparseVector, Vector}
import breeze.numerics._
import java.util.{Date, Random}
import sparkapplication.BaseSparkLocal
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Example2 extends BaseSparkLocal {
  def main(args:Array[String]):Unit = {
      val spark = this.basicSpark
      import spark.implicits._

    val nodeMaxDegree = 2
    val list1 = List((1:Long, (2:Long, 3.2:Double)), (1:Long, (3:Long, 2.2:Double)), (1:Long, (4:Long, 1.2:Double)),
      (1:Long, (5:Long, 0.2:Double)), (6:Long, (7:Long, 6.2:Double)), (6:Long, (8:Long, 8.2:Double)))
    val list2 = spark.sparkContext.parallelize(list1)
    val list3 = list2.combineByKey(dstIdWeight =>{
      implicit object ord extends Ordering[(Long, Double)]{
        override def compare(p1:(Long, Double), p2:(Long, Double)):Int = {
          p2._2.compareTo(p1._2)
        }
      }
      val priorityQueue = new mutable.PriorityQueue[(Long, Double)]()
      priorityQueue.enqueue(dstIdWeight)
      priorityQueue
    },(priorityQueue:mutable.PriorityQueue[(Long,Double)], dstIdWeight)=>{
      if(priorityQueue.size < nodeMaxDegree){
        priorityQueue.enqueue(dstIdWeight)
      }else{
        if(priorityQueue.head._2 < dstIdWeight._2){
          priorityQueue.dequeue()
          priorityQueue.enqueue(dstIdWeight)
        }
      }
      priorityQueue
    },(priorityQueuePre:mutable.PriorityQueue[(Long,Double)],
       priorityQueueLast:mutable.PriorityQueue[(Long,Double)])=>{
      while(priorityQueueLast.nonEmpty){
        val dstIdWeight = priorityQueueLast.dequeue()
        if(priorityQueuePre.size < nodeMaxDegree){
          priorityQueuePre.enqueue(dstIdWeight)
        }else{
          if(priorityQueuePre.head._2 < dstIdWeight._2){
            priorityQueuePre.dequeue()
            priorityQueuePre.enqueue(dstIdWeight)
          }
        }
      }
      priorityQueuePre
    }).map{case (srcId, dstIdWeightPriorityQueue) => (srcId, dstIdWeightPriorityQueue.toArray.sortBy(-_._2).mkString("@"))}





    list3.foreach(println)


























































  }
}