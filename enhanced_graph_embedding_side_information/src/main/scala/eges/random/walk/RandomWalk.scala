package eges.random.walk

import scala.util.Try
import java.util.Random
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import eges.random.sample.Alias
import org.apache.spark.graphx._
import eges.random.walk.Attributes._
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast

class RandomWalk extends Serializable {

  var nodeIterationNum = 10
  var nodeWalkLength = 78
  var returnParameter = 1.0
  var inOutParameter = 1.0
  var nodeMaxDegree = 200
  var numPartitions = 500
  var nodeIndex: Broadcast[mutable.HashMap[String, Long]] = _
  var indexNode: Broadcast[mutable.HashMap[Long, String]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _
  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  /**
    * 每个节点产生路径的数量
    *
    * */
  def setNodeIterationNum(value:Int):this.type = {
    require(value > 0, s"nodeIterationNum must be more than 0, but it is $value")
    nodeIterationNum = value
    this
  }

  /**
    * 每个路径的最大长度
    *
    * */
  def setNodeWalkLength(value:Int):this.type = {
    require(value >= 2, s"nodeWalkLength must be not less than 2, but it is $value")
    nodeWalkLength = value - 2
    this
  }

  /**
    * 往回走的参数
    *
    * */
  def setReturnParameter(value:Double):this.type = {
    require(value > 0, s"returnParameter must be more than 0, but it is $value")
    returnParameter = value
    this
  }

  /**
    * 往外走的参数
    *
    * */
  def setInOutParameter(value:Double):this.type = {
    require(value > 0, s"inOutParameter must be more than 0, but it is $value")
    inOutParameter = value
    this
  }

  /**
    * 每个节点最多的邻居数量
    *
    * */
  def setNodeMaxDegree(value:Int):this.type = {
    require(value > 0, s"nodeMaxDegree must be more than 0, but it is $value")
    nodeMaxDegree = value
    this
  }

  /**
    * 分区数量, 并行度
    *
    * */
  def setNumPartitions(value:Int):this.type = {
    require(value > 0, s"numPartitions must be more than 0, but it is $value")
    numPartitions = value
    this
  }

  /**
    * 每个节点游走的路径
    *
    * */
  def fit(node2Weight: RDD[(String, String, Double)]): RDD[(String, String)] = {

    // 广播部分变量
    val sc = node2Weight.context
    val nodeIterationNumBroadcast = sc.broadcast(nodeIterationNum)
    val returnParameterBroadcast = sc.broadcast(returnParameter)
    val inOutParameterBroadcast = sc.broadcast(inOutParameter)
    val nodeMaxDegreeBroadcast = sc.broadcast(nodeMaxDegree)

    // 将字符串节点与节点的权重转化为长整型与长整型的权重
    val inputTriplets = processData(node2Weight, sc)
    inputTriplets.cache()
    inputTriplets.first()  // 行动操作

    // 节点对应的节点属性
    indexedNodes = inputTriplets.map{ case (node1, node2, weight) => (node1, (node2, weight)) }
      .combineByKey(dstIdWeight =>{
        implicit object ord extends Ordering[(Long, Double)]{
          override def compare(p1:(Long, Double), p2:(Long, Double)):Int = {
            p2._2.compareTo(p1._2)
          }
        }
        val priorityQueue = new mutable.PriorityQueue[(Long, Double)]()
        priorityQueue.enqueue(dstIdWeight)
        priorityQueue
      },(priorityQueue:mutable.PriorityQueue[(Long,Double)], dstIdWeight)=>{
        if(priorityQueue.size < nodeMaxDegreeBroadcast.value){
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
          if(priorityQueuePre.size < nodeMaxDegreeBroadcast.value){
            priorityQueuePre.enqueue(dstIdWeight)
          }else{
            if(priorityQueuePre.head._2 < dstIdWeight._2){
              priorityQueuePre.dequeue()
              priorityQueuePre.enqueue(dstIdWeight)
            }
          }
        }
        priorityQueuePre
      }).map{ case (srcId, dstIdWeightPriorityQueue) =>
      (srcId, NodeAttr(neighbors = dstIdWeightPriorityQueue.toArray))
    }.map(k => ((new Random).nextInt(numPartitions), k)).repartition(numPartitions).map(k => k._2)
    indexedNodes.cache()
    indexedNodes.first()  // 行动操作
    inputTriplets.unpersist()

    //边的属性
    indexedEdges = indexedNodes.flatMap { case (srcId, srcIdAttr) =>
      srcIdAttr.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.map(k => ((new Random).nextInt(numPartitions), k)).repartition(numPartitions).map(k => k._2)
    indexedEdges.cache()
    indexedEdges.first()  // 行动操作

    // 通过节点属性和边的属性构建图
    graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr]{ case (vertexId, vertexAttr) =>
      try{
        val (j, q) = Alias.setupAlias(vertexAttr.neighbors)
        val pathArray = ArrayBuffer[Array[Long]]()
        for(_ <- 0 until nodeIterationNumBroadcast.value){
          val nextNodeIndex = Alias.drawAlias(j, q)
          val path = Array(vertexId, vertexAttr.neighbors(nextNodeIndex)._1)
          pathArray.append(path)
        }
        vertexAttr.path = pathArray.toArray
        vertexAttr
      } catch {
        case _:Exception => Attributes.NodeAttr()
      }
    }.mapTriplets{ edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      val dstAttrNeighbors = Try(edgeTriplet.dstAttr.neighbors).getOrElse(Array.empty[(Long, Double)])
      val (j, q) = Alias.setupEdgeAlias(returnParameterBroadcast.value, inOutParameterBroadcast.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, dstAttrNeighbors)
      edgeTriplet.attr.J = j
      edgeTriplet.attr.q = q
      edgeTriplet.attr.dstNeighbors = dstAttrNeighbors.map(_._1)
      edgeTriplet.attr
    }
    graph.cache()

    // 所有边的源节点、目的节点和边的属性
    val edgeAttr = graph.triplets.map{ edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.map(k => ((new Random).nextInt(numPartitions), k)).repartition(numPartitions).map(k => k._2)
    edgeAttr.cache()
    edgeAttr.first()    // 行动操作
    indexedNodes.unpersist()
    indexedEdges.unpersist()

    // 随机游走产生序列数据
    for (iter <- 0 until nodeIterationNum) {
      var randomWalk = graph.vertices.filter{ case (vertexId, vertexAttr) =>
        val vertexNeighborsLength = Try(vertexAttr.neighbors.length).getOrElse(0)
        vertexNeighborsLength > 0
      }.map { case (vertexId, vertexAttr) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(vertexAttr.path(iter):_*)
        (vertexId, pathBuffer)
      }
      randomWalk.cache()
      randomWalk.first()   // 行动操作

      for (_ <- 0 until nodeWalkLength) {
        randomWalk = randomWalk.map { case (srcNodeId, pathBuffer) =>
          val prevNodeId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId = pathBuffer.last

          (s"$prevNodeId$currentNodeId", (srcNodeId, pathBuffer))
        }.join(edgeAttr).map { case (edge, ((srcNodeId, pathBuffer), attr)) =>
          try {
            val nextNodeIndex = Alias.drawAlias(attr.J, attr.q)
            val nextNodeId = attr.dstNeighbors(nextNodeIndex)
            pathBuffer.append(nextNodeId)

            (srcNodeId, pathBuffer)
          } catch {
            case _: Exception => (srcNodeId, pathBuffer)
          }
        }
        randomWalk.cache()
        randomWalk.first()  // 行动操作
      }

      if (randomWalkPaths != null) {
        randomWalkPaths = randomWalkPaths.union(randomWalk)
      } else {
        randomWalkPaths = randomWalk
      }
      randomWalkPaths.cache()
      randomWalkPaths.first()  // 行动操作
      randomWalk.unpersist()
    }
    graph.unpersist()
    edgeAttr.unpersist()

    // 将长整型节点与路径转化为字符串节点与路径, 路径用","分开
    val paths = longToString(randomWalkPaths)
    paths.cache()
    paths.first()  // 行动操作
    randomWalkPaths.unpersist()
    paths

  }

  /**
    * 将字符串节点与节点的权重转化为长整型与长整型的权重
    *
    * */
  def processData(node2Weight: RDD[(String, String, Double)], sc:SparkContext): RDD[(Long, Long, Double)] = {
    val nodeIndexArray = node2Weight.flatMap(node => Array(node._1, node._2))
      .distinct().zipWithIndex().collect()
    val indexNodeArray = nodeIndexArray.map{case (node, index) => (index, node)}
    indexNode = sc.broadcast(mutable.HashMap(indexNodeArray:_*))
    nodeIndex = sc.broadcast(mutable.HashMap(nodeIndexArray:_*))
    val inputTriplets = node2Weight.map{ case (node1, node2, weight) =>
      val node1Index = nodeIndex.value(node1)
      val node2Index = nodeIndex.value(node2)
      (node1Index, node2Index, weight)
    }
    inputTriplets
  }

  /**
    * 将长整型节点与路径转化为字符串节点与路径, 路径用","分开
    *
    * */
  def longToString(srcNodeIdPaths: RDD[(Long, ArrayBuffer[Long])]): RDD[(String, String)] = {
    val pathsResult = srcNodeIdPaths.map{case (srcNodeId, paths) =>
      val srcNodeIdString = indexNode.value(srcNodeId)
      val pathsString = paths.map(index => indexNode.value(index)).mkString(",")
      (srcNodeIdString, pathsString)
    }
    pathsResult
  }

}