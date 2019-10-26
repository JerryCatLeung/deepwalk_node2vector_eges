package eges.random.walk

import java.io.Serializable

object Attributes {

  /**
    * 节点属性
    * @param neighbors  节点对应的邻居节点和权重
    * @param path       以该节点开头的路径的前两个节点
    *
    * */
  case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                      var path: Array[Array[Long]] = Array.empty[Array[Long]]) extends Serializable

  /**
    * 边属性
    * @param dstNeighbors  目的节点对应的邻居节点
    * @param J             Alias抽样方法返回值
    * @param q             Alias抽样方法返回值
    *
    * */
  case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
                      var J: Array[Int] = Array.empty[Int],
                      var q: Array[Double] = Array.empty[Double]) extends Serializable

}
