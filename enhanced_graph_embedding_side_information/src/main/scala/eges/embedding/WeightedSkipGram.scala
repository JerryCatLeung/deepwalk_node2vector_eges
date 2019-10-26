package eges.embedding

import java.util.Random
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import breeze.linalg.DenseVector
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel

class WeightedSkipGram extends Serializable {

  var vectorSize = 4
  var windowSize = 2
  var negativeSampleNum = 1
  var perSampleMaxNum = 10
  var subSample: Double = 0.0
  var learningRate: Double = 0.02
  var iterationNum = 5
  var isNoShowLoss = false
  var numPartitions = 200
  var nodeSideCount: Int = 0
  var nodeSideTable: Array[(String, Int)] = _
  var nodeSideHash = mutable.HashMap.empty[String, Int]
  var sampleTableNum: Int = 1e8.toInt
  lazy val sampleTable: Array[Int] = new Array[Int](sampleTableNum)

  /**
    * 每个节点embedding长度
    *
    * */
  def setVectorSize(value:Int):this.type = {
    require(value > 0, s"vectorSize must be more than 0, but it is $value")
    vectorSize = value
    this
  }

  /**
    * 窗口大小
    *
    * */
  def setWindowSize(value:Int):this.type = {
    require(value > 0, s"windowSize must be more than 0, but it is $value")
    windowSize = value
    this
  }

  /**
    * 负采样的样本个数
    *
    * */
  def setNegativeSampleNum(value:Int):this.type = {
    require(value > 0, s"negativeSampleNum must be more than 0, but it is $value")
    negativeSampleNum = value
    this
  }

  /**
    * 负采样时, 每个负样本最多采样的次数
    *
    * */
  def setPerSampleMaxNum(value:Int):this.type = {
    require(value > 0, s"perSampleMaxNum must be more than 0, but it is $value")
    perSampleMaxNum = value
    this
  }

  /**
    * 高频节点的下采样率
    *
    * */
  def setSubSample(value:Double):this.type = {
    require(value >= 0.0 && value <= 1.0, s"subSample must be not less than 0.0 and not more than 1.0, but it is $value")
    subSample = value
    this
  }

  /**
    * 初始化学习率
    *
    * */
  def setLearningRate(value:Double):this.type = {
    require(value > 0.0, s"learningRate must be more than 0.0, but it is $value")
    learningRate = value
    this
  }

  /**
    * 迭代次数
    *
    * */
  def setIterationNum(value:Int):this.type = {
    require(value > 0, s"iterationNum must be more than 0, but it is $value")
    iterationNum = value
    this
  }

  /**
    * 是否显示损失, 不建议显示损失值, 这样增加计算量
    *
    * */
  def setIsNoShowLoss(value:Boolean):this.type = {
    isNoShowLoss = value
    this
  }

  /**
    * 分区数量
    *
    * */
  def setNumPartitions(value:Int):this.type = {
    require(value > 0, s"numPartitions must be more than 0, but it is $value")
    numPartitions = value
    this
  }

  /**
    * 采样数组大小
    *
    * */
  def setSampleTableNum(value:Int):this.type = {
    require(value > 0, s"sampleTableNum must be more than 0, but it is $value")
    sampleTableNum = value
    this
  }

  /**
    * 训练
    *
    * */
  def fit(dataSet: RDD[Array[Array[String]]]): RDD[(String, String)] = {
    // 重新分区
    val dataSetRepartition = dataSet.map(k => ((new Random).nextInt(numPartitions), k))
      .repartition(numPartitions).map(k => k._2)
    dataSetRepartition.persist(StorageLevel.MEMORY_AND_DISK)
    val sc = dataSetRepartition.context
    // 初始化 WeightedSkipGram
    initWeightedSkipGram(dataSetRepartition)
    // 边和embedding的长度
    val sideInfoNum = dataSetRepartition.first().head.length
    val sideInfoNumBroadcast = sc.broadcast(sideInfoNum)
    val vectorSizeBroadcast = sc.broadcast(vectorSize)
    // 节点权重初始化
    val nodeWeight = dataSetRepartition.flatMap(x => x)
      .map(nodeInfo => nodeInfo.head + "#Weight").distinct()
      .map(nodeWeight => (nodeWeight, Array.fill[Double](sideInfoNumBroadcast.value)((new Random).nextGaussian())))
    // 节点Zu初始化
    val nodeZu = dataSetRepartition.flatMap(x => x)
      .map(nodeInfo => nodeInfo.head + "#Zu").distinct()
      .map(nodeZu => (nodeZu, Array.fill[Double](vectorSizeBroadcast.value)( ((new Random).nextDouble()-0.5)/vectorSizeBroadcast.value ) ))
    // 节点和边信息初始化
    val nodeAndSideVector = dataSetRepartition.flatMap(x => x).flatMap(x => x).distinct()
      .map(nodeAndSide => (nodeAndSide, Array.fill[Double](vectorSizeBroadcast.value)( ((new Random).nextDouble()-0.5)/vectorSizeBroadcast.value ) ))
    // 节点权重、节点和边信息初始化
    val embeddingHashInit = mutable.HashMap(nodeWeight.union(nodeZu).union(nodeAndSideVector).collectAsMap().toList:_*)
    // println("embedding初始化:")
    // embeddingHashInit.map(k => (k._1, k._2.mkString("@"))).foreach(println)

    // 广播部分变量
    val nodeSideTableBroadcast = sc.broadcast(nodeSideTable)
    val nodeSideHashBroadcast = sc.broadcast(nodeSideHash)
    val sampleTableBroadcast = sc.broadcast(sampleTable)
    val subSampleBroadcast = sc.broadcast(subSample)
    val windowSizeBroadcast = sc.broadcast(windowSize)
    val negativeSampleNumBroadcast = sc.broadcast(negativeSampleNum)
    val perSampleMaxNumBroadcast = sc.broadcast(perSampleMaxNum)
    val sampleTableNumBroadcast = sc.broadcast(sampleTableNum)
    val nodeSideCountBroadcast = sc.broadcast(nodeSideCount)
    val isNoShowLossBroadcast = sc.broadcast(isNoShowLoss)
    // val lambdaBroadcast = sc.broadcast(learningRate)

    // 训练
    var embeddingHashBroadcast = sc.broadcast(embeddingHashInit)
    for(iter <- 0 until iterationNum){
      val lambdaBroadcast = sc.broadcast(if(learningRate/(1+iter) >= learningRate/5) learningRate/(1+iter) else learningRate/5)
      // val lambdaBroadcast = sc.broadcast(if(learningRate/(1+iter) >= 0.000001) learningRate/(1+iter) else 0.000001)
      // val lambdaBroadcast = sc.broadcast(Array(0.5, 0.4, 0.2, 0.1, 0.05, 0.05)(iter))
      // 训练结果
      val trainResult = dataSetRepartition.mapPartitions(dataIterator =>
        trainWeightedSkipGram(dataIterator, embeddingHashBroadcast.value, nodeSideTableBroadcast.value,
          nodeSideHashBroadcast.value, sampleTableBroadcast.value, subSampleBroadcast.value, vectorSizeBroadcast.value,
          sideInfoNumBroadcast.value, windowSizeBroadcast.value, negativeSampleNumBroadcast.value, perSampleMaxNumBroadcast.value,
          sampleTableNumBroadcast.value, nodeSideCountBroadcast.value, isNoShowLossBroadcast.value))
      trainResult.persist(StorageLevel.MEMORY_AND_DISK)
      // 节点权重的梯度聚类
      val trainWeightResult = trainResult.filter(k => k._1.endsWith("#Weight") && !k._1.equals("LossValue"))
        .aggregateByKey((new DenseVector[Double](Array.fill[Double](sideInfoNumBroadcast.value)(0.0)), 0L))(
          (vector, array) => (vector._1 + new DenseVector[Double](array._1), vector._2 + array._2),
          (vector1, vector2) => (vector1._1 + vector2._1, vector1._2 + vector2._2) )
      trainWeightResult.persist(StorageLevel.MEMORY_AND_DISK)
      // 节点和边embedding的梯度聚类
      val trainVectorResult = trainResult.filter(k => !k._1.endsWith("#Weight") && !k._1.equals("LossValue"))
        .aggregateByKey((new DenseVector[Double](Array.fill[Double](vectorSizeBroadcast.value)(0.0)), 0L))(
          (vector, array) => (vector._1 + new DenseVector[Double](array._1), vector._2 + array._2),
          (vector1, vector2) => (vector1._1 + vector2._1, vector1._2 + vector2._2) )
      trainVectorResult.persist(StorageLevel.MEMORY_AND_DISK)
      // 计算平均损失
      if(isNoShowLoss){
        val trainLossResult = trainResult.filter(k => k._1.equals("LossValue"))
          .aggregateByKey((new DenseVector[Double](Array.fill[Double](1)(0.0)), 0L))(
            (vector, array) => (vector._1 + new DenseVector[Double](array._1), vector._2 + array._2),
            (vector1, vector2) => (vector1._1 + vector2._1, vector1._2 + vector2._2)
          ).map(k => (k._1, k._2._1/k._2._2.toDouble))
          .map(k => (k._1, k._2.toArray.head)).first()._2
        println(s"====第${iter+1}轮====平均损失:$trainLossResult====")
      }
      val trainNodeSideResultUnion = trainWeightResult.union(trainVectorResult)
        .map{ case (key, (gradientVectorSum, num)) =>
          val embeddingRaw = new DenseVector[Double](embeddingHashBroadcast.value(key))
          val embeddingUpdate = embeddingRaw - 100 * lambdaBroadcast.value * gradientVectorSum / num.toDouble
          (key, embeddingUpdate.toArray)
        }
      var trainSideVectorResultMap = mutable.HashMap(trainNodeSideResultUnion.collectAsMap().toList:_*)
      trainSideVectorResultMap = embeddingHashBroadcast.value.++(trainSideVectorResultMap)
      embeddingHashBroadcast = sc.broadcast(trainSideVectorResultMap)
      trainResult.unpersist()
      trainWeightResult.unpersist()
      trainVectorResult.unpersist()
    }
    dataSetRepartition.unpersist()

    // 每个节点加权向量
    var embeddingHashResult = embeddingHashBroadcast.value
    val nodeHvArray = nodeSideTable.map(k => k._1.split("@"))
      .map(nodeInfo => {
        val node = nodeInfo.head
        var eavSum = 0.0
        var eavWvAggSum = new DenseVector[Double](Array.fill[Double](vectorSize)(0.0))
        val keyEA = node + "#Weight"
        val valueEA = embeddingHashResult.getOrElse(keyEA, Array.fill[Double](sideInfoNum)(0.0))
        for(n <- 0 until sideInfoNum) {
          val keyNodeOrSide = nodeInfo(n)
          val valueNodeOrSide = embeddingHashResult.getOrElse(keyNodeOrSide, Array.fill[Double](vectorSize)(0.0))
          eavWvAggSum = eavWvAggSum + math.exp(valueEA(n)) * new DenseVector[Double](valueNodeOrSide)
          eavSum = eavSum + math.exp(valueEA(n))
        }
        val Hv = (eavWvAggSum/eavSum).toArray
        (nodeInfo.mkString("#"), Hv)
      })
    val nodeHvMap = mutable.Map(nodeHvArray:_*)
    embeddingHashResult = embeddingHashResult.++(nodeHvMap)
    val embeddingResult = sc.parallelize(embeddingHashResult.map(k => (k._1, k._2.mkString("@"))).toList, numPartitions)
    embeddingResult
  }

  /**
    * 初始化
    *
    * */
  def initWeightedSkipGram(dataSet: RDD[Array[Array[String]]]): Unit = {
    // 节点和边信息出现的频次数组
    nodeSideTable = dataSet.flatMap(x => x)
      .map(nodeInfo => (nodeInfo.mkString("@"), 1))
      .reduceByKey(_ + _)
      .collect().sortBy(-_._2)
    // 节点和边信息总数量
    nodeSideCount = nodeSideTable.map(_._2).par.sum
    //节点和边信息出现的频次哈希map
    nodeSideHash = mutable.HashMap(nodeSideTable:_*)
    // 节点和边信息尺寸
    val nodeSideSize = nodeSideTable.length
    // Negative Sampling 负采样初始化
    var a = 0
    val power = 0.75
    var nodesPow = 0.0
    while (a < nodeSideSize) {
      nodesPow += Math.pow(nodeSideTable(a)._2, power)
      a = a + 1
    }
    var b = 0
    var freq = Math.pow(nodeSideTable(b)._2, power) / nodesPow
    var c = 0
    while (c < sampleTableNum) {
      sampleTable(c) = b
      if ((c.toDouble + 1.0) / sampleTableNum >= freq) {
        b = b + 1
        if (b >= nodeSideSize) {
          b = nodeSideSize - 1
        }
        freq += Math.pow(nodeSideTable(b)._2, power) / nodesPow
      }
      c = c + 1
    }
  }

  /**
    * 每个分区训练
    *
    * */
  def trainWeightedSkipGram(dataSet: Iterator[Array[Array[String]]],
                            embeddingHash: mutable.HashMap[String, Array[Double]],
                            nodeSideTable: Array[(String, Int)],
                            nodeSideHash: mutable.HashMap[String, Int],
                            sampleTable: Array[Int],
                            subSample: Double,
                            vectorSize: Int,
                            sideInfoNum: Int,
                            windowSize: Int,
                            negativeSampleNum: Int,
                            perSampleMaxNum: Int,
                            sampleTableNum: Int,
                            nodeSideCount: Int,
                            isNoShowLoss: Boolean): Iterator[(String, (Array[Double], Long))] = {
    val gradientUpdateHash = new mutable.HashMap[String, (Array[Double], Long)]()
    var lossSum = 0.0
    var lossNum = 0L
    for(data <- dataSet) {
      // 高频节点的下采样
      val dataSubSample = ArrayBuffer[Array[String]]()
      for( nodeOrSideInfoArray <- data){
        if(subSample > 0.0) {
          val nodeOrSideInfoFrequency = nodeSideHash(nodeOrSideInfoArray.mkString("@"))
          val keepProbability = (Math.sqrt(nodeOrSideInfoFrequency/ (subSample * nodeSideCount)) + 1.0) * (subSample * nodeSideCount) / nodeOrSideInfoFrequency
          if (keepProbability >= (new Random).nextDouble()) {
            dataSubSample.append(nodeOrSideInfoArray)
          }
        } else {
          dataSubSample.append(nodeOrSideInfoArray)
        }
      }
      val dssl = dataSubSample.length
      for(i <- 0 until dssl){
        val mainNodeInfo = dataSubSample(i)
        val mainNode = mainNodeInfo.head
        var eavSum = 0.0
        var eavWvAggSum = new DenseVector[Double](Array.fill[Double](vectorSize)(0.0))
        val keyEA = mainNode + "#Weight"
        val valueEA = embeddingHash(keyEA)
        for(k <- 0 until sideInfoNum) {
          val keyNodeOrSide = mainNodeInfo(k)
          val valueNodeOrSide = embeddingHash(keyNodeOrSide)
          eavWvAggSum = eavWvAggSum + math.exp(valueEA(k)) * new DenseVector[Double](valueNodeOrSide)
          eavSum = eavSum + math.exp(valueEA(k))
        }
        val Hv = eavWvAggSum/eavSum
        // 主节点对应窗口内的正样本集合
        val mainSlaveNodeSet = dataSubSample.slice(math.max(0, i-windowSize), math.min(i+windowSize, dssl-1) + 1)
            .map(array => array.head).toSet
        for(j <- math.max(0, i-windowSize) to math.min(i+windowSize, dssl-1)){
          if(j != i) {
            // 正样本训练
            val slaveNodeInfo = dataSubSample(j)
            val slaveNode = slaveNodeInfo.head
            embeddingUpdate(mainNodeInfo, slaveNodeInfo, embeddingHash, gradientUpdateHash, valueEA, eavWvAggSum, eavSum, Hv, 1.0, vectorSize, sideInfoNum)
            //  正样本计算损失值
            if(isNoShowLoss){
              val keySlaveNode = slaveNode + "#Zu"
              val valueSlaveNode = embeddingHash(keySlaveNode)
              val HvZu = Hv.t * new DenseVector[Double](valueSlaveNode)
              val logarithmRaw = math.log(1.0 / (1.0 + math.exp(math.max(math.min(-HvZu, 20.0), -20.0))))
              val logarithm = math.max(math.min(logarithmRaw, 0.0), -20.0)
              lossSum = lossSum - logarithm
              lossNum = lossNum + 1L
            }
            // 负样本采样和训练
            for(_ <- 0 until negativeSampleNum){
              // 负样本采样
              var sampleNodeInfo = slaveNodeInfo
              var sampleNode = slaveNode
              var sampleNum = 0
              while(mainSlaveNodeSet.contains(sampleNode) && sampleNum < perSampleMaxNum) {
                val index = sampleTable((new Random).nextInt(sampleTableNum))
                sampleNodeInfo = nodeSideTable(index)._1.split("@")
                sampleNode = sampleNodeInfo.head
                sampleNum = sampleNum + 1
              }
              // 负样本训练
              embeddingUpdate(mainNodeInfo, sampleNodeInfo, embeddingHash, gradientUpdateHash, valueEA, eavWvAggSum, eavSum, Hv, 0.0, vectorSize, sideInfoNum)
              // 负样本计算损失值
              if(isNoShowLoss){
                val keySampleNode = sampleNodeInfo.head + "#Zu"
                val valueSampleNode = embeddingHash(keySampleNode)
                val HvZu = Hv.t * new DenseVector[Double](valueSampleNode)
                val logarithmRaw = math.log(1.0 - 1.0 / (1.0 + math.exp(math.max(math.min(-HvZu, 20.0), -20.0))))
                val logarithm = math.max(math.min(logarithmRaw, 0.0), -20.0)
                lossSum = lossSum - logarithm
                lossNum = lossNum + 1L
              }
            }
          }
        }
      }
    }
    if(isNoShowLoss){
      gradientUpdateHash.put("LossValue", (Array(lossSum), lossNum))
    }
    gradientUpdateHash.toIterator
  }

  /**
    * 梯度更新
    *
    * */
  def embeddingUpdate(mainNodeInfo: Array[String],
                      slaveNodeInfo: Array[String],
                      embeddingHash: mutable.HashMap[String, Array[Double]],
                      gradientUpdateHash: mutable.HashMap[String, (Array[Double], Long)],
                      mainNodeWeightRaw: Array[Double],
                      eavWvAggSum: DenseVector[Double],
                      eavSum: Double,
                      Hv: DenseVector[Double],
                      label: Double,
                      vectorSize: Int,
                      sideInfoNum: Int): Unit = {
    val keySlaveNode = slaveNodeInfo.head + "#Zu"
    val valueSlaveNode = embeddingHash(keySlaveNode)
    val HvZu = Hv.t * new DenseVector[Double](valueSlaveNode)
    // 更新从节点Zu的梯度
    val gradientZu = (1.0 / (1.0 + math.exp(math.max(math.min(-HvZu, 20.0), -20.0))) - label) * Hv
    var gradientZuSum = gradientUpdateHash.getOrElseUpdate(keySlaveNode, (Array.fill[Double](vectorSize)(0.0), 0L))._1
    gradientZuSum = (new DenseVector[Double](gradientZuSum) + gradientZu).toArray
    var gradientZuNum = gradientUpdateHash.getOrElseUpdate(keySlaveNode, (Array.fill[Double](vectorSize)(0.0), 0L))._2
    gradientZuNum = gradientZuNum + 1L
    gradientUpdateHash.put(keySlaveNode, (gradientZuSum, gradientZuNum))
    // 更新主节点边权重的梯度和边信息的梯度
    val gradientHv = (1.0 / (1.0 + math.exp(math.max(math.min(-HvZu, 20.0), -20.0))) - label) * new DenseVector[Double](valueSlaveNode)
    val mainNodeWeightGradientSum = gradientUpdateHash.getOrElseUpdate(mainNodeInfo.head + "#Weight", (Array.fill[Double](sideInfoNum)(0.0), 0L))._1
    var mainNodeWeightGradientNum = gradientUpdateHash.getOrElseUpdate(mainNodeInfo.head + "#Weight", (Array.fill[Double](sideInfoNum)(0.0), 0L))._2
    for(m <- 0 until sideInfoNum) {
      val wsvVectorRaw = new DenseVector[Double](embeddingHash(mainNodeInfo(m)))
      // 更新主节点边权重的梯度
      val gradientAsv = gradientHv.t * (eavSum * math.exp(mainNodeWeightRaw(m)) * wsvVectorRaw -  math.exp(mainNodeWeightRaw(m)) * eavWvAggSum)/(eavSum * eavSum)
      mainNodeWeightGradientSum(m) = mainNodeWeightGradientSum(m) + gradientAsv
      // 更新主节点边信息的梯度
      var gradientWsvSum = new DenseVector(gradientUpdateHash.getOrElseUpdate(mainNodeInfo(m), (Array.fill[Double](vectorSize)(0.0), 0L))._1)
      var gradientWsvNum = gradientUpdateHash.getOrElseUpdate(mainNodeInfo(m), (Array.fill[Double](vectorSize)(0.0), 0L))._2
      gradientWsvSum = gradientWsvSum + math.exp(mainNodeWeightRaw(m)) * gradientHv / eavSum
      gradientWsvNum = gradientWsvNum + 1L
      gradientUpdateHash.put(mainNodeInfo(m), (gradientWsvSum.toArray, gradientWsvNum))
    }
    mainNodeWeightGradientNum = mainNodeWeightGradientNum + 1L
    gradientUpdateHash.put(mainNodeInfo.head + "#Weight", (mainNodeWeightGradientSum, mainNodeWeightGradientNum))
  }
}