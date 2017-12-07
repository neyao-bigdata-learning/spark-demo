package org.oursight.demo.spark.mllib.lr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaonengjun on 2017/10/19 下午3:43.
  */
object LrForIrisFlowers {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 1. 读取数据
    val data = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr/iris.data")
    val parsedData = data.map {line =>
      val parts = line.split(",")
      LabeledPoint( if(parts(4)=="Iris-setosa") 0.toDouble else if (parts(4)=="Iris-versicolor")1.toDouble else 2
        .toDouble,
        Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble))
    }

    parsedData.foreach( x=> println(x))

    // 2. 进行数据集的划分，这里划分60%的训练集和40%的测试集：
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1).cache()

    // 3. 训练模型
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(trainingData)

    model.clearThreshold() // 清掉0.5的阈值，输出原始值


    // 4. 用测试结果集进行测试 将测试集的结果进行输出
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)  // 预测的label和实际label的对比
    }

    // 输出结果集
    predictionAndLabels.foreach( x=>println(x))


    //5. 输出测试的结果集
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precesion = metrics.precision
    val recall =  metrics.recall

    println("precesion: " + precesion)
    println("recall: " + recall)

  }


}
