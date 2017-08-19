package org.oursight.demo.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 样本的数据：
  * #用户ID, 平均相似度  最大相似度 命中率
  * Created by yaonengjun on 2017/8/18 下午8:15.
  */
object LinearRegression {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 1、读取样本数据
    val traningDataRDD = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr_traning.txt")
    val exampleFromTraningDataRDD = traningDataRDD.map { line =>
      println(line)
      val parts = line.split(", ")
      println("parts: " + parts.deep.mkString("\n"))
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val counts = exampleFromTraningDataRDD.count()

    // 2、构建线性回归模型
    val numIterations = 100
    val stepSize = 1
    val minBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(exampleFromTraningDataRDD, numIterations, stepSize, minBatchFraction)
    model.weights
    model.intercept

    // 3、对样本进行测试
    val prediction = model.predict(exampleFromTraningDataRDD.map(_.features))
    //    val prediction = model.predict(exampleFromTraningDataRDD.map(f => f.features))
    val predictionAndLabel = prediction.zip(exampleFromTraningDataRDD.map(_.label))
    val printPredic = predictionAndLabel.take(50)
    println("prediction" + "\t\t\t" + "label")
    for (i <- 0 to printPredic.length - 1) {
      println(printPredic(i)._1 + "\t\t\t" + printPredic(i)._2)
    }
    println


    // 4、计算误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / counts);
    println(s"test RMSE= $rmse")
    println

    // 5、预测一条新数据

    var d: Array[Double] = Array[Double](0.5, 0.6, 0.7) //输入特征值，本条特征值和标签777的特征完全一样
    var v: linalg.Vector = Vectors.dense(d)
    println("---- predict ----")
    println(model.predict(v)) // 可以看到，这个最终取值和777这个标签的值是完全一致的

    d = Array[Double](0.5, 0.6, 0.8)  // 和标签777略有区别
    v = Vectors.dense(d)
    println(model.predict(v))  // 可以看到，两次计算出来的值，差异并不大

    println("-----------------")
//    System.out.println(model.predict(v))
//    System.out.println(model1.predict(v))
//    System.out.println(model2.predict(v))


    // 6、保存模型
//    val modelPath = "/Users/neyao/dev/spark/data/demo/lr_model"
//    model.save(sc, modelPath)
//    val loadedModel = LinearRegressionModel.load(sc, modelPath)
//    println("originModel=" + model)
//    println("loadedModel=" + loadedModel)


  }
}
