package org.oursight.demo.spark.mllib.lr

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaonengjun on 2017/10/19 下午4:18.
  */
object LoadMySampleDataInScala {

  private var sc: SparkContext = _
  private var model: LogisticRegressionModel = _

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[4]")
    sc = new SparkContext(conf)

    //    train_model

    //    testModel
    predict("/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr/to_predic_vectors.txt")
  }

  def loadModel(modelPath: String): GeneralizedLinearModel = {
    // 读取训练好的模型
    val model = LogisticRegressionModel.load(sc, modelPath)
    //清除threshold  预测值返回0-1的double
    model.clearThreshold()
    //    model.setThreshold(0.49)  //FIXME  默认是0.5，不应该改的，但是不改的话所有命中的值都是0.5
    return model
  }

  def predict(input: String): Unit = {
    val localModel = loadModel("/Users/neyao/temp/model")
    println(localModel)

//    val feature_size = 3313929
    val elements = Array(13680, 115892, 201403, 233585, 247271, 287553, 287817, 310455, 313541, 350655, 475296, 481106,
      499874, 523463, 529427, 561599, 608485, 653263, 669351, 718995, 747161, 752687, 803648, 808373, 820217, 1040871, 1093807, 1119602, 1169672, 1261106, 1270114, 1355124, 1421515, 1424847, 1453347, 1528963, 1553626, 1672825, 1705023, 1719851, 1743773, 1819827, 1908653, 1910600, 1921119, 2085606, 2117622, 2144811, 2244713, 2269233, 2407375, 2501470, 2538442, 2573307, 2591918, 2592701, 2624670, 2637661, 2652093, 2870098, 2890624, 2934555, 2935186, 3001127, 3007567, 3066041, 3066529, 3101696, 3122666, 3284822, 3286075)
    val values = new Array[Double](elements.length)
    for (i <- 0 to elements.length -1) {
      values(i) = 1.0
    }

//    val testDataVector = Vectors.sparse(feature_size, elements, values)
//    val testDataVector = Vectors.sparse(elements.length, elements, values)

//    println(model.predict(testDataVector))


    //    double[]
    //    Vector v = Vectors.s

    //    val rawdata = MLUtils.loadLibSVMFile(sc, input);
    //
    //    rawdata.foreach(line =>
    //      println( localModel.predict(line.features) )
    //    )

    //    val dv = Vectors.dense(3680, 115892, 201403, 233585, 247271, 287553, 287817, 310455, 313541, 350655, 475296,
    //      481106, 499874, 523463, 529427, 561599, 608485, 653263, 669351, 718995, 747161, 752687, 803648, 808373, 820217, 1040871, 1093807, 1119602, 1169672, 1261106, 1270114, 1355124, 1421515, 1424847, 1453347, 1528963, 1553626, 1672825, 1705023, 1719851, 1743773, 1819827, 1908653, 1910600, 1921119, 2085606, 2117622, 2144811, 2244713, 2269233, 2407375, 2501470, 2538442, 2573307, 2591918, 2592701, 2624670, 2637661, 2652093, 2870098, 2890624, 2934555, 2935186, 3001127, 3007567, 3066041, 3066529, 3101696, 3122666, 3284822, 3286075);
    //    println(localModel.predict(dv))
  }

  /**
    * 对测试数据进行预测 并统计ROC AOC曲线
    */
  def testModel(): Unit = {

    val modelPath = "/Users/neyao/temp/model"
    val localModel = loadModel(modelPath)


    //    val data = MLUtils.loadLibSVMFile(sc,
    //      "/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr/training_data_illegal_service.txt")
    //    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    //    val trainingData = splits(0).cache()
    //    val testData = splits(1).cache()

    val testData = MLUtils.loadLibSVMFile(sc,
      "/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr/training_data_illegal_service.txt")
    //分别预测正负例
    val testDataPositive = testData.filter(labeledPoint => labeledPoint.label == 1)
    val testDataNegitive = testData.filter(labeledPoint => labeledPoint.label == 0)


    val posScoreAndLabel = testDataPositive.map { posData =>
      //println("test features 1: " + posData.features)
      (localModel.predict(posData.features), 1.0)
      //      (localModel.predict(posData.features), posData.features)
    }
    val negScoreAndLabel = testDataNegitive.map { negData =>
      (localModel.predict(negData.features), 0.0)
    }

    val positiveResult = testDataPositive.map { posData =>
      //            (localModel.predict(posData.features), posData.features)
      LabeledPoint.apply(localModel.predict(posData.features), posData.features)
    }
    val positiveResultFalse = positiveResult.filter(labeledPoint =>
      labeledPoint.label <= 0.5
    )

    val folderPath = "/Users/neyao/temp/model/test"
    FileUtils.deleteQuietly(new File(folderPath))

    positiveResult.repartition(1).saveAsTextFile(folderPath + "/positiveResult11111")
    positiveResultFalse.repartition(1).saveAsTextFile(folderPath + "/positiveResult22222")
    //    MLUtils.saveAsLibSVMFile(positiveResult.repartition(1), folderPath +"/positiveResult11111")
    //    MLUtils.saveAsLibSVMFile(positiveResultFalse.repartition(1), folderPath +"/positiveResult22222")


    val tp = posScoreAndLabel.filter(label => label._1 > 0.5)
    val fp = posScoreAndLabel.filter(label => label._1 <= 0.5)
    val tn = negScoreAndLabel.filter(label => label._1 < 0.5)
    val fn = negScoreAndLabel.filter(label => label._1 >= 0.5)

    println(posScoreAndLabel.count() + negScoreAndLabel.count())
    println("tp: " + tp.count())
    println("fp: " + fp.count())
    println("tn: " + tn.count())
    println("fn: " + fn.count())


    fp.repartition(1).saveAsTextFile(folderPath + "/fp.txt")
    fn.repartition(1).saveAsTextFile(folderPath + "/fn.txt")

    val precision = tp.count().toDouble / (tp.count() + fp.count())
    val recall = tp.count().toDouble / ((tp.count() + fn.count()))
    val metrics = new BinaryClassificationMetrics(negScoreAndLabel ++ posScoreAndLabel)


    println()
    println()
    println("------------------")
    println(localModel.getClass.getSimpleName + ", Area under PR:" + metrics.areaUnderPR() + ",Area under ROC:" + metrics
      .areaUnderROC() + ",neg size:" + negScoreAndLabel.count() + ", pos size:" + posScoreAndLabel.count() + ",tp:" + tp + ",tn:" + tn + ", precision:" + precision + ",reacll:" + recall)
    println("------------------")
    println()
    println()
  }


  def train_model: Unit = {
    //    Logger.getLogger("org").setLevel(Level.WARN)
    //    Logger.getLogger("akka").setLevel(Level.WARN)
    //    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[4]")
    //    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc,
      "/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr/training_data_illegal_service.txt")


    //    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    //    val trainingData = splits(0).cache()
    //    val testData = splits(1).cache()

    val trainingData = data
    val testData = data

    println(data.count())

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(trainingData)

    model.clearThreshold() // 清掉0.5的阈值，输出原始值

    //保存结果
    val folderPath = "/Users/neyao/temp/model"
    FileUtils.deleteQuietly(new File(folderPath))
    model.save(sc, folderPath)

    println("model saved: " + model)

    // 4. 用测试结果集进行测试 将测试集的结果进行输出
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      println("test features: " + features)
      (prediction, label) // 预测的label和实际label的对比
    }

    // 输出结果集
    //    predictionAndLabels.foreach( x=>println(x))

    val testDataPositive = testData.filter(labeledPoint => labeledPoint.label == 1)
    val testDataNegitive = testData.filter(labeledPoint => labeledPoint.label == 0)

    testDataPositive.saveAsTextFile(folderPath + "/testDataPositive.txt")
    testDataNegitive.saveAsTextFile(folderPath + "/testDataNegitive.txt")

    testDataNegitive.foreach(x => println(x))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println(metrics.areaUnderROC())
    println(metrics.areaUnderPR())
  }

}
