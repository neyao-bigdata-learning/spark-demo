package org.oursight.demo.spark.mllib.advance

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.{IDFModel, _}
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaonengjun on 2017/8/23 下午4:32.
  */

//class UseLRClassifyModel()

class LrClassifyModelInChinese {


  val positive_file = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_words_positive.txt"
  val negitive_file = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_words_negitive.txt"


  private var idfModel: IDFModel = _

  private var negTrainRDD: RDD[LabeledPoint] = _ //反例训练数据RDD
  private var posTrainRDD: RDD[LabeledPoint] = _ //正例训练数据RDD
  private var negTestRDD: RDD[LabeledPoint] = _ //反例测试数据RDD
  private var posTestRDD: RDD[LabeledPoint] = _ //正例测试数据RDD

  private var scalerModel: StandardScalerModel = _

  private var model: LogisticRegressionModel = _

  private var sc: SparkContext = _
  var modelPath = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/model/gambling/lrModel"

  //  def main(args: Array[String]): Unit = {
  //    loadTrainData
  ////    buildModel
  //  loadModel()
  //        val line = "手机 免费 百万 好玩 刺激 50万 百亿 领先 值得 亚洲 便捷 等待 平台 巨资 游戏 信赖 品牌 打造 国际 玩家 天天 红包 十分 存取 每天 20点 一期";
  //        println(predictModel(line))
  //  }

  def loadTrainData(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //1、构建Spark对象
    val conf = new SparkConf().setAppName("MyCF").setMaster("local[2]")
    sc = new SparkContext(conf)

    val traningRate = 0.8

    //赌博正例
    val rdd_positive = sc.textFile(positive_file).randomSplit(Array(traningRate, 1 - traningRate), 1000L);
    val rdd_positive_taining = rdd_positive(0)
    val rdd_positive_test = rdd_positive(1)
    // 赌博反例
    val rdd_negitive = sc.textFile(negitive_file).randomSplit(Array(traningRate, 1 - traningRate), 1000L);
    val rdd_negitive_taining = rdd_negitive(0)
    val rdd_negitive_test = rdd_negitive(1)

    // 计算TF值
    val tf = new HashingTF
    val tf_values_positive_traning = rdd_positive_taining.map(line => tf.transform(line.split(" ").map(_.toLowerCase).filter(word => (!(word.matches("\\d+") || word.matches("[a-zA-Z]+"))))))
    val tf_values_positive_test = rdd_positive_test.map(line => tf.transform(line.split(" ").map(_.toLowerCase).filter(word => (!(word.matches("\\d+") || word.matches("[a-zA-Z]+"))))))
    val tf_values_negitive_traning = rdd_negitive_taining.map(line => tf.transform(line.split(" ").map(_.toLowerCase)
      .filter(word => (!(word.matches("\\d+") || word.matches("[a-zA-Z]+"))))))
    val tf_values_negitive_test = rdd_negitive_test.map(line => tf.transform(line.split(" ").map(_.toLowerCase).filter(word => (!(word.matches("\\d+") || word.matches("[a-zA-Z]+"))))))

    idfModel = new IDF().fit(tf_values_positive_traning ++ tf_values_negitive_traning)
    val negTFIDFs = idfModel.transform(tf_values_negitive_traning)
    val posTFIDFs = idfModel.transform(tf_values_positive_traning)
    val negTFIDFsTest = idfModel.transform(tf_values_negitive_test)
    val posTFIDFsTest = idfModel.transform(tf_values_positive_test)

    val normalizer = new Normalizer
    val normalNeg = normalizer.transform(negTFIDFs)
    val normalPos = normalizer.transform(posTFIDFs)
    val normalNegTest = normalizer.transform(negTFIDFsTest)
    val normalPosTest = normalizer.transform(posTFIDFsTest)
    //    val negExamples = normalNeg.map(features => LabeledPoint(0, features))
    //    val posExamples = normalPos.map(features => LabeledPoint(1, features))

    //标准化特征向量 为保证矩阵稀疏性,暂时不减均值
    scalerModel = new StandardScaler(false, true).fit(normalNeg ++ normalPos)
    val localScalerModel = scalerModel
    negTrainRDD = normalNeg.map(features => LabeledPoint(0, localScalerModel.transform(features)))
    posTrainRDD = normalPos.map(features => LabeledPoint(1, localScalerModel.transform(features)))
    negTestRDD = normalNegTest.map(features => LabeledPoint(0, localScalerModel.transform(features)))
    posTestRDD = normalPosTest.map(features => LabeledPoint(1, localScalerModel.transform(features)))

  }


  /**
    * LR策略建立预测模型
    */
  def buildModel() = {
    //    loadTrainData
    val trainingData = negTrainRDD ++ posTrainRDD
    //创建逻辑回归
    val lrLearner = new LogisticRegressionWithLBFGS()
    //训练模型
    val model = lrLearner.run(trainingData)
    //保存模型文件
    val file = new File(modelPath)
    FileUtils.deleteQuietly(file)
    model.save(sc, modelPath)

  }

  def loadModel(): GeneralizedLinearModel = {
    // 读取训练好的模型
    model = LogisticRegressionModel.load(sc, modelPath)
    //清除threshold  预测值返回0-1的double
    model.clearThreshold()
    //    model.setThreshold(0.49)  //FIXME  默认是0.5，不应该改的，但是不改的话所有命中的值都是0.5
    model
  }

  def predictModel(input: String): Double = {
    val tf = new HashingTF
    //将要预测的
    val inputArray = input.split(" ", 0)
    //tf-idf化 去掉纯字母以及纯数字的词
    val tfIdf = idfModel.transform(tf.transform(inputArray.map(_.toLowerCase).filter(word => (!(word.matches("\\d+") || word.matches("[a-zA-Z]+"))))))

    //正则化
    val normalizer = new Normalizer
    val normalPredict = normalizer.transform(tfIdf)
    println()
    println("------------")
    println(normalPredict)
    println("------------")
    println()

    //标准化
    val scalerPredict = scalerModel.transform(normalPredict)

    model.predict(scalerPredict)
  }

  /**
    * 对测试数据进行预测 并统计ROC AOC曲线
    */
  /* def testModel(): Unit = {

     val localModel = loadModel()
     //分别预测正负例
     val negScoreAndLabel = negTestRDD.map { negData =>
       (localModel.predict(negData.features), 0.0)
     }
     val posScoreAndLabel = posTestRDD.map { posData =>
       (localModel.predict(posData.features), 1.0)
     }
     val metrics = new BinaryClassificationMetrics(negScoreAndLabel ++ posScoreAndLabel)
     println("====== Test result =====")
     println(localModel.getClass.getSimpleName + ", Area under PR:" + metrics.areaUnderPR() + ",Area under ROC:" + metrics
       .areaUnderROC() + ",neg size:" + negScoreAndLabel.count() + ", pos size:" + posScoreAndLabel.count())
     println("======================")
   }*/

  /**
    * 对测试数据进行预测 并统计ROC AOC曲线
    */
  def testModel(): Unit = {

    val localModel = loadModel
    //分别预测正负例
    val posScoreAndLabel = posTestRDD.map { posData =>
      (localModel.predict(posData.features), 1.0)
    }
    val negScoreAndLabel = negTestRDD.map { negData =>
      (localModel.predict(negData.features), 0.0)
    }

    val tp = posScoreAndLabel.filter( label => label._1 > 0.5).count()
    val tn = negScoreAndLabel.filter( label => label._1 <= 0.5).count()
    val precision = tp.toDouble / (tp + negScoreAndLabel.count() - tn)
    val recall = tp.toDouble / posScoreAndLabel.count()
    val metrics = new BinaryClassificationMetrics(negScoreAndLabel ++ posScoreAndLabel)


    println()
    println()
    println("------------------")
    println(scalerModel)
    println(localModel.getClass.getSimpleName + ", Area under PR:" + metrics.areaUnderPR() + ",Area under ROC:" + metrics
      .areaUnderROC() + ",neg size:" + negScoreAndLabel.count() + ", pos size:" + posScoreAndLabel.count() + ",tp:" + tp + ",tn:" + tn + ", precision:" + precision + ",reacll:" + recall)
    println("------------------")
    println()
    println()
  }

}
