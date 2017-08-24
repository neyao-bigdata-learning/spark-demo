package org.oursight.demo.spark.mllib.advance

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Created by yaonengjun on 2017/8/22 下午3:22.
  */
object SimpleLrSpamClassification {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("LrSpamClassification").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val spam = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/traning_files/spam/spam.txt")
    val ham = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/traning_files/spam/normal.txt")

    //创建一个HashingTF实例来把邮件文本映射为包含25000特征的向量
    val tf = new HashingTF(numFeatures = 25000)

    //各邮件都被切分为单词，每个单词被映射为一个特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    //创建LabeledPoint数据集分别存放垃圾邮件(spam)和正常邮件(ham)的例子
    spamFeatures.collect().foreach { x => print(x + " ,") }
    hamFeatures.collect().foreach { x => print(x + " ,") }

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 逻辑回归是迭代算法，所以缓存训练数据的RDD


    //使用SGD算法运行逻辑回归
    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingData)

    //以垃圾邮件和正常邮件的例子分别进行测试。
    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    val posTest1Example = tf.transform("I really wish well to all my friends.".split(" "))
    val posTest2Example = tf.transform("He stretched into his pocket for some money.".split(" "))
    val posTest3Example = tf.transform("He entrusted his money to me.".split(" "))
    val posTest4Example = tf.transform("Where do you keep your money?".split(" "))
    val posTest5Example = tf.transform("She borrowed some money of me.".split(" "))

    //首先使用，一样的HashingTF特征来得到特征向量，然后对该向量应用得到的模型
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

    println(s"posTest1Example for negative test example: ${model.predict(posTest1Example)}")
    println(s"posTest2Example for negative test example: ${model.predict(posTest2Example)}")
    println(s"posTest3Example for negative test example: ${model.predict(posTest3Example)}")
    println(s"posTest4Example for negative test example: ${model.predict(posTest4Example)}")
    println(s"posTest5Example for negative test example: ${model.predict(posTest5Example)}")

    sc.stop()
  }

}
