package org.oursight.demo.spark.mllib.advance

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.oursight.demo.spark.util.MyTokenizer

/**
  * Created by yaonengjun on 2017/8/22 下午5:20.
  */
object MyLrInSacala {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("LrSpamClassification").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val gambling_positive = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_words_positive.txt")
    val gambling_negitive = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples" +
      "/gambling_words_negitive.txt").flatMap(line => line.split(" "))

//    gambling_positive.collect().foreach(x => println(x))
    gambling_negitive.collect().foreach(x => println(x))


    //创建一个HashingTF实例来把邮件文本映射为包含25000特征的向量
    val tf = new HashingTF(numFeatures = 25000)

    //各邮件都被切分为单词，每个单词被映射为一个特征
    val gambling_positive_features = gambling_positive.map(line => tf.transform(Array(line)))
    val gambling_negitive_features = gambling_negitive.map(line => tf.transform(Array(line)))

    //创建LabeledPoint数据集分别存放垃圾邮件(spam)和正常邮件(ham)的例子
    gambling_positive_features.collect().foreach { x => print(x + " ,") }
    gambling_negitive_features.collect().foreach { x => print(x + " ,") }

    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    val positiveExamples = gambling_positive_features.map(features => LabeledPoint(1, features))
    val negativeExamples = gambling_negitive_features.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 逻辑回归是迭代算法，所以缓存训练数据的RDD


    //使用SGD算法运行逻辑回归
    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingData)

//    val line1 = MyTokenizer.apply("我想买彩票", " ");
    val line1 = MyTokenizer.apply("深圳泊众国内最专业棋牌游戏开发商 国内最大棋牌游戏开发商 首    页 精品手游 街机电玩 休闲棋牌 1分钟快速了解 选产品 产品选对基本就成功了一半完善棋牌产品包含游戏大厅网站前后台代理及运营系统等 点击在线咨询更多 问售后 良好的服务是持久成功的保障泊众提供免费培训架设Bug修正1对1专职售后等服务 点击在线咨询更多 看公司 选择优秀的棋牌游戏开发公司对运营商来说至关重要评价标准包括公司资质团队经验公司规模 购买产品上线运营 确认需求选择直接购买游戏和平台或定制开发游戏打造属于自己的棋牌游戏平台上线运营 点击在线咨询更多 购买现在游戏最快3天上线 多款热门游戏快速吸引玩家 李逵劈鱼 金鲨银鲨 斗地主 智勇三张 百变牛牛 二八杠 三十秒 至尊五张 奔驰宝马 二人雀神 血战麻将 跑胡子 更多游戏点击在线咨询 强大平台助推棋牌运营 十年沉淀之作棋牌行业最专业功能丰富强大经过持续运营验证 专业高端类型紫金游 经典界面布局玩家易接受上手快平台及游戏开源支持二次开发 大众休闲类型面对面 耗时一年倾力打造全新游戏平台平台采用DX技术开发 防作弊竞技类型同桌游 黑灰极简风格突显棋牌竞技本质多重防作弊技术游戏公平公正 黑色简约竞技风云版 完美售后服务为运营成功添筹码 •5天免费棋牌系统运营培训 •1年免费技术支持服务 •78小时专职在线 •永久免费BUG修复 •系统安装一条龙服务 •包教会使用棋牌软件 电话咨询 官方热线点击拨打", " ");
    println("line1: " + line1)
    val example1 = tf.transform(line1.split(" "))
    println
    println
    println(s"Prediction for positive test example: ${model.predict(example1)}")
    println(s"Prediction for negative test example: ${model.predict(example1)}")

    //以垃圾邮件和正常邮件的例子分别进行测试。
    //  val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    //  val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    //  val posTest1Example = tf.transform("I really wish well to all my friends.".split(" "))
    //  val posTest2Example = tf.transform("He stretched into his pocket for some money.".split(" "))
    //  val posTest3Example = tf..transform("He entrusted his money to me.".split(" "))
    //  val posTest4Example = tf.transform("Where do you keep your money?".split(" "))
    //  val posTest5Example = tf.transform("She borrowed some money of me.".split(" "))

    //首先使用，一样的HashingTF特征来得到特征向量，然后对该向量应用得到的模型
    //  println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    //  println(s"Prediction for negative test example: ${model.predict(negTestExample)}")
    //
    //
    //  println(s"posTest1Example for negative test example: ${model.predict(posTest1Example)}")
    //  println(s"posTest2Example for negative test example: ${model.predict(posTest2Example)}")
    //  println(s"posTest3Example for negative test example: ${model.predict(posTest3Example)}")
    //  println(s"posTest4Example for negative test example: ${model.predict(posTest4Example)}")
    //  println(s"posTest5Example for negative test example: ${model.predict(posTest5Example)}")

    sc.stop()
  }

}
