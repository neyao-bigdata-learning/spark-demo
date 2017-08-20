package org.oursight.demo.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaonengjun on 2017/8/20 下午6:39.
  */
object CollaborativeFilteringInScala {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //1、构建Spark对象
    val conf = new SparkConf().setAppName("MyCF")
    val sc = new SparkContext(conf)

    // 2、读取样本数据
    val dataPath = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/collaborative_filtering_10.txt"
    val data = sc.textFile(dataPath)
//    val dataRDD = data.map(_.split(" ")).map(
//      f => (ItemPref(f(0), f(1), f(2).toDouble))  // TODO  ItemRef需要自己实现
//    ).cache()
  }


}
