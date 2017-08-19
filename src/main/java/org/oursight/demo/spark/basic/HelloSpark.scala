package org.oursight.demo.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaonengjun on 2017/8/18 下午7:00.
  */
object HelloSparkInScala {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

        readFromText
//    wordCount
  }

  def basic: Unit = {
    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val data = Array(1, 2, 3, 4, 6, 3, 4)
    val dataRDD = sc.parallelize(data);

    //    val length = dataRDD.map(s => s.)
  }

  def readFromText: Unit = {
    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val testFile = "/Users/neyao/dev/spark/data/demo/words.txt"
    val lines = sc.textFile(testFile);

    val lineLengthRDD = lines.map {s => s.length}
    //    val lineLengthRDD = lines.map(s => s)
    //    println("lineLengthRDD.count(): " + lineLengthRDD.count())

    var totalLengthRDD = lineLengthRDD.reduce {(a, b) => a + b}
    println(totalLengthRDD)


  }

  def wordCount: Unit = {
    val conf = new SparkConf().setAppName("MyScalaTestApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val testFile = "/Users/neyao/dev/spark/data/demo/words.txt"
    val linesRDD = sc.textFile(testFile)
    println(linesRDD.count())
    //    println(linesRDD.take(3))
    println()

    val wordsRDD = linesRDD.flatMap(line => line.split(" "))
    println(wordsRDD.count())
    //    println(wordsRDD.take(3))
    println()


    val wordCountPairRDD = wordsRDD.map(word => (word, 1))
    val wordCountsRDD = wordCountPairRDD.reduceByKey((a, b) => a + b)
    val wordCountsAsArray = wordCountsRDD.collect()
    println(wordCountsAsArray)
    println(wordCountsAsArray.length)
    for (pair <- wordCountsAsArray) {
      println(pair._1 + ": " + pair._2)
    }

  }
}
