package org.oursight.demo.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import scala.Tuple2;

/**
 * Spark Streaming教程：
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html
 *
 * 完整的例子：https://github.com/apache/spark/blob/v2.2.0/examples/src/main/java/org/apache/spark/examples/streaming/JavaNetworkWordCount.java
 *
 * 使用netcat（mac上自带）来启动一个端口在9999的监听服务，命令行：nc -lk 9999
 *
 * Created by yaonengjun on 2017/8/3 下午2:32.
 */
public class HelloSparkStreaming {

  public static void main(String[] args) throws Exception {
    // 手工将日志关闭,
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
    // 注意，这里至少需要有2个, 不能写成local
    // 见：https://stackoverflow.com/questions/28050262/spark-streaming-network-wordcount-py-does-not-print-result
    sparkConf.setMaster("local[*]");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

    String tcpHost = "localhost";
    Integer tcpPort = 9999;

    //从TCP端口来进行监听
    JavaReceiverInputDStream<String> linesInput = streamingContext.socketTextStream(tcpHost, tcpPort);

    JavaDStream<String> words = linesInput.flatMap(
            (FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator()
    );
//    JavaDStream<String> word = linesInput.flatMap(
//            new FlatMapFunction<String, String>() {
//              @Override
//              public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//              }
//            }
//    );


    long t1 = System.currentTimeMillis();
    JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairDStream<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2);
//    counts.persist();
    counts.print();
    long t2 = System.currentTimeMillis();

    System.out.println();
    System.out.println(new Date() + " done. time cost: " + (t2 - t1) + " ms");
    System.out.println();



    streamingContext.start();
    streamingContext.awaitTermination();


  }

//  private static Function f2 = new Function<Tuple2<Tuple2<Account, IdeaEntity>, Double>, Boolean>() {
//    @Override
//    public Boolean call(Tuple2<Tuple2<Account, IdeaEntity>, Double> v1) throws Exception {
//      if (v1._2() > similarityValve) {
//        return true;
//      }
//
//      return false;
//    }
//  };

}
