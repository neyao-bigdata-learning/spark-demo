package org.oursight.demo.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.oursight.demo.spark.util.Utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import scala.Tuple2;

/**
 * Spark Streaming教程：
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * <p>
 * 完整的例子：https://github.com/apache/spark/blob/v2.2.0/examples/src/main/java/org/apache/spark/examples/streaming/JavaNetworkWordCount.java
 * <p>
 * 使用netcat（mac上自带）来启动一个端口在9999的监听服务，命令行：nc -lk 9999
 * <p>
 * Created by yaonengjun on 2017/8/3 下午2:32.
 */
public class StreamInputFromCustomReceiver {

  public static Queue<String> stringQueue = new LinkedBlockingQueue<>();
  public static Queue<JavaRDD<String>> queue = new LinkedList();

  private static JavaSparkContext sc;
  private static JavaStreamingContext streamingContext;

  private static Function<String, String> f1 = new Function<String, String>() {
    @Override
    public String call(String s) throws Exception {
      System.out.println("input: " + s);
      return s;
    }
  };

  public static void main(String[] args) throws Exception {
    // 手工将日志关闭,
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");

    sparkConf.setMaster("local[*]");

    streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
    sc = streamingContext.sparkContext();


    runningSparkStream();


  }

  private static void runningSparkStream() throws InterruptedException {

    JavaReceiverInputDStream<String> inputDStream = streamingContext.receiverStream(new CustomReceiver());

    JavaDStream<String> words = inputDStream.map(
            new Function<String, String>() {
              @Override
              public String call(String s) throws Exception {
                System.out.println(new Date() + " receiverd: " + s);
                System.out.println();
                return s;
              }
            }
    );
//    JavaDStream<String> words = streamingContext.queueStream(queue);

    words.print();
    words.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
      @Override
      public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
        System.out.println("==== " + time + "  ====");
        stringJavaRDD.foreach(
                new VoidFunction<String>() {
                  @Override
                  public void call(String s) throws Exception {
                    System.out.println("S in final RDD: " + s);
                  }
                }
        );
        System.out.println("==============");
      }
    });


    /*JavaRDD<String> javaRDD = streamingContext.sparkContext().parallelize(tempStringList);
    Queue<JavaRDD<String>> queue = new LinkedBlockingQueue<>();
    queue.offer(javaRDD);

    JavaDStream<String> words = streamingContext.queueStream(queue);
    words.print();*/


    long t1 = System.currentTimeMillis();
    JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairDStream<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2);
//    counts.persist();
    counts.print();
    long t2 = System.currentTimeMillis();

    System.out.println("-----");
    System.out.println("queue size: " + queue.size());
    System.out.println(new Date() + " done. time cost: " + (t2 - t1) + " ms, counts: " + counts.toString());
    System.out.println("-----");
    System.out.println();
    System.out.println();

    streamingContext.start();
    streamingContext.awaitTermination();


  }


}
