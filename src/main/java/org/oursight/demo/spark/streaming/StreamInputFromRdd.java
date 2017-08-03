package org.oursight.demo.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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
public class StreamInputFromRdd {

  public static Queue<String> stringQueue = new LinkedBlockingQueue<>();
public static Queue<JavaRDD<String>> queue = new LinkedList();

  private static JavaSparkContext sc;
  private static JavaStreamingContext streamingContext;

  private  static Function<String, String> f1 = new Function<String, String>() {
    @Override
    public String call(String s) throws Exception {
      System.out.println("input: "+ s);
      return s;
    }
  };

  public static void main(String[] args) throws Exception {
    // 手工将日志关闭,
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");

    // 注意，这里至少需要有2个, 不能写成local
    // 见：https://stackoverflow.com/questions/28050262/spark-streaming-network-wordcount-py-does-not-print-result
    sparkConf.setMaster("local[*]");

    // 注意：这里不能new 一个sparkContent，JavaStreamingContext 在new的时候就会创建sparkcontext
    // 需要用JavaSparkContext的时候，直接streamingContext.sparkContext()即可取到
//    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
    sc = streamingContext.sparkContext();


//    Thread t = new Thread(new Producer(sc));
//    t.start();


      runningSparkStream();


  }

  private static void runningSparkStream() throws InterruptedException {
//    JavaInputDStream<String> inputDStream = streamingContext.queueStream(queue, true);
//    inputDStream.print();
//
//    JavaDStream<String> words = inputDStream.map(
//            (Function<String, String>) s -> s
//    );



    List<String> tempStringList = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      String s = stringQueue.poll();
      System.out.println();
      if (s != null) {
        tempStringList.add(s);
      }
    }
    //

    System.out.println("tempStringListe: " + tempStringList);

    JavaRDD<String> rdd = streamingContext.sparkContext().parallelize(tempStringList);
    Queue<JavaRDD<String>> interQueue = new LinkedBlockingQueue();
    interQueue.offer(rdd);

//    queue.offer(rdd);*/

//    JavaInputDStream<String> inputDStream2 = streamingContext.queueStream(queue, false);
//    JavaDStream<String> words = inputDStream2.map(
//           f1
//    );

    // 这样做只会执行一次
    JavaDStream<String> words = streamingContext.queueStream(interQueue);
    words.print();


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


//  private static void runningSparkStream2() throws InterruptedException {
////    JavaInputDStream<String> inputDStream = streamingContext.queueStream(queue, true);
////    inputDStream.print();
////
////    JavaDStream<String> words = inputDStream.map(
////            (Function<String, String>) s -> s
////    );
//
//
//    List<String> strings = new ArrayList<>();
//    strings.add("aaa");
//    strings.add("aaa");
//    strings.add("bbb");
//
//
//    JavaRDD<String> rdd = streamingContext.sparkContext().parallelize(strings);
//    Queue<JavaRDD<String>> queue111 = new LinkedBlockingQueue();
//    queue111.offer(rdd);
//
//    JavaInputDStream<JavaRDD<String>> inputDStream = streamingContext.queueStream(queue111, true);
//
////    JavaDStream<String> words = streamingContext.queueStream(queue111);
////    words.print();
//
//
//    long t1 = System.currentTimeMillis();
//
//
//    JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
//    JavaPairDStream<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2);
////    counts.persist();
//    counts.print();
//    long t2 = System.currentTimeMillis();
//
//    System.out.println("-----");
////    System.out.println("queue size: " + queue.size());
//    System.out.println(new Date() + " done. time cost: " + (t2 - t1) + " ms, counts: " + counts.toString());
//    System.out.println("-----");
//    System.out.println();
//    System.out.println();
//
//    streamingContext.start();
//    streamingContext.awaitTermination();
//
//
//  }


  static class Producer implements Runnable {

    private JavaSparkContext sc;

    public Producer(JavaSparkContext sc) {
      this.sc = sc;
    }

    @Override
    public void run() {
      while (true) {

        List<String> list = new ArrayList();
        for (int i = 0; i < 5; i++) {

          String s = Utils.random(3);
          list.add(s);
          stringQueue.offer(s);
          System.out.println(new Date() + " String " + s + " added to queque");

        }
        JavaRDD<String> rdd = sc.parallelize(list);
        queue.offer(rdd);
        System.out.println(" size: " + queue.size());
        System.out.println();
        System.out.println();


        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

}
