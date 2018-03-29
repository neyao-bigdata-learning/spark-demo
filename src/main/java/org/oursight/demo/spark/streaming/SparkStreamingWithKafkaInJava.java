package org.oursight.demo.spark.streaming;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

/**
 * 参照的官方文档参见：https://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html
 */
public class SparkStreamingWithKafkaInJava {

  /**
   * 使用这种方式来读取，是标准的基于SparkStreaming的方式来读取，每个一个固定的时间来读取这个批次的数据。
   * 用这种方式，可以在Spark的管理界面 http://localhost:4040 上看到Streaming的相关信息
   *
   * 这种读取方式是纯粹的流的方式，在Spark停止服务或者重启期间，数据是会被丢掉的。
   */
  public static void main1(String[] args) throws Exception {
    // 手工将日志关闭,
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
    // 注意，这里至少需要有2个, 不能写成local
    // 见：https://stackoverflow.com/questions/28050262/spark-streaming-network-wordcount-py-does-not-print-result
    sparkConf.setMaster("local[*]");

    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));


    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);


    // =============== consuming method 1 ================

    Collection<String> topics = Arrays.asList("test");

    JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );
    // 遍历这个Map
    JavaPairDStream<String, String> pairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
    pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
      @Override
      public void call(JavaPairRDD<String, String> rdd) throws Exception {
        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
          @Override
          public void call(Tuple2<String, String> tuple2) throws Exception {
            System.out.println(new Date());
            System.out.println("key: " + tuple2._1());
            System.out.println("value: " + tuple2._2());
            System.out.println();
          }
        });
      }
    });

    streamingContext.start();
    streamingContext.awaitTermination();



  }


  /**
   * 这种方式是官方文档中采用的方式。
   *
   * 使用这种方式来读取，在Spark的管理界面 http://localhost:4040 上看不到Streaming的相关信息
   * 似乎是通过直接读取Kafka  offset的方式来直接生成RDD.
   *
   * 这种方式在重启spark期间，是不会丢失诗句的。
   */
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


    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);


    OffsetRange[] offsetRanges = {
            // topic, partition, inclusive starting offset, exclusive ending offset
            OffsetRange.create("test", 0, 0, 100),
            OffsetRange.create("test", 1, 0, 100)
    };

    JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
            streamingContext.sparkContext(),
            kafkaParams,
            offsetRanges,
            LocationStrategies.PreferConsistent()
    );

    rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
      @Override
      public void call(ConsumerRecord<String, String> record) throws Exception {
        System.out.println(new Date());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println();
      }
    });




    streamingContext.start();
    streamingContext.awaitTermination();



  }



}
