package org.oursight.demo.spark.streaming;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
public class SparkStreamingWithKafkaInJava {

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

    Collection<String> topics = Arrays.asList("test");

    JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

    JavaPairDStream<String, String> pairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
    pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
      @Override
      public void call(JavaPairRDD<String, String> rdd) throws Exception {
        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
          @Override
          public void call(Tuple2<String, String> tuple2) throws Exception {
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



}
