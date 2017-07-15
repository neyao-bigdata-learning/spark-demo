package org.oursight.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.tools.cmd.gen.AnyVals;

/**
 * 建议将spark的日子级别调整为warn以便更好的看到输出效果， spark的日志配置在：
 * /Users/neyao/dev/spark/spark-2.2.0-bin-hadoop2.7/conf/log4j.properties
 *
 * Created by neyao@github.com on 2016/4/12.
 */
public class ComputeWithCollection {

  public static void main(String[] args) {


//    test();
    compute();
  }

//  public static void test() {
//
//    System.out.println("===========");
//    System.out.println("test");
//    System.out.println("========");
//
//  }

  public static void compute() {

    // 准备数据
    List<String> target = new ArrayList<>();
    target.add("张三历史万古我");
    target.add("历史万古我");
    target.add("王五历史万古我");
    target.add("张三李四万古我");

    List<String> sample = new ArrayList<>();
    sample.add("张三");
    sample.add("李四");
    sample.add("王五");


    //开始计算
    long t1 = System.currentTimeMillis();
    SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
    sparkConf.setMaster("local");
    //sparkConf.setAppName("MyHelloSparkApp");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> targetRDD = sc.parallelize(target);
    targetRDD.saveAsTextFile("/Users/neyao/Desktop/spark_rdd_output/targetRDD");
    JavaRDD<String> sampleRDD = sc.parallelize(sample);


    // 将两个做笛卡尔乘积
    JavaPairRDD<String, String> mergedRDD = targetRDD.cartesian((JavaRDDLike) sampleRDD);
    System.out.println("---- merge start ----");
    mergedRDD.saveAsTextFile("/Users/neyao/Desktop/spark_rdd_output/mergedRDD");
    for (Tuple2<String, String> all : mergedRDD.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    System.out.println(mergedRDD.toDebugString());
    System.out.println("---- merge done ----");
    System.out.println();
    System.out.println();


    System.out.println("---- MAP 1 start ----");
    // 计算：包含关系
    JavaPairRDD<Tuple2<String, String>, Boolean> mergedRDD1 = mergedRDD.mapToPair(
            (PairFunction<Tuple2<String, String>, Tuple2<String, String>, Boolean>) t -> {
              String s = t._1() + "  " + t._2();
              boolean result = t._1().contains(t._2());
              Tuple2 tuple2 = new Tuple2(t, result);
              return tuple2;
            });
    // 这个是上边的lambda表达式的java代码
//    JavaPairRDD<String, Boolean> mergedRDDNew = mergedRDD.mapToPair(new PairFunction<Tuple2<String, String>, String,
//            Boolean>() {
//
//      public Tuple2<String, Boolean> call(Tuple2<String, String> t) {
//        String s = t._1() + "  " + t._2();
//        boolean result = t._1().contains(t._2());
//        Tuple2 tuple2 = new Tuple2(s, result);
//        return tuple2;
//      }
//
//    });

    for (Tuple2<Tuple2<String, String>, Boolean> all : mergedRDD1.collect()) {
      System.out.println(all._1()._1() + "    " + "    " + all._1()._2() + "    " + all._2());
    }
    System.out.println(mergedRDD.toDebugString());
    System.out.println("---- MAP 1 done ----");
    System.out.println();
    System.out.println();


    // 计算：计算出现次数
    // 1）先进行Map


    JavaPairRDD<Tuple2<String, String>, Integer> mergedRDD2 = mergedRDD1.mapToPair(
            (PairFunction<Tuple2<Tuple2<String, String>, Boolean>, Tuple2<String, String>, Integer>) t -> {

              if (t._2 == true) {
                return new Tuple2(t._1, 1);
              }
              return new Tuple2(t._1, 0);
            });

    JavaPairRDD<String, Integer> mergedRDD2_key1 = mergedRDD1.mapToPair(
            (PairFunction<Tuple2<Tuple2<String, String>, Boolean>, String, Integer>) t -> {

              if (t._2 == true) {
                return new Tuple2(t._1._1, 1);
              }
              return new Tuple2(t._1._1, 0);
            });

    JavaPairRDD<String, Integer> mergedRDD2_key2 = mergedRDD1.mapToPair(
            (PairFunction<Tuple2<Tuple2<String, String>, Boolean>, String, Integer>) t -> {

              if (t._2 == true) {
                return new Tuple2(t._1._2, 1);
              }
              return new Tuple2(t._1._2, 0);
            });

    JavaPairRDD<String, Integer> mergedRDD2_key1_reduced = mergedRDD2_key1.reduceByKey(
            (Function2<Integer, Integer, Integer>) (a, b) -> a + b
    );

    JavaPairRDD<String, Integer> mergedRDD2_key2_reduced = mergedRDD2_key2.reduceByKey(
            (Function2<Integer, Integer, Integer>) (a, b) -> a + b
    );

    System.out.println();
    System.out.println("===================== 所有结果汇总开始 =====================");
    System.out.println();

    // 打印输出结果
    System.out.println("---- 1. merge 笛卡尔乘积 ----");
    for (Tuple2<String, String> all : mergedRDD.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    System.out.println(mergedRDD.toDebugString());
    System.out.println("--------");
    System.out.println();

    System.out.println("---- 2. map to contains result ----");
    for (Tuple2<Tuple2<String, String>, Boolean> all : mergedRDD1.collect()) {
      System.out.println(all._1()._1() + "    " + "    " + all._1()._2() + "    " + all._2());
    }
    System.out.println(mergedRDD.toDebugString());
    System.out.println("--------");
    System.out.println();
    System.out.println();

    System.out.println("---- 3.1 map by key 1 ----");
    for (Tuple2<String, Integer> all : mergedRDD2_key1.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    System.out.println("--------");
    System.out.println();

    System.out.println("---- 3.2 reduce by key 1 ----");
    for (Tuple2<String, Integer> all : mergedRDD2_key1_reduced.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    System.out.println("--------");
    System.out.println();

    System.out.println("---- 4.1 map by key 2 ----");
    for (Tuple2<String, Integer> all : mergedRDD2_key2.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    System.out.println("--------");
    System.out.println();

    System.out.println("---- 4.2 reduce by key 2 ----");
    for (Tuple2<String, Integer> all : mergedRDD2_key2_reduced.collect()) {
      System.out.println(all._1() + "    " + all._2());
    }
    mergedRDD2_key2_reduced.saveAsTextFile("/Users/neyao/Desktop/spark_rdd_output/mergedRDD2_key2_reduced");
    System.out.println("--------");
    System.out.println();

    System.out.println("===================== 所有结果汇总结束 =====================");
    System.out.println();
    System.out.println();


    long t2 = System.currentTimeMillis();

    System.out.println();
    System.out.println();
    System.out.println("========================= RESULT =========================");
    System.out.println("time cost: " + (t2 - t1) + " ms");
    System.out.println("=========================================================");
    System.out.println();
    System.out.println();

  }


}
