package org.oursight.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Created by yaonengjun on 2017/7/15 下午4:13.
 */
public class PairRDDExamples {

  public static void main(String[] args) {
    flatMapValues();
  }

  public static void flatMapValues() {
    SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
    sparkConf.setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);


    List<Integer> list1 = new ArrayList<>();
    list1.add(11);
    list1.add(22);
    list1.add(33);

    List<Integer> list2 = new ArrayList<>();
    list2.add(33);
    list2.add(44);

    Map<String, List<Integer>> map = new HashMap<>();
    map.put("aa", list1);
    map.put("bb", list2);

    List<Tuple2<String, List<Integer>>> list = new ArrayList<>();
    Tuple2<String, List<Integer>> t1 = new Tuple2<>("aa", list1);
    Tuple2<String, List<Integer>> t2 = new Tuple2<>("bb", list2);
    list.add(t1);
    list.add(t2);

    JavaPairRDD<String, List<Integer>> pairRDD1 = sc.parallelizePairs(list);
    System.out.println("---- pairRDD1 start ----");
    for (Tuple2<String, List<Integer>> all : pairRDD1.collect()) {
      System.out.println(all._1() + "    " + "    " + all._2());
    }
    System.out.println("---- pairRDD1 done ----");
    System.out.println();
    System.out.println();

    //  ===  flatMapValues  ===
    JavaPairRDD<String, Integer> pairRDD2 = pairRDD1.flatMapValues(
            (Function<List<Integer>, Iterable<Integer>>) v1 -> v1

    );
    // 上边lamda表达式的Java形式, 的到的pairRDD3和pairRDD2是一模一样的
    JavaPairRDD<String, Integer> pairRDD3 = pairRDD1.flatMapValues(
            new Function<List<Integer>, Iterable<Integer>>() {

              @Override
              public Iterable<Integer> call(List<Integer> v1) throws Exception {
                return v1;
              }
            }

    );

    System.out.println("---- pairRDD2 start ----");
    for (Tuple2<String, Integer> all : pairRDD2.collect()) {
      System.out.println(all._1() + "    " + "    " + all._2());
    }
    System.out.println("---- pairRDD2 done ----");
    System.out.println();
    System.out.println();


    // === convert to RDD ==

    // lambda 表达式
//    JavaRDD<Tuple2<String, Integer>> rdd1_from_pairRDD2 = pairRDD2.map(
//            (Function<Tuple2<String, Integer>, Tuple2<String, Integer>>) v1 -> v1
//    );
    JavaRDD<Tuple2<String, Integer>> rdd1_from_pairRDD2 = pairRDD2.map(
            new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return v1;
              }
            }
    );
    System.out.println("---- rdd1_from_pairRDD2 start ----");
    for (Tuple2<String, Integer> all : rdd1_from_pairRDD2.collect()) {
      System.out.println(all);
//      System.out.println(all._1() + "    " + "    " + all._2());
    }
    System.out.println("---- rdd1_from_pairRDD2 done ----");
    System.out.println();
    System.out.println();


  }

  public static void allDemo() {
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // convert from other RDD
    JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));
    JavaPairRDD<String, String> prdd = line1.mapToPair(new PairFunction<String, String, String>() {
      public Tuple2<String, String> call(String x) throws Exception {
        return new Tuple2(x.split(" ")[0], x);
      }
    });
    System.out.println("111111111111mapToPair:");
    prdd.foreach(new VoidFunction<Tuple2<String, String>>() {
      public void call(Tuple2<String, String> x) throws Exception {
        System.out.println(x);
      }
    });

    // parallelizePairs
    Tuple2 t1 = new Tuple2(1, 5);
    Tuple2 t2 = new Tuple2(1, 3);
    Tuple2 t3 = new Tuple2(3, 7);
    List list1 = new ArrayList<Tuple2>();
    list1.add(t1);
    list1.add(t2);
    list1.add(t3);
    JavaPairRDD<Integer, Integer> line2 = sc.parallelizePairs(list1);
    line2.persist(StorageLevel.MEMORY_ONLY());

    System.out.println("22222222222222222parallelize:");
    line2.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });

    // reduceByKey
    JavaPairRDD<Integer, Integer> line3 = line2.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer x, Integer y) throws Exception {
        return (x + y);
      }
    });
    System.out.println("333333333333reduceByKey:");
    line3.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });

    // groupByKey
    JavaPairRDD<Integer, Iterable<Integer>> line4 = line2.groupByKey();

    System.out.println("44444444444444groupByKey:");
    line4.foreach(new VoidFunction<Tuple2<Integer, Iterable<Integer>>>() {
      public void call(Tuple2<Integer, Iterable<Integer>> x) throws Exception {
        System.out.println(x);
      }
    });

    // mapValues
    JavaPairRDD<Integer, Integer> line5 = line2.mapValues(new Function<Integer, Integer>() {
      public Integer call(Integer x) throws Exception {
        return x * x;
      }
    });

    System.out.println("555555555555555mapValues:");
    line5.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });

    // flatMapValues
    JavaPairRDD<Integer, Integer> line6 = line2.flatMapValues(new Function<Integer, Iterable<Integer>>() {
      public Iterable<Integer> call(Integer x) throws Exception {
        ArrayList list = new ArrayList();
        list.add(x);
        return list;
      }
    });

    System.out.println("666666666666flatMapValues:");
    line6.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });

    // keys
    JavaRDD<Integer> line7 = line2.keys();
    System.out.println("777777777777keys:");
    line7.foreach(new VoidFunction<Integer>() {
      public void call(Integer x) throws Exception {
        System.out.println(x);
      }
    });

    // values
    JavaRDD<Integer> line8 = line2.values();
    System.out.println("888888888888888values:");
    line8.foreach(new VoidFunction<Integer>() {
      public void call(Integer x) throws Exception {
        System.out.println(x);
      }
    });

    // sortByKey
    JavaPairRDD<Integer, Integer> line9 = line2.sortByKey(false);
    // JavaPairRDD<Integer, Integer> line9 = line2.sortByKey(new
    // MyComparator(), true);  MyComparator必须实现Comparator接口和Serializable接口
    System.out.println("9999999999999sortByKey:");
    line9.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });

    // filter
    JavaPairRDD<Integer, Integer> line10 = line2.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
      public Boolean call(Tuple2<Integer, Integer> x) throws Exception {
        return (x._1 > 2);
      }
    });
    System.out.println("aaaaaaaaaaaaaaaafilter:");
    line10.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
      public void call(Tuple2<Integer, Integer> x) throws Exception {
        System.out.println(x);
      }
    });


  }
}

