package org.oursight.demo.spark.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by neyao@github.com on 2016/4/12.
 */
public class HelloSpark {

  public static void main(String[] args) {

//    showBasicUsage();
//        wordCount();
    wordCountingWithKey();
  }

  public static void showBasicUsage() {
    String testFile = "/Users/neyao/dev/spark/data/demo/words.txt";

    long t1 = System.currentTimeMillis();
    SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
    sparkConf.setMaster("local");
    //sparkConf.setAppName("MyHelloSparkApp");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);


    JavaRDD<String> data = sc.textFile(testFile).cache();


    long countA = data.filter(new Function<String, Boolean>() {
      public Boolean call(String s) throws Exception {
//                System.out.println("s in countA = [" + s + "]");
        return s.contains("lsi");

      }
    }).count();

    long countB = data.filter(new Function<String, Boolean>() {
      public Boolean call(String s) throws Exception {
//                System.out.println("s in countB = [" + s + "]");
        return s.contains("zhangsan");
      }
    }).count();
    long t2 = System.currentTimeMillis();

    System.out.println();
    System.out.println();
    System.out.println("========================= RESULT=========================");
    System.out.println("Lines with yao: " + countA + ", lines with zhangsan: " + countB);
    System.out.println("time cost: " + (t2 - t1) + " ms");
    System.out.println("=========================================================");
    System.out.println();
    System.out.println();

  }

  public static void wordCountingWithKey() {
    String testFile = "/Users/neyao/dev/spark/data/demo/words.txt";

    long t1 = System.currentTimeMillis();
    SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
    sparkConf.setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<String> data = sc.textFile(testFile);

    JavaRDD<String> words = data.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        String[] strings = s.split(" ");
        List<String>  result = Arrays.asList(strings);
        return result.iterator();
      }
    });

    System.out.println("================== count ==================");
    System.out.println("words.toDebugString() -----> " + words.toDebugString());
    System.out.println();
    System.out.println("words.toString()-----> " + words.toString());
    System.out.println("words.toDebugString()-----> " + words.toDebugString());
//    System.out.println("words.toArray()-----> " + words.toArray());
    System.out.println("============================================");

    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        System.out.println("s in Tuple2 = [" + s + "]");
        return new Tuple2<String, Integer>(s, 1);
      }
    });


    System.out.println("================== pairs ==================");
    System.out.println("pairs.toDebugString() -----> " + pairs.toDebugString());
    System.out.println();
    System.out.println("pairs.toString()-----> " + pairs.toString());
//    System.out.println("pairs.toArray()-----> " + pairs.toArray());
    System.out.println("============================================");


    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer a, Integer b) {
        System.out.println("counts, a:" + a + "; b:" + b);
        return a + b;
      }

    });
//    counts.max()

//    counts.max()


    long t2 = System.currentTimeMillis();

    System.out.println();
    System.out.println();
    System.out.println("========================= RESULT =========================");
    counts.saveAsTextFile("/home/neyao/spark/data/words.txt");
    System.out.println("time cost: " + (t2 - t1) + " ms");
    System.out.println("=========================================================");
    System.out.println();
    System.out.println();


  }

//    public static void testScala() {
//
//        pString("1232");
//
//        def pString(s:String) {
//            println("InnerClass.pString: " + s)
//        }
//    }

  /**
   * A simple method that shows the usage of java lambda expression
   */
  public static void testLambda() {
    String[] atp = {"Rafael Nadal", "Novak Djokovic",
            "Stanislas Wawrinka",
            "David Ferrer", "Roger Federer",
            "Andy Murray", "Tomas Berdych",
            "Juan Martin Del Potro"};
    List<String> players = Arrays.asList(atp);

    players.forEach((player) -> System.out.println(player + ";"));
  }


}
