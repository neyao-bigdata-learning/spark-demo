package org.oursight.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * Created by neyao@github.com on 2016/4/12.
 */
public class HelloSpark {

    public static void main(String[] args) {
        String testFile = "/opt/spark/test.txt";

        SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = sparkContext.textFile(testFile).cache();

        long countA = data.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("yao");
            }
        }).count();

        long countB = data.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("zhangsan");
            }
        }).count();

        System.out.println();
        System.out.println();
        System.out.println("=================== RESULT ===================");
        System.out.println("Lines with yao: " + countA + ", lines with zhangsan: " + countB);
        System.out.println("=========================================================");
        System.out.println();
        System.out.println();

    }

    /**
     * A simple method that shows the usage of java lambda expression
     */
    public static void testLambda() {
        String[] atp = {"Rafael Nadal", "Novak Djokovic",
                "Stanislas Wawrinka",
                "David Ferrer","Roger Federer",
                "Andy Murray","Tomas Berdych",
                "Juan Martin Del Potro"};
        List<String> players =  Arrays.asList(atp);

        players.forEach((player) -> System.out.println(player + ";"));
    }
}
