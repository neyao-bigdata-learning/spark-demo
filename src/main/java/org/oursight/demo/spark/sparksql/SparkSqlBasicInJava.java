package org.oursight.demo.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by yaonengjun on 2017/8/1 下午11:37.
 */
public class SparkSqlBasicInJava {


  public static void main(String[] args) {
    // 手工将日志关闭,
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
    sparkConf.setMaster("local");
    sparkConf.setAppName("MyHelloSparkApp");
//
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(sc);

    SparkSession session = SparkSession
            .builder()
            .appName("SparkSQL Example in Java")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();

    System.out.println(session);

//    Dataset<Row> df = session.read().csv("person.csv");
    Dataset<Row> df = session.read().csv("/Users/neyao/workspace/mine/spark-demo/src/main/resources/person.csv");
    df.show();
    df.printSchema();


//    df.withColumnRenamed("_c0", "name");
//    df.withColumnRenamed("_c1", "age");
//    df.withColumnRenamed("_c2", "weight");

    String[] names =  {"name", "age", "weight"};
    df = df.toDF(names);

    System.out.println("After renamed column");
    df.show();
    df.printSchema();

    df.select("age").show();
    System.out.println();
    df.select("age").show(2);

    df.groupBy("age").count().show();

  }
}

