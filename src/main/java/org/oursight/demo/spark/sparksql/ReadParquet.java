package org.oursight.demo.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

//import org.apache.spark.sql.Row;

/**
 * Created by yaonengjun on 2017/8/1 下午11:37.
 */
public class ReadParquet {


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

    Dataset<Row> dataset = sqlContext.read().parquet
            ("/Users/neyao/workspace/mine/spark-demo/src/main/resources/model/gambling/lrModel/data/part-00000-5e53cb5f-7e18-4f32-b09c-3367a5ce764c.snappy.parquet");

    dataset.show();
    dataset.select("weights").write().json("/Users/neyao/1111.txt");
  }
}

