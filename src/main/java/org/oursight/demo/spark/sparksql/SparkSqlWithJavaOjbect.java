package org.oursight.demo.spark.sparksql;

import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.oursight.demo.spark.util.Utils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

//import org.apache.spark.sql.Row;

/**
 * Created by yaonengjun on 2017/8/1 下午11:37.
 */
public class SparkSqlWithJavaOjbect {


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

    Encoder<PersonBo> personEncoder = Encoders.bean(PersonBo.class);
    List<PersonBo> personList = new ArrayList<>();
    for (int i = 1; i <= 20; i++) {
      PersonBo p = new PersonBo();
      p.setId(i);
      p.setAge(RandomUtils.nextInt(1, 50));
      p.setName(Utils.random(2));
      p.setWeight(RandomUtils.nextDouble(0, 1));
      personList.add(p);


    }

    List<SonBo> sonList = new ArrayList<>();
    for (int i = 1; i <= 20; i++) {
      SonBo s = new SonBo();
      s.setName(Utils.random(3));
      s.setSonAge(RandomUtils.nextInt(1, 20));
      sonList.add(s);
    }


//    session.read().format().jdbc()
      Dataset<PersonBo> personDs = session.createDataset(personList, personEncoder);
      personDs.show();
      System.out.println("size :" + personDs.count());
      ;

      Encoder<SonBo> sonEncoder = Encoders.bean(SonBo.class);
      Dataset<SonBo> sonDs = session.createDataset(sonList, sonEncoder);
      sonDs.show();


      Dataset<Row> joinDs = personDs.join(sonDs, "name");
    System.out.println("join ds");
      joinDs.show();

//    Dataset<Row> outerJoinDs = personDs.jojoin(sonDs, "name");
//    System.out.println("join ds");
//    joinDs.show();

      long t2 = System.currentTimeMillis();
      System.out.println("Counting by name: ");
      Dataset<Row> countingDs = personDs.groupBy("name").count();
      long t3 = System.currentTimeMillis();
      System.out.println("time cost: " + (t3 - t2) + " ms");

      countingDs.show();


      personDs.createOrReplaceTempView("person_all");

      Dataset<Row> result = session.sql("select avg(weight) from person_all");
      result.show();


      personDs.printSchema();
      Dataset<Row> result1 = personDs.groupBy("name").max("age");
      result1.show();

      Dataset<Row> result2 = personDs.groupBy("name").sum("age");
      result2.show();


      Dataset<Row> result3 = personDs.groupBy("name").avg("age");
      result3.show();

      JavaRDD<Row> javaRDD = result3.toJavaRDD();
      List<Row> rows = javaRDD.collect();
      for (Row row : rows) {
        System.out.println("-->" + row.toString());
        System.out.println("-->" + row.get(0) + ": " + row.get(1));
        System.out.println();
      }


      Dataset<Row> result4 = result2.join(result3, "name").join(countingDs, "name");
      result4.show();

      result4.filter(col("count").gt(1)).show();


    }
}

