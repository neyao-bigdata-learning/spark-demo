package org.oursight.demo.spark.sparksql;

import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

    Encoder<Person> personEncoder = Encoders.bean(Person.class);
    List<Person> personList = new ArrayList<>();
    for (int i = 1; i <= 20; i++) {
      Person p = new Person();
      p.setId(i);
      p.setAge(RandomUtils.nextInt());
      p.setName(Utils.random(3));
      p.setWeight(RandomUtils.nextDouble());
      personList.add(p);
    }
    Dataset<Person> personDs = session.createDataset(personList, personEncoder);
    personDs.show();
    System.out.println("size :" + personDs.count());;


    long t2 = System.currentTimeMillis();
    System.out.println("Counting by name: ");
    Dataset<Row> countingDs = personDs.groupBy("name").count();
    long t3 = System.currentTimeMillis();
    System.out.println("time cost: " + (t3-t2) +" ms");
    countingDs.show();


    personDs.createOrReplaceTempView("person_all");

    Dataset<Row> result = session.sql("select avg(weight) from person_all");
    result.show();

//    Dataset<Row> result1 = session.sql(
//            "select id, name,  from person_all"
//    );





  }
}

