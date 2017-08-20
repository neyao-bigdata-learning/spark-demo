package org.oursight.demo.spark.statistics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by yaonengjun on 2017/8/20 下午7:11.
 */
public class SparkStatisticsBasic {
  private static final Pattern SPACE_PATTERN = Pattern.compile(" ");

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf().setAppName("Statistics").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> data = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/statistics.txt");
    JavaRDD<Vector> parsedData = data.map(s -> {
      double[] values = Arrays.asList(SPACE_PATTERN.split(s))
              .stream()
              .mapToDouble(Double::parseDouble)
              .toArray();
      return Vectors.dense(values);
    });

    MultivariateStatisticalSummary summary = Statistics.colStats(parsedData.rdd());
    System.out.println("均值:"+summary.mean());
    System.out.println("方差:"+summary.variance());
    System.out.println("每列非零统计量个数:"+summary.numNonzeros());
    System.out.println("总记录数（即行数）:"+summary.count());
    System.out.println("最大值:"+summary.max());  // 注意这里是指每列的最大值
    System.out.println("最小值:"+summary.min()); // 注意这里是指每列的最小值
  }
}
