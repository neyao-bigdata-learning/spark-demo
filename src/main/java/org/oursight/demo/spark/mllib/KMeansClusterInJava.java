package org.oursight.demo.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * see：http://blog.selfup.cn/728.html
 * <p>
 * Created by yaonengjun on 2017/8/20 下午3:46.
 */
public class KMeansClusterInJava {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf()
            .setAppName("K-Means")
            .setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> data = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/k-means.txt");
    JavaRDD<Vector> parsedData = data.map(s -> {
      double[] values = Arrays.asList(SPACE.split(s))
              .stream()
              .mapToDouble(Double::parseDouble)
              .toArray();
      return Vectors.dense(values);
    });

    int numClusters = 3; //预测分为3个簇类
    int numIterations = 20; //迭代20次
    int runs = 10; //运行10次，选出最优解
    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, runs);

    //计算测试数据分别属于那个簇类
    print(parsedData.map(v -> v.toString()
            + " belong to cluster :" + clusters.predict(v)).collect());
    //计算cost
    double wssse = clusters.computeCost(parsedData.rdd());
    System.out.println("Within Set Sum of Squared Errors = " + wssse);

    //打印出中心点
    System.out.println("Cluster centers:");
    for (Vector center : clusters.clusterCenters()) {
      System.out.println(" " + center);
    }
    //进行一些预测
    System.out.println("Prediction of (1.1, 2.1, 3.1): "
            + clusters.predict(Vectors.dense(new double[]{1.1, 2.1, 3.1})));
    System.out.println("Prediction of (10.1, 9.1, 11.1): "
            + clusters.predict(Vectors.dense(new double[]{10.1, 9.1, 11.1})));
    System.out.println("Prediction of (21.1, 17.1, 16.1): "
            + clusters.predict(Vectors.dense(new double[]{21.1, 17.1, 16.1})));
  }

  public static <T> void print(Collection<T> c) {
    for (T t : c) {
      System.out.println(t.toString());
    }
  }
}
