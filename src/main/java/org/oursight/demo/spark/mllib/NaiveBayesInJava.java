package org.oursight.demo.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Arrays;

/**
 * 训练数据中，其中第一列代表类别，训练数据中有三种类别：0、1、2。第2-4列代表数据的三个维度，可以想象成前文中性别分类算法中的头发长度、服饰和体型这三个要素。
 * Created by yaonengjun on 2017/8/20 下午4:05.
 */
public class NaiveBayesInJava {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> data = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/naive_bayes.txt");
    // 训练数据中，其中第一列代表类别，训练数据中有三种类别：0、1、2。第2-4列代表数据的三个维度，可以想象成前文中性别分类算法中的头发长度、服饰和体型这三个要素。

    RDD<LabeledPoint> parsedData = data.map(line -> {
      String[] parts = line.split(",");
      double[] values = Arrays.stream(parts[1].split(" "))
              .mapToDouble(Double::parseDouble)
              .toArray();
      //LabeledPoint代表一条训练数据，即打过标签的数据
      return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
    }).rdd();

    //分隔为两个部分，60%的数据用于训练，40%的用于测试
    RDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].toJavaRDD();
    JavaRDD<LabeledPoint> test = splits[1].toJavaRDD();

    //训练模型， Additive smoothing的值为1.0（默认值）
    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

    JavaRDD<Double> prediction = test.map(p -> model.predict(p.features()));
    JavaPairRDD<Double, Double> predictionAndLabel = prediction.zip(test.map(LabeledPoint::label));
    //用测试数据来验证模型的精度
    double accuracy = 1.0 * predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / test.count();
    System.out.println("计算精度=" + accuracy);

    //预测类别
    System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[]{0.5, 3.0, 0.5})));
    System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{1.5, 0.4, 0.6})));
    System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[]{0.3, 0.4, 2.6})));
  }
}
