package org.oursight.demo.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LassoModel;
import org.apache.spark.mllib.regression.LassoWithSGD;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.RidgeRegressionModel;
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD;

import java.util.Arrays;

import scala.Tuple2;

/**
 * Created by yaonengjun on 2017/8/20 下午4:25.
 */
public class LinearRegressionInJava {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    SparkConf sparkConf = new SparkConf()
            .setAppName("Regression")
            .setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> data = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources/lr_traning.txt");
    JavaRDD<LabeledPoint> parsedData = data.map(line -> {
      String[] parts = line.split(",");
      double[] ds = Arrays.stream(parts[1].split(" "))
              .mapToDouble(Double::parseDouble)
              .toArray();
      return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(ds));
    }).cache();

    int numIterations = 100; //迭代次数
    LinearRegressionModel model = LinearRegressionWithSGD.train(parsedData.rdd(), numIterations);
    RidgeRegressionModel model1 = RidgeRegressionWithSGD.train(parsedData.rdd(), numIterations);
    LassoModel model2 = LassoWithSGD.train(parsedData.rdd(), numIterations);

    System.out.println("model traning data:");
    print(parsedData, model);
    System.out.println();

    System.out.println("model1 traning data:");
    print(parsedData, model1);
    System.out.println();

    System.out.println("model2 traning data:");
    print(parsedData, model2);
    System.out.println();



    //预测一条新数据
    double[] d = new double[]{0.5, 0.6, 0.7};  // 和标签777完全一致
    Vector v = Vectors.dense(d);
    System.out.println("和标签777完全一致的数据，计算结果");
    System.out.println(model.predict(v));
    System.out.println(model1.predict(v));
    System.out.println(model2.predict(v));
    System.out.println();

    d = new double[]{0.5, 0.6, 0.8};  // 和标签777略有区别
    v = Vectors.dense(d);
    System.out.println("和标签777略有区别的数据，计算结果");
    System.out.println(model.predict(v));
    System.out.println(model1.predict(v));
    System.out.println(model2.predict(v));
    System.out.println();

    d = new double[]{0.1, 0.1, 0.1};  // 和标签777差得很远
    v = Vectors.dense(d);
    System.out.println("和标签777差得很远的数据，计算结果");
    System.out.println(model.predict(v));
    System.out.println(model1.predict(v));
    System.out.println(model2.predict(v));
    System.out.println();


  }

  public static void print(JavaRDD<LabeledPoint> parsedData, GeneralizedLinearModel model) {
    JavaPairRDD<Double, Double> valuesAndPreds = parsedData.mapToPair(point -> {
      double prediction = model.predict(point.features()); //用模型预测训练数据
      System.out.println(point.label() + ": " + prediction);
      return new Tuple2<>(point.label(), prediction);
    });

    Double MSE = valuesAndPreds.mapToDouble((Tuple2<Double, Double> t) -> Math.pow(t._1() - t._2(), 2)).mean(); //计算预测值与实际值差值的平方值的均值
    System.out.println(model.getClass().getName() + " training Mean Squared Error = " + MSE);
  }
}
