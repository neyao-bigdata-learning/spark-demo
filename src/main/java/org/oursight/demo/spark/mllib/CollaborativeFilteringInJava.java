package org.oursight.demo.spark.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * 注意：本类的几个关键点：
 * 1）类本身必须是实现 Serializable的
 * 2）numIterations大于5时，要设置-Xss10m，否则运行时会StackOverFlow， see：http://f.dataguru.cn/spark-368353-1-1.html
 *
 * 测试数据集中，其中第一列为userid，第二列为movieid，第三列为评分，第四列为timestamp（未使用）。
 * Created by yaonengjun on 2017/8/20 下午5:33.
 */
public class CollaborativeFilteringInJava implements Serializable {

  private static final Pattern SPACE_PATTERN = Pattern.compile(" ");

  public MatrixFactorizationModel buildModel(RDD<Rating> rdd) { //训练模型
    int rank = 10;
    int numIterations = 20;
//    ALS.checkpointInterval = 2;
    MatrixFactorizationModel model = ALS.train(rdd, rank, numIterations, 0.01);

    return model;
  }

  public RDD<Rating>[] splitData() { //分割数据，一部分用于训练，一部分用于测试
    SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = sc.textFile("/Users/neyao/workspace/mine/spark-demo/src/main/resources" +
            "/collaborative_filtering_100.txt");
    JavaRDD<Rating> ratings = lines.map(line -> {
      String[] tok = SPACE_PATTERN.split(line);
      int x = Integer.parseInt(tok[0]);
      int y = Integer.parseInt(tok[1]);
      double rating = Double.parseDouble(tok[2]);
      return new Rating(x, y, rating);
    });
    RDD<Rating>[] splits = ratings.rdd().randomSplit(new double[]{0.6,0.4}, 11L);
    return splits;
  }

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);

    CollaborativeFilteringInJava cf = new CollaborativeFilteringInJava();
    RDD<Rating>[] splits = cf.splitData();
    MatrixFactorizationModel model = cf.buildModel(splits[0]);

    System.out.println("------  训练数据集 ------");
    Double MSE = cf.getMSE(splits[0].toJavaRDD(), model);
    System.out.println("Mean Squared Error = " + MSE); //训练数据的MSE
    System.out.println("------------------------");
    System.out.println();

    System.out.println("------  目标数据集 ------");
    Double MSE1 = cf.getMSE(splits[1].toJavaRDD(), model);
    System.out.println("Mean Squared Error1 = " + MSE1); //测试数据的MSE
    System.out.println("------------------------");
    System.out.println();
  }

  public Double getMSE(JavaRDD<Rating> ratings, MatrixFactorizationModel model) { //计算MSE
    JavaPairRDD usersProducts = ratings.mapToPair(rating -> new Tuple2<>(rating.user(), rating.product()));
    JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(usersProducts.rdd())
            .toJavaRDD()
            .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
              @Override
              public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
              }
            });

    JavaPairRDD<Tuple2<Integer, Integer>, Double> ratesAndPreds = ratings
            .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
              @Override
              public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                Tuple2 t1 = new Tuple2<>(rating.user(), rating.product());
                System.out.println("t1: " + t1 +"   rating: " + rating.rating());
                return new Tuple2<>(t1, rating.rating());
              }
            });
    JavaPairRDD joins = ratesAndPreds.join(predictions);

    return joins.mapToDouble(new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
      @Override
      public double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> t) throws Exception {
        double err = t._2()._1() - t._2()._2();
        return err * err;
      }
    }).mean();
  }
}
