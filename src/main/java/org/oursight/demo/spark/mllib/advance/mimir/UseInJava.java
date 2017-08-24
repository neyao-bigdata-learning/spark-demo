package org.oursight.demo.spark.mllib.advance.mimir;


import org.apache.commons.io.FileUtils;
import org.oursight.demo.spark.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by yaonengjun on 2017/8/24 下午4:34.
 */
public class UseInJava {


  private static final String MODEL_PATH = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/model/gambling" +
          "/lrModel";

  public static void main(String[] args) throws Exception {
//    train();
    predict("/Users/neyao/temp/test_004.txt");
  }

  public static void train() throws Exception {
    FileUtils.deleteDirectory(new File(MODEL_PATH));
    UseLRClassifyModel model = new UseLRClassifyModel();
    model.loadTrainData();
    model.buildModel();

    model.testModel();
  }

  public static void predict(String resultFilePath) throws Exception {
    UseLRClassifyModel model = new UseLRClassifyModel();
    model.loadTrainData();
    model.loadModel();


//    String line = "(D6.CC)国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖国际品牌, (D6.CC)国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖,(D6.CC)国际品牌," +
//            "值得信赖国际品牌,值得信赖国际品{}a{}a{}a{}";
//    System.out.println(model.predictModel(line));
    List<String> lines = FileUtils.readLines(new File("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_sample_lines.txt"));
    for (String onLineInFile : lines) {
//      /Users/neyao/Desktop
      double result = model.predictModel(onLineInFile);
      FileUtil.appendFile(resultFilePath, result + "    " + onLineInFile);
    }

    System.out.println("====== done ======");
  }
}
