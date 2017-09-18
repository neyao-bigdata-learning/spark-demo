package org.oursight.demo.spark.mllib.advance.mimir;


import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;

import org.apache.commons.io.FileUtils;
import org.oursight.demo.spark.mllib.advance.LrClassifyModelInChinese;
import org.oursight.demo.spark.util.ChineseUtil;
import org.oursight.demo.spark.util.FileUtil;

import java.io.File;
import java.util.List;

/**
 * Created by yaonengjun on 2017/8/24 下午4:34.
 */
public class UseInJava {


  private static final String MODEL_PATH = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/model/gambling" +
          "/lrModel";

  public static void main(String[] args) throws Exception {
    train();
//    predict("/Users/neyao/temp/test_010.txt");
  }

  public static void train() throws Exception {
    FileUtils.deleteDirectory(new File(MODEL_PATH));
    LrClassifyModelInChinese model = new LrClassifyModelInChinese();
    model.loadTrainData();
    model.buildModel();

    model.testModel();
  }

  public static void predict(String resultFilePath) throws Exception {
    LrClassifyModelInChinese model = new LrClassifyModelInChinese();
    model.loadTrainData();
    model.loadModel();


//    String line = "(D6.CC)国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖国际品牌, (D6.CC)国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖国际品牌,值得信赖,(D6.CC)国际品牌," +
//            "值得信赖国际品牌,值得信赖国际品{}a{}a{}a{}";
//    List<Term> termList = NLPTokenizer.segment(line);
//    StringBuffer str = new StringBuffer();
//    for (Term term : termList) {
//      if (!ChineseUtil.isChinese(term.word))
//        continue;
//
//      str.append(term.word).append(" ");
//
//    }
//
//    System.out.println(model.predictModel(str.toString()));


    List<String> lines = FileUtils.readLines(new File("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_sample_lines.txt"));
    for (String onLineInFile : lines) {
//      /Users/neyao/Desktop
      List<Term> termList = NLPTokenizer.segment(onLineInFile);
      StringBuffer str = new StringBuffer();
      for (Term term : termList) {
        if (!ChineseUtil.isChinese(term.word))
          continue;

        str.append(term.word).append(" ");

      }

//      System.out.println(model.predictModel(str.toString()));
      double result = model.predictModel(str.toString());
      FileUtil.appendFile(resultFilePath, result + "    " + onLineInFile);
    }

    System.out.println("====== done ======");
  }
}
