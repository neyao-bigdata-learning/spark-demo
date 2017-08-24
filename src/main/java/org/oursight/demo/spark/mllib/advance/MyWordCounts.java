package org.oursight.demo.spark.mllib.advance;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.oursight.demo.spark.util.ChineseUtil;
import org.oursight.demo.spark.util.FileUtil;
import org.oursight.demo.spark.util.MapUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yaonengjun on 2017/8/22 下午3:45.
 */
public class MyWordCounts {

  public static void main(String[] args) throws IOException {
    segmentByFile2();
//    segmentByFile();

//    String line = "(5900.cc),国际品牌,值得信赖! 百万玩家 刺激好玩 手机平台 天天6合 90秒一期 十分一期 存取便捷 亚洲领先 (5900.cc)每天20点免费抢50万红包,资百亿巨资打造游戏平苔! " +
//            "勿须等待";
//    segmentOneLine(line);
  }

  public static void segmentOneLine(String oneLine) throws IOException {

    List<String> lines = new ArrayList<>();
    lines.add(oneLine);

    Map<String, Integer> wordsFrequncy = new HashMap<>();
//    List<String> allWords = new ArrayList<>();
    for (String line : lines) {
      List<Term> termList = NLPTokenizer.segment(line);
      for (Term term : termList) {
        if (!ChineseUtil.isChinese(term.word))
          continue;

        if (term.word.length() == 1)
          continue;

//        allWords.add(term.word);
        Integer count = wordsFrequncy.get(term.word);
        if (count == null) {
          wordsFrequncy.put(term.word, 1);
        } else {
          wordsFrequncy.put(term.word, count + 1);
        }
      }
    }
    Map<String, Integer> wordsFrequncySorted = MapUtil.sortByComparator(wordsFrequncy, true);
    MapUtil.printMap(wordsFrequncySorted);
    System.out.println(wordsFrequncySorted.size());

    StringBuffer line = new StringBuffer();
    for (String s : wordsFrequncySorted.keySet()) {
      line.append(s).append(" ");

    }
    System.out.println(line.toString());
  }


  public static void segmentByFile() throws IOException {
    List<String> lines = FileUtils.readLines(new File
            ("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_sample_lines.txt"));


    Map<String, Integer> wordsFrequncy = new HashMap<>();
    for (String line : lines) {
      List<Term> termList = NLPTokenizer.segment(line);
      for (Term term : termList) {
        if (!ChineseUtil.isChinese(term.word))
          continue;

//        if (term.word.length() == 1)
//          continue;

        Integer count = wordsFrequncy.get(term.word);
        if (count == null) {
          wordsFrequncy.put(term.word, 1);
        } else {
          wordsFrequncy.put(term.word, count + 1);
        }
      }
    }
//    Map<String, Integer> wordsFrequncySorted = MapUtil.sortByComparator(wordsFrequncy, false);
    Map<String, Integer> wordsFrequncySorted = MapUtil.sortByComparator(wordsFrequncy, true);
    MapUtil.printMap(wordsFrequncySorted);
    System.out.println(wordsFrequncySorted.size());

    String filePath = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_words_positive.txt";
    FileUtils.forceDelete(new File(filePath));

    for (String s : wordsFrequncySorted.keySet()) {
      StringBuffer line = new StringBuffer();
      line.append(s).append(" ");
      FileUtil.appendFile(filePath, line.toString());
    }
//    System.out.println(line.toString());

  }

  public static void segmentByFile2() throws IOException {
    List<String> lines = FileUtils.readLines(new File
            ("/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_sample_lines.txt"));


    String filePath = "/Users/neyao/workspace/mine/spark-demo/src/main/resources/samples/gambling_words_positive.txt";
    FileUtils.forceDelete(new File(filePath));

    for (String line : lines) {
      List<Term> termList = NLPTokenizer.segment(line);
      StringBuffer str = new StringBuffer();
      for (Term term : termList) {
        if (!ChineseUtil.isChinese(term.word))
          continue;

        str.append(term.word).append(" ");

      }
      FileUtil.appendFile(filePath, str.toString());
    }


    System.out.println("==== done ====");

  }

}
