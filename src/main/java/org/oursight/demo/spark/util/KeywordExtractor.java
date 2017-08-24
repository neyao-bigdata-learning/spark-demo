package org.oursight.demo.spark.util;

import com.hankcs.hanlp.HanLP;

import java.util.List;

/**
 */
public class KeywordExtractor {

  public static List<String> apply(String content, int size) {
    List<String> keywordList = HanLP.extractKeyword(content, size);
    return keywordList;
  }
}
