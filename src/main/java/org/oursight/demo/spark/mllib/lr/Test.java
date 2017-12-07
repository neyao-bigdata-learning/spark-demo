package org.oursight.demo.spark.mllib.lr;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;

import java.util.List;

/**
 * Created by yaonengjun on 2017/10/19 上午11:47.
 */
public class Test {

  public static void main(String[] args) {

    String line = "任性付,,3分钟取v现,24小时服务 任性付快速秒回款,操作方便简单快捷,热线:15502063330(微号)";
    List<Term> termList = NLPTokenizer.segment(line);

    for (Term term : termList) {
      System.out.print(term.word +"  ");
    }
  }
}
