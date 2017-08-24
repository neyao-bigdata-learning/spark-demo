package org.oursight.demo.spark.util;

/**
 * Created by yaonengjun on 2017/8/22 下午4:33.
 */
public class ChineseUtil {

  // GENERAL_PUNCTUATION 判断中文的“号
  // CJK_SYMBOLS_AND_PUNCTUATION 判断中文的。号
  // HALFWIDTH_AND_FULLWIDTH_FORMS 判断中文的，号
  private static final boolean isChinese(char c) {
    Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
    if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
            || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
            || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
            || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
            || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
            || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
      return true;
    }
    return false;
  }

  public static final boolean isChinese(String strName) {
    char[] ch = strName.toCharArray();
    for (int i = 0; i < ch.length; i++) {
      char c = ch[i];
      if (isChinese(c)) {
        return true;
      }
    }
    return false;
  }


}
