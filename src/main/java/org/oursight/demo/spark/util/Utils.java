package org.oursight.demo.spark.util;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Created by yaonengjun on 2017/8/3 下午4:07.
 */
public class Utils {

  private static final char[] chars = new char[]{'a','b','c','d'};

  public static String random(int length) {
    return RandomStringUtils.random(3, chars);
  }
}
