package org.oursight.demo.spark.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class CosSimilarity {

  /**
   * 输入两个文本，返回余弦相似度，使用textrank分词
   *
   * @param text1
   * @param text2
   * @param size
   * @return
   */
  public static double apply(String text1, String text2, int size) {
    List<String> strings1 = KeywordExtractor.apply(text1, size);
    List<String> strings2 = KeywordExtractor.apply(text2, size);
    return avoidNaN(apply(strings1, strings2));
  }

  /**
   * 输入两个字符串数组，返回余弦相似度
   *
   * @param strings1
   * @param strings2
   * @return
   */
  public static double apply(List<String> strings1, List<String> strings2) {
    Map<String, int[]> vectorMap = new HashMap<>();
    for (String str1 : strings1) {
      if (vectorMap.containsKey(str1)) {
        vectorMap.get(str1)[0]++;
      } else {
        int[] tempArray = new int[2];
        tempArray[0] = 1;
        tempArray[1] = 0;
        vectorMap.put(str1, tempArray);
      }
    }
    for (String str2 : strings2) {
      if (vectorMap.containsKey(str2)) {
        vectorMap.get(str2)[1]++;
      } else {
        int[] tempArray = new int[2];
        tempArray[0] = 0;
        tempArray[1] = 1;
        vectorMap.put(str2, tempArray);
      }
    }
    return avoidNaN(pointMulti(vectorMap) / sqrtMulti(vectorMap));
  }

  private static double sqrtMulti(Map<String, int[]> paramMap) {
    double result;
    result = squares(paramMap);
    result = Math.sqrt(result);
    return result;
  }

  // 求平方和
  private static double squares(Map<String, int[]> paramMap) {
    double result1 = 0;
    double result2 = 0;
    Set<String> keySet = paramMap.keySet();
    for (String str : keySet) {
      int temp[] = paramMap.get(str);
      result1 += (temp[0] * temp[0]);
      result2 += (temp[1] * temp[1]);
    }
    return result1 * result2;
  }

  // 点乘法
  private static double pointMulti(Map<String, int[]> paramMap) {
    double result = 0;
    Set<String> keySet = paramMap.keySet();
    for (String str : keySet) {
      int temp[] = paramMap.get(str);
      result += (temp[0] * temp[1]);
    }
    return result;
  }

  public static void main(String[] args) {
    String s1 = "({1}手机版)-({1}手机版)-({1}手机版)-({1}手机版)加群跟上高手98%准!我们的宗旨:惠及万人,万人中!.,";
    String s2 = "{w88}-{w88},国际品牌,值得信奈., ";
    List<String> strings1 = KeywordExtractor.apply(s1, 5);
    List<String> strings2 = KeywordExtractor.apply(s2, 5);
    System.out.println(strings1);
    System.out.println(strings2);
    System.out.println(apply(s1, s2, 5));
  }

  private static double avoidNaN(double d) {
    return Double.isNaN(d) ? 0.0D : d;
  }


}
