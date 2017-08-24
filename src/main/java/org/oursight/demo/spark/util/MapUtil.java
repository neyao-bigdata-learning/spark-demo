package org.oursight.demo.spark.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by yaonengjun on 2017/8/22 下午4:28.
 */
public class MapUtil {

  public static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order) {

    List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

    // Sorting the list based on values
    Collections.sort(list, new Comparator<Entry<String, Integer>>() {
      public int compare(Entry<String, Integer> o1,
                         Entry<String, Integer> o2) {
        if (order) {
          return o1.getValue().compareTo(o2.getValue());
        } else {
          return o2.getValue().compareTo(o1.getValue());

        }
      }
    });

    // Maintaining insertion order with the help of LinkedList
    Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
    for (Entry<String, Integer> entry : list) {
      sortedMap.put(entry.getKey(), entry.getValue());
    }

    return sortedMap;
  }

  public static void printMap(Map<String, Integer> map) {
    for (Entry<String, Integer> entry : map.entrySet()) {
      System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
    }
  }
}
