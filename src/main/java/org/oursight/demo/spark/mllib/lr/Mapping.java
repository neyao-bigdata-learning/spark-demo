package org.oursight.demo.spark.mllib.lr;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by yaonengjun on 2017/10/19 下午5:56.
 */
public class Mapping {

  public static void main(String[] args) throws IOException {




    String filePath = "/Users/neyao/temp/111.txt/part-00000-e4251e08-bc65-4b63-8ec4-a2c1912fce96.json";

    String rawContent = FileUtils.readFileToString(new File(filePath));

    String content = StringUtils.removeStart(rawContent, "{\"weights\":{\"type\":1,\"values\":[");
     content = StringUtils.remove(content, "]}}");

    String[] array = StringUtils.split(content, ",");

    System.out.println(array.length);
    for (String s : array) {
      System.out.println(s);
    }
  }
}
