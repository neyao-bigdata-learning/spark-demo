package org.oursight.demo.spark.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class FileUtil {

  public static void appendFile(String fileName, String line) {
    List<String> lines = new ArrayList<>();
    lines.add(line);
    appendFile(fileName, lines);
  }

  /**
   * @param fileName 文件名全路径,如果文件对应的文件名不存在会创建
   * @param lines
   * @throws IOException
   */
  public static void appendFile(String fileName, List<String> lines) {
    if (fileName == null || "".equals(fileName))
      return;

    if (lines == null || lines.size() == 0) {
      return;
    }

    File file = new File(fileName);
    BufferedWriter bw = null;
    FileWriter fw = null;
    try {
      if (file.exists() == false) {
        boolean result = file.createNewFile();
        if (result)
          file = new File(fileName);
//        LOGGER.trace("create file {}: {}", fileName, result);
      }

      fw = new FileWriter(file.getAbsoluteFile(), true);
      bw = new BufferedWriter(fw);
      for (String line : lines) {
        bw.write(line);
        bw.write("\r\n");
//        LOGGER.trace("fileName: {}, lines: {}", fileName, line);
      }

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {

        if (bw != null)
          bw.close();
        if (fw != null)
          fw.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }
}
