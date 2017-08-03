package org.oursight.demo.spark.streaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.oursight.demo.spark.util.Utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import scala.tools.cmd.gen.AnyVals;

/**
 * Created by yaonengjun on 2017/8/3 下午6:30.
 */
public class CustomReceiver extends Receiver<String> {

  private boolean threadDone = false;


  public CustomReceiver() {
    super(StorageLevel.MEMORY_AND_DISK_2());
  }

  @Override
  public void onStart() {
   Thread t = new Thread(new Producer());
   t.start();
  }

  @Override
  public void onStop() {
    threadDone = true;
  }

   class Producer implements Runnable {

    @Override
    public void run() {
      while (!threadDone) {

        String s = Utils.random(3);
        store(s);
        System.out.println(new Date() + " Producer: "+ s + " stored");
        System.out.println();

        try {
          Thread.sleep(2000L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
