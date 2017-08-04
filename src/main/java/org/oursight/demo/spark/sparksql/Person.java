package org.oursight.demo.spark.sparksql;

import java.io.Serializable;

/**
 * Created by yaonengjun on 2017/8/4 下午2:15.
 */
public class Person implements Serializable {

  private int id;

  private String name;

  private int age;

  private double weight;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }
}
