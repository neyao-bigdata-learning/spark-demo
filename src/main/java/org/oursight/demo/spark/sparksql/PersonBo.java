package org.oursight.demo.spark.sparksql;

import java.io.Serializable;

/**
 * Created by yaonengjun on 2017/8/4 下午2:15.
 */
public class PersonBo implements Serializable {

  private int id;

  private String name;

  private int age;

  private double weight;

  private SonBo son;

  public PersonBo() {
    this.son = new SonBo();
  }

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
    this.son.setName(name);
    this.name = name;
  }

  public int getAge() {
    return age;

  }

  public void setAge(int age) {
//    if(this.son == null) {
//      this.son = new SonBo();
//    }
    this.son.setSonAge(age/2);
    this.age = age;

  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public SonBo getSon() {
    return son;
  }

  public void setSon(SonBo son) {
    this.son = son;
  }
}
