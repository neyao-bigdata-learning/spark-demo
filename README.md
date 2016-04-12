Overview
--------
This project shows the usage of spark java code.

Usage
-----
**1. How to build**
Using maven, i.g. mvn package

**2. How to submit to spark**
Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit --class "org.oursight.demo.spark.HelloSpark" --master local ../mine-job/spark-demo-1.0.0.jar

- master parameter:
--MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.


Reference
---------
- Quick start docs:  
-- Official quick start doc: https://spark.apache.org/docs/latest/quick-start.html
-- A Chinese version quick start doc:http://colobu.com/2014/12/08/spark-quick-start/