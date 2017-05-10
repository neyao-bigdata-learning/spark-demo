Overview
--------
This project shows the usage of spark java code.

运行方式
-----
**1. 打包**
Using maven, i.g. mvn package

**2. 在spark中运行**
有两种方式可以在spark中运行：
1）命令行的方式
Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit --class "org.oursight.demo.spark.HelloSpark" --master local ../mine-job/spark-demo-1.0.0.jar

其中的master参数最为关键，指spark主节点的URL。
可以有以下选择：
local 本地单线程
local[K] 本地多线程（指定K个内核）
local[*] 本地多线程（指定所有可用内核）
spark://HOST:PORT 连接到指定的 Spark standalone cluster master，需要指定端口。
mesos://HOST:PORT 连接到指定的 Mesos 集群，需要指定端口。
yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
yarn-cluster集群模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。

2）直接通过java -cp运行
假设
- SPARK_HOME=/home/neyao/spark/spark1.6.3-bin-hadoop2.6
- <SPARK_HOME>下创建了一个目录extlib放我们的jar

java -Dspark.master=local -cp <SPARK_HOME>/lib/*:<SPARK_HOME>/extlib/spark-demo-1.0.0.jar org.oursight.demo.spark.HelloSpark

也可以直接在代码中
SparkConf sparkConf = new SparkConf().setAppName("Neyao's Spark Helloworld");
sparkConf.setMaster("local");

Reference
---------
- Quick start docs:  
-- Official quick start doc: https://spark.apache.org/docs/latest/quick-start.html
-- A Chinese version quick start doc:http://colobu.com/2014/12/08/spark-quick-start/

- Spark functions:  
https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/function/package-summary.html