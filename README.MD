hudi官方文档：https://hudi.apache.org/docs/quick-start-guide

使用前提    
1、spark2.4+,在cdh集成的spark2中存在parquet和avro本版过低问题。本实例在`SPARK2_ON_YARN-2.4.0.cloudera1.jar`的CDH环境中需要下载发行版的spark将以下jar替换。暂未验证会不会影响其他功能的使用。
```
mkdir /opt/cloudera/parcels/SPARK2/lib/spark2/jars_bak
mv /opt/cloudera/parcels/SPARK2/lib/spark2/jars/parquet-* /opt/cloudera/parcels/SPARK2/lib/spark2/jars_bak/
mv /opt/cloudera/parcels/SPARK2/lib/spark2/jars/avro-* /opt/cloudera/parcels/SPARK2/lib/spark2/jars_bak/
scp -r /root/workspace/spark/spark-2.4.5-bin-hadoop2.6/jars/avro-* root@cdhserver[1-3]:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/
scp -r /root/workspace/spark/spark-2.4.5-bin-hadoop2.6/jars/parquet-* root@cdhserver[1-3]:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/
```

2、编译对应版本的spark-hudi进行源码编译
···
git clone https://github.com/apache/incubator-hudi.git && cd incubator-hudi
mvn clean package -DskipTests -DskipITs
···

3、提交运行spark job
···
spark2-submit --master yarn --class com.landy.spark.hudi.QuitStartDemo hdfs:///workspace/spark/hudi/spark-hudi-example-1.0-SNAPSHOT.jar --executor-memory 1G --num-executors 1 --jars hdfs:////workspace/spark/hudi/jars/hudi-spark-bundle_2.11-0.5.2-incubating.jar 

···