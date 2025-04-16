export JAVA_HOME=/etc/alternatives/jre_17
export SPARK_HOME=/opt/spark/spark-3.5.1-bin-hadoop3
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

DP_LF_564871=dummy spark-submit --master yarn --conf spark.driver.extraClassPath=/opt/tpcds/jars/datapelago-gv-bundle-spark3.5_2.12-amzn_2023_x86_64-1.4.0-SNAPSHOT-s3-7apr.jar:/opt/tpcds/jars/jackson-databind-2.15.2.jar:/opt/tpcds/jars/jackson-annotations-2.15.2.jar:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/hadoop-hdfs.jar:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.12.705.jar:/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.23.18.jar:/usr/lib/hadoop/lib/* --conf spark.executor.extraClassPath=/opt/tpcds/jars/datapelago-gv-bundle-spark3.5_2.12-amzn_2023_x86_64-1.4.0-SNAPSHOT-s3-7apr.jar:/opt/tpcds/jars/jackson-databind-2.15.2.jar:/opt/tpcds/jars/jackson-annotations-2.15.2.jar:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/hadoop-hdfs.jar:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.12.705.jar:/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.23.18.jar:/usr/lib/hadoop/lib/* --conf spark.driver.log.level=ERROR  --conf spark.executor.log.level=WARN --conf spark.plugins=org.apache.gluten.GlutenPlugin --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=4g --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager --conf spark.gluten.dp.enabled=true --conf spark.gluten.dp.subid=test --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"  --conf spark.executorEnv.DP_LF_564871=dummy tpcdsdplggdriver.py