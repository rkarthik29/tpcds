from urllib.parse import urlparse
from s3fs import S3FileSystem
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import time

class TpcdsRunner():
    def __init__(self, spark, data_location):
        self.spark = spark
        self.location = data_location

    def load_tables(self):
        parsed = urlparse(self.location)
        if parsed.scheme == 'file' or parsed.scheme == '':
            tables = os.listdir(parsed.path)
        elif parsed.scheme == 's3a':
            fs= S3FileSystem()
            print(parsed.path)
            tables = fs.ls(parsed.path)
        else:
            raise Exception(f'Unsupported scheme: {parsed.scheme}')
        for table in tables[1:]:
            if table=='.DS_Store' or table.strip() == '' or table==parsed.path:
                continue
            print(table)
            df = self.spark.read.parquet(f"s3a://{table}")
            df.createOrReplaceTempView(table.split('/')[-1])

    def run_query(self, queries=[]):
        if not queries:
            queries =[x.replace('.sql','') for x in os.listdir('queries/')]
        runtimes={}
        for query in queries:
            start = time.time()
            querytxt = open(f'queries/{query}.sql').read()
            result = self.spark.sql(querytxt)
            result.show()
            runtimes[query] = time.time() - start
        return runtimes

if __name__="__main__":
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'  
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    classpath="/var/lib/hadoop-hdfs/tpcds/gluten-velox-bundle-spark3.5_2.12-amzn_2023_x86_64-1.4.0-SNAPSHOT-nolic.jar:/opt/tpcds/jars/jackson-databind-2.15.2.jar:/opt/tpcds/jars/jackson-annotations-2.15.2.jar:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/hadoop-hdfs.jar:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.12.705.jar:/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.23.18.jar:/usr/lib/hadoop/lib/*"
    conf=SparkConf() \
        .setMaster("local[*]") \
        .set("spark.executor.memory", "4g")\
        .set("spark.driver.memory", "4g") \
        .set("spark.driver.extraClassPath", classpath) \
        .set("spark.executor.extraClassPath",classpath) 
    
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    data_location = os.getenv('TPCDS_DATA_LOCATION', 'data/')
    runner=TpcdsRunner(spark, data_location)
    runner.load_tables()
    runtimes = runner.run_query(queries=['q23a', 'q23b', 'q14a', 'q14b'])
    print(runtimes)