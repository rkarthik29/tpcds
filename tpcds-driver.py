
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
            tables = fs.ls(parsed.path)
        else:
            raise Exception(f'Unsupported scheme: {parsed.scheme}')
        for table in os.listdir(self.location):
            if table=='.DS_Store' or table.strip() == '':
                continue
            df = self.spark.read.parquet(f'{self.location}/' + table)
            df.createOrReplaceTempView(table)

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

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'  
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'  

conf=SparkConf() \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "4g")\
    .set("spark.driver.memory", "4g") \
    .set("spark.driver.extraClassPath", "/usr/lib/hadoop/*") \
    .set("spark.executor.extraClassPath", "/usr/lib/hadoop/*") 

sc = SparkContext(conf=conf)
spark = SparkSession(sc)
data_location = os.getenv('TPCDS_DATA_LOCATION', 'data/')
runner=TpcdsRunner(spark, data_location)
runner.load_tables()
runtimes = runner.run_query(queries=['q23a', 'q23b', 'q14a', 'q14b'])
print(runtimes)