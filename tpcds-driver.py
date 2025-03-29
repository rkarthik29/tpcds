
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import time

class TpcdsRunner():
    def __init__(self, spark, data_location):
        self.spark = spark
        self.location = data_location

    def load_tables(self):
        for table in os.listdir(self.location):
            if table=='.DS_Store':
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
    

conf=SparkConf() \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "4g")\
    .set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
data_location = '/Users/karthiknarayanan/Downloads/tpcds_10'
runner=TpcdsRunner(spark, data_location)
runner.load_tables()
runtimes = runner.run_query(queries=['q23a', 'q23b', 'q14a', 'q14b'])
print(runtimes)