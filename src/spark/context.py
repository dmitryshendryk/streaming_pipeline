import pyspark
from pyspark import SparkConf

from pyspark import SparkContext, SQLContext

path = '/Users/dmitry/Documents/metadata.json'

# You can configure the SparkContext

class AppSparkContext:
    def __init__(self) -> None:
        conf = SparkConf()
        conf.set('spark.sql.shuffle.partitions', '2100')
        conf.set("spark.executor.cores", "5")
        self.sc = SparkContext(appName='mm_exp', conf=conf).getOrCreate()
        SparkContext.setSystemProperty('spark.executor.memory', '10g')
        SparkContext.setSystemProperty('spark.driver.memory', '10g')

        self.sqlContext = pyspark.SQLContext(self.sc)

    def read_file(self, path):

        df = self.sqlContext.read.json(path)
        return df