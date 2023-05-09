from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from newlookup.config.ConfigStore import *
from newlookup.udfs.UDFs import *

def fooDataset(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("id", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("/tmp")
