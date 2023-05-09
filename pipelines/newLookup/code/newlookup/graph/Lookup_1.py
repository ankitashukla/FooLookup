from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from newlookup.config.ConfigStore import *
from newlookup.udfs.UDFs import *

def Lookup_1(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''id''']
    valueColumns = ['''id''']
    createLookup("Report", in0, spark, keyColumns, valueColumns)
