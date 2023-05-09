from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from newlookup.config.ConfigStore import *
from newlookup.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(lookup("Report", col("id")).getField("id").alias("fromLookup"))
