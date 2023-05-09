from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from newlookup.config.ConfigStore import *
from newlookup.udfs.UDFs import *
from prophecy.utils import *
from newlookup.graph import *

def pipeline(spark: SparkSession) -> None:
    df_fooDataset = fooDataset(spark)
    Lookup_1(spark, df_fooDataset)
    df_Reformat_1 = Reformat_1(spark, df_fooDataset)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/newLookup")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/newLookup")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
