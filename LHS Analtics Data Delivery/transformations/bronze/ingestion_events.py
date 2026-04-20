
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_events"
)
def bronze_patients():
    df =( 
        spark.readStream
            .format("cloudfiles")
            .option("cloudFiles.format", "csv")
            .option("pathGlobFilter", "events.csv")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.maxFilesPerTrigger", 5)
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/lhsdataproject/lhs/landing/events/")  
        )
    df = df.withColumn("Load_Datetime", F.current_timestamp())\
                   .withColumn("file_path",F.col("_metadata.file_path"))
    return df
    
