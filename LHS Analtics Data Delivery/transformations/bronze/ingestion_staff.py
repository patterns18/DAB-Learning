



from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_staff"
)
def bronze_staff():
    df =( 
        spark.readStream
            .format("cloudfiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "staff.json")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.maxFilesPerTrigger", 5)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/lhsdataproject/lhs/landing/staff/")  
        )
    df = df.withColumn("Load_Datetime", F.current_timestamp())\
                   .withColumn("file_path",F.col("_metadata.file_path"))
    return df
    
