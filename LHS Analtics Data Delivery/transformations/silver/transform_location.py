from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_timestamp
from pyspark import pipelines as dp



@dp.temporary_view(
    name = "stg_silver_location",
    )

def silver_location():
    df = spark.readStream.table("stg_silver_events")
    df = df.select("LocationHashKey","Facility","Building", "Unit", "bronze_Load_Datetime")
    df = df.dropDuplicates()
    df = df.withColumn("silver_load_time", F.col("current_timestamp"))
    
    return df

dp.create_streaming_table(
    name = "lhsdataproject.silver.silver_location"
    )

dp.create_auto_cdc_flow(
  target = "lhsdataproject.silver.silver_location",
  source = "stg_silver_location",
  keys = ["LocationHashKey"],
  stored_as_scd_type = 2,
  sequence_by = "silver_load_time"
)