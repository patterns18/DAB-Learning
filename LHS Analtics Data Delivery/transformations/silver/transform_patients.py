
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.temporary_view(
    name= "stg_silver_patients"
)
@dp.expect_or_drop("valid_patient_PIN", "PIN IS NOT NULL")
def stg_silver_patients():
    df = (
        spark.readStream.table("lhsdataproject.bronze.bronze_patients")
    )

    df = (df.withColumnRenamed("BirthDtTm", "BirthDate")
           .withColumn("silver_load_time", F.col("current_timestamp"))
           .dropDuplicates()
    )
    return df


dp.create_streaming_table(
  name = "lhsdataproject.silver.silver_patients",
  comment = "Cleaned Patients Records"
)

dp.create_auto_cdc_flow(
  target = "lhsdataproject.silver.silver_patients",
  source = "stg_silver_patients",
  keys = ["PIN"],
  sequence_by = "silver_load_time",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
)