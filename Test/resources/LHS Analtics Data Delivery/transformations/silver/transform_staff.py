
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.temporary_view(
    name= "stg_silver_staff"
)
@dp.expect_or_drop("valid_Staff_ID", "StaffID IS NOT NULL")
def stg_silver_patients():
    df = (
        spark.readStream.table("lhsdataproject.bronze.bronze_staff")
    )

    df = (df.withColumn("silver_load_time", F.col("current_timestamp"))
           .dropDuplicates()
           .select("StaffID", "FirstName", "LastName", "Email", "PhoneExt", "Department", "HireDate",
                   "StaffType","IsActive", "Shift", "Specialty","_rescued_data", "Load_Datetime", "silver_load_time","file_path")
           .drop("_rescued_data")
    )
    return df


dp.create_streaming_table(
  name = "lhsdataproject.silver.silver_staff",
  comment = "Cleaned Staff Records"
)

dp.create_auto_cdc_flow(
  target = "lhsdataproject.silver.silver_staff",
  source = "stg_silver_staff",
  keys = ["StaffID"],
  sequence_by = "silver_load_time",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
)