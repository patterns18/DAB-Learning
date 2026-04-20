
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

def merge_silver_to_gold():
    tgt = DeltaTable.forName(spark, "lhsdataproject.gold.DimLocation")
    src = spark.table("lhsdataproject.silver.silver_location")  # AutoCDC SCD2

    tgt.alias("tgt").merge(
        src.alias("src"),
        "tgt.LocationHashKey = src.LocationHashKey AND src.__START_AT = tgt.EffStartDatetime"
    ).whenMatchedUpdate(
        condition="tgt.EffEndDatetime <> src.__END_AT",
        set={
            "EffEndDatetime": col("src.__END_AT")
        }
    ).whenNotMatchedInsert(
        values={
            "LocationHashKey": col("src.LocationHashKey"),
            "Facility": col("src.Facility"),
            "Building": col("src.Building"),
            "Unit": col("src.Unit"),
            "LoadDatetime": current_timestamp(),
            "EffStartDatetime": col("src.__START_AT"),
            "EffEndDatetime": col("src.__END_AT")
        }
    ).execute()

merge_silver_to_gold()