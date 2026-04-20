
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

def merge_silver_to_gold():
    tgt = DeltaTable.forName(spark, "lhsdataproject.gold.DimEventType")
    src = spark.table("lhsdataproject.silver.silver_eventtype")  # AutoCDC SCD2

    tgt.alias("tgt").merge(
        src.alias("src"),
        "tgt.EventTypeHashKey = src.EventTypeHashKey AND src.__START_AT = tgt.EffStartDatetime"
    ).whenMatchedUpdate(
        condition="tgt.EffEndDatetime <> src.__END_AT",
        set={
            "EffEndDatetime": col("src.__END_AT")
        }
    ).whenNotMatchedInsert(
        values={
            "EventTypeHashKey": col("src.EventTypeHashKey"),
            "EventType": col("src.EventType"),
            "EffStartDatetime": col("src.__START_AT"),
            "EffEndDatetime": col("src.__END_AT"),
            "LoadDatetime": current_timestamp()
  
        }
    ).execute()

merge_silver_to_gold()


