

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="lhsdataproject")
args, unknown = parser.parse_known_args()
catalog = args.catalog


def merge_silver_to_gold():

    tgt = DeltaTable.forName(spark, f"{catalog}.gold.dimorganism")
    src = spark.table( "lhsdataproject.silver.silver_organism")  # AutoCDC SCD2

    tgt.alias("tgt").merge(
        src.alias("src"),
        "tgt.OrganismHashKey = src.OrganismHashKey AND src.__START_AT = tgt.EffStartDatetime"
    ).whenMatchedUpdate(
        condition="tgt.EffEndDatetime <> src.__END_AT",
        set={
            "EffEndDatetime": col("src.__END_AT")
        }
    ).whenNotMatchedInsert(
        values={
            
            "OrganismHashKey": "src.OrganismHashKey",
            "Species": "src.Species",
            "Organism": "src.Organism",
            "LoadDatetime": current_timestamp(),
            "EffStartDatetime": col("src.__START_AT"),
            "EffEndDatetime": col("src.__END_AT")
        }
    ).execute()

merge_silver_to_gold()
