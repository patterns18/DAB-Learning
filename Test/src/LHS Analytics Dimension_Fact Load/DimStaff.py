
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="lhsdataproject")
args, unknown = parser.parse_known_args()
catalog = args.catalog


def merge_silver_to_gold():
    tgt = DeltaTable.forName(spark, f"{catalog}.gold.DimStaff")
    src = spark.table( "lhsdataproject.silver.silver_staff")  # AutoCDC SCD2

    tgt.alias("tgt").merge(
        src.alias("src"),
        "tgt.StaffID = src.StaffID AND src.__START_AT = tgt.EffStartDatetime"
    ).whenMatchedUpdate(
        condition="tgt.EffEndDatetime <> src.__END_AT",
        set={
            "EffEndDatetime": col("src.__END_AT")
        }
    ).whenNotMatchedInsert(
        values={
            "StaffID": col("src.StaffID"),
            "FirstName": col("src.FirstName"),
            "LastName": col("src.LastName"),
            "Email": col("src.Email"),
            "PhoneExt": col("src.PhoneExt"),
            "Department": col("src.Department"),
            "HireDate": col("src.HireDate"),
            "StaffType": col("src.StaffType"),
            "IsActive": col("src.IsActive"),
            "Shift": col("src.Shift"),
            "Specialty": col("src.Specialty"),
            "EffStartDatetime": col("src.__START_AT"),
            "EffEndDatetime": col("src.__END_AT"),
            "LoadDatetime": current_timestamp()
  
        }
    ).execute()

merge_silver_to_gold()