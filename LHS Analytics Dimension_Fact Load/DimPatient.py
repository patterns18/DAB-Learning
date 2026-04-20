
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

def merge_silver_to_gold():
    tgt = DeltaTable.forName(spark, "lhsdataproject.gold.dimpatients")
    src = spark.table("lhsdataproject.silver.silver_patients")  # AutoCDC SCD2

    tgt.alias("tgt").merge(
        src.alias("src"),
        "tgt.PIN = src.PIN AND src.__START_AT = tgt.EffStartDatetime"
    ).whenMatchedUpdate(
        condition="tgt.EffEndDatetime <> src.__END_AT",
        set={
            "EffEndDatetime": col("src.__END_AT")
        }
    ).whenNotMatchedInsert(
        values={
            "PIN": col("src.PIN"),
            "FirstName": col("src.FirstName"),
            "LastName": col("src.LastName"),
            "BirthDate": col("src.BirthDate"),
            "EffStartDatetime": col("src.__START_AT"),
            "EffEndDatetime": col("src.__END_AT"),
            "Sex": col("src.Sex"),
            "Address": col("src.Address"),
            "City": col("src.City"),
            "Province": col("src.Province"),
            "PostalCode": col("src.PostalCode"),
            "PhoneNumber": col("src.PhoneNumber"),
            "Email": col("src.Email"),
            "EmergencyContact": col("src.EmergencyContact"),
            "EmergencyPhone": col("src.EmergencyPhone"),
            "RegistrationDate": col("src.RegistrationDate"),
            "IsActive": col("src.IsActive"),
            "LoadDatetime": current_timestamp()
        }
    ).execute()

merge_silver_to_gold()