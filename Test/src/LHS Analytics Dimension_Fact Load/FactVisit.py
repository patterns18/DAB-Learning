
from pyspark.sql.functions import col, current_timestamp, date_format, to_timestamp, to_date
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="lhsdataproject")
args, unknown = parser.parse_known_args()
catalog = args.catalog


def merge_silver_to_gold():

    df_dimlocation = spark.read.table( f"{catalog}.gold.DimLocation").filter(col("EffEndDatetime").isNull())
    df_dimeventtype = spark.read.table( f"{catalog}.gold.DimEventType").filter(col("EffEndDatetime").isNull())
    df_dimprocedure = spark.read.table( f"{catalog}.gold.DimProcedure").filter(col("EffEndDatetime").isNull())
    df_dimorganism  = spark.read.table( f"{catalog}.gold.DimOrganism").filter(col("EffEndDatetime").isNull())
    df_patient = spark.read.table( f"{catalog}.gold.DimPatients").filter(col("EffEndDatetime").isNull())
    df_staff = spark.read.table( f"{catalog}.gold.DimStaff").filter(col("EffEndDatetime").isNull())

    fact_event = spark.read.table("lhsdataproject.silver.silver_events")

    fact_event =  (fact_event.alias("fact")
                    .join(df_dimlocation.alias("loc"), 
                        how= "left", on=col("fact.LocationHashKey") == col("loc.LocationHashKey") )
                    .join(df_dimeventtype.alias("eventt"), 
                        how= "left",   on=col("fact.EventTypeHashKey") == col("eventt.EventTypeHashKey") )
                    .join(df_dimprocedure.alias("prod"), 
                        how= "left",   on=col("fact.ProcedureHashKey") == col("prod.ProcedureHashKey") )
                    .join(df_dimorganism.alias("orgn"), 
                        how= "left",   on=col("fact.OrganismHashKey") == col("orgn.OrganismHashKey") )
                    .join(df_staff.alias("staff"), 
                        how= "left",   on=col("fact.EventStaff") == col("staff.StaffID") )
                    .join(df_patient.alias("pat"), 
                        how= "left",   on=col("fact.PIN") == col("pat.PIN") )
                    .withColumn("BusinessDate", current_timestamp().cast("date"))     
                    .withColumn("ts", to_timestamp("EventTime", "H:mm")) 
                    .withColumn("DimEventTimeID", date_format("ts", "HHmm").cast("bigint")) 
                    .withColumn("FullTime", date_format("ts", "HH:mm:ss"))    
                    .withColumn("DimEventDateID", date_format(to_date("EventDate"),"yyyyMMdd").cast("bigint"))
                    .select( 
                            "BusinessDate",
                            col("pat.DimPatientID").alias("DimPatientID"),
                            "DimEventDateID",
                            "DimEventTimeID",
                            col("eventt.DimEventTypeID").alias("DimEventTypeID"),
                            col("loc.DimLocationID").alias("DimLocationID"),
                            col("prod.DimProcedureID").alias("DimProcedureID"),
                            col("orgn.DimOrganismID").alias("DimOrganismID"),
                            col("staff.DimStaffID").alias("DimStaffID"),
                            col("fact.EventNote").alias("EventNote"),
                            col("fact.TestResult").alias("TestResult"),
                            current_timestamp().alias("LoadDatetime"),
                            current_timestamp().alias("EffStartDatetime"),
                            current_timestamp().alias("EffEndDatetime")
                            )
                )
    return fact_event

fact_event = merge_silver_to_gold()


fact_event.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.gold.FactEvent")
