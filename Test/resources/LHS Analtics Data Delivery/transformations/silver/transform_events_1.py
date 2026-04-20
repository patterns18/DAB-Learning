from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_timestamp
from pyspark import pipelines as dp


@dp.temporary_view(
    name="stg_silver_events"
)

def stg_silver_events():
    df = spark.readStream.table("lhsdataproject.bronze.bronze_events")
    df = (
            df.withColumn( "EventDateTime",F.to_timestamp(F.col("EventDtTm"), "M/d/yyyy H:mm"))
                .withColumn("bronze_Load_Datetime", F.current_timestamp())
        )
    
    df =  df.withColumn("EventDate", F.to_date("EventDateTime"))\
            .withColumn("EventTime", F.date_format("EventDateTime", "H:mm"))\
            .withColumn("Facility",  
                        expr("CASE WHEN EventType = 'Transfer' and EventNote LIKE '%/%/%' THEN trim(split(EventNote, '/')[0]) END"))\
            .withColumn("Building",
                        expr("CASE WHEN EventType = 'Transfer' and EventNote LIKE '%/%/%' THEN trim(split(EventNote, '/')[1]) END"))\
            .withColumn("Unit", 
                        expr("CASE WHEN EventType = 'Transfer' and EventNote LIKE '%/%/%' THEN trim(split(EventNote, '/')[2]) END"))\
            .withColumn("Species", 
                        expr("CASE WHEN EventType = 'Micro-Identification' and EventNote LIKE '%/%' THEN trim(split(EventNote, '/')[0]) END"))\
            .withColumn("Organism", 
                        expr("CASE WHEN EventType = 'Micro-Identification' and EventNote LIKE '%/%' THEN trim(split(EventNote, '/')[1]) END"))\
            .withColumn("TestResult",
                        expr("CASE WHEN EventType = 'Micro-Identification' and EventNote LIKE '%/%' THEN 'Positive'\
                                WHEN EventType = 'Micro-Identification' and EventNote IS NULL THEN 'Sample Clear' END"))\
            .withColumn("MicrobiologyTest", 
                        expr("CASE WHEN EventType in ('Micro-End', 'Micro-Start') THEN EventNote END"))\
            .withColumn("MedicalSpecialty",
                        expr("CASE WHEN EventType in ('Surg-End', 'Surg-Start') AND EventNote LIKE '%/%' THEN trim(split(EventNote, '/')[0]) END"))\
            .withColumn("SurgicalProcedure", 
                        expr("CASE WHEN EventType in ('Surg-End', 'Surg-Start') AND EventNote LIKE '%/%' THEN trim(split(EventNote, '/')[1]) END"))\
            .withColumn("DischargeStaff", 
                        expr("CASE WHEN EventType in ('Discharge') THEN EventStaff END"))\
            .withColumnRenamed("Visit", "VisitID")     

    df = (
            df.select("VisitID", "EventType", "EventDate", "EventTime","PIN", "EventNote", "EventStaff", "Facility", "Building", "Unit", "Species", "Organism", "TestResult",   "MicrobiologyTest", "MedicalSpecialty", "SurgicalProcedure","DischargeStaff", "_rescued_data", "bronze_Load_Datetime")
        )

    df = df.withColumn("LocationHashKey", F.sha2(F.concat_ws("||", "Facility", "Building", "Unit"), 256))
    df = df.withColumn("ProcedureHashKey", F.sha2(F.concat_ws("||", "MedicalSpecialty", "SurgicalProcedure"), 256))
    df = df.withColumn("OrganismHashKey", F.sha2(F.concat_ws("||", "Species", "Organism"), 256))
    df = df.withColumn("MicrobiologyTestHashKey", F.sha2(F.concat_ws("||", "MicrobiologyTest"), 256))
    df = df.withColumn("EventTypeHashKey", F.sha2(F.concat_ws("||", "EventType"), 256))

    return df
          



