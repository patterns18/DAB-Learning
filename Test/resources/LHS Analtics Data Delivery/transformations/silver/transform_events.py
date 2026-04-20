from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_timestamp
from pyspark import pipelines as dp


dp.create_streaming_table(
    name = "lhsdataproject.silver.silver_events"
)

@dp.append_flow(target=  "lhsdataproject.silver.silver_events")
def silver_events():
    df = spark.readStream.table("stg_silver_events")
    df = df.select("VisitID", "EventType", "EventDate", "EventTime","PIN", "EventNote", "EventStaff", "Facility",
                    "Building", "Unit", "Species", "Organism", "TestResult",   "MicrobiologyTest", "MedicalSpecialty",
                     "SurgicalProcedure","DischargeStaff", "_rescued_data", "bronze_Load_Datetime",
                     "LocationHashKey","ProcedureHashKey", "OrganismHashKey", "MicrobiologyTestHashKey","EventTypeHashKey")
    
    return df


