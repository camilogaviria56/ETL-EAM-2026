from pyspark.sql import functions as F
from pyspark.sql import SparkSession
CATALOG = "airline_mantenimiento"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
VOLUME_NAME = "landing"

VUELOS_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/vuelos_diarios.csv"
AEROPUERTOS_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/aeropuertos.csv"
AERONAVES_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/aeronaves.csv"
MANTENIMIENTOS_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/mantenimientos_rds.csv"
spark = SparkSession.builder.getOrCreate()
#crear esquema silver 
spark.sql("CREATE SCHEMA IF NOT EXISTS airline_mantenimiento.gold")
#leemos las tablas bronze
vuelos_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")
aeronaves_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")
aeropuertos_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")
mantenimientos_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")
# llamar las columnas que necesito y eliminar duplicados por id correspondiente

vuelos_silver = vuelos_df.drop("ingestion_time").dropDuplicates(["vuelo_id"])
aeronaves_silver = aeronaves_df.drop("ingestion_time").dropDuplicates(["aeronave_id"])
aeropuertos_silver = aeropuertos_df.drop("ingestion_time").dropDuplicates(["aeropuerto_id"])
mantenimientos_silver = mantenimientos_df.drop("ingestion_time").dropDuplicates(["mantenimiento_id"])
#guardar tablas como tablas delta
vuelos_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
mantenimientos_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

print ("bronze Proceso terminado")
print(f"tablas creadas: {CATALOG}.{BRONZE_SCHEMA}.vuelos,aeronaves,aeropuertos,mantenimientos")

