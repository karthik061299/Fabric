
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import time
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("BronzeLayerIngestion").getOrCreate()

# Load credentials securely from Azure Key Vault
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# Get current user's identity for audit logging
try:
    current_user = mssparkutils.env.getUserName()
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        current_user = "Unknown User"

# Define source and target details
source_system = "PostgreSQL"
database_name = "DE"
source_schema = "tests"
target_bronze_path = "abfss://Analytical_POC@onelake.dfs.fabric.microsoft.com/Inventory.Lakehouse/Tables/Bronze/"

# Define tables to ingest (from mapping)
tables = [
    "Applicants",
    "Applications",
    "Card_Products",
    "Credit_Scores",
    "Document_Submissions",
    "Verification_Results",
    "Underwriting_Decisions",
    "Campaigns",
    "Application_Campaigns",
    "Activations",
    "Fraud_Checks",
    "Offers",
    "Offer_Performance",
    "Address_History",
    "Employment_Info"
]

# Define audit table schema
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False)
])

def log_audit(record_id, source_table, processing_time, status):
    current_time = datetime.now()
    audit_df = spark.createDataFrame(
        [(
            record_id,
            source_table,
            current_time,
            current_user,
            processing_time,
            status
        )],
        schema=audit_schema
    )
    audit_df.write.format("delta").mode("append").saveAsTable("bz_audit")

def load_to_bronze(table_name, record_id):
    start_time = time.time()
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{source_schema}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .load()

        row_count = df.count()

        df = df.withColumn("Load_Date", current_timestamp()) \
               .withColumn("Update_Date", current_timestamp()) \
               .withColumn("Source_System", lit(source_system))

        df.write.format("delta") \
           .mode("overwrite") \
           .save(f"{target_bronze_path}bz_{table_name.lower()}")
        
        processing_time = int(time.time() - start_time)
        log_audit(
            record_id=record_id,
            source_table=table_name,
            processing_time=processing_time,
            status=f"Success - {row_count} rows processed"
        )
        print(f"Successfully loaded {table_name} with {row_count} rows")
    except Exception as e:
        processing_time = int(time.time() - start_time)
        log_audit(
            record_id=record_id,
            source_table=table_name,
            processing_time=processing_time,
            status=f"Failed - {str(e)}"
        )
        print(f"Failed to load {table_name}: {str(e)}")
        raise e

try:
    print(f"Starting data ingestion process by user: {current_user}")
    for idx, table in enumerate(tables, 1):
        load_to_bronze(table, idx)
except Exception as e:
    print(f"Ingestion process failed: {str(e)}")
finally:
    spark.stop()