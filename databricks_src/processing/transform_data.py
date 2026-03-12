import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp, when, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType
import sys
from src.utils.logger import get_logger
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import yaml
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

logger = get_logger(__name__, log_file="logs/transform_data_silver.log")

# ---------------------------------------
# Fix Hadoop on Windows
# ---------------------------------------
os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";D:\\hadoop\\bin"

# PROCESSED_DATA_PATH = "data/processed"
# BAD_DATA_PATH = "logs/bad_records/"
# CONFIG_PATH = "D:/de_project/data/run_config.json"

def load_config():
    with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
        return yaml.safe_load(f)

def config_value():
    config_val = load_config()
    PROCESSED_DATA_PATH = config_val['local']['delta_path']
    CONFIG_PATH = config_val['local']['config_path']
    BAD_DATA_PATH = config_val['local']['bad_data_path']
    return PROCESSED_DATA_PATH, CONFIG_PATH, BAD_DATA_PATH

# ---------------------------------------
# Create Spark Session
# ---------------------------------------
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("AircraftDataProcessingDQ")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------
# Get raw file path from run_config
# ---------------------------------------
def get_latest_raw_file(CONFIG_PATH):
    if not os.path.exists(CONFIG_PATH):
        logger.error("run_config.json not found. Bronze layer may not have run.")
        raise FileNotFoundError("run_config.json not found. Run Bronze ingestion first.")

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    raw_file = config.get("raw_file_path")

    if not raw_file:
        logger.error("raw_file_path missing in run_config.json.")
        raise KeyError("raw_file_path not found in run_config.json.")

    if not os.path.exists(raw_file):
        logger.error(f"Raw file does not exist at path: {raw_file}")
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    logger.info(f"Read raw file path from run_config: {raw_file}")
    return raw_file


class AircraftSchema:
    def __init__(self):
        self.schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", LongType(), True),
            StructField("last_contact", LongType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("sensors", StringType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", LongType(), True)
        ])
    
    def clean_data(self, raw_data):
        clean_list = []
        for row in raw_data:
            clean_list.append([
                str(row[0]) if row[0] is not None else None,      # icao24
                str(row[1]) if row[1] is not None else None,      # callsign
                str(row[2]) if row[2] is not None else None,      # origin_country
                int(row[3]) if row[3] is not None else None,      # time_position
                int(row[4]) if row[4] is not None else None,      # last_contact
                float(row[5]) if row[5] is not None else None,    # longitude
                float(row[6]) if row[6] is not None else None,    # latitude
                float(row[7]) if row[7] is not None else None,    # baro_altitude
                bool(row[8]) if row[8] is not None else None,     # on_ground
                float(row[9]) if row[9] is not None else None,    # velocity
                float(row[10]) if row[10] is not None else None,  # true_track
                float(row[11]) if row[11] is not None else None,  # vertical_rate
                None,                                             # sensors (unused)
                float(row[13]) if row[13] is not None else None,  # geo_altitude
                str(row[14]) if row[14] is not None else None,    # squawk
                bool(row[15]) if row[15] is not None else None,   # spi
                int(row[16]) if row[16] is not None else None     # position_source
            ])
        return clean_list


# ---------------------------------------
# Main processing logic
# ---------------------------------------
def process_aircraft_data(spark,CONFIG_PATH):

    # 1 Load raw JSON file from run_config
    raw_file = get_latest_raw_file(CONFIG_PATH)
    logger.info(f"Latest raw file to process: {raw_file}")

    with open(raw_file, "r") as f:
        raw_data = json.load(f)

    if not raw_data or len(raw_data) == 0:
        logger.error("Raw JSON file is empty.")
        raise Exception("Raw JSON file is empty.")

    # 2 Define Spark schema
    aircraft_schema = AircraftSchema()
    schema = aircraft_schema.schema
    logger.info("Defined Spark schema for aircraft data.")

    # 3 Convert raw array to Spark compatible list with type casting
    clean_list = aircraft_schema.clean_data(raw_data)
    logger.info("Cleaned raw data and prepared list for Spark DataFrame creation.")

    # 4 Convert Python list to Spark DataFrame
    states = spark.createDataFrame(clean_list, schema)
    logger.info("Converted raw JSON data to Spark DataFrame.")

    # -------------------------------
    # 5 DATA QUALITY CHECKS
    # -------------------------------

    # 5.1 Minimum record count
    total_records = states.count()
    logger.info(f"Total records in DataFrame: {total_records}")

    # if total_records < 5000:
    #     logger.error(f"DQ FAILED: Record count below threshold. Actual count: {total_records}")
    #     raise Exception("DQ FAILED: Record count below threshold (5000).")
    return states
    # 5.2 Null checks for critical fields
def data_quality_checks(states,BAD_DATA_PATH):
    null_df = states.filter(
        col("icao24").isNull() |
        col("origin_country").isNull() |
        col("latitude").isNull() |
        col("longitude").isNull()
    )

    if null_df.count() > 0:
        os.makedirs(BAD_DATA_PATH, exist_ok=True)
        null_df.write.mode("overwrite").json(f"{BAD_DATA_PATH}/null_records/")
        logger.warning("DQ WARNING: Found records with null critical fields. Logged to bad_records/null_records/")

    # Keep only valid rows
    df_clean = states.filter(
        col("icao24").isNotNull() &
        col("origin_country").isNotNull() &
        col("latitude").isNotNull() &
        col("longitude").isNotNull()
    )

    # 5.3 Latitude and longitude range validation
    df_valid_geo = df_clean.filter(
        (col("latitude") >= -90) & (col("latitude") <= 90) &
        (col("longitude") >= -180) & (col("longitude") <= 180)
    )

    df_invalid_geo = df_clean.subtract(df_valid_geo)

    if df_invalid_geo.count() > 0:
        df_invalid_geo.write.mode("overwrite").json(f"{BAD_DATA_PATH}/invalid_geo/")
        logger.warning("DQ WARNING: Found records with invalid geo coordinates. Logged to bad_records/invalid_geo/")
    
    return df_valid_geo
    # 5.4 Remove duplicates
def final_clean_df(df_valid_geo):
    df_final = df_valid_geo.dropDuplicates(["icao24", "last_contact"])
    logger.info(f"Removed duplicates. Remaining records: {df_final.count()}")

    # 5.5 Status and status_reason logic
    current_ts = unix_timestamp(current_timestamp())

    df_final = df_final.withColumn(
        "status",
        when(col("on_ground") == True, "LANDED")
        .when((current_ts - col("last_contact")) > 21600, "EXPIRED")
        .when((current_ts - col("last_contact")) > 10800, "INACTIVE")
        .otherwise("ACTIVE")
    ).withColumn(
        "status_reason",
        when(col("on_ground") == True, "on_ground_flag")
        .when((current_ts - col("last_contact")) > 21600, "last_contact_exceeded_6hrs")
        .when((current_ts - col("last_contact")) > 10800, "last_contact_exceeded_3hrs")
        .otherwise("in_air")
    ).withColumn(
    "source_type",
    when(col("position_source") == 0, "ADS-B")
    .when(col("position_source") == 1, "ASTERIX")
    .when(col("position_source") == 2, "MLAT")
    .when(col("position_source") == 3, "FLARM")
    .otherwise("UNKNOWN")
)
    logger.info("Flight status business logic applied.")

    # 5.6 Convert timestamps to readable format
    df_final = df_final.withColumn(
        "time_position",
        from_unixtime(col("time_position"))
    ).withColumn(
        "last_contact",
        from_unixtime(col("last_contact"))
    )
    logger.info("Converted time_position and last_contact from epoch to datetime.")

    # 5.7 Add ingestion metadata
    df_final = df_final.withColumn("ingestion_time", current_timestamp())
    df_final.show(5, truncate=False)
    final_count = df_final.count()
    logger.info(f"Final valid records after all DQ checks: {final_count}")
    return df_final,final_count
    # -------------------------------
    # 6 SAVE PROCESSED OUTPUT
    # -------------------------------

def save_silver_layer(df_final, final_count, PROCESSED_DATA_PATH, CONFIG_PATH):
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    output_path = PROCESSED_DATA_PATH
    spark = SparkSession.getActiveSession()

    if not DeltaTable.isDeltaTable(spark, output_path):
        logger.info("First run detected. Writing Delta table for the first time.")
        df_final.write \
            .format("delta") \
            .partitionBy("origin_country") \
            .mode("overwrite") \
            .save(output_path)
        logger.info(f"Delta table created at: {output_path}")

    else:
        logger.info("Existing Delta table detected. Performing merge/upsert.")
        delta_table = DeltaTable.forPath(spark, output_path)

        delta_table.alias("existing").merge(
            df_final.alias("new"),
            "existing.icao24 = new.icao24"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        logger.info(f"Merge complete. Records processed: {final_count}")

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    config["silver_output_path"] = output_path
    config["silver_record_count"] = final_count

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)

    logger.info("Updated run_config.json with Silver output path and record count.")

# ---------------------------------------
# Run workflow
# ---------------------------------------
if __name__ == "__main__":
    spark = create_spark_session()
    try:
        PROCESSED_DATA_PATH, CONFIG_PATH, BAD_DATA_PATH = config_value()
        process = process_aircraft_data(spark, CONFIG_PATH)
        valid_geo_df = data_quality_checks(process, BAD_DATA_PATH)
        final_df, final_count = final_clean_df(valid_geo_df)
        save_silver_layer(final_df, final_count, PROCESSED_DATA_PATH, CONFIG_PATH)
        logger.info("Silver layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Silver layer pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()