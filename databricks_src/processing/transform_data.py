import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp, when, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType
from delta.tables import DeltaTable
from databricks.sdk.runtime import *
from databricks_src.utils.logger import get_logger
import yaml
from datetime import datetime

WORKSPACE_PATH = os.environ.get("WORKSPACE_PATH")

# ---------------------------------------
# Load Config
# ---------------------------------------
def load_config():
    with open(f"{WORKSPACE_PATH}/config.yaml", "r") as f:
        return yaml.safe_load(f)

def config_value():
    config_val = load_config()
    PROCESSED_DATA_PATH = config_val['databricks']['delta_path']
    CONFIG_PATH         = config_val['databricks']['config_path']
    BAD_DATA_PATH       = config_val['databricks']['bad_data_path']
    LOG_PATH            = config_val['databricks']['silver_log']
    return PROCESSED_DATA_PATH, CONFIG_PATH, BAD_DATA_PATH, LOG_PATH


# ---------------------------------------
# Get raw file path from run_config
# ---------------------------------------
def get_latest_raw_file(CONFIG_PATH, logger):
    try:
        config_str = dbutils.fs.head(CONFIG_PATH)
        config = json.loads(config_str)
    except Exception as e:
        logger.error("run_config.json not found. Bronze layer may not have run.")
        raise FileNotFoundError("run_config.json not found. Run Bronze ingestion first.")

    raw_file = config.get("raw_file_path")

    if not raw_file:
        logger.error("raw_file_path missing in run_config.json.")
        raise KeyError("raw_file_path not found in run_config.json.")

    logger.info(f"Read raw file path from run_config: {raw_file}")
    return raw_file, config


# ---------------------------------------
# Aircraft Schema
# ---------------------------------------
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
                str(row[0]) if row[0] is not None else None,
                str(row[1]) if row[1] is not None else None,
                str(row[2]) if row[2] is not None else None,
                int(row[3]) if row[3] is not None else None,
                int(row[4]) if row[4] is not None else None,
                float(row[5]) if row[5] is not None else None,
                float(row[6]) if row[6] is not None else None,
                float(row[7]) if row[7] is not None else None,
                bool(row[8]) if row[8] is not None else None,
                float(row[9]) if row[9] is not None else None,
                float(row[10]) if row[10] is not None else None,
                float(row[11]) if row[11] is not None else None,
                None,
                float(row[13]) if row[13] is not None else None,
                str(row[14]) if row[14] is not None else None,
                bool(row[15]) if row[15] is not None else None,
                int(row[16]) if row[16] is not None else None
            ])
        return clean_list


# ---------------------------------------
# Process Aircraft Data
# ---------------------------------------
def process_aircraft_data(spark, CONFIG_PATH, logger):

    raw_file, _ = get_latest_raw_file(CONFIG_PATH, logger)
    logger.info(f"Latest raw file to process: {raw_file}")

    # Read JSON from ADLS
    raw_str = dbutils.fs.head(raw_file, 100000000)
    raw_data = json.loads(raw_str)

    if not raw_data or len(raw_data) == 0:
        logger.error("Raw JSON file is empty.")
        raise Exception("Raw JSON file is empty.")

    aircraft_schema = AircraftSchema()
    schema = aircraft_schema.schema
    logger.info("Defined Spark schema for aircraft data.")

    clean_list = aircraft_schema.clean_data(raw_data)
    logger.info("Cleaned raw data and prepared list for Spark DataFrame creation.")

    states = spark.createDataFrame(clean_list, schema)
    logger.info("Converted raw JSON data to Spark DataFrame.")

    total_records = states.count()
    logger.info(f"Total records in DataFrame: {total_records}")

    if total_records < 5000:
        logger.error(f"DQ FAILED: Record count below threshold. Actual count: {total_records}")
        raise Exception("DQ FAILED: Record count below threshold (5000).")

    return states


# ---------------------------------------
# Data Quality Checks
# ---------------------------------------
def data_quality_checks(states, BAD_DATA_PATH, logger,timestamp):
    null_df = states.filter(
        col("icao24").isNull() |
        col("origin_country").isNull() |
        (col("origin_country") == "NULL") |
        (col("origin_country") == "") |
        col("latitude").isNull() |
        col("longitude").isNull()
    )

    if null_df.count() > 0:
        null_df.write.mode("overwrite").json(f"{BAD_DATA_PATH}/null_records/{timestamp}")
        logger.warning("DQ WARNING: Found records with null critical fields. Logged to bad_records/null_records/")

    df_clean = states.filter(
        col("icao24").isNotNull() &
        col("origin_country").isNotNull() &
        col("latitude").isNotNull() &
        col("longitude").isNotNull()
    )

    df_valid_geo = df_clean.filter(
        (col("latitude") >= -90) & (col("latitude") <= 90) &
        (col("longitude") >= -180) & (col("longitude") <= 180)
    )

    df_invalid_geo = df_clean.filter(
        (col("latitude") < -90) | (col("latitude") > 90) |
        (col("longitude") < -180) | (col("longitude") > 180)
    )

    if df_invalid_geo.count() > 0:
        df_invalid_geo.write.mode("overwrite").json(f"{BAD_DATA_PATH}/invalid_geo/{timestamp}")
        logger.warning("DQ WARNING: Found records with invalid geo coordinates. Logged to bad_records/invalid_geo/")

    return df_valid_geo


# ---------------------------------------
# Final Clean DataFrame
# ---------------------------------------
def final_clean_df(df_valid_geo, logger):
    df_final = df_valid_geo.dropDuplicates(["icao24", "last_contact"])
    logger.info(f"Removed duplicates. Remaining records: {df_final.count()}")

    current_ts = unix_timestamp(current_timestamp())

    df_final = df_final.withColumn(
        "status",
        when(col("on_ground") == True, "LANDED")
        .when((current_ts - col("last_contact")) > 21600, "EXPIRED")
        .when((current_ts - col("last_contact")) > 10800, "INACTIVE")
        .otherwise("Airborne")
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

    df_final = df_final.withColumn(
        "time_position", from_unixtime(col("time_position"))
    ).withColumn(
        "last_contact", from_unixtime(col("last_contact"))
    )
    logger.info("Converted timestamps from epoch to datetime.")

    df_final = df_final.withColumn("ingestion_time", current_timestamp())
    df_final.show(5, truncate=False)
    final_count = df_final.count()
    logger.info(f"Final valid records after all DQ checks: {final_count}")
    return df_final, final_count


# ---------------------------------------
# Save Silver Layer
# ---------------------------------------
def save_silver_layer(df_final, final_count, PROCESSED_DATA_PATH, CONFIG_PATH, logger):
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

    # Update run_config
    config_str = dbutils.fs.head(CONFIG_PATH)
    config = json.loads(config_str)
    config["silver_output_path"] = output_path
    config["silver_record_count"] = final_count
    dbutils.fs.put(CONFIG_PATH, json.dumps(config, indent=4), overwrite=True)
    logger.info("Updated run_config.json with Silver output path and record count.")


# ---------------------------------------
# Main
# ---------------------------------------
def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    PROCESSED_DATA_PATH, CONFIG_PATH, BAD_DATA_PATH, LOG_PATH = config_value()
    logger = get_logger(__name__, f'{LOG_PATH}/{timestamp}'+'_silver.log')
    spark = SparkSession.getActiveSession()
    try:
        states = process_aircraft_data(spark, CONFIG_PATH, logger)
        valid_geo_df = data_quality_checks(states, BAD_DATA_PATH, logger,timestamp)
        final_df, final_count = final_clean_df(valid_geo_df, logger)
        save_silver_layer(final_df, final_count, PROCESSED_DATA_PATH, CONFIG_PATH, logger)
        logger.info("Silver layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Silver layer pipeline failed: {str(e)}")
        raise

main()