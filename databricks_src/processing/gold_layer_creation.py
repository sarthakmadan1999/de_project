import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    GOLD_PATH   = config_val['databricks']['gold_path']
    CONFIG_PATH = config_val['databricks']['config_path']
    SILVER_PATH = config_val['databricks']['delta_path']
    LOG_PATH    = config_val['databricks']['gold_log']
    return GOLD_PATH, CONFIG_PATH, SILVER_PATH, LOG_PATH


# ---------------------------------------
# Read Silver Delta
# ---------------------------------------
def read_silver_layer(spark, SILVER_PATH, logger):
    logger.info(f"Reading Silver Delta table from: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)
    total = df.count()
    logger.info(f"Total records read from Silver: {total}")
    return df


# ---------------------------------------
# Transform for Gold
# ---------------------------------------
def transform_gold(silver_df, logger):
    df = silver_df.filter(col("status") != "EXPIRED")
    logger.info(f"Records after filtering EXPIRED: {df.count()}")

    gold_df = df.select(
        col("icao24"),
        col("callsign"),
        col("origin_country"),
        col("latitude"),
        col("longitude"),
        col("baro_altitude"),
        col("velocity"),
        col("on_ground"),
        col("true_track"),
        col("vertical_rate"),
        col("time_position"),
        col("last_contact"),
        col("status"),
        col("status_reason"),
        col("source_type"),
        col("ingestion_time")
    )

    final_count = gold_df.count()
    logger.info(f"Final Gold records: {final_count}")
    return gold_df, final_count


# ---------------------------------------
# Save Gold Layer
# ---------------------------------------
def save_gold_layer(gold_df, final_count, GOLD_PATH, CONFIG_PATH, logger):
    gold_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(GOLD_PATH)

    logger.info(f"Gold layer saved at: {GOLD_PATH}")

    # Update run_config
    config_str = dbutils.fs.head(CONFIG_PATH)
    config = json.loads(config_str)
    config["gold_output_path"] = GOLD_PATH
    config["gold_record_count"] = final_count
    dbutils.fs.put(CONFIG_PATH, json.dumps(config, indent=4), overwrite=True)

    logger.info("Updated run_config.json with Gold output path and record count.")


# ---------------------------------------
# Main
# ---------------------------------------
def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    GOLD_PATH, CONFIG_PATH, SILVER_PATH, LOG_PATH = config_value()
    logger = get_logger(__name__, f'{LOG_PATH}/{timestamp}_gold.log')
    spark = SparkSession.getActiveSession()
    try:
        silver_df = read_silver_layer(spark, SILVER_PATH, logger)
        gold_df, final_count = transform_gold(silver_df, logger)
        save_gold_layer(gold_df, final_count, GOLD_PATH, CONFIG_PATH, logger)
        logger.info("Gold layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Gold layer pipeline failed: {str(e)}")
        raise

main()