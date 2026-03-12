import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
from src.utils.logger import get_logger
from delta import configure_spark_with_delta_pip
import yaml
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

logger = get_logger(__name__, log_file="logs/gold_layer.log")

os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";D:\\hadoop\\bin"

# SILVER_PATH = "D:/de_project/data/processed/aircraft_processed_delta/"
# GOLD_PATH = "D:/de_project/data/gold/aircraft_gold/"
# CONFIG_PATH = "D:/de_project/data/run_config.json"

def load_config():
    with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
        return yaml.safe_load(f)

def config_value():
    config_val = load_config()
    GOLD_PATH = config_val['local']['gold_path']
    CONFIG_PATH = config_val['local']['config_path']
    SILVER_PATH = config_val['local']['delta_path']
    return GOLD_PATH, CONFIG_PATH, SILVER_PATH

# ---------------------------------------
# Spark Session
# ---------------------------------------
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("AircraftGoldLayer")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------
# Read Silver Delta
# ---------------------------------------
def read_silver_layer(spark,SILVER_PATH):
    logger.info(f"Reading Silver Delta table from: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)
    total = df.count()
    logger.info(f"Total records read from Silver: {total}")
    return df


# ---------------------------------------
# Transform for Gold
# ---------------------------------------
def transform_gold(silver_df):
    # Filter EXPIRED records
    df = silver_df.filter(col("status") != "EXPIRED")
    logger.info(f"Records after filtering EXPIRED: {df.count()}")

    # Select relevant columns for analytics
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

    # Remove duplicates keeping latest per aircraft
    # gold_df = gold_df.dropDuplicates(["icao24"])
    # final_count = gold_df.count()
    # logger.info(f"Final Gold records after dedup: {final_count}")

    return gold_df, final_count


# ---------------------------------------
# Save Gold Layer
# ---------------------------------------
def save_gold_layer(gold_df, final_count, GOLD_PATH, CONFIG_PATH):
    gold_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("origin_country") \
        .save(GOLD_PATH)

    logger.info(f"Gold layer saved at: {GOLD_PATH}")

    # Update run_config
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    config["gold_output_path"] = GOLD_PATH
    config["gold_record_count"] = final_count

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)

    logger.info("Updated run_config.json with Gold output path and record count.")


# ---------------------------------------
# Run workflow
# ---------------------------------------
if __name__ == "__main__":
    spark = create_spark_session()
    try:
        GOLD_PATH, CONFIG_PATH, SILVER_PATH = config_value()
        silver_df = read_silver_layer(spark, SILVER_PATH)
        gold_df, final_count = transform_gold(silver_df)
        save_gold_layer(gold_df, final_count, GOLD_PATH, CONFIG_PATH)
        logger.info("Gold layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Gold layer pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()