from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.avro.functions import from_avro
import requests
import logging

logger = logging.getLogger(__name__)


def read_kafka_cdc_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    schema_registry_url: str,
    topic: str,
    starting_offsets: str = "latest"
) -> DataFrame:
    """
    Read CDC stream from Kafka with Avro deserialization
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema Registry URL
        topic: Kafka topic name
        starting_offsets: "earliest" or "latest"
        
    Returns:
        DataFrame with deserialized CDC data
    """
    logger.info(f"Reading CDC stream from Kafka ({starting_offsets})...")
    
    # Fetch Avro schema
    try:
        schema_url = f"{schema_registry_url}/subjects/{topic}-value/versions/latest"
        response = requests.get(schema_url)
        response.raise_for_status()
        schema_json = response.json()['schema']
        logger.info("Schema fetched successfully")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch schema: {e}")
        raise RuntimeError(f"Schema Registry unavailable: {e}")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()
    
    # Deserialize Avro (skip Confluent magic bytes)
    cdc_df = kafka_df.select(
        from_avro(
            expr("substring(value, 6, length(value)-5)"),
            schema_json
        ).alias("data")
    ).select("data.*")
    
    logger.info(f"CDC stream created ({starting_offsets})")
    return cdc_df
