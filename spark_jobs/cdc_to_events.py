from common.spark_session import create_spark_session, stop_spark_session
from common.kafka_reader import read_kafka_cdc_stream
from common.enrichment import enrich_document_events
from common.data_quality import validate_enriched_events
from common.config import config
from common.logger import setup_logging

logger = setup_logging("CDCToEvents")


def main():
    logger.info("=" * 60)
    logger.info("Starting CDC → Events Pipeline")
    logger.info("=" * 60)
    
    spark = create_spark_session("CDCToEvents")
    
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog_name}.events")
        
        cdc_stream = read_kafka_cdc_stream(
            spark,
            config.kafka.bootstrap_servers,
            config.kafka.schema_registry_url,
            config.kafka.documents_topic,
            starting_offsets="earliest"
        )
        
        enriched = enrich_document_events(cdc_stream)
        validated = validate_enriched_events(enriched)
        
        logger.info(f"Writing to {config.iceberg.events_table}...")
        query = validated \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .trigger(processingTime=config.streaming.events_trigger) \
            .option("checkpointLocation", config.checkpoints.events) \
            .option("fanout-enabled", "true") \
            .partitionBy("event_date") \
            .toTable(config.iceberg.events_table)
        
        logger.info(f"Pipeline running → {config.iceberg.events_table}")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
