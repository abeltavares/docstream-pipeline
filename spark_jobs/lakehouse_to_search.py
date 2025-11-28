from common.spark_session import create_spark_session, stop_spark_session
from common.transformations import convert_timestamps_to_iso
from common.config import config
from common.logger import setup_logging

logger = setup_logging("LakehouseToSearch")


def write_to_opensearch(batch_df, batch_id):
    """Write batch to OpenSearch"""
    if batch_df.isEmpty():
        return
    
    try:
        batch_df.write \
            .format("org.opensearch.spark.sql") \
            .option("opensearch.nodes", config.opensearch.host) \
            .option("opensearch.port", str(config.opensearch.port)) \
            .option("opensearch.mapping.id", "document_id") \
            .mode("append") \
            .save(config.opensearch.index_name)
        
        logger.info(f"Batch {batch_id}: Indexed {batch_df.count()} documents")
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}")


def main():
    logger.info("=" * 60)
    logger.info("Starting Lakehouse → OpenSearch Pipeline")
    logger.info("=" * 60)
    
    spark = create_spark_session("LakehouseToSearch")
    
    try:
        stream = spark.readStream.format("iceberg").table(config.iceberg.events_table)
        
        prepared = convert_timestamps_to_iso(
            stream.select(
                "document_id", "title", "status", "template_category",
                "created_at", "sent_at", "viewed_at", "signed_at", "completed_at",
                "time_to_view_minutes", "time_to_sign_minutes", "time_to_complete_minutes",
                "event_date", "event_hour", "is_business_hours", "processing_timestamp"
            ),
            ["created_at", "sent_at", "viewed_at", "signed_at", "completed_at", "processing_timestamp"]
        )
        
        logger.info(f"Indexing to {config.opensearch.index_name}...")
        query = prepared \
            .writeStream \
            .foreachBatch(write_to_opensearch) \
            .option("checkpointLocation", config.checkpoints.search) \
            .trigger(processingTime=config.streaming.search_trigger) \
            .start()
        
        logger.info("Pipeline running")
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
