from pyspark.sql.functions import col, when, window, count, avg, date_format, hour, current_timestamp

from common.spark_session import create_spark_session, stop_spark_session
from common.kafka_reader import read_kafka_cdc_stream
from common.enrichment import enrich_document_events
from common.transformations import add_columns
from common.data_quality import validate_aggregated_metrics
from common.config import config
from common.logger import setup_logging

logger = setup_logging("CDCToMetrics")


def compute_metrics(enriched_df):
    """Compute 15-minute windowed metrics"""
    watermarked = enriched_df.withWatermark("updated_at", config.streaming.watermark_delay)
    
    metrics = watermarked \
        .groupBy(
            window(col("updated_at"), config.streaming.metrics_window),
            col("template_category")
        ) \
        .agg(
            count(when(col("status") == "draft", 1)).alias("documents_created"),
            count(when(col("status") == "sent", 1)).alias("documents_sent"),
            count(when(col("status") == "viewed", 1)).alias("documents_viewed"),
            count(when(col("status") == "signed", 1)).alias("documents_signed"),
            count(when(col("status") == "completed", 1)).alias("documents_completed"),
            avg("time_to_send_minutes").alias("avg_time_to_send"),
            avg("time_to_view_minutes").alias("avg_time_to_view"),
            avg("time_to_sign_minutes").alias("avg_time_to_sign"),
            avg("time_to_complete_minutes").alias("avg_time_to_complete"),
            count("*").alias("total_updates")
        )
    
    return add_columns(metrics, [
        ("sent_to_viewed_rate", when(col("documents_sent") > 0, col("documents_viewed") / col("documents_sent")).otherwise(0.0)),
        ("viewed_to_signed_rate", when(col("documents_viewed") > 0, col("documents_signed") / col("documents_viewed")).otherwise(0.0)),
        ("completion_rate", when(col("documents_sent") > 0, col("documents_completed") / col("documents_sent")).otherwise(0.0)),
        ("window_start", col("window.start")),
        ("window_end", col("window.end")),
        ("metric_date", date_format(col("window.start"), "yyyy-MM-dd")),
        ("metric_hour", hour(col("window.start"))),
        ("processing_timestamp", current_timestamp())
    ])


def main():
    logger.info("=" * 60)
    logger.info("Starting CDC → Metrics Pipeline")
    logger.info("=" * 60)
    
    spark = create_spark_session("CDCToMetrics")
    
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog_name}.metrics")
        
        cdc_stream = read_kafka_cdc_stream(
            spark,
            config.kafka.bootstrap_servers,
            config.kafka.schema_registry_url,
            config.kafka.documents_topic,
            starting_offsets="latest"
        )
        
        enriched = enrich_document_events(cdc_stream)
        metrics = compute_metrics(enriched)
        validated = validate_aggregated_metrics(metrics)
        
        logger.info(f"Writing to {config.iceberg.metrics_table}...")
        query = validated \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .trigger(processingTime=config.streaming.metrics_trigger) \
            .option("checkpointLocation", config.checkpoints.metrics) \
            .toTable(config.iceberg.metrics_table)
        
        logger.info(f"Pipeline running → {config.iceberg.metrics_table}")
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
