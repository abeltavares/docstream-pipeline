from common.spark_session import create_spark_session, stop_spark_session
from common.transformations import convert_timestamps_to_iso
from common.config import config
from common.logger import setup_logging


logger = setup_logging("OpenSearchIndexer")


class OpenSearchIndexer:
    """Index enriched events to OpenSearch"""
    
    def __init__(self):
        self.spark = create_spark_session("OpenSearchIndexer")
        
        # Use centralized config
        self.source_table = f"{config.iceberg.catalog_name}.{config.iceberg.namespace_events}.{config.iceberg.table_documents}"
        self.opensearch_index = config.opensearch.index_name
        
        logger.info(f"Source table: {self.source_table}")
        logger.info(f"OpenSearch index: {self.opensearch_index}")
        logger.info(f"OpenSearch host: {config.opensearch.host}:{config.opensearch.port}")
    
    def read_enriched_stream(self):
        """Read enriched events from Iceberg"""
        logger.info(f"Reading enriched events from {self.source_table}...")
        
        enriched_stream = self.spark.readStream \
            .format("iceberg") \
            .table(self.source_table)
        
        logger.info("Enriched stream created")
        return enriched_stream
    
    def prepare_for_opensearch(self, df):
        """Prepare data for OpenSearch indexing"""
        logger.info("Preparing data for OpenSearch...")
        
        # Select relevant columns
        selected = df.select(
            "document_id", "title", "status", "template_category",
            "created_at", "sent_at", "viewed_at", "signed_at", "completed_at",
            "time_to_view_minutes", "time_to_sign_minutes", "time_to_complete_minutes",
            "event_date", "event_hour", "is_business_hours", "processing_timestamp"
        )
        
        # Convert timestamps to ISO 8601 for OpenSearch
        timestamp_cols = [
            "created_at", "sent_at", "viewed_at", 
            "signed_at", "completed_at", "processing_timestamp"
        ]
        
        opensearch_ready = convert_timestamps_to_iso(selected, timestamp_cols)
        
        logger.info("Data prepared for OpenSearch")
        return opensearch_ready
    
    def write_batch_to_opensearch(self, batch_df, batch_id):
        """Write a batch to OpenSearch"""
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        try:
            batch_df.write \
                .format("org.opensearch.spark.sql") \
                .option("opensearch.nodes", config.opensearch.host) \
                .option("opensearch.port", str(config.opensearch.port)) \
                .option("opensearch.mapping.id", "document_id") \
                .option("opensearch.batch.size.entries", "500") \
                .option("opensearch.batch.size.bytes", "5mb") \
                .option("opensearch.write.operation", "upsert") \
                .mode("append") \
                .save(self.opensearch_index)
            
            count = batch_df.count()
            logger.info(f"Batch {batch_id}: Indexed {count} documents to OpenSearch")
            
        except Exception as e:
            logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
            # Don't raise - allow pipeline to continue
    
    def run(self):
        """Run the OpenSearch indexing pipeline"""
        logger.info("=" * 60)
        logger.info("Starting OpenSearch Indexer Job")
        logger.info("=" * 60)
        
        try:
            # Read enriched events
            enriched_stream = self.read_enriched_stream()
            
            # Prepare for OpenSearch
            opensearch_stream = self.prepare_for_opensearch(enriched_stream)
            
            # Write to OpenSearch
            logger.info(f"Starting OpenSearch indexing to index: {self.opensearch_index}...")
            query = opensearch_stream \
                .writeStream \
                .foreachBatch(self.write_batch_to_opensearch) \
                .option("checkpointLocation", config.checkpoints.opensearch) \
                .trigger(processingTime="30 seconds") \
                .option("maxRecordsPerBatch", "1000") \
                .start()
            
            logger.info(f"Pipeline running - indexing to {self.opensearch_index}")
            logger.info(f"Checkpoint: {config.checkpoints.opensearch}")
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(self.spark)


def main():
    job = OpenSearchIndexer()
    job.run()


if __name__ == "__main__":
    main()
