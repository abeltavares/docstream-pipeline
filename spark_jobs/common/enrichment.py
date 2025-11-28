from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, unix_timestamp, date_format, hour, lit, current_timestamp

def enrich_document_events(df: DataFrame) -> DataFrame:
    """
    Enrich document events with derived time metrics and event metadata
    
    This function applies the same transformations used in both:
    - CDC → Events pipeline
    - CDC → Metrics pipeline
    
    Transformations:
    1. Cast timestamps from Avro long (milliseconds) to Spark timestamp
    2. Compute time-to-event metrics (in minutes)
    3. Add event metadata (date, hour, business hours flag)
    4. Add processing timestamp
    
    Args:
        df: Raw CDC DataFrame from Kafka (after Avro deserialization)
        
    Returns:
        Enriched DataFrame with all derived columns
    """
    
    # Step 1: Cast timestamps
    # Spark's Avro reader handles union types automatically, so direct cast works
    df = df \
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("sent_at", col("sent_at").cast("timestamp")) \
        .withColumn("viewed_at", col("viewed_at").cast("timestamp")) \
        .withColumn("signed_at", col("signed_at").cast("timestamp")) \
        .withColumn("completed_at", col("completed_at").cast("timestamp")) \
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    
    # Step 2: Compute time-to-event metrics
    df = df \
        .withColumn(
            "time_to_send_minutes",
            when(col("sent_at").isNotNull(),
                 (unix_timestamp("sent_at") - unix_timestamp("created_at")) / 60.0
            ).otherwise(None)
        ) \
        .withColumn(
            "time_to_view_minutes",
            when(col("viewed_at").isNotNull() & col("sent_at").isNotNull(),
                 (unix_timestamp("viewed_at") - unix_timestamp("sent_at")) / 60.0
            ).otherwise(None)
        ) \
        .withColumn(
            "time_to_sign_minutes",
            when(col("signed_at").isNotNull() & col("sent_at").isNotNull(),
                 (unix_timestamp("signed_at") - unix_timestamp("sent_at")) / 60.0
            ).otherwise(None)
        ) \
        .withColumn(
            "time_to_complete_minutes",
            when(col("completed_at").isNotNull() & col("sent_at").isNotNull(),
                 (unix_timestamp("completed_at") - unix_timestamp("sent_at")) / 60.0
            ).otherwise(None)
        )
    
    # Step 3: Add event metadata
    df = df \
        .withColumn("event_date", date_format("updated_at", "yyyy-MM-dd")) \
        .withColumn("event_hour", hour("updated_at")) \
        .withColumn(
            "is_business_hours",
            when((hour("updated_at") >= 8) & (hour("updated_at") < 18), lit(True))
            .otherwise(lit(False))
        ) \
        .withColumn("processing_timestamp", current_timestamp())
    
    return df
