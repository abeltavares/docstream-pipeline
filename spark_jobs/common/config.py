from dataclasses import dataclass, field
import os


@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092')
    schema_registry_url: str = os.environ.get('KAFKA_SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    topic_prefix: str = os.environ.get('KAFKA_TOPIC_PREFIX', 'dbserver1')

    @property
    def documents_topic(self) -> str:
        return f"{self.topic_prefix}.public.documents"


@dataclass
class IcebergConfig:
    catalog_name: str = os.environ.get('ICEBERG_CATALOG_NAME', 'docstream_catalog')
    namespace_events: str = os.environ.get('ICEBERG_NAMESPACE_EVENTS', 'events')
    namespace_metrics: str = os.environ.get('ICEBERG_NAMESPACE_METRICS', 'metrics')
    
    @property
    def events_table(self) -> str:
        return f"{self.catalog_name}.{self.namespace_events}.documents"
    
    @property
    def metrics_table(self) -> str:
        return f"{self.catalog_name}.{self.namespace_metrics}.documents"

@dataclass
class OpenSearchConfig:
    host: str = os.environ.get('OPENSEARCH_HOST', 'opensearch')
    port: int = int(os.environ.get('OPENSEARCH_PORT', '9200'))
    index_name: str = os.environ.get('OPENSEARCH_INDEX_NAME', 'documents')
    batch_size_entries: int = 1000
    batch_size_bytes: str = "10mb"


@dataclass
class CheckpointConfig:
    base_path: str = os.environ.get('CHECKPOINT_BASE_PATH', 's3a://checkpoints')
    
    @property
    def events(self) -> str:
        return f"{self.base_path}/cdc-to-events"
    
    @property
    def metrics(self) -> str:
        return f"{self.base_path}/cdc-to-metrics"
    
    @property
    def search(self) -> str:
        return f"{self.base_path}/lakehouse-to-search"


@dataclass
class StreamingConfig:
    events_trigger: str = os.environ.get('STREAMING_EVENTS_TRIGGER', '30 seconds')
    metrics_trigger: str = os.environ.get('STREAMING_METRICS_TRIGGER', '1 minute')
    search_trigger: str = os.environ.get('STREAMING_SEARCH_TRIGGER', '30 seconds')
    
    watermark_delay: str = os.environ.get('STREAMING_WATERMARK_DELAY', '2 minutes')
    metrics_window: str = os.environ.get('STREAMING_METRICS_WINDOW', '15 minutes')


@dataclass
class Config:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)
    opensearch: OpenSearchConfig = field(default_factory=OpenSearchConfig)
    checkpoints: CheckpointConfig = field(default_factory=CheckpointConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)


config = Config()
