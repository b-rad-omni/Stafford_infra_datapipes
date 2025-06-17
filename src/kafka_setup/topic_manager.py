# src/kafka_setup/topic_manager.py
"""
Kafka Topic Manager

This module handles the creation and management of Kafka topics for our data pipeline.
It ensures topics are properly configured with appropriate partitions, replication,
and retention settings for different data types.
"""

import logging
import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import TopicAlreadyExistsError, KafkaError
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int  # Message retention in milliseconds
    segment_ms: int  # Segment rolling time
    compression_type: str = 'snappy'
    min_insync_replicas: int = 1
    cleanup_policy: str = 'delete'  # 'delete' or 'compact'
    
    @property
    def retention_days(self) -> float:
        """Get retention period in days."""
        return self.retention_ms / (1000 * 60 * 60 * 24)
    
    @property
    def config_dict(self) -> Dict[str, str]:
        """Get Kafka topic configuration as a dictionary."""
        return {
            'retention.ms': str(self.retention_ms),
            'segment.ms': str(self.segment_ms),
            'compression.type': self.compression_type,
            'min.insync.replicas': str(self.min_insync_replicas),
            'cleanup.policy': self.cleanup_policy,
            'unclean.leader.election.enable': 'false',  # Prevent data loss
            'message.timestamp.type': 'CreateTime'  # Use producer timestamp
        }


class KafkaTopicManager:
    """
    Manages Kafka topics for our data pipeline.
    
    This class handles:
    - Creating topics with appropriate configurations
    - Updating topic configurations
    - Monitoring topic health
    - Managing topic lifecycle (cleanup, deletion)
    """
    
    # Default topic configurations for different data types
    DEFAULT_CONFIGS = {
        'clickstream': TopicConfig(
            name='clickstream',
            partitions=3,  # Parallel processing capability
            replication_factor=1,  # For local development; use 3 in production
            retention_ms=604800000,  # 7 days - clickstream data is high volume
            segment_ms=86400000,  # 1 day segments
            compression_type='snappy'  # Good balance of speed and compression
        ),
        'orders': TopicConfig(
            name='orders',
            partitions=3,
            replication_factor=1,
            retention_ms=2592000000,  # 30 days - orders are important, keep longer
            segment_ms=86400000,
            compression_type='gzip'  # Better compression for less frequent data
        ),
        'processed_events': TopicConfig(
            name='processed_events',
            partitions=2,
            replication_factor=1,
            retention_ms=259200000,  # 3 days - intermediate processing results
            segment_ms=43200000,  # 12 hour segments
            compression_type='snappy'
        ),
        'dead_letter': TopicConfig(
            name='dead_letter',
            partitions=1,  # Low volume, failed messages
            replication_factor=1,
            retention_ms=604800000,  # 7 days to investigate failures
            segment_ms=86400000,
            compression_type='gzip'
        )
    }
    
    def __init__(self, bootstrap_servers: str = None):
        """
        Initialize the Kafka topic manager.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.admin_client = None
        
        logger.info(f"Kafka Topic Manager initialized with servers: {self.bootstrap_servers}")
        
    def connect(self) -> bool:
        """
        Establish connection to Kafka cluster.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            logger.info(f"Connecting to Kafka cluster at {self.bootstrap_servers}...")
            
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(','),
                client_id='topic_manager',
                request_timeout_ms=30000,
                connections_max_idle_ms=540000
            )
            
            # Test connection by listing topics
            existing_topics = self.admin_client.list_topics()
            logger.info(f"✅ Connected to Kafka. Found {len(existing_topics)} existing topics")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def create_topics(self, topic_configs: Optional[List[TopicConfig]] = None) -> Dict[str, bool]:
        """
        Create all required topics for the pipeline.
        
        Args:
            topic_configs: List of topic configurations. If None, uses defaults.
            
        Returns:
            Dictionary mapping topic names to creation success status
        """
        if not self.admin_client:
            if not self.connect():
                raise ConnectionError("Failed to connect to Kafka")
        
        # Use provided configs or defaults
        configs_to_create = topic_configs or list(self.DEFAULT_CONFIGS.values())
        
        # Get existing topics
        existing_topics = set(self.admin_client.list_topics())
        
        # Create NewTopic objects for topics that don't exist
        new_topics = []
        results = {}
        
        for config in configs_to_create:
            if config.name in existing_topics:
                logger.info(f"Topic '{config.name}' already exists, skipping creation")
                results[config.name] = True
            else:
                logger.info(f"Creating topic '{config.name}' with {config.partitions} partitions...")
                
                new_topic = NewTopic(
                    name=config.name,
                    num_partitions=config.partitions,
                    replication_factor=config.replication_factor,
                    topic_configs=config.config_dict
                )
                new_topics.append(new_topic)
        
        # Create topics in Kafka
        if new_topics:
            try:
                fs = self.admin_client.create_topics(new_topics, validate_only=False)
                
                # Wait for each topic creation to complete
                for topic, f in fs.items():
                    try:
                        f.result()  # Block until topic is created
                        logger.info(f"✅ Topic '{topic}' created successfully")
                        results[topic] = True
                    except TopicAlreadyExistsError:
                        logger.info(f"Topic '{topic}' already exists")
                        results[topic] = True
                    except Exception as e:
                        logger.error(f"❌ Failed to create topic '{topic}': {e}")
                        results[topic] = False
                        
            except Exception as e:
                logger.error(f"Failed to create topics: {e}")
                for config in configs_to_create:
                    if config.name not in results:
                        results[config.name] = False
        
        return results
    
    def update_topic_config(self, topic_name: str, config_updates: Dict[str, str]) -> bool:
        """
        Update configuration for an existing topic.
        
        Args:
            topic_name: Name of the topic to update
            config_updates: Dictionary of configuration parameters to update
            
        Returns:
            True if update successful, False otherwise
        """
        if not self.admin_client:
            if not self.connect():
                return False
        
        try:
            logger.info(f"Updating configuration for topic '{topic_name}'...")
            
            # Create config resource
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = {resource: config_updates}
            
            # Apply configuration updates
            fs = self.admin_client.alter_configs(configs)
            
            # Wait for completion
            for resource, f in fs.items():
                f.result()  # Block until configuration is updated
                logger.info(f"✅ Configuration updated for topic '{topic_name}'")
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to update topic configuration: {e}")
            return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a topic from Kafka.
        
        Use with caution! This permanently removes the topic and all its data.
        
        Args:
            topic_name: Name of the topic to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        if not self.admin_client:
            if not self.connect():
                return False
        
        try:
            logger.warning(f"⚠️  Deleting topic '{topic_name}'...")
            
            fs = self.admin_client.delete_topics([topic_name], timeout_ms=30000)
            
            # Wait for deletion to complete
            for topic, f in fs.items():
                f.result()
                logger.info(f"✅ Topic '{topic}' deleted successfully")
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
    
    def list_topics(self) -> List[str]:
        """
        List all topics in the Kafka cluster.
        
        Returns:
            List of topic names
        """
        if not self.admin_client:
            if not self.connect():
                return []
        
        try:
            topics = self.admin_client.list_topics()
            # Filter out internal topics (start with __)
            user_topics = [t for t in topics if not t.startswith('__')]
            return sorted(user_topics)
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    def get_topic_metadata(self, topic_name: str) -> Optional[Dict]:
        """
        Get detailed metadata for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary containing topic metadata or None if not found
        """
        if not self.admin_client:
            if not self.connect():
                return None
        
        try:
            # Check if topic exists
            existing_topics = self.admin_client.list_topics()
            
            if topic_name not in existing_topics:
                logger.warning(f"Topic '{topic_name}' not found")
                return None
            
            # Try to get partition count using a consumer
            # This is the most compatible way across different kafka-python versions
            from kafka import KafkaConsumer
            
            try:
                # Create a temporary consumer to query topic metadata
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers.split(','),
                    group_id='metadata-checker',
                    enable_auto_commit=False,
                    consumer_timeout_ms=1000
                )
                
                # Get partitions for the topic
                partitions = consumer.partitions_for_topic(topic_name)
                consumer.close()
                
                if partitions:
                    return {
                        'name': topic_name,
                        'partitions': len(partitions),
                        'partition_ids': sorted(list(partitions))
                    }
                else:
                    # Topic exists but couldn't get partition info
                    return {
                        'name': topic_name,
                        'partitions': 'unknown',
                        'exists': True
                    }
                    
            except Exception as inner_e:
                logger.debug(f"Could not get detailed metadata via consumer: {inner_e}")
                # Return basic info that topic exists
                return {
                    'name': topic_name,
                    'exists': True,
                    'partitions': 'unknown'
                }
                
        except Exception as e:
            logger.error(f"Failed to get topic metadata: {e}")
            return None
    
    def test_kafka_connection(self) -> bool:
        """
        Test Kafka connectivity by sending and receiving a test message.
        
        Returns:
            True if test successful, False otherwise
        """
        test_topic = 'test_connection'
        test_message = {
            'test': 'connection',
            'timestamp': time.time(),
            'manager': 'KafkaTopicManager'
        }
        
        try:
            logger.info("Testing Kafka connection...")
            
            # Create producer
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=10000
            )
            
            # Send test message
            future = producer.send(test_topic, value=test_message)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"✅ Test message sent to topic '{record_metadata.topic}' "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            
            producer.close()
            
            # Create consumer to verify
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=self.bootstrap_servers.split(','),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='test-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=5000  # Stop after 5 seconds
            )
            
            # Read message
            message_found = False
            for message in consumer:
                if message.value.get('manager') == 'KafkaTopicManager':
                    logger.info(f"✅ Test message received: {message.value}")
                    message_found = True
                    break
            
            consumer.close()
            
            if message_found:
                logger.info("✅ Kafka connection test successful!")
                return True
            else:
                logger.warning("⚠️  Test message not received")
                return False
                
        except Exception as e:
            logger.error(f"❌ Kafka connection test failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources."""
        if self.admin_client:
            self.admin_client.close()
            logger.info("Kafka admin client closed")


# CLI interface for managing topics
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Topic Manager for Data Pipeline')
    parser.add_argument('command', choices=['create', 'list', 'delete', 'test', 'metadata'],
                        help='Command to execute')
    parser.add_argument('--topic', type=str, help='Topic name (for delete/metadata commands)')
    parser.add_argument('--kafka-servers', type=str, 
                        help='Kafka bootstrap servers (comma-separated)')
    
    args = parser.parse_args()
    
    # Create manager instance
    manager = KafkaTopicManager(bootstrap_servers=args.kafka_servers)
    
    try:
        if args.command == 'create':
            # Create all default topics
            logger.info("Creating all pipeline topics...")
            results = manager.create_topics()
            
            # Summary
            successful = sum(1 for success in results.values() if success)
            logger.info(f"\n📊 Created {successful}/{len(results)} topics successfully")
            
        elif args.command == 'list':
            # List all topics
            topics = manager.list_topics()
            if topics:
                logger.info(f"\n📋 Topics in Kafka cluster ({len(topics)} total):")
                for topic in topics:
                    # Get metadata for each topic
                    metadata = manager.get_topic_metadata(topic)
                    if metadata:
                        logger.info(f"  - {topic} ({metadata['partitions']} partitions)")
                    else:
                        logger.info(f"  - {topic}")
            else:
                logger.info("No topics found in cluster")
                
        elif args.command == 'delete':
            if not args.topic:
                logger.error("Topic name required for delete command")
            else:
                # Confirm deletion
                response = input(f"⚠️  Are you sure you want to delete topic '{args.topic}'? (yes/no): ")
                if response.lower() == 'yes':
                    success = manager.delete_topic(args.topic)
                    if success:
                        logger.info(f"Topic '{args.topic}' deleted")
                else:
                    logger.info("Deletion cancelled")
                    
        elif args.command == 'test':
            # Test Kafka connection
            success = manager.test_kafka_connection()
            if not success:
                logger.error("Kafka connection test failed")
                exit(1)
                
        elif args.command == 'metadata':
            if not args.topic:
                logger.error("Topic name required for metadata command")
            else:
                metadata = manager.get_topic_metadata(args.topic)
                if metadata:
                    logger.info(f"\n📊 Metadata for topic '{args.topic}':")
                    logger.info(json.dumps(metadata, indent=2))
                else:
                    logger.error(f"Could not get metadata for topic '{args.topic}'")
                    
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        manager.cleanup()