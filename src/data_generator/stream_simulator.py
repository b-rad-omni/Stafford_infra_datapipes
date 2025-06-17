# src/data_generator/stream_simulator.py
"""
Data Stream Simulator

This module continuously generates e-commerce data to simulate a live system.
It creates realistic traffic patterns that vary by time of day and includes
both clickstream events and order data. The simulator can send data to Kafka
or write to files as a fallback.
"""

import time
import json
import random
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

from generator import EcommerceDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataStreamSimulator:
    """
    Simulates continuous data generation for our pipeline.
    
    This class creates realistic patterns including:
    - Time-based traffic variations (peak hours, quiet nights)
    - Customer behavior patterns (browsing leading to purchases)
    - Seasonal and weekly patterns
    - Burst events (sales, marketing campaigns)
    """
    
    def __init__(self, kafka_bootstrap_servers: str = None, 
                 output_dir: str = None,
                 num_customers: int = 100):
        """
        Initialize the stream simulator.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses (comma-separated)
            output_dir: Directory to write files if Kafka is unavailable
            num_customers: Number of customers to simulate
        """
        self.generator = EcommerceDataGenerator()
        self.kafka_producer = None
        self.kafka_servers = kafka_bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.output_dir = Path(output_dir or os.getenv('DATA_OUTPUT_PATH', 'data/raw'))
        
        # Ensure output directories exist
        (self.output_dir / 'clickstream').mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'orders').mkdir(parents=True, exist_ok=True)
        
        # Initialize customer pool
        logger.info(f"Creating {num_customers} customers...")
        self.customers = [self.generator.generate_customer() for _ in range(num_customers)]
        
        # Track active sessions
        self.active_sessions = {}  # customer_id -> session_data
        
        # Statistics tracking
        self.stats = {
            'events_sent': 0,
            'orders_sent': 0,
            'kafka_failures': 0,
            'start_time': datetime.now()
        }
        
        # Thread pool for concurrent event generation
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # Graceful shutdown handling
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Stream simulator initialized with {num_customers} customers")
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received, stopping simulator...")
        self.running = False
        
    def start_kafka_producer(self) -> bool:
        """
        Initialize Kafka producer with proper error handling.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Connecting to Kafka brokers: {self.kafka_servers}")
            
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Producer configuration for reliability
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='snappy',  # Compress data for efficiency
                batch_size=16384,  # Batch messages for better throughput
                linger_ms=10,  # Small delay to allow batching
                buffer_memory=33554432,  # 32MB buffer
                # Timeout settings
                request_timeout_ms=30000,
                metadata_max_age_ms=300000
            )
            
            # Test connection by requesting metadata
            self.kafka_producer.bootstrap_connected()
            logger.info("✅ Kafka producer connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            logger.info("Will fall back to file output")
            self.kafka_producer = None
            return False
    
    def simulate_traffic(self, duration_minutes: int = 60, real_time: bool = True):
        """
        Main simulation loop that generates traffic patterns.
        
        Args:
            duration_minutes: How long to run the simulation
            real_time: If True, simulate real-time delays between events
        """
        logger.info(f"Starting traffic simulation for {duration_minutes} minutes...")
        
        # Try to connect to Kafka
        self.start_kafka_producer()
        
        end_time = time.time() + (duration_minutes * 60)
        
        while self.running and time.time() < end_time:
            try:
                # Calculate current traffic intensity based on time of day
                traffic_multiplier = self._calculate_traffic_multiplier()
                
                # Base events per second, adjusted by time of day
                base_events_per_second = 10
                events_per_second = base_events_per_second * traffic_multiplier
                
                # Generate batch of events
                self._generate_batch_events(int(events_per_second))
                
                # Log statistics periodically
                if self.stats['events_sent'] % 100 == 0:
                    self._log_statistics()
                
                # Sleep to maintain realistic timing
                if real_time:
                    time.sleep(1)  # One second between batches
                    
            except Exception as e:
                logger.error(f"Error in simulation loop: {e}")
                time.sleep(5)  # Back off on error
        
        # Cleanup
        self._shutdown()
    
    def _calculate_traffic_multiplier(self) -> float:
        """
        Calculate traffic intensity based on time of day and day of week.
        
        Returns a multiplier for the base traffic rate.
        Real e-commerce sites have predictable traffic patterns.
        """
        now = datetime.now()
        hour = now.hour
        weekday = now.weekday()  # 0 = Monday, 6 = Sunday
        
        # Weekend vs weekday base multiplier
        if weekday in [5, 6]:  # Saturday, Sunday
            base_multiplier = 1.2
        else:
            base_multiplier = 1.0
        
        # Hour of day patterns (in local time)
        hour_multipliers = {
            # Night hours (12 AM - 6 AM): Very low traffic
            0: 0.2, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.2,
            # Morning (6 AM - 9 AM): Increasing traffic
            6: 0.3, 7: 0.5, 8: 0.7,
            # Work hours (9 AM - 12 PM): Moderate traffic
            9: 0.8, 10: 0.9, 11: 1.0,
            # Lunch (12 PM - 2 PM): Peak traffic
            12: 1.3, 13: 1.2,
            # Afternoon (2 PM - 5 PM): Steady traffic
            14: 1.0, 15: 0.9, 16: 0.9,
            # After work (5 PM - 8 PM): Increasing traffic
            17: 1.1, 18: 1.3, 19: 1.5,
            # Evening (8 PM - 12 AM): Peak traffic
            20: 1.8, 21: 2.0, 22: 1.7, 23: 1.2
        }
        
        # Special events can cause traffic spikes
        # For example, simulate a flash sale every day at 8 PM
        if hour == 20 and random.random() < 0.1:  # 10% chance of flash sale
            logger.info("🎯 Flash sale event! Traffic spike initiated")
            return base_multiplier * hour_multipliers[hour] * 3.0
        
        return base_multiplier * hour_multipliers.get(hour, 1.0)
    
    def _generate_batch_events(self, num_events: int):
        """
        Generate a batch of mixed events (clickstream + orders).
        
        The ratio of clickstream to orders reflects real patterns where
        many people browse but fewer complete purchases.
        """
        # Typical e-commerce conversion rate is 2-3%
        # So roughly 97-98% of events are clickstream, 2-3% result in orders
        order_probability = 0.02
        
        # Submit event generation tasks to thread pool
        futures = []
        
        for _ in range(num_events):
            customer = random.choice(self.customers)
            
            if random.random() < order_probability:
                # Generate an order
                future = self.executor.submit(self._generate_and_send_order, customer)
            else:
                # Generate clickstream event
                future = self.executor.submit(self._generate_and_send_clickstream, customer)
            
            futures.append(future)
        
        # Wait for all events to be processed
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error generating event: {e}")
    
    def _generate_and_send_clickstream(self, customer: Dict):
        """Generate and send a clickstream event."""
        # Check if customer has an active session
        session_id = self.active_sessions.get(customer['customer_id'])
        
        # 70% chance to continue existing session, 30% to start new
        if session_id and random.random() < 0.7:
            event = self.generator.generate_clickstream_event(customer, session_id)
        else:
            # Start new session
            event = self.generator.generate_clickstream_event(customer)
            self.active_sessions[customer['customer_id']] = event['session_id']
        
        # Send to Kafka or write to file
        self._send_event('clickstream', event, key=customer['customer_id'])
        
    def _generate_and_send_order(self, customer: Dict):
        """Generate and send an order."""
        order = self.generator.generate_order(customer)
        
        # Also generate a checkout completion event
        if customer['customer_id'] in self.active_sessions:
            checkout_event = {
                'event_id': self.generator.fake.uuid4(),
                'customer_id': customer['customer_id'],
                'session_id': self.active_sessions[customer['customer_id']],
                'event_type': 'checkout_complete',
                'timestamp': order['order_date'],
                'order_id': order['order_id'],
                'order_total': order['total']
            }
            self._send_event('clickstream', checkout_event, key=customer['customer_id'])
        
        # Send order to orders topic
        self._send_event('orders', order, key=customer['customer_id'])
        
    def _send_event(self, topic: str, event: Dict, key: Optional[str] = None):
        """
        Send event to Kafka or write to file as fallback.
        
        Args:
            topic: Kafka topic name ('clickstream' or 'orders')
            event: Event data to send
            key: Optional key for Kafka partitioning
        """
        if self.kafka_producer:
            try:
                # Send to Kafka asynchronously
                future = self.kafka_producer.send(
                    topic,
                    value=event,
                    key=key
                )
                
                # Add callback for error handling
                future.add_callback(self._on_send_success, topic)
                future.add_errback(self._on_send_error, topic, event)
                
            except (KafkaError, KafkaTimeoutError) as e:
                logger.error(f"Kafka send failed: {e}")
                self.stats['kafka_failures'] += 1
                self._write_to_file(topic, event)
        else:
            # Fallback to file writing
            self._write_to_file(topic, event)
    
    def _on_send_success(self, record_metadata, topic):
        """Callback for successful Kafka sends."""
        if topic == 'clickstream':
            self.stats['events_sent'] += 1
        else:
            self.stats['orders_sent'] += 1
    
    def _on_send_error(self, excp, topic, event):
        """Callback for failed Kafka sends."""
        logger.error(f'Failed to send to {topic}: {excp}')
        self.stats['kafka_failures'] += 1
        # Write to file as fallback
        self._write_to_file(topic, event)
    
    def _write_to_file(self, topic: str, event: Dict):
        """
        Write event to file when Kafka is unavailable.
        
        Files are organized by date and topic for easy processing.
        """
        date_str = datetime.now().strftime('%Y%m%d')
        hour_str = datetime.now().strftime('%H')
        
        # Create hourly files
        filename = self.output_dir / topic / f'{date_str}_{hour_str}.jsonl'
        
        try:
            with open(filename, 'a') as f:
                f.write(json.dumps(event) + '\n')
            
            # Update statistics
            if topic == 'clickstream':
                self.stats['events_sent'] += 1
            else:
                self.stats['orders_sent'] += 1
                
        except Exception as e:
            logger.error(f"Failed to write to file {filename}: {e}")
    
    def _log_statistics(self):
        """Log current simulation statistics."""
        runtime = (datetime.now() - self.stats['start_time']).total_seconds()
        events_per_second = self.stats['events_sent'] / max(runtime, 1)
        
        logger.info(
            f"📊 Stats - Events: {self.stats['events_sent']}, "
            f"Orders: {self.stats['orders_sent']}, "
            f"Rate: {events_per_second:.2f} events/sec, "
            f"Kafka failures: {self.stats['kafka_failures']}"
        )
    
    def _shutdown(self):
        """Clean shutdown of the simulator."""
        logger.info("Shutting down stream simulator...")
        
        # Stop executor
        self.executor.shutdown(wait=True)
        
        # Flush and close Kafka producer
        if self.kafka_producer:
            logger.info("Flushing Kafka producer...")
            self.kafka_producer.flush(timeout=10)
            self.kafka_producer.close()
        
        # Final statistics
        self._log_statistics()
        logger.info("✅ Stream simulator stopped successfully")
    
    def generate_historical_data(self, days: int = 30, events_per_day: int = 10000):
        """
        Generate historical data for initial pipeline testing.
        
        This creates data files for the past N days to simulate
        having historical data to process.
        """
        logger.info(f"Generating {days} days of historical data...")
        
        for day_offset in range(days, 0, -1):
            date = datetime.now() - timedelta(days=day_offset)
            date_str = date.strftime('%Y%m%d')
            
            logger.info(f"Generating data for {date_str}...")
            
            # Generate events for each hour of the day
            for hour in range(24):
                timestamp = date.replace(hour=hour, minute=0, second=0)
                
                # Vary events by hour
                hour_multiplier = self._calculate_traffic_multiplier()
                hour_events = int(events_per_day / 24 * hour_multiplier)
                
                # Generate clickstream events
                clickstream_file = self.output_dir / 'clickstream' / f'{date_str}_{hour:02d}.jsonl'
                orders_file = self.output_dir / 'orders' / f'{date_str}_{hour:02d}.jsonl'
                
                with open(clickstream_file, 'w') as cf, open(orders_file, 'w') as of:
                    for _ in range(hour_events):
                        customer = random.choice(self.customers)
                        
                        # Generate event with historical timestamp
                        event = self.generator.generate_clickstream_event(customer)
                        event_time = timestamp + timedelta(
                            minutes=random.randint(0, 59),
                            seconds=random.randint(0, 59)
                        )
                        event['timestamp'] = event_time.isoformat()
                        
                        cf.write(json.dumps(event) + '\n')
                        
                        # Occasionally generate an order
                        if random.random() < 0.02:  # 2% conversion rate
                            order = self.generator.generate_order(customer, event_time)
                            of.write(json.dumps(order) + '\n')
            
            logger.info(f"✅ Generated data for {date_str}")
        
        logger.info(f"✅ Historical data generation complete!")


# Main execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Data Stream Simulator')
    parser.add_argument('--mode', choices=['stream', 'historical'], default='stream',
                        help='Run mode: stream (real-time) or historical (batch generation)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration in minutes for stream mode')
    parser.add_argument('--days', type=int, default=30,
                        help='Number of days for historical mode')
    parser.add_argument('--customers', type=int, default=100,
                        help='Number of customers to simulate')
    parser.add_argument('--kafka-servers', type=str,
                        help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--output-dir', type=str,
                        help='Output directory for data files')
    parser.add_argument('--no-real-time', action='store_true',
                        help='Disable real-time delays (generate as fast as possible)')
    
    args = parser.parse_args()
    
    # Create simulator
    simulator = DataStreamSimulator(
        kafka_bootstrap_servers=args.kafka_servers,
        output_dir=args.output_dir,
        num_customers=args.customers
    )
    
    try:
        if args.mode == 'stream':
            # Real-time streaming mode
            simulator.simulate_traffic(
                duration_minutes=args.duration,
                real_time=not args.no_real_time
            )
        else:
            # Historical data generation mode
            simulator.generate_historical_data(days=args.days)
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Simulator error: {e}")
        raise
    finally:
        if simulator.kafka_producer:
            simulator.kafka_producer.close()