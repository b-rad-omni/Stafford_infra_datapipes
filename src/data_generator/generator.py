"""
E-commerce Data Generator Module

This module creates realistic fake data for our data pipeline, simulating
an online store with customers, products, and transactions. The data patterns
mimic real-world e-commerce behavior including:
- Customer profiles with realistic demographics
- Product catalog with categories and pricing
- Shopping behavior patterns (browsing, cart actions, purchases)
- Temporal patterns (peak hours, seasonal trends)
"""

import json
import random
from datetime import datetime, timedelta
from faker import Faker
import logging
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Configure logging with a clear format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EcommerceDataGenerator:
    """
    Generates realistic e-commerce data for our pipeline.
    
    This class creates fake but realistic data including:
    - Customer profiles
    - Product catalog
    - Orders with multiple items
    - Clickstream events (page views, searches, cart actions)
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the data generator.
        
        Args:
            seed: Random seed for reproducible data generation
        """
        # Set random seed for reproducibility if provided
        if seed:
            random.seed(seed)
            Faker.seed(seed)
        
        self.fake = Faker()
        self.products = self._initialize_products()
        self.customers = []  # Will be populated as we generate customers
        self.order_counter = 1000  # Starting order number
        
        logger.info(f"Initialized data generator with {len(self.products)} products")
        
    def _initialize_products(self) -> List[Dict]:
        """
        Create a realistic product catalog.
        
        Returns a list of product dictionaries with pricing that makes sense
        for each category. Products have both a selling price and cost to
        enable profit margin calculations.
        """
        # Define product categories with realistic items and price ranges
        categories = {
            'Electronics': {
                'items': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 
                         'Camera', 'Monitor', 'Keyboard', 'Mouse', 'Speaker'],
                'price_range': (50, 2000),
                'margin': 0.15  # 15% profit margin
            },
            'Clothing': {
                'items': ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes', 
                         'Sweater', 'Shorts', 'Hat', 'Socks', 'Belt'],
                'price_range': (10, 200),
                'margin': 0.40  # 40% profit margin
            },
            'Home & Garden': {
                'items': ['Lamp', 'Chair', 'Table', 'Rug', 'Curtains', 
                         'Plant Pot', 'Bedding', 'Mirror', 'Clock', 'Vase'],
                'price_range': (15, 500),
                'margin': 0.35  # 35% profit margin
            },
            'Books': {
                'items': ['Fiction Novel', 'Biography', 'Cookbook', 'Textbook', 'Comics',
                         'Self-Help', 'History', 'Science', 'Art Book', 'Travel Guide'],
                'price_range': (5, 50),
                'margin': 0.25  # 25% profit margin
            },
            'Sports & Outdoors': {
                'items': ['Yoga Mat', 'Dumbbell Set', 'Running Shoes', 'Bicycle', 'Tent',
                         'Backpack', 'Water Bottle', 'Fitness Tracker', 'Ball', 'Helmet'],
                'price_range': (10, 800),
                'margin': 0.30  # 30% profit margin
            }
        }
        
        products = []
        product_id = 1
        
        # Generate multiple variants of each product
        for category, config in categories.items():
            for item in config['items']:
                # Create 3-5 variants of each item (different brands/models)
                num_variants = random.randint(3, 5)
                
                for i in range(num_variants):
                    # Generate a brand name and model
                    brand = self.fake.company().split()[0]  # Take first word of company name
                    model = f"{self.fake.word().title()} {random.choice(['Pro', 'Plus', 'Elite', 'Basic', 'Premium'])}"
                    
                    # Calculate price within the category range
                    min_price, max_price = config['price_range']
                    price = round(random.uniform(min_price, max_price), 2)
                    
                    # Calculate cost based on margin
                    cost = round(price * (1 - config['margin']), 2)
                    
                    products.append({
                        'product_id': f'PROD-{product_id:04d}',
                        'name': f'{brand} {item} {model}',
                        'category': category,
                        'subcategory': item,
                        'brand': brand,
                        'price': price,
                        'cost': cost,
                        'margin': round(price - cost, 2),
                        'margin_percentage': config['margin'],
                        'in_stock': random.choice([True, True, True, False]),  # 75% in stock
                        'rating': round(random.uniform(3.0, 5.0), 1),
                        'review_count': random.randint(0, 500)
                    })
                    product_id += 1
                    
        return products
    
    def generate_customer(self) -> Dict:
        """
        Generate a realistic customer profile.
        
        Creates customers with:
        - Consistent names and contact information
        - Realistic address data
        - Customer lifetime value indicators
        - Registration dates that make sense
        """
        # Generate basic customer info
        first_name = self.fake.first_name()
        last_name = self.fake.last_name()
        
        # Create email that matches the name (more realistic)
        email_providers = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{random.choice(email_providers)}"
        
        # Generate address components
        address = self.fake.street_address()
        city = self.fake.city()
        state = self.fake.state()
        zip_code = self.fake.zipcode()
        
        # Customer segment (affects behavior)
        segments = ['budget', 'regular', 'premium', 'vip']
        segment_weights = [40, 35, 20, 5]  # Distribution of customer types
        segment = random.choices(segments, weights=segment_weights)[0]
        
        # Registration date (customers registered over the past 3 years)
        days_since_registration = random.randint(1, 1095)  # 1 to 3 years
        registration_date = datetime.now() - timedelta(days=days_since_registration)
        
        customer = {
            'customer_id': f'CUST-{self.fake.uuid4()[:8].upper()}',
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': self.fake.phone_number(),
            'address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'country': 'USA',
            'registration_date': registration_date.isoformat(),
            'customer_segment': segment,
            'preferred_categories': random.sample(
                ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports & Outdoors'], 
                k=random.randint(1, 3)
            ),
            'marketing_consent': random.choice([True, True, False]),  # 66% opt-in
            'account_status': 'active'
        }
        
        # Add to our customer list for future reference
        self.customers.append(customer)
        
        return customer
    
    def generate_order(self, customer: Dict, order_date: Optional[datetime] = None) -> Dict:
        """
        Generate a realistic order for a customer.
        
        Order characteristics are influenced by:
        - Customer segment (VIP customers buy more)
        - Time of day (affects payment methods)
        - Product availability
        """
        # Use provided date or generate one
        if not order_date:
            order_date = self.fake.date_time_between(start_date='-30d', end_date='now')
        
        # Determine number of items based on customer segment
        segment_item_weights = {
            'budget': [60, 30, 8, 2],      # Mostly 1-2 items
            'regular': [40, 40, 15, 5],     # Balanced
            'premium': [20, 40, 30, 10],    # More items
            'vip': [10, 30, 40, 20]         # Bulk buyers
        }
        
        weights = segment_item_weights.get(customer['customer_segment'], [50, 30, 15, 5])
        num_items = random.choices([1, 2, 3, 4], weights=weights)[0]
        
        # Select products based on customer preferences
        available_products = [p for p in self.products if p['in_stock']]
        
        # Filter by preferred categories if possible
        preferred_products = [
            p for p in available_products 
            if p['category'] in customer.get('preferred_categories', [])
        ]
        
        # Use preferred products if available, otherwise any products
        product_pool = preferred_products if len(preferred_products) >= num_items else available_products
        ordered_products = random.sample(product_pool, min(num_items, len(product_pool)))
        
        # Generate order items
        order_items = []
        subtotal = 0
        
        for product in ordered_products:
            # Quantity based on product type and customer segment
            if product['category'] in ['Books', 'Clothing']:
                quantity = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
            else:
                quantity = random.choices([1, 2], weights=[85, 15])[0]
            
            # Apply occasional discounts
            discount_percentage = random.choices(
                [0, 0.10, 0.15, 0.20, 0.25],
                weights=[70, 15, 8, 5, 2]
            )[0]
            
            unit_price = product['price']
            discounted_price = round(unit_price * (1 - discount_percentage), 2)
            item_total = round(discounted_price * quantity, 2)
            
            order_items.append({
                'product_id': product['product_id'],
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percentage': discount_percentage,
                'discounted_price': discounted_price,
                'total_price': item_total
            })
            
            subtotal += item_total
        
        # Calculate order totals
        tax_rate = 0.08  # 8% sales tax
        
        # Free shipping for orders over $50 or for premium/vip customers
        shipping = 0 if subtotal >= 50 or customer['customer_segment'] in ['premium', 'vip'] else 5.99
        
        tax = round(subtotal * tax_rate, 2)
        total = round(subtotal + tax + shipping, 2)
        
        # Determine order status based on date
        days_old = (datetime.now() - order_date).days
        if days_old > 7:
            status = 'delivered'
        elif days_old > 3:
            status = random.choice(['shipped', 'delivered'])
        elif days_old > 1:
            status = random.choice(['processing', 'shipped'])
        else:
            status = random.choice(['pending', 'processing'])
        
        # Payment method varies by time of day and customer segment
        hour = order_date.hour
        if customer['customer_segment'] in ['premium', 'vip']:
            payment_method = random.choices(
                ['credit_card', 'paypal', 'apple_pay'],
                weights=[60, 25, 15]
            )[0]
        else:
            payment_method = random.choices(
                ['credit_card', 'debit_card', 'paypal'],
                weights=[45, 40, 15]
            )[0]
        
        order = {
            'order_id': f'ORD-{self.order_counter:06d}',
            'customer_id': customer['customer_id'],
            'order_date': order_date.isoformat(),
            'status': status,
            'items': order_items,
            'item_count': len(order_items),
            'total_quantity': sum(item['quantity'] for item in order_items),
            'subtotal': round(subtotal, 2),
            'tax': tax,
            'shipping': shipping,
            'total': total,
            'payment_method': payment_method,
            'shipping_address': {
                'address': customer['address'],
                'city': customer['city'],
                'state': customer['state'],
                'zip_code': customer['zip_code'],
                'country': customer['country']
            },
            'device': random.choice(['web', 'mobile_app', 'mobile_web']),
            'customer_segment': customer['customer_segment']
        }
        
        self.order_counter += 1
        return order
    
    def generate_clickstream_event(self, customer: Dict, session_id: Optional[str] = None) -> Dict:
        """
        Generate website interaction events.
        
        Creates realistic browsing patterns including:
        - Page views with realistic navigation patterns
        - Product interactions
        - Search behavior
        - Cart actions that align with purchasing patterns
        """
        # Event types and their relative frequencies
        event_weights = {
            'page_view': 40,
            'product_view': 25,
            'search': 15,
            'add_to_cart': 10,
            'remove_from_cart': 3,
            'checkout_start': 5,
            'checkout_complete': 2
        }
        
        event_type = random.choices(
            list(event_weights.keys()),
            weights=list(event_weights.values())
        )[0]
        
        # Generate or use provided session ID
        if not session_id:
            session_id = f'SESSION-{self.fake.uuid4()[:12].upper()}'
        
        # Base event structure
        event = {
            'event_id': self.fake.uuid4(),
            'customer_id': customer['customer_id'],
            'session_id': session_id,
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'device_type': self._detect_device_type(self.fake.user_agent()),
            'referrer': random.choice([
                'https://google.com',
                'https://facebook.com',
                'direct',
                'https://instagram.com',
                'email_campaign'
            ])
        }
        
        # Add event-specific data
        if event_type == 'page_view':
            pages = [
                '/', '/products', '/about', '/contact', '/deals',
                '/category/electronics', '/category/clothing', '/cart', '/account'
            ]
            event['page_url'] = random.choice(pages)
            event['page_title'] = self._get_page_title(event['page_url'])
            
        elif event_type in ['product_view', 'add_to_cart', 'remove_from_cart']:
            product = random.choice(self.products)
            event['product_id'] = product['product_id']
            event['product_name'] = product['name']
            event['product_category'] = product['category']
            event['product_price'] = product['price']
            event['page_url'] = f'/product/{product["product_id"]}'
            
            if event_type in ['add_to_cart', 'remove_from_cart']:
                event['quantity'] = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
                
        elif event_type == 'search':
            # Generate realistic search queries
            search_terms = [
                'laptop', 'shoes', 'phone case', 'desk lamp', 'running shoes',
                'wireless headphones', 'coffee maker', 'yoga mat', 'backpack'
            ]
            event['search_query'] = random.choice(search_terms)
            event['results_count'] = random.randint(0, 150)
            event['page_url'] = f'/search?q={event["search_query"].replace(" ", "+")}'
            
        elif event_type in ['checkout_start', 'checkout_complete']:
            event['page_url'] = '/checkout'
            event['checkout_step'] = 'start' if event_type == 'checkout_start' else 'complete'
            if event_type == 'checkout_complete':
                # Add order ID for completed checkouts
                event['order_id'] = f'ORD-{self.order_counter:06d}'
        
        return event
    
    def _detect_device_type(self, user_agent: str) -> str:
        """Detect device type from user agent string."""
        user_agent_lower = user_agent.lower()
        
        if any(device in user_agent_lower for device in ['iphone', 'android', 'mobile']):
            return 'mobile'
        elif any(device in user_agent_lower for device in ['ipad', 'tablet']):
            return 'tablet'
        else:
            return 'desktop'
    
    def _get_page_title(self, url: str) -> str:
        """Generate appropriate page title for URL."""
        titles = {
            '/': 'Home - Your Online Store',
            '/products': 'All Products',
            '/about': 'About Us',
            '/contact': 'Contact Us',
            '/deals': 'Special Deals',
            '/cart': 'Shopping Cart',
            '/account': 'My Account'
        }
        
        # Handle category pages
        if '/category/' in url:
            category = url.split('/')[-1].title()
            return f'{category} Products'
        
        return titles.get(url, 'Page')
    
    def generate_customer_session(self, customer: Dict, num_events: int = None) -> List[Dict]:
        """
        Generate a complete browsing session for a customer.
        
        This creates a realistic sequence of events that might lead to a purchase.
        Sessions follow common patterns like:
        - Browse -> View Products -> Add to Cart -> Checkout
        - Search -> View Results -> View Product -> Leave
        """
        if num_events is None:
            # Most sessions are short, some are long
            num_events = random.choices(
                [1, 2, 3, 5, 8, 12, 20],
                weights=[10, 20, 25, 20, 15, 8, 2]
            )[0]
        
        session_id = f'SESSION-{self.fake.uuid4()[:12].upper()}'
        events = []
        
        # Sessions usually start with homepage or search
        first_event_type = random.choices(
            ['page_view', 'search'],
            weights=[70, 30]
        )[0]
        
        # Track session state
        products_viewed = []
        items_in_cart = []
        
        for i in range(num_events):
            if i == 0:
                # First event
                event = self.generate_clickstream_event(customer, session_id)
                if first_event_type == 'page_view':
                    event['event_type'] = 'page_view'
                    event['page_url'] = '/'
                else:
                    event['event_type'] = 'search'
            else:
                # Subsequent events based on session flow
                last_event = events[-1]
                
                if last_event['event_type'] == 'search' and random.random() < 0.7:
                    # After search, likely to view a product
                    event = self.generate_clickstream_event(customer, session_id)
                    event['event_type'] = 'product_view'
                    
                elif last_event['event_type'] == 'product_view' and random.random() < 0.3:
                    # Sometimes add to cart after viewing
                    event = self.generate_clickstream_event(customer, session_id)
                    event['event_type'] = 'add_to_cart'
                    event['product_id'] = last_event.get('product_id')
                    items_in_cart.append(event['product_id'])
                    
                elif len(items_in_cart) > 0 and random.random() < 0.2:
                    # If items in cart, might start checkout
                    event = self.generate_clickstream_event(customer, session_id)
                    event['event_type'] = 'checkout_start'
                    
                elif last_event['event_type'] == 'checkout_start' and random.random() < 0.7:
                    # Often complete checkout after starting
                    event = self.generate_clickstream_event(customer, session_id)
                    event['event_type'] = 'checkout_complete'
                    
                else:
                    # Random event
                    event = self.generate_clickstream_event(customer, session_id)
            
            # Add timestamp progression (events are seconds apart)
            if i > 0:
                last_timestamp = datetime.fromisoformat(events[-1]['timestamp'])
                # Time between events varies (3 seconds to 2 minutes)
                seconds_apart = random.choices(
                    [3, 5, 10, 20, 30, 60, 120],
                    weights=[10, 20, 30, 20, 10, 8, 2]
                )[0]
                new_timestamp = last_timestamp + timedelta(seconds=seconds_apart)
                event['timestamp'] = new_timestamp.isoformat()
            
            events.append(event)
        
        return events


# Example usage and testing
if __name__ == "__main__":
    # Create generator instance
    generator = EcommerceDataGenerator(seed=42)  # Use seed for reproducible testing
    
    # Generate and display sample data
    print("=== Sample Customer ===")
    customer = generator.generate_customer()
    print(json.dumps(customer, indent=2))
    
    print("\n=== Sample Order ===")
    order = generator.generate_order(customer)
    print(json.dumps(order, indent=2))
    
    print("\n=== Sample Clickstream Event ===")
    event = generator.generate_clickstream_event(customer)
    print(json.dumps(event, indent=2))
    
    print("\n=== Sample Session (5 events) ===")
    session_events = generator.generate_customer_session(customer, num_events=5)
    for i, event in enumerate(session_events):
        print(f"Event {i+1}: {event['event_type']} at {event['timestamp']}")
    
    print(f"\n✅ Data generator initialized with {len(generator.products)} products")
    print(f"📊 Product categories: {set(p['category'] for p in generator.products)}")