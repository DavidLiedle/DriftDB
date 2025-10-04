"""
Test data generation utilities for DriftDB tests
"""
import random
import string
from datetime import datetime, timedelta
from typing import List, Tuple, Any
import json


class TestDataGenerator:
    """Generate test data for DriftDB tests"""

    def __init__(self, seed: int = None):
        """Initialize generator with optional seed for reproducibility"""
        if seed:
            random.seed(seed)

    @staticmethod
    def random_string(length: int = 10) -> str:
        """Generate a random string"""
        return ''.join(random.choices(string.ascii_letters, k=length))

    @staticmethod
    def random_email() -> str:
        """Generate a random email address"""
        username = TestDataGenerator.random_string(8).lower()
        domain = random.choice(['example.com', 'test.org', 'demo.net'])
        return f"{username}@{domain}"

    @staticmethod
    def random_name() -> str:
        """Generate a random person name"""
        first_names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank',
                      'Grace', 'Henry', 'Iris', 'Jack', 'Kate', 'Leo']
        last_names = ['Smith', 'Johnson', 'Brown', 'Wilson', 'Moore',
                     'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White']
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    @staticmethod
    def random_date(start_date: datetime = None,
                   end_date: datetime = None) -> datetime:
        """Generate a random date between start and end"""
        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now()

        time_delta = end_date - start_date
        random_days = random.randint(0, time_delta.days)
        return start_date + timedelta(days=random_days)

    @staticmethod
    def random_json(depth: int = 2) -> dict:
        """Generate random JSON object"""
        if depth <= 0:
            return random.choice([
                TestDataGenerator.random_string(),
                random.randint(0, 1000),
                random.random(),
                random.choice([True, False]),
                None
            ])

        obj = {}
        num_fields = random.randint(1, 5)
        for _ in range(num_fields):
            key = TestDataGenerator.random_string(5)
            if random.random() < 0.7:  # 70% chance of simple value
                obj[key] = TestDataGenerator.random_json(0)
            else:  # 30% chance of nested object
                obj[key] = TestDataGenerator.random_json(depth - 1)

        return obj

    def generate_users(self, count: int = 100) -> List[Tuple]:
        """Generate user records"""
        users = []
        for i in range(1, count + 1):
            users.append((
                i,  # id
                self.random_name(),  # name
                self.random_email(),  # email
                random.randint(18, 80),  # age
                random.choice(['active', 'inactive', 'pending']),  # status
                self.random_date()  # created_at
            ))
        return users

    def generate_orders(self, count: int = 1000,
                       user_count: int = 100) -> List[Tuple]:
        """Generate order records"""
        orders = []
        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']

        for i in range(1, count + 1):
            orders.append((
                i,  # order_id
                random.randint(1, user_count),  # user_id
                round(random.uniform(10.0, 1000.0), 2),  # total
                random.choice(statuses),  # status
                self.random_date(),  # order_date
                json.dumps(self.random_json())  # metadata
            ))
        return orders

    def generate_events(self, count: int = 10000) -> List[Tuple]:
        """Generate event records for time-series data"""
        events = []
        event_types = ['click', 'view', 'purchase', 'signup', 'logout']
        current_time = datetime.now() - timedelta(days=30)

        for i in range(1, count + 1):
            events.append((
                i,  # event_id
                random.choice(event_types),  # event_type
                random.randint(1, 1000),  # user_id
                json.dumps(self.random_json(1)),  # payload
                current_time  # timestamp
            ))
            # Advance time randomly
            current_time += timedelta(seconds=random.randint(1, 60))

        return events

    def generate_hierarchical_data(self, levels: int = 3,
                                  items_per_level: int = 3) -> List[Tuple]:
        """Generate hierarchical/tree data"""
        data = []
        id_counter = 1

        def generate_level(parent_id: int, level: int):
            nonlocal id_counter
            if level >= levels:
                return

            for _ in range(items_per_level):
                current_id = id_counter
                data.append((
                    current_id,  # id
                    parent_id,  # parent_id
                    self.random_string(10),  # name
                    level,  # level
                    json.dumps({'data': self.random_json(1)})  # metadata
                ))
                id_counter += 1
                generate_level(current_id, level + 1)

        # Generate root nodes
        for _ in range(items_per_level):
            current_id = id_counter
            data.append((
                current_id,  # id
                None,  # parent_id (NULL for root)
                self.random_string(10),  # name
                0,  # level
                json.dumps({'data': self.random_json(1)})  # metadata
            ))
            id_counter += 1
            generate_level(current_id, 1)

        return data

    @staticmethod
    def generate_time_series(start_time: datetime,
                           interval_seconds: int,
                           count: int) -> List[Tuple]:
        """Generate time-series data with regular intervals"""
        data = []
        current_time = start_time

        for i in range(count):
            data.append((
                i + 1,  # id
                current_time,  # timestamp
                random.uniform(0, 100),  # value
                random.choice(['sensor1', 'sensor2', 'sensor3'])  # source
            ))
            current_time += timedelta(seconds=interval_seconds)

        return data

    @staticmethod
    def generate_large_text(size_kb: int = 1) -> str:
        """Generate large text data for testing text handling"""
        chars_needed = size_kb * 1024
        text = ""
        while len(text) < chars_needed:
            text += TestDataGenerator.random_string(100) + " "
        return text[:chars_needed]


# Convenience functions
def generate_test_users(count: int = 100) -> List[Tuple]:
    """Generate test user data"""
    gen = TestDataGenerator()
    return gen.generate_users(count)


def generate_test_orders(count: int = 1000) -> List[Tuple]:
    """Generate test order data"""
    gen = TestDataGenerator()
    return gen.generate_orders(count)


def generate_test_events(count: int = 10000) -> List[Tuple]:
    """Generate test event data"""
    gen = TestDataGenerator()
    return gen.generate_events(count)