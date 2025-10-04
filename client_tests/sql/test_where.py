#!/usr/bin/env python3
"""Test WHERE clause functionality in DriftDB"""

import socket
import struct
import sys

def send_message(sock, msg_type, data):
    """Send a PostgreSQL protocol message"""
    if msg_type:
        length = len(data) + 4
        msg = msg_type.encode('ascii') + struct.pack('!I', length) + data
    else:
        msg = data
    sock.send(msg)

def recv_message(sock):
    """Receive a PostgreSQL protocol message"""
    header = sock.recv(5)
    if len(header) < 5:
        return None, None
    msg_type = chr(header[0])
    length = struct.unpack('!I', header[1:5])[0]
    data = sock.recv(length - 4) if length > 4 else b''
    return msg_type, data

def send_query(sock, query):
    """Send a query and get results"""
    send_message(sock, 'Q', (query + '\x00').encode('utf-8'))
    results = []
    columns = []
    error_msg = None

    while True:
        msg_type, data = recv_message(sock)
        if msg_type == 'T':  # Row description
            # Parse column descriptions
            num_fields = struct.unpack('!H', data[0:2])[0]
            offset = 2
            for _ in range(num_fields):
                # Extract column name
                name_end = data.find(b'\x00', offset)
                col_name = data[offset:name_end].decode('utf-8')
                columns.append(col_name)
                # Skip past the rest of the field description
                offset = name_end + 19
        elif msg_type == 'D':  # Data row
            # Parse data row
            num_values = struct.unpack('!H', data[0:2])[0]
            offset = 2
            row = []
            for _ in range(num_values):
                value_len = struct.unpack('!i', data[offset:offset+4])[0]
                offset += 4
                if value_len == -1:
                    row.append(None)
                else:
                    value = data[offset:offset+value_len].decode('utf-8', errors='ignore')
                    row.append(value)
                    offset += value_len
            results.append(row)
        elif msg_type == 'C':  # Command complete
            tag = data.decode('utf-8', errors='ignore').rstrip('\x00')
            break
        elif msg_type == 'E':  # Error
            error_msg = data.decode('utf-8', errors='ignore')
            print(f"  ❌ Error: {error_msg}")
            break
        elif msg_type == 'Z':  # Ready for query
            break

    return columns, results, error_msg

def main():
    # Connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 5433))

    # SSL negotiation
    sock.send(struct.pack('!II', 8, 80877103))
    sock.recv(1)  # Should be 'N'

    # Startup
    params = b'user\x00test\x00database\x00driftdb\x00\x00'
    sock.send(struct.pack('!II', 8 + len(params), 196608) + params)

    # Read until ready
    while True:
        msg_type, _ = recv_message(sock)
        if msg_type == 'Z':
            break

    print("🎯 DriftDB WHERE Clause Test")
    print("=" * 50)

    # Create test table
    print("\n1️⃣ Creating test table...")
    send_query(sock, "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, department VARCHAR, salary INT)")
    print("  ✓ Table created")

    # Insert test data
    print("\n2️⃣ Inserting test data...")
    test_data = [
        (1, 'Alice', 'Engineering', 120000),
        (2, 'Bob', 'Sales', 85000),
        (3, 'Charlie', 'Engineering', 105000),
        (4, 'Diana', 'HR', 95000),
        (5, 'Eve', 'Sales', 92000),
        (6, 'Frank', 'Engineering', 115000),
    ]

    for id, name, dept, salary in test_data:
        query = f"INSERT INTO employees (id, name, department, salary) VALUES ({id}, '{name}', '{dept}', {salary})"
        send_query(sock, query)
        print(f"  ✓ Inserted: {name}")

    # Test queries with WHERE clauses
    print("\n3️⃣ Testing WHERE clauses...")

    # Test 1: Simple equality
    print("\n  📊 SELECT * FROM employees WHERE department = 'Engineering'")
    columns, results, _ = send_query(sock, "SELECT * FROM employees WHERE department = 'Engineering'")
    if results:
        print(f"  Columns: {columns}")
        for row in results:
            print(f"    {row}")
    else:
        print("    No results")

    # Test 2: Numeric comparison
    print("\n  📊 SELECT * FROM employees WHERE salary > 100000")
    columns, results, _ = send_query(sock, "SELECT * FROM employees WHERE salary > 100000")
    if results:
        print(f"  Columns: {columns}")
        for row in results:
            print(f"    {row}")
    else:
        print("    No results")

    # Test 3: Multiple conditions with AND
    print("\n  📊 SELECT * FROM employees WHERE department = 'Sales' AND salary > 90000")
    columns, results, _ = send_query(sock, "SELECT * FROM employees WHERE department = 'Sales' AND salary > 90000")
    if results:
        print(f"  Columns: {columns}")
        for row in results:
            print(f"    {row}")
    else:
        print("    No results")

    # Test 4: Different operators
    print("\n  📊 SELECT * FROM employees WHERE salary >= 95000 AND salary <= 110000")
    columns, results, _ = send_query(sock, "SELECT * FROM employees WHERE salary >= 95000 AND salary <= 110000")
    if results:
        print(f"  Columns: {columns}")
        for row in results:
            print(f"    {row}")
    else:
        print("    No results")

    # Test 5: Not equal
    print("\n  📊 SELECT * FROM employees WHERE department != 'Sales'")
    columns, results, _ = send_query(sock, "SELECT * FROM employees WHERE department != 'Sales'")
    if results:
        print(f"  Columns: {columns}")
        for row in results:
            print(f"    {row}")
    else:
        print("    No results")

    print("\n✅ WHERE clause tests complete!")

    # Terminate
    send_message(sock, 'X', b'')
    sock.close()

if __name__ == "__main__":
    main()