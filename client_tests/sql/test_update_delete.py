#!/usr/bin/env python3
"""Test UPDATE and DELETE functionality"""

import socket
import struct

def send_message(sock, msg_type, data):
    if msg_type:
        length = len(data) + 4
        msg = msg_type.encode('ascii') + struct.pack('!I', length) + data
    else:
        msg = data
    sock.send(msg)

def recv_message(sock):
    header = sock.recv(5)
    if len(header) < 5:
        return None, None
    msg_type = chr(header[0])
    length = struct.unpack('!I', header[1:5])[0]
    data = sock.recv(length - 4) if length > 4 else b''
    return msg_type, data

def send_query(sock, query):
    print(f"\n>>> {query}")
    send_message(sock, 'Q', (query + '\x00').encode('utf-8'))
    rows = 0
    while True:
        msg_type, data = recv_message(sock)
        if msg_type == 'D':
            rows += 1
        elif msg_type == 'C':
            tag = data.decode('utf-8', errors='ignore').rstrip('\x00')
            print(f"  ✅ {tag} ({rows} data rows)")
        elif msg_type == 'E':
            error = data.decode('utf-8', errors='ignore')
            print(f"  ❌ ERROR: {error}")
        elif msg_type == 'Z':
            break

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

print("🎯 UPDATE and DELETE Test")
print("=" * 50)

# Create table
send_query(sock, "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price INT, stock INT)")

# Insert test data
print("\n📝 Inserting test data...")
send_query(sock, "INSERT INTO products (id, name, price, stock) VALUES (1, 'Laptop', 999, 10)")
send_query(sock, "INSERT INTO products (id, name, price, stock) VALUES (2, 'Mouse', 25, 100)")
send_query(sock, "INSERT INTO products (id, name, price, stock) VALUES (3, 'Keyboard', 75, 50)")
send_query(sock, "INSERT INTO products (id, name, price, stock) VALUES (4, 'Monitor', 299, 20)")

# Show initial data
print("\n📊 Initial data:")
send_query(sock, "SELECT * FROM products")

# UPDATE tests
print("\n🔄 Testing UPDATE...")

# Update single row
send_query(sock, "UPDATE products SET price = 899 WHERE id = 1")

# Update multiple rows
send_query(sock, "UPDATE products SET stock = stock + 10 WHERE price < 100")

# Update all rows (no WHERE)
send_query(sock, "UPDATE products SET stock = 999")

# Show after updates
print("\n📊 After updates:")
send_query(sock, "SELECT * FROM products")

# DELETE tests
print("\n🗑️ Testing DELETE...")

# Delete single row
send_query(sock, "DELETE FROM products WHERE id = 4")

# Delete multiple rows
send_query(sock, "DELETE FROM products WHERE price < 50")

# Show after deletes
print("\n📊 After deletes:")
send_query(sock, "SELECT * FROM products")

# Test time travel (see deleted data)
print("\n⏰ Time travel (viewing deleted data):")
send_query(sock, "SELECT * FROM products AS OF @seq:1")

print("\n✅ Test complete!")

# Terminate
send_message(sock, 'X', b'')
sock.close()