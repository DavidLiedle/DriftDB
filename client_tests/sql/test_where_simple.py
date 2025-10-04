#!/usr/bin/env python3
"""Simple WHERE clause test"""

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
        if msg_type == 'T':
            print("  Row description received")
        elif msg_type == 'D':
            rows += 1
        elif msg_type == 'C':
            tag = data.decode('utf-8', errors='ignore').rstrip('\x00')
            print(f"  {tag} - {rows} rows")
        elif msg_type == 'E':
            error = data.decode('utf-8', errors='ignore')
            print(f"  ERROR: {error}")
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

print("🎯 Simple WHERE Test")
print("=" * 50)

# Create simple table
send_query(sock, "CREATE TABLE test (id INT PRIMARY KEY, value INT)")

# Insert test data
send_query(sock, "INSERT INTO test (id, value) VALUES (1, 100)")
send_query(sock, "INSERT INTO test (id, value) VALUES (2, 200)")
send_query(sock, "INSERT INTO test (id, value) VALUES (3, 300)")

# Test queries
send_query(sock, "SELECT * FROM test")
send_query(sock, "SELECT * FROM test WHERE value = 200")
send_query(sock, "SELECT * FROM test WHERE value > 150")
send_query(sock, "SELECT * FROM test WHERE id = 2")

print("\n✅ Test complete")

# Terminate
send_message(sock, 'X', b'')
sock.close()