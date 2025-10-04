#!/usr/bin/env python3
"""Test PostgreSQL connection and queries with DriftDB server"""

import sys
import socket
import struct

def send_message(sock, msg_type, data):
    """Send a PostgreSQL protocol message"""
    if msg_type:
        # Regular message with type
        length = len(data) + 4  # data + length field
        msg = msg_type.encode('ascii') + struct.pack('!I', length) + data
    else:
        # Startup message (no type)
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

def test_queries():
    """Test PostgreSQL queries against DriftDB"""
    try:
        # Connect
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 5433))
        print("✓ Connected to DriftDB on port 5433")

        # Send SSL request
        ssl_request = struct.pack('!II', 8, 80877103)
        sock.send(ssl_request)

        # Get SSL response
        response = sock.recv(1)
        if response == b'N':
            print("✓ SSL negotiation complete (no SSL)")

        # Send startup message
        params = b'user\x00test\x00database\x00driftdb\x00\x00'
        startup = struct.pack('!II', 8 + len(params), 196608) + params
        sock.send(startup)
        print("✓ Sent startup message")

        # Read until ready
        ready = False
        while not ready:
            msg_type, data = recv_message(sock)
            if msg_type == 'R':
                auth_type = struct.unpack('!I', data[:4])[0]
                if auth_type == 0:
                    print("✓ Authentication OK")
            elif msg_type == 'K':
                print("✓ Received backend key data")
            elif msg_type == 'S':
                print("✓ Received parameter status")
            elif msg_type == 'Z':
                status = data[0] if data else 0
                print(f"✓ Ready for query (status: {chr(status) if status else 'unknown'})")
                ready = True

        # Test queries
        print("\n🔍 Testing SQL queries...")

        # Query 1: SELECT 1
        print("\nQuery: SELECT 1")
        send_message(sock, 'Q', b'SELECT 1\x00')

        while True:
            msg_type, data = recv_message(sock)
            if msg_type == 'T':  # Row description
                print("  ✓ Received row description")
            elif msg_type == 'D':  # Data row
                print("  ✓ Received data row")
            elif msg_type == 'C':  # Command complete
                print("  ✓ Command complete")
            elif msg_type == 'Z':  # Ready for query
                print("  ✓ Ready for next query")
                break

        # Query 2: CREATE TABLE
        print("\nQuery: CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR)")
        send_message(sock, 'Q', b'CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR)\x00')

        while True:
            msg_type, data = recv_message(sock)
            if msg_type == 'C':  # Command complete
                print("  ✓ Table created")
            elif msg_type == 'E':  # Error
                print(f"  ✗ Error: {data}")
            elif msg_type == 'Z':  # Ready for query
                break

        # Query 3: INSERT
        print("\nQuery: INSERT INTO test_table (id, name) VALUES (1, 'Alice')")
        send_message(sock, 'Q', b"INSERT INTO test_table (id, name) VALUES (1, 'Alice')\x00")

        while True:
            msg_type, data = recv_message(sock)
            if msg_type == 'C':  # Command complete
                print("  ✓ Insert complete")
            elif msg_type == 'E':  # Error
                print(f"  ✗ Error: {data}")
            elif msg_type == 'Z':  # Ready for query
                break

        # Query 4: SELECT from table
        print("\nQuery: SELECT * FROM test_table")
        send_message(sock, 'Q', b'SELECT * FROM test_table\x00')

        while True:
            msg_type, data = recv_message(sock)
            if msg_type == 'T':  # Row description
                print("  ✓ Received row description")
            elif msg_type == 'D':  # Data row
                print("  ✓ Received data row")
            elif msg_type == 'C':  # Command complete
                print("  ✓ Query complete")
            elif msg_type == 'E':  # Error
                print(f"  ✗ Error: {data}")
            elif msg_type == 'Z':  # Ready for query
                break

        # Terminate
        send_message(sock, 'X', b'')
        sock.close()

        print("\n✅ All tests completed successfully!")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_queries()
    sys.exit(0 if success else 1)