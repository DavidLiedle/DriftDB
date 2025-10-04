#!/usr/bin/env python3
"""Test PostgreSQL connection to DriftDB server"""

import sys

# Try to connect with basic socket communication first
import socket

def test_postgres_connection():
    """Test basic PostgreSQL connection"""
    try:
        # Connect to server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 5433))
        print("✓ Connected to DriftDB server on port 5433")

        # Send SSL request (8 bytes: length=8, code=80877103)
        ssl_request = bytes([0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        sock.send(ssl_request)
        print("✓ Sent SSL request")

        # Read response (should be 'N' for no SSL)
        response = sock.recv(1)
        if response == b'N':
            print("✓ Server responded: No SSL (as expected)")
        else:
            print(f"✗ Unexpected response: {response}")

        # Send startup message
        # Version 3.0 = 196608 = 0x00030000
        startup_params = b'user\x00driftdb\x00database\x00driftdb\x00\x00'
        startup_len = 4 + 4 + len(startup_params)  # length + version + params

        startup_msg = (
            startup_len.to_bytes(4, 'big') +  # Total length
            bytes([0, 3, 0, 0]) +  # Version 3.0
            startup_params  # Parameters
        )

        sock.send(startup_msg)
        print("✓ Sent startup message")

        # Read authentication request
        auth_response = sock.recv(1024)
        if auth_response:
            print(f"✓ Received authentication request ({len(auth_response)} bytes)")
            # Check if it's AuthenticationCleartextPassword (type 'R', auth type 3)
            if auth_response[0] == ord('R'):
                print("✓ Server requests authentication")

        sock.close()
        print("\n✓ Basic PostgreSQL protocol test successful!")
        return True

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_postgres_connection()
    sys.exit(0 if success else 1)