#!/bin/bash
# DriftDB Working Demo - Showcasing Time-Travel Database Features

set -e

echo "================================================"
echo "     DriftDB Demo - Time-Travel Database       "
echo "================================================"
echo

# Clean up any existing test data
echo "📦 Setting up fresh database..."
rm -rf demo_data
./target/release/driftdb init demo_data

echo
echo "📊 Creating products table..."
./target/release/driftdb sql --data demo_data -e 'CREATE TABLE products (pk=id, INDEX(name, category))'

echo
echo "➕ Inserting initial products..."
./target/release/driftdb sql --data demo_data -e 'INSERT INTO products {"id": "p1", "name": "Laptop", "category": "Electronics", "price": 999, "stock": 10}'
./target/release/driftdb sql --data demo_data -e 'INSERT INTO products {"id": "p2", "name": "Mouse", "category": "Electronics", "price": 25, "stock": 50}'
./target/release/driftdb sql --data demo_data -e 'INSERT INTO products {"id": "p3", "name": "Notebook", "category": "Stationery", "price": 5, "stock": 100}'

echo
echo "📋 Current state of products:"
./target/release/driftdb sql --data demo_data -e 'SELECT * FROM products'

echo
echo "🔄 Updating prices (Black Friday sale - 20% off)..."
./target/release/driftdb sql --data demo_data -e 'PATCH products KEY "p1" SET {"price": 799}'
./target/release/driftdb sql --data demo_data -e 'PATCH products KEY "p2" SET {"price": 20}'
./target/release/driftdb sql --data demo_data -e 'PATCH products KEY "p3" SET {"price": 4}'

echo
echo "📋 Products after price update:"
./target/release/driftdb sql --data demo_data -e 'SELECT * FROM products'

echo
echo "📦 Stock update - items sold..."
./target/release/driftdb sql --data demo_data -e 'PATCH products KEY "p1" SET {"stock": 7}'
./target/release/driftdb sql --data demo_data -e 'PATCH products KEY "p2" SET {"stock": 35}'

echo
echo "⏰ TIME TRAVEL DEMO"
echo "==================="
echo
echo "🕐 Query products at sequence 3 (before any updates):"
./target/release/driftdb sql --data demo_data -e 'SELECT * FROM products AS OF @seq:3'

echo
echo "🕑 Query products at sequence 6 (after price update, before stock update):"
./target/release/driftdb sql --data demo_data -e 'SELECT * FROM products AS OF @seq:6'

echo
echo "🕒 Current state (after all updates):"
./target/release/driftdb sql --data demo_data -e 'SELECT * FROM products'

echo
echo "================================================"
echo "              Demo Complete!                    "
echo "================================================"
echo
echo "Key Features Demonstrated:"
echo "✅ Create tables with indexes"
echo "✅ Insert JSON documents"
echo "✅ Update specific fields with PATCH"
echo "✅ Time-travel queries with AS OF @seq:N"
echo "✅ Full audit trail - nothing is lost!"
echo
echo "DriftDB: Your database with a time machine! 🚀"