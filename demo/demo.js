/**
 * DriftDB Interactive Demo
 * A browser-based demo showcasing time-travel database queries
 */

// ============================================================================
// Mock Database with Time-Travel History
// ============================================================================

class DriftDBMock {
    constructor() {
        this.datasets = {
            ecommerce: this.generateEcommerceData(),
            users: this.generateUsersData(),
            inventory: this.generateInventoryData()
        };
        this.currentDataset = 'ecommerce';
    }

    generateEcommerceData() {
        const now = new Date();
        const history = [];

        // Generate history of changes over 10 sequence numbers
        for (let seq = 1; seq <= 10; seq++) {
            const timestamp = new Date(now.getTime() - (10 - seq) * 3600000); // 1 hour apart
            const data = [];

            // Order 1: Pending -> Paid -> Shipped -> Delivered
            if (seq >= 1) {
                data.push({
                    id: 1,
                    customer_id: 101,
                    customer_name: 'Alice Johnson',
                    product: 'Laptop Pro 15',
                    amount: 1299.99,
                    status: seq >= 8 ? 'delivered' : seq >= 5 ? 'shipped' : seq >= 3 ? 'paid' : 'pending',
                    created_at: new Date(now.getTime() - 10 * 3600000).toISOString(),
                    updated_at: timestamp.toISOString()
                });
            }

            // Order 2: Pending -> Cancelled
            if (seq >= 2) {
                data.push({
                    id: 2,
                    customer_id: 102,
                    customer_name: 'Bob Smith',
                    product: 'Wireless Mouse',
                    amount: 29.99,
                    status: seq >= 6 ? 'cancelled' : 'pending',
                    created_at: new Date(now.getTime() - 9 * 3600000).toISOString(),
                    updated_at: timestamp.toISOString()
                });
            }

            // Order 3: Always paid
            if (seq >= 3) {
                data.push({
                    id: 3,
                    customer_id: 103,
                    customer_name: 'Carol Davis',
                    product: 'USB-C Cable',
                    amount: 19.99,
                    status: seq >= 4 ? 'paid' : 'pending',
                    created_at: new Date(now.getTime() - 8 * 3600000).toISOString(),
                    updated_at: timestamp.toISOString()
                });
            }

            // Order 4: Processing -> Shipped
            if (seq >= 5) {
                data.push({
                    id: 4,
                    customer_id: 101,
                    customer_name: 'Alice Johnson',
                    product: 'Mechanical Keyboard',
                    amount: 149.99,
                    status: seq >= 9 ? 'shipped' : 'processing',
                    created_at: new Date(now.getTime() - 6 * 3600000).toISOString(),
                    updated_at: timestamp.toISOString()
                });
            }

            // Order 5: Recent order
            if (seq >= 8) {
                data.push({
                    id: 5,
                    customer_id: 104,
                    customer_name: 'David Wilson',
                    product: 'Monitor 27"',
                    amount: 399.99,
                    status: seq >= 10 ? 'paid' : 'pending',
                    created_at: new Date(now.getTime() - 3 * 3600000).toISOString(),
                    updated_at: timestamp.toISOString()
                });
            }

            history.push({
                seq: seq,
                timestamp: timestamp,
                data: data
            });
        }

        return history;
    }

    generateUsersData() {
        const now = new Date();
        const history = [];

        for (let seq = 1; seq <= 10; seq++) {
            const timestamp = new Date(now.getTime() - (10 - seq) * 3600000);
            const data = [];

            if (seq >= 1) {
                data.push({
                    id: 1,
                    email: 'alice@example.com',
                    name: 'Alice Johnson',
                    status: seq >= 7 ? 'inactive' : 'active',
                    role: 'admin',
                    last_login: seq >= 7 ? new Date(now.getTime() - 4 * 3600000).toISOString() : timestamp.toISOString(),
                    created_at: new Date(now.getTime() - 10 * 3600000).toISOString()
                });
            }

            if (seq >= 2) {
                data.push({
                    id: 2,
                    email: 'bob@example.com',
                    name: 'Bob Smith',
                    status: 'active',
                    role: seq >= 6 ? 'manager' : 'user',
                    last_login: timestamp.toISOString(),
                    created_at: new Date(now.getTime() - 9 * 3600000).toISOString()
                });
            }

            if (seq >= 4) {
                data.push({
                    id: 3,
                    email: 'carol@example.com',
                    name: 'Carol Davis',
                    status: 'active',
                    role: 'user',
                    last_login: timestamp.toISOString(),
                    created_at: new Date(now.getTime() - 7 * 3600000).toISOString()
                });
            }

            if (seq >= 8) {
                data.push({
                    id: 4,
                    email: 'david@example.com',
                    name: 'David Wilson',
                    status: 'active',
                    role: 'user',
                    last_login: timestamp.toISOString(),
                    created_at: new Date(now.getTime() - 3 * 3600000).toISOString()
                });
            }

            history.push({
                seq: seq,
                timestamp: timestamp,
                data: data
            });
        }

        return history;
    }

    generateInventoryData() {
        const now = new Date();
        const history = [];

        for (let seq = 1; seq <= 10; seq++) {
            const timestamp = new Date(now.getTime() - (10 - seq) * 3600000);
            const data = [
                {
                    id: 1,
                    sku: 'LAP-001',
                    product_name: 'Laptop Pro 15',
                    quantity: 50 - seq * 2,
                    price: 1299.99,
                    location: 'Warehouse A',
                    updated_at: timestamp.toISOString()
                },
                {
                    id: 2,
                    sku: 'MOU-002',
                    product_name: 'Wireless Mouse',
                    quantity: 200 - seq * 5,
                    price: 29.99,
                    location: 'Warehouse B',
                    updated_at: timestamp.toISOString()
                },
                {
                    id: 3,
                    sku: 'CAB-003',
                    product_name: 'USB-C Cable',
                    quantity: 500 - seq * 10,
                    price: 19.99,
                    location: 'Warehouse B',
                    updated_at: timestamp.toISOString()
                },
                {
                    id: 4,
                    sku: 'KEY-004',
                    product_name: 'Mechanical Keyboard',
                    quantity: 80 - seq * 3,
                    price: 149.99,
                    location: 'Warehouse A',
                    updated_at: timestamp.toISOString()
                }
            ];

            history.push({
                seq: seq,
                timestamp: timestamp,
                data: data
            });
        }

        return history;
    }

    query(sql, asOfSeq = null) {
        const dataset = this.datasets[this.currentDataset];
        const targetSeq = asOfSeq !== null ? asOfSeq : dataset.length;
        const snapshot = dataset.find(s => s.seq === targetSeq);

        if (!snapshot) {
            throw new Error(`No data found for sequence ${targetSeq}`);
        }

        // Simple SQL parser (supports basic SELECT queries)
        return this.executeQuery(sql, snapshot.data);
    }

    executeQuery(sql, data) {
        sql = sql.trim();

        // Handle SELECT queries
        if (sql.toUpperCase().startsWith('SELECT')) {
            return this.executeSelect(sql, data);
        }

        throw new Error('Only SELECT queries are supported in demo mode');
    }

    executeSelect(sql, data) {
        // Very simple SQL parser - just for demo purposes
        const upperSql = sql.toUpperCase();

        // Extract table name
        const fromMatch = upperSql.match(/FROM\s+(\w+)/);
        if (!fromMatch) {
            throw new Error('FROM clause required');
        }

        let results = [...data];

        // Extract WHERE conditions
        const whereMatch = sql.match(/WHERE\s+(.+?)(?:ORDER BY|LIMIT|GROUP BY|$)/i);
        if (whereMatch) {
            const conditions = whereMatch[1].trim();
            results = results.filter(row => this.evaluateWhere(row, conditions));
        }

        // Extract columns
        const selectMatch = sql.match(/SELECT\s+(.+?)\s+FROM/i);
        if (selectMatch) {
            const columnsStr = selectMatch[1].trim();
            if (columnsStr !== '*') {
                const columns = columnsStr.split(',').map(c => c.trim());
                results = results.map(row => {
                    const newRow = {};
                    columns.forEach(col => {
                        const colName = col.split(/\s+as\s+/i)[0].trim();
                        const alias = col.split(/\s+as\s+/i)[1]?.trim() || colName;
                        if (row.hasOwnProperty(colName)) {
                            newRow[alias] = row[colName];
                        }
                    });
                    return newRow;
                });
            }
        }

        // Handle COUNT(*)
        if (upperSql.includes('COUNT(*)')) {
            return [{ 'count': results.length }];
        }

        // Handle LIMIT
        const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
        if (limitMatch) {
            const limit = parseInt(limitMatch[1]);
            results = results.slice(0, limit);
        }

        return results;
    }

    evaluateWhere(row, conditions) {
        // Very simple condition evaluator
        // Supports: column = 'value', column != 'value', column > number, column < number

        // Handle AND
        if (conditions.toUpperCase().includes(' AND ')) {
            const parts = conditions.split(/\s+AND\s+/i);
            return parts.every(part => this.evaluateWhere(row, part));
        }

        // Handle OR
        if (conditions.toUpperCase().includes(' OR ')) {
            const parts = conditions.split(/\s+OR\s+/i);
            return parts.some(part => this.evaluateWhere(row, part));
        }

        // Parse single condition
        let match = conditions.match(/(\w+)\s*=\s*['"]([^'"]+)['"]/);
        if (match) {
            const [, column, value] = match;
            return row[column] == value;
        }

        match = conditions.match(/(\w+)\s*!=\s*['"]([^'"]+)['"]/);
        if (match) {
            const [, column, value] = match;
            return row[column] != value;
        }

        match = conditions.match(/(\w+)\s*>\s*(\d+\.?\d*)/);
        if (match) {
            const [, column, value] = match;
            return parseFloat(row[column]) > parseFloat(value);
        }

        match = conditions.match(/(\w+)\s*<\s*(\d+\.?\d*)/);
        if (match) {
            const [, column, value] = match;
            return parseFloat(row[column]) < parseFloat(value);
        }

        match = conditions.match(/(\w+)\s*>=\s*(\d+\.?\d*)/);
        if (match) {
            const [, column, value] = match;
            return parseFloat(row[column]) >= parseFloat(value);
        }

        match = conditions.match(/(\w+)\s*<=\s*(\d+\.?\d*)/);
        if (match) {
            const [, column, value] = match;
            return parseFloat(row[column]) <= parseFloat(value);
        }

        return true;
    }

    getTableName() {
        const names = {
            'ecommerce': 'orders',
            'users': 'users',
            'inventory': 'inventory'
        };
        return names[this.currentDataset];
    }

    getCurrentSnapshot() {
        return this.datasets[this.currentDataset][this.datasets[this.currentDataset].length - 1];
    }

    getSnapshotAt(seq) {
        return this.datasets[this.currentDataset].find(s => s.seq === seq);
    }
}

// ============================================================================
// UI Controller
// ============================================================================

class DemoController {
    constructor() {
        this.db = new DriftDBMock();
        this.currentSeq = 10; // Start at latest
        this.exampleQueries = this.getExampleQueries();
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.populateExampleQueries();
        this.updateTimeDisplay();
        this.loadDefaultQuery();
    }

    setupEventListeners() {
        // Time slider
        document.getElementById('time-slider').addEventListener('input', (e) => {
            this.currentSeq = parseInt(e.target.value);
            this.updateTimeDisplay();
        });

        // Run query button
        document.getElementById('run-query').addEventListener('click', () => {
            this.runQuery();
        });

        // Clear editor button
        document.getElementById('clear-editor').addEventListener('click', () => {
            document.getElementById('sql-editor').value = '';
        });

        // Format query button
        document.getElementById('format-query').addEventListener('click', () => {
            this.formatQuery();
        });

        // Dataset selector
        document.getElementById('dataset-selector').addEventListener('change', (e) => {
            this.db.currentDataset = e.target.value;
            this.exampleQueries = this.getExampleQueries();
            this.populateExampleQueries();
            this.loadDefaultQuery();
            this.clearResults();
        });

        // Enter key in editor (Ctrl+Enter to run)
        document.getElementById('sql-editor').addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                this.runQuery();
            }
        });
    }

    getExampleQueries() {
        const dataset = this.db.currentDataset;
        const tableName = this.db.getTableName();

        const queries = {
            ecommerce: [
                {
                    title: '📦 All Orders',
                    description: 'View all orders at current time',
                    sql: `SELECT * FROM ${tableName}`
                },
                {
                    title: '✅ Shipped Orders',
                    description: 'Find orders that are shipped',
                    sql: `SELECT id, customer_name, product, status FROM ${tableName} WHERE status = 'shipped'`
                },
                {
                    title: '💰 High Value Orders',
                    description: 'Orders over $100',
                    sql: `SELECT * FROM ${tableName} WHERE amount > 100`
                },
                {
                    title: '👤 Alice\'s Orders',
                    description: 'All orders by Alice Johnson',
                    sql: `SELECT * FROM ${tableName} WHERE customer_name = 'Alice Johnson'`
                },
                {
                    title: '❌ Cancelled Orders',
                    description: 'View cancelled orders',
                    sql: `SELECT id, customer_name, product, amount FROM ${tableName} WHERE status = 'cancelled'`
                },
                {
                    title: '📊 Order Count',
                    description: 'Total number of orders',
                    sql: `SELECT COUNT(*) FROM ${tableName}`
                }
            ],
            users: [
                {
                    title: '👥 All Users',
                    description: 'View all users',
                    sql: `SELECT * FROM ${tableName}`
                },
                {
                    title: '✅ Active Users',
                    description: 'Only active users',
                    sql: `SELECT * FROM ${tableName} WHERE status = 'active'`
                },
                {
                    title: '🔑 Admins & Managers',
                    description: 'Users with elevated roles',
                    sql: `SELECT name, email, role FROM ${tableName} WHERE role != 'user'`
                },
                {
                    title: '📊 User Count',
                    description: 'Total users in system',
                    sql: `SELECT COUNT(*) FROM ${tableName}`
                }
            ],
            inventory: [
                {
                    title: '📦 All Inventory',
                    description: 'View all inventory',
                    sql: `SELECT * FROM ${tableName}`
                },
                {
                    title: '⚠️ Low Stock',
                    description: 'Items with quantity < 50',
                    sql: `SELECT * FROM ${tableName} WHERE quantity < 50`
                },
                {
                    title: '🏢 Warehouse A',
                    description: 'Items in Warehouse A',
                    sql: `SELECT * FROM ${tableName} WHERE location = 'Warehouse A'`
                },
                {
                    title: '💰 Expensive Items',
                    description: 'Products over $100',
                    sql: `SELECT * FROM ${tableName} WHERE price > 100`
                }
            ]
        };

        return queries[dataset];
    }

    populateExampleQueries() {
        const container = document.getElementById('example-queries');
        container.innerHTML = '';

        this.exampleQueries.forEach((query, index) => {
            const div = document.createElement('div');
            div.className = 'example-query';
            div.innerHTML = `
                <div class="title">${query.title}</div>
                <div class="description">${query.description}</div>
            `;
            div.addEventListener('click', () => {
                document.getElementById('sql-editor').value = query.sql;
                this.runQuery();
            });
            container.appendChild(div);
        });
    }

    loadDefaultQuery() {
        if (this.exampleQueries.length > 0) {
            document.getElementById('sql-editor').value = this.exampleQueries[0].sql;
        }
    }

    updateTimeDisplay() {
        const snapshot = this.db.getSnapshotAt(this.currentSeq);
        if (snapshot) {
            const timeStr = snapshot.timestamp.toLocaleString();
            document.getElementById('current-time-display').textContent =
                this.currentSeq === 10 ? 'Latest (Now)' : timeStr;
            document.getElementById('seq-display').textContent = `@seq:${this.currentSeq}`;
            document.getElementById('timestamp-display').textContent = timeStr;
        }
    }

    formatQuery() {
        const editor = document.getElementById('sql-editor');
        let sql = editor.value.trim();

        // Simple SQL formatting
        sql = sql.replace(/\s+/g, ' ');
        sql = sql.replace(/SELECT/gi, 'SELECT\n  ');
        sql = sql.replace(/FROM/gi, '\nFROM');
        sql = sql.replace(/WHERE/gi, '\nWHERE\n  ');
        sql = sql.replace(/AND/gi, '\n  AND');
        sql = sql.replace(/ORDER BY/gi, '\nORDER BY');
        sql = sql.replace(/LIMIT/gi, '\nLIMIT');

        editor.value = sql;
    }

    runQuery() {
        const sql = document.getElementById('sql-editor').value.trim();
        if (!sql) {
            this.showError('Please enter a SQL query');
            return;
        }

        const resultsSection = document.getElementById('results-section');
        resultsSection.innerHTML = '<h3>Query Results</h3><div class="loading">Executing query...</div>';

        // Simulate async query execution
        setTimeout(() => {
            try {
                const results = this.db.query(sql, this.currentSeq);
                this.displayResults(results, sql);
            } catch (error) {
                this.showError(error.message);
            }
        }, 300);
    }

    displayResults(results, sql) {
        const resultsSection = document.getElementById('results-section');
        resultsSection.innerHTML = '<h3>Query Results</h3>';

        // Add info
        const info = document.createElement('div');
        info.className = 'results-info';
        const snapshot = this.db.getSnapshotAt(this.currentSeq);
        info.innerHTML = `
            <strong>Rows returned:</strong> ${results.length} |
            <strong>Query time:</strong> ${snapshot.timestamp.toLocaleString()} (Seq: ${this.currentSeq})
        `;
        resultsSection.appendChild(info);

        if (results.length === 0) {
            const empty = document.createElement('div');
            empty.className = 'empty-state';
            empty.innerHTML = '<p>No rows returned</p>';
            resultsSection.appendChild(empty);
            return;
        }

        // Create table
        const container = document.createElement('div');
        container.className = 'results-container';

        const table = document.createElement('table');

        // Header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        const columns = Object.keys(results[0]);
        columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Body
        const tbody = document.createElement('tbody');
        results.forEach(row => {
            const tr = document.createElement('tr');
            columns.forEach(col => {
                const td = document.createElement('td');
                const value = row[col];
                td.textContent = value !== null && value !== undefined ? value : 'NULL';
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        container.appendChild(table);
        resultsSection.appendChild(container);
    }

    showError(message) {
        const resultsSection = document.getElementById('results-section');
        resultsSection.innerHTML = '<h3>Query Results</h3>';

        const error = document.createElement('div');
        error.className = 'error-message';
        error.innerHTML = `<strong>Error:</strong> ${message}`;
        resultsSection.appendChild(error);
    }

    clearResults() {
        const resultsSection = document.getElementById('results-section');
        resultsSection.innerHTML = `
            <h3>Query Results</h3>
            <div class="empty-state">
                <p>Run a query to see results</p>
                <p style="font-size: 0.9em; margin-top: 10px;">Try clicking an example query or write your own SQL</p>
            </div>
        `;
    }
}

// ============================================================================
// Initialize Demo
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    const demo = new DemoController();

    // Make it available globally for debugging
    window.demoController = demo;

    console.log('DriftDB Interactive Demo loaded!');
    console.log('Try the example queries or write your own SQL');
    console.log('Use the time-travel slider to query data at different points in time');
});
