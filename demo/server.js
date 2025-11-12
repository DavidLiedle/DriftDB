#!/usr/bin/env node

/**
 * Simple HTTP server for DriftDB Interactive Demo
 *
 * Serves the demo page and optionally proxies requests to a real DriftDB server
 *
 * Usage:
 *   node server.js [port]
 *   node server.js 3000
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const PORT = process.argv[2] || 8080;
const DRIFTDB_HOST = process.env.DRIFTDB_HOST || 'localhost';
const DRIFTDB_PORT = process.env.DRIFTDB_PORT || 5433;

// MIME types
const MIME_TYPES = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon'
};

// Serve static files
function serveStaticFile(filePath, res) {
    const ext = path.extname(filePath);
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, data) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('File not found');
            } else {
                res.writeHead(500);
                res.end('Server error');
            }
        } else {
            res.writeHead(200, { 'Content-Type': contentType });
            res.end(data);
        }
    });
}

// Create HTTP server
const server = http.createServer((req, res) => {
    const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
    let pathname = parsedUrl.pathname;

    // Add CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
    }

    // Route requests
    if (pathname === '/') {
        pathname = '/index.html';
    }

    // API proxy to real DriftDB (if implemented)
    if (pathname.startsWith('/api/')) {
        handleApiRequest(req, res, pathname);
        return;
    }

    // Serve static files
    const filePath = path.join(__dirname, pathname);

    // Security check: prevent directory traversal
    if (!filePath.startsWith(__dirname)) {
        res.writeHead(403);
        res.end('Forbidden');
        return;
    }

    serveStaticFile(filePath, res);
});

// Handle API requests (proxy to DriftDB)
function handleApiRequest(req, res, pathname) {
    if (pathname === '/api/query') {
        if (req.method === 'POST') {
            let body = '';
            req.on('data', chunk => {
                body += chunk.toString();
            });
            req.on('end', () => {
                try {
                    const { sql, asOfSeq } = JSON.parse(body);
                    // TODO: Connect to real DriftDB server
                    // For now, return mock response
                    res.writeHead(501, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        error: 'Real DriftDB connection not yet implemented. Use Demo Mode.'
                    }));
                } catch (error) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: error.message }));
                }
            });
        } else {
            res.writeHead(405);
            res.end('Method not allowed');
        }
    } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'API endpoint not found' }));
    }
}

// Start server
server.listen(PORT, () => {
    console.log('╔════════════════════════════════════════════════════════════════╗');
    console.log('║          DriftDB Interactive Demo Server                       ║');
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log('');
    console.log(`  🚀 Server running at: http://localhost:${PORT}`);
    console.log('');
    console.log('  📖 Open your browser and navigate to the URL above');
    console.log('');
    console.log('  Mode: Demo Mode (Mock Data)');
    console.log('  - The demo runs entirely in your browser');
    console.log('  - Sample data showcases time-travel queries');
    console.log('  - No actual DriftDB server needed');
    console.log('');
    console.log('  To connect to a real DriftDB server:');
    console.log(`    DRIFTDB_HOST=localhost DRIFTDB_PORT=5433 node server.js ${PORT}`);
    console.log('');
    console.log('  Press Ctrl+C to stop the server');
    console.log('');
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\n  👋 Shutting down server...');
    server.close(() => {
        console.log('  ✅ Server stopped\n');
        process.exit(0);
    });
});

process.on('SIGTERM', () => {
    server.close(() => {
        process.exit(0);
    });
});
