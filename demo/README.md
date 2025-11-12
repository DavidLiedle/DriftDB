# DriftDB Interactive Demo

An interactive web-based demonstration of DriftDB's time-travel database capabilities. Experience querying data at any point in time through an intuitive SQL editor with a visual time-travel slider.

![DriftDB Demo](screenshot.png)

## Features

- **Time-Travel Slider**: Visually navigate through database history
- **SQL Editor**: Write and execute queries in real-time
- **Multiple Datasets**: Switch between e-commerce, user management, and inventory scenarios
- **Example Queries**: Pre-built queries to showcase time-travel capabilities
- **Real-Time Results**: Instant query execution with formatted table display
- **Mock Data Mode**: Works standalone without requiring a DriftDB server
- **Responsive Design**: Works on desktop and mobile devices

## Quick Start

### Option 1: Direct Browser Access (Simplest)

Just open `index.html` in your web browser:

```bash
# From the demo directory
open index.html
# or
firefox index.html
# or
chrome index.html
```

The demo runs entirely in your browser with no dependencies!

### Option 2: Local HTTP Server (Recommended)

Using Node.js:

```bash
# From the demo directory
node server.js 8080

# Then open http://localhost:8080 in your browser
```

Using Python:

```bash
# Python 3
python3 -m http.server 8080

# Python 2
python -m SimpleHTTPServer 8080

# Then open http://localhost:8080 in your browser
```

### Option 3: Deploy to GitHub Pages

```bash
# From your repository root
git add demo/
git commit -m "Add interactive demo"
git push

# Enable GitHub Pages in repository settings
# Source: main branch, /demo folder
# Your demo will be at: https://yourusername.github.io/driftdb/
```

## Using the Demo

### 1. Time-Travel Slider

The slider at the top lets you query data at different points in time:

- **Drag the slider** to select a sequence number (1-10)
- **See the timestamp** update to show when that data existed
- **Run queries** to see results at that point in time

### 2. SQL Editor

Write standard SQL queries:

```sql
-- View all orders
SELECT * FROM orders

-- Filter by status
SELECT * FROM orders WHERE status = 'shipped'

-- High-value orders
SELECT * FROM orders WHERE amount > 100

-- Specific customer
SELECT * FROM orders WHERE customer_name = 'Alice Johnson'
```

**Keyboard Shortcuts:**
- `Ctrl+Enter` - Run query
- `Format SQL` button - Auto-format your SQL

### 3. Example Queries

Click any example query in the sidebar to:
- Load the query into the editor
- Automatically execute it
- See results instantly

### 4. Multiple Datasets

Switch between datasets using the dropdown:

- **E-Commerce Orders**: Order lifecycle (pending → paid → shipped → delivered)
- **User Management**: User status and role changes over time
- **Inventory Tracking**: Stock quantity changes

## Sample Scenarios

### Scenario 1: Order Status Changes

1. Select the "E-Commerce Orders" dataset
2. Set time slider to sequence **3** (early time)
3. Run: `SELECT * FROM orders WHERE id = 1`
4. **Observe**: Order status is "pending"
5. Move slider to sequence **8** (later time)
6. Run the same query
7. **Observe**: Order status is now "delivered"

**What you learn**: How DriftDB tracks changes over time

### Scenario 2: Finding When Data Changed

1. Start at sequence **1** (earliest)
2. Run: `SELECT * FROM orders WHERE status = 'cancelled'`
3. **Result**: 0 rows (no cancelled orders yet)
4. Move to sequence **6**
5. Run the same query
6. **Result**: 1 row (order was cancelled at this point)

**What you learn**: Pinpoint exactly when changes occurred

### Scenario 3: Debugging Production Issues

Imagine a customer complains: "My order was marked delivered but I never received it!"

1. Query their order at current time: `SELECT * FROM orders WHERE customer_id = 101`
2. Use time-travel to see order history at different points
3. Find when status changed to "delivered"
4. Cross-reference with shipping logs to investigate

**What you learn**: How time-travel helps with debugging

## Technical Details

### Architecture

```
demo/
├── index.html          # Main demo page
├── demo.js            # Interactive demo logic
├── server.js          # Optional HTTP server
└── README.md          # This file
```

### How It Works

1. **Mock Database**: Generates realistic sample data with 10 sequence numbers
2. **Time-Travel Logic**: Queries return data as it existed at the selected sequence
3. **SQL Parser**: Simple parser supporting SELECT, WHERE, and basic operators
4. **Pure JavaScript**: No frameworks or dependencies required

### Supported SQL Features (Demo Mode)

- `SELECT * FROM table`
- `SELECT column1, column2 FROM table`
- `WHERE` conditions:
  - Equality: `column = 'value'`
  - Inequality: `column != 'value'`
  - Comparisons: `column > number`, `column < number`, `>=`, `<=`
  - Logic: `AND`, `OR`
- `COUNT(*)`
- `LIMIT n`

### Data Model

Each dataset has 10 sequence numbers representing points in time:

```
Sequence 1  →  Sequence 2  →  ... →  Sequence 10
(oldest)                              (latest/now)
```

Data evolves over time:
- Orders change status
- Users get promoted to different roles
- Inventory quantities decrease

## Connecting to Real DriftDB

The demo currently runs in mock mode. To connect to a real DriftDB server:

### Future Enhancement

```javascript
// In demo.js, add DriftDB client connection
class DriftDBClient {
    constructor(host, port) {
        this.host = host;
        this.port = port;
    }

    async query(sql, asOfSeq) {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, asOfSeq })
        });
        return response.json();
    }
}
```

Then update the connection mode selector to switch between mock and real data.

## Customization

### Adding Your Own Dataset

In `demo.js`, add a new dataset generator:

```javascript
generateYourData() {
    const history = [];
    for (let seq = 1; seq <= 10; seq++) {
        const data = [
            { id: 1, name: 'Item 1', value: seq * 10 },
            // ... your data
        ];
        history.push({
            seq: seq,
            timestamp: new Date(Date.now() - (10 - seq) * 3600000),
            data: data
        });
    }
    return history;
}
```

### Customizing Appearance

Edit the CSS in `index.html`:

```css
/* Change color scheme */
header {
    background: linear-gradient(135deg, #your-color1, #your-color2);
}

.btn-primary {
    background: #your-primary-color;
}
```

## Deployment Options

### 1. Static Site Hosting

Works on any static file host:
- **GitHub Pages** (free)
- **Netlify** (free)
- **Vercel** (free)
- **AWS S3** + CloudFront
- **Google Cloud Storage**
- **Azure Static Web Apps**

### 2. Docker

Create `Dockerfile`:

```dockerfile
FROM nginx:alpine
COPY demo/ /usr/share/nginx/html/
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

Build and run:

```bash
docker build -t driftdb-demo .
docker run -p 8080:80 driftdb-demo
```

### 3. Cloud Functions

Deploy as a serverless function:

```javascript
// AWS Lambda, Google Cloud Functions, etc.
exports.handler = async (event) => {
    const html = fs.readFileSync('index.html', 'utf8');
    return {
        statusCode: 200,
        headers: { 'Content-Type': 'text/html' },
        body: html
    };
};
```

## Browser Compatibility

- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

**Note**: Requires ES6 support (class syntax, arrow functions, etc.)

## Performance

- **Load time**: < 1 second
- **Query execution**: < 100ms (mock data)
- **Memory usage**: < 10MB
- **No external dependencies**: 100% self-contained

## Troubleshooting

### Demo doesn't load

**Issue**: Blank page or console errors

**Solution**:
1. Check browser console (F12) for errors
2. Ensure JavaScript is enabled
3. Try a different browser
4. Use HTTP server instead of file:// protocol

### Time slider doesn't work

**Issue**: Moving slider doesn't update results

**Solution**:
1. Click "Run Query" button after moving slider
2. Or use an example query to auto-execute

### No results showing

**Issue**: Query runs but no data displayed

**Solution**:
1. Check the WHERE conditions match data
2. Try simpler query: `SELECT * FROM orders`
3. Move time slider to different sequence

## Future Enhancements

Planned features for v2:

- [ ] Real DriftDB server connection
- [ ] Query history and favorites
- [ ] Export results to CSV/JSON
- [ ] Visual query builder (drag-and-drop)
- [ ] Diff view between two time points
- [ ] Query performance metrics
- [ ] Dark mode
- [ ] Collaborative queries (share URL)
- [ ] Syntax highlighting in SQL editor
- [ ] Auto-complete for table/column names

## Contributing

Want to improve the demo? Here's how:

1. **Report Issues**: Open an issue on GitHub
2. **Submit PRs**: Fork, make changes, submit pull request
3. **Add Datasets**: Create interesting sample scenarios
4. **Improve UI**: Better design, animations, accessibility
5. **Add Features**: Implement items from the roadmap

## License

MIT License - Same as DriftDB

## Links

- [DriftDB Repository](https://github.com/driftdb/driftdb)
- [DriftDB Documentation](https://docs.driftdb.com)
- [Report Issues](https://github.com/driftdb/driftdb/issues)
- [Community Discord](https://discord.gg/driftdb)

## Credits

Created by the DriftDB team to showcase time-travel database capabilities.

Special thanks to:
- All DriftDB contributors
- PostgreSQL community for SQL inspiration
- Users who provided feedback

---

**Ready to try the real thing?**

Install DriftDB:
```bash
cargo install driftdb-server
driftdb-server --data-path ./data
```

Connect with any PostgreSQL client and experience time-travel queries in your own data!
