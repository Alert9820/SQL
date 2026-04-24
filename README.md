# 🗄 SQL Analytics Engine — Automated SQL Intelligence

> Upload any CSV — the engine loads it into a relational database and automatically runs 15+ SQL queries including GROUP BY, HAVING, CTEs, Window Functions, Subqueries, and Rankings. Results appear on an interactive dashboard with a live SQL editor.

🔗 **Live Demo:** [your-app.onrender.com](https://sql-7fbz.onrender.com)

---

## ✨ What It Does

```
Upload CSV / Excel
      ↓
Auto Clean (duplicates · nulls · outliers)
      ↓
Load into Database (MySQL locally · SQLite on Render)
      ↓
15+ SQL Queries Auto-Execute
(GROUP BY · HAVING · CTE · Window Functions · Subqueries)
      ↓
Interactive Dashboard + Live SQL Editor
      ↓
Export all queries as .sql file
```

---

## 🧠 SQL Queries — What Runs Automatically

| Query | SQL Concept |
|---|---|
| Total & Average Revenue | Aggregate functions — SUM, AVG, MAX, MIN |
| Revenue by Category | GROUP BY + ORDER BY |
| Top 5 Performers | ORDER BY DESC + LIMIT |
| Bottom 5 Performers | ORDER BY ASC + LIMIT |
| High Revenue Filter | HAVING clause |
| Profit Analysis | Computed columns — arithmetic in SQL |
| Revenue Trend | Time-series GROUP BY on date column |
| Revenue Share % | CTE (Common Table Expression) |
| Category Ranking | RANK() Window Function |
| Running Total | Cumulative SUM() Window Function |
| Above Average Records | Subquery inside WHERE clause |
| NULL Value Count | CASE WHEN for data quality |
| Distinct Value Counts | COUNT DISTINCT per column |
| Qty vs Revenue | Correlation analysis |
| Dataset Overview | COUNT + basic aggregation |

---

## ⚡ Key Features

### 🔄 Auto Database Load
- CSV/Excel → SQLite (Render) or MySQL (local) automatically
- Column names sanitized for SQL safety
- Feature engineering: `Profit`, `Profit_Margin_Pct` auto-added

### 💻 Live SQL Editor
- Write and run custom SELECT queries directly in the browser
- Results shown instantly in a table
- Only SELECT allowed — read-safe

### 📤 Export .sql File
- Download all auto-generated queries as a `.sql` file
- Clean, commented, ready for portfolio or submission

### 🗄 MySQL + SQLite
- **Local development:** MySQL (connect via `MYSQL_URL` env var)
- **Production (Render):** SQLite — zero setup, built into Python

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python, Flask |
| Database | MySQL (local) · SQLite (production) |
| ORM / Query | SQLAlchemy, raw SQL |
| Data Processing | Pandas, NumPy |
| Frontend | HTML, CSS, JavaScript, Chart.js |
| UI Aesthetic | Terminal / SQL Workbench theme |
| Deployment | Docker, Render.com |

---

## 📁 Project Structure

```
sql-engine/
├── app.py           # Flask backend — ETL + SQL engine + API
├── index.html       # Terminal-style dashboard UI
├── requirements.txt # Python dependencies
├── Dockerfile       # Python 3.11.9 container
└── README.md
```

---

## ⚙️ Local Setup

### Prerequisites
- Python 3.11+
- MySQL (optional — SQLite works without it)

```bash
# Clone
git clone https://github.com/Alert9820/sql-engine.git
cd sql-engine

# Install
pip install -r requirements.txt

# Run (SQLite mode — no setup needed)
python app.py

# Open
http://localhost:10000
```

### Local MySQL Mode

```bash
# Set environment variables
export USE_MYSQL=true
export MYSQL_URL=mysql+pymysql://root:yourpassword@localhost/yourdbname

# Run
python app.py
```

### Docker

```bash
docker build -t sql-engine .
docker run -p 10000:10000 sql-engine
```

---

## 🌐 Deploy on Render

1. Push to GitHub
2. Render → New → Web Service → Connect repo
3. Select **Docker** as runtime
4. Deploy — SQLite mode runs automatically ✅

**For MySQL on Render:**
Add environment variables in Render dashboard:
- `USE_MYSQL` = `true`
- `MYSQL_URL` = your MySQL connection string

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Serve dashboard UI |
| `POST` | `/upload` | Upload CSV/Excel → run ETL + SQL |
| `GET` | `/results/{job_id}` | Get all query results |
| `POST` | `/query/{job_id}` | Run custom SQL query |
| `GET` | `/export/{job_id}` | Download all queries as .sql file |
| `GET` | `/health` | Health check + DB type |

---

## 📊 Auto-Detected Columns

| Pattern | Column Names |
|---|---|
| Revenue | `revenue`, `sales`, `income`, `amount`, `price` |
| Cost | `cost`, `expense`, `spend` |
| Category | `category`, `product`, `region`, `department` |
| Date | `date`, `month`, `period`, `year` |
| Quantity | `qty`, `units`, `quantity`, `volume` |

---

## 💡 Use Cases

- Data Analysts — Explore any dataset with SQL instantly
- Students — Learn SQL concepts (GROUP BY, CTE, Window Functions) with real output
- Data Engineers — Prototype SQL pipelines before building production ETL
- Interviewers Demo — Live SQL running on real data = strong portfolio piece

---

## 🎨 UI Design

Terminal / SQL Workbench aesthetic:
- macOS-style window chrome (red/yellow/green dots)
- Fira Code monospace font
- Green-on-dark terminal colors
- SQL syntax highlighting in query viewer
- Sidebar navigation with 5 views

---

## 👨‍💻 Author

**Sunny Mukesh Chaurasiya**
Portfolio project demonstrating: SQL query design · ETL pipeline · MySQL + SQLite · Flask API · Docker deployment · Interactive dashboard

---

## 📄 License

MIT License

---

<div align="center"><strong>🗄 SQL Analytics Engine — Because SQL should do the work, not you.</strong></div>

