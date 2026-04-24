"""
SQL Analytics Engine
Auto CSV → MySQL (local) / SQLite (Render) → 15+ SQL queries → Dashboard
Flask + SQLAlchemy + Pandas
"""

import os, io, uuid, json, traceback, math, re
from pathlib import Path
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename

# ── DB Setup ─────────────────────────────────────────────────────────────────
USE_MYSQL = os.environ.get("USE_MYSQL", "false").lower() == "true"
MYSQL_URL = os.environ.get("MYSQL_URL", "")  # mysql+pymysql://user:pass@host/db

if USE_MYSQL and MYSQL_URL:
    from sqlalchemy import create_engine, text
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    DB_TYPE = "mysql"
else:
    import sqlite3
    DB_PATH = Path("sql_engine.db")
    DB_TYPE = "sqlite"

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 300 * 1024 * 1024

SESSIONS = {}
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

# ── Helpers ──────────────────────────────────────────────────────────────────
def clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, 'item'):
        val = v.item()
        return None if isinstance(val, float) and (math.isnan(val) or math.isinf(val)) else val
    return v

def clean_dict(d):
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    if isinstance(d, list):
        return [clean_dict(i) for i in d]
    return clean(d)

def safe_col(name):
    """Make column name SQL-safe."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name).strip())

def detect_cols(columns, df=None):
    rev  = next((c for c in columns if any(k in c.lower() for k in ['revenue','sales','income','amount','price','total'])), None)
    cost = next((c for c in columns if any(k in c.lower() for k in ['cost','expense','spend'])), None)
    date = next((c for c in columns if any(k in c.lower() for k in ['date','month','period','year','time'])), None)
    qty  = next((c for c in columns if any(k in c.lower() for k in ['qty','units','quantity','count','volume'])), None)

    # Category: must have low cardinality (not names/IDs)
    cat = None
    for c in columns:
        if any(k in c.lower() for k in ['category','product','region','segment','department','type','channel']):
            if df is not None:
                nuniq = df[c].nunique()
                if nuniq <= 50:  # skip if too many unique values (names, IDs etc)
                    cat = c
                    break
            else:
                cat = c
                break
    return rev, cost, cat, date, qty

# ── SQLite runner ─────────────────────────────────────────────────────────────
def run_query_sqlite(sql, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    finally:
        conn.close()

# ── MySQL runner ──────────────────────────────────────────────────────────────
def run_query_mysql(sql):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        cols = list(result.keys())
        rows = [dict(zip(cols, r)) for r in result.fetchall()]
        return rows, cols

def run_query(sql, db_path=None):
    if DB_TYPE == "mysql":
        return run_query_mysql(sql)
    return run_query_sqlite(sql, db_path or DB_PATH)

# ── Load CSV to DB ────────────────────────────────────────────────────────────
def load_csv_to_db(df, table_name, db_path):
    if DB_TYPE == "mysql":
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
    else:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=1000)
        conn.close()

# ── 15+ SQL Queries ───────────────────────────────────────────────────────────
def build_queries(table, columns, rev, cost, cat, date, qty):
    T = table
    queries = {}

    # Always available
    queries['overview'] = {
        'title': 'Dataset Overview',
        'sql': f"SELECT COUNT(*) as total_rows, COUNT(DISTINCT rowid) as unique_rows FROM {T}" if DB_TYPE == 'sqlite'
               else f"SELECT COUNT(*) as total_rows FROM {T}",
        'desc': 'Basic row count of the dataset'
    }

    num_cols = [c for c in columns if c not in [cat, date]]

    # Revenue queries
    if rev:
        queries['total_revenue'] = {
            'title': 'Total & Average Revenue',
            'sql': f"""SELECT
                        ROUND(SUM({rev}), 2) AS total_revenue,
                        ROUND(AVG({rev}), 2) AS avg_revenue,
                        ROUND(MAX({rev}), 2) AS max_revenue,
                        ROUND(MIN({rev}), 2) AS min_revenue
                       FROM {T}""",
            'desc': f'Aggregate revenue metrics from {rev} column'
        }

        if cat:
            queries['revenue_by_cat'] = {
                'title': f'Revenue by {cat} — GROUP BY',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS total_revenue,
                            ROUND(AVG({rev}), 2) AS avg_revenue,
                            COUNT(*) AS transactions
                           FROM {T}
                           GROUP BY {cat}
                           ORDER BY total_revenue DESC""",
                'desc': f'GROUP BY query grouping revenue by {cat}'
            }

            queries['top_performers'] = {
                'title': f'Top 5 {cat} by Revenue — ORDER BY + LIMIT',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS total_revenue
                           FROM {T}
                           GROUP BY {cat}
                           ORDER BY total_revenue DESC
                           LIMIT 5""",
                'desc': 'ORDER BY with LIMIT to find top performers'
            }

            queries['bottom_performers'] = {
                'title': f'Bottom 5 {cat} by Revenue — ORDER BY ASC',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS total_revenue
                           FROM {T}
                           GROUP BY {cat}
                           ORDER BY total_revenue ASC
                           LIMIT 5""",
                'desc': 'Bottom performers using ascending ORDER BY'
            }

            queries['having_filter'] = {
                'title': f'High Revenue {cat} — HAVING Clause',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS total_revenue
                           FROM {T}
                           GROUP BY {cat}
                           HAVING total_revenue > (SELECT AVG(sub.total) FROM
                               (SELECT SUM({rev}) as total FROM {T} GROUP BY {cat}) sub)
                           ORDER BY total_revenue DESC""",
                'desc': 'HAVING filters groups above average — more powerful than WHERE'
            }

        if cost:
            queries['profit_analysis'] = {
                'title': 'Profit Analysis — Computed Column',
                'sql': f"""SELECT
                            {'`' + cat + '`' if cat else '"All"'} {'AS ' + cat if cat else ''},
                            ROUND(SUM({rev}), 2) AS revenue,
                            ROUND(SUM({cost}), 2) AS total_cost,
                            ROUND(SUM({rev}) - SUM({cost}), 2) AS net_profit,
                            ROUND((SUM({rev}) - SUM({cost})) / SUM({rev}) * 100, 2) AS margin_pct
                           FROM {T}
                           {'GROUP BY `' + cat + '` ORDER BY net_profit DESC' if cat else ''}""",
                'desc': 'Computed profit and margin using SQL arithmetic'
            }

        if date:
            queries['revenue_trend'] = {
                'title': f'Revenue Trend by {date} — Time Series',
                'sql': f"""SELECT
                            {date},
                            ROUND(SUM({rev}), 2) AS total_revenue,
                            COUNT(*) AS transactions
                           FROM {T}
                           GROUP BY {date}
                           ORDER BY {date}""",
                'desc': 'Time-series aggregation ordered by date'
            }

        # CTE
        if cat:
            queries['cte_ranked'] = {
                'title': f'CTE — Revenue Share per {cat}',
                'sql': f"""WITH revenue_totals AS (
                                SELECT
                                    {cat},
                                    SUM({rev}) AS cat_revenue
                                FROM {T}
                                GROUP BY {cat}
                           ),
                           grand_total AS (
                                SELECT SUM({rev}) AS total FROM {T}
                           )
                           SELECT
                               r.{cat},
                               ROUND(r.cat_revenue, 2) AS revenue,
                               ROUND(r.cat_revenue / g.total * 100, 2) AS revenue_share_pct
                           FROM revenue_totals r, grand_total g
                           ORDER BY revenue DESC""",
                'desc': 'CTE (Common Table Expression) to compute revenue share percentage'
            }

        # Window Functions (SQLite 3.25+ and MySQL 8+)
        if cat:
            queries['window_rank'] = {
                'title': f'Window Function — RANK() by Revenue',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS total_revenue,
                            RANK() OVER (ORDER BY SUM({rev}) DESC) AS revenue_rank
                           FROM {T}
                           GROUP BY {cat}
                           ORDER BY revenue_rank""",
                'desc': 'RANK() window function to rank categories by revenue'
            }

            queries['window_running'] = {
                'title': 'Window Function — Running Total (Cumulative Sum)',
                'sql': f"""SELECT
                            {cat},
                            ROUND(SUM({rev}), 2) AS revenue,
                            ROUND(SUM(SUM({rev})) OVER (ORDER BY SUM({rev}) DESC), 2) AS running_total
                           FROM {T}
                           GROUP BY {cat}
                           ORDER BY revenue DESC""",
                'desc': 'Cumulative SUM using window function — shows running total'
            }

        # Subquery
        queries['above_avg'] = {
            'title': 'Subquery — Records Above Average Revenue',
            'sql': f"""SELECT * FROM {T}
                       WHERE {rev} > (SELECT AVG({rev}) FROM {T})
                       ORDER BY {rev} DESC
                       LIMIT 20""",
            'desc': 'Subquery inside WHERE clause to filter above-average records'
        }

    # NULL analysis
    null_checks = " + ".join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)" for c in columns[:6]])
    queries['null_check'] = {
        'title': 'Data Quality — NULL Value Count',
        'sql': f"""SELECT
                    {', '.join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls" for c in columns[:6]])}
                   FROM {T}""",
        'desc': 'CASE WHEN to count NULL values per column — data quality check'
    }

    # Distinct counts
    queries['distinct_counts'] = {
        'title': 'Distinct Value Counts per Column',
        'sql': f"""SELECT
                    {', '.join([f"COUNT(DISTINCT {c}) AS {c}_unique" for c in columns[:6]])}
                   FROM {T}""",
        'desc': 'COUNT DISTINCT to understand cardinality of each column'
    }

    if qty and rev:
        queries['qty_revenue_corr'] = {
            'title': f'{qty} vs Revenue — Correlation Analysis',
            'sql': f"""SELECT
                        ROUND(AVG({qty}), 2) AS avg_qty,
                        ROUND(AVG({rev}), 2) AS avg_revenue,
                        ROUND(MAX({rev}) / MAX({qty}), 2) AS max_rev_per_unit,
                        ROUND(SUM({rev}) / SUM({qty}), 2) AS overall_rev_per_unit
                       FROM {T}
                       WHERE {qty} > 0""",
            'desc': f'Relationship between {qty} and {rev}'
        }

    return queries

# ── ETL Pipeline ──────────────────────────────────────────────────────────────
def run_pipeline(file_bytes, filename):
    logs = []

    # Read
    if filename.lower().endswith('.csv'):
        df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
    else:
        df = pd.read_excel(io.BytesIO(file_bytes))

    logs.append(f"Loaded {len(df):,} rows × {len(df.columns)} columns from '{filename}'")

    # Clean column names
    df.columns = [safe_col(c) for c in df.columns]
    orig_cols = list(df.columns)

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    logs.append(f"Removed {before - len(df):,} duplicate rows")

    # Fill missing
    filled = 0
    for col in df.columns:
        miss = df[col].isna().sum()
        if miss == 0: continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
        filled += miss
    logs.append(f"Imputed {filled:,} missing values (numeric→median, text→mode)")

    # Feature engineering
    rev, cost, cat, date, qty = detect_cols(df.columns.tolist())
    if rev and cost and cost in df.columns:
        df['Profit'] = pd.to_numeric(df[rev], errors='coerce') - pd.to_numeric(df[cost], errors='coerce')
        df['Profit_Margin_Pct'] = (df['Profit'] / pd.to_numeric(df[rev], errors='coerce') * 100).round(2)
        logs.append(f"Engineered 'Profit' and 'Profit_Margin_Pct' columns")

    # Load to DB
    job_id = str(uuid.uuid4())
    table_name = f"data_{job_id[:8]}"
    db_path = Path(f"db_{job_id[:8]}.db")

    load_csv_to_db(df, table_name, db_path)
    logs.append(f"Loaded {len(df):,} rows into {'MySQL' if DB_TYPE == 'mysql' else 'SQLite'} table '{table_name}'")

    # Detect cols again after engineering
    rev, cost, cat, date, qty = detect_cols(df.columns.tolist(), df)
    queries = build_queries(table_name, df.columns.tolist(), rev, cost, cat, date, qty)

    # Run all queries
    results = {}
    for key, q in queries.items():
        try:
            rows, cols = run_query(q['sql'], db_path)
            results[key] = {
                'title': q['title'],
                'desc': q['desc'],
                'sql': q['sql'],
                'columns': cols,
                'rows': rows[:100],
                'row_count': len(rows),
                'error': None
            }
            logs.append(f"✓ Query '{q['title']}' → {len(rows)} rows")
        except Exception as e:
            results[key] = {
                'title': q['title'],
                'desc': q['desc'],
                'sql': q['sql'],
                'columns': [],
                'rows': [],
                'row_count': 0,
                'error': str(e)
            }
            logs.append(f"✗ Query '{q['title']}' failed: {str(e)[:60]}")

    # Stats
    num_cols = df.select_dtypes(include=np.number).columns.tolist()
    stats = {}
    for col in num_cols[:8]:
        s = df[col].dropna()
        stats[col] = {
            'min': clean(round(float(s.min()), 2)),
            'max': clean(round(float(s.max()), 2)),
            'mean': clean(round(float(s.mean()), 2)),
            'sum': clean(round(float(s.sum()), 2)),
        }

    # Chart data — always convert to numeric first
    chart = {}
    if rev and rev in df.columns:
        df[rev] = pd.to_numeric(df[rev], errors='coerce').fillna(0)
    if cost and cost in df.columns:
        df[cost] = pd.to_numeric(df[cost], errors='coerce').fillna(0)
    if qty and qty in df.columns:
        df[qty] = pd.to_numeric(df[qty], errors='coerce').fillna(0)

    if rev and cat and cat in df.columns:
        try:
            grp = df.groupby(cat)[rev].sum().sort_values(ascending=False).head(8)
            chart['cat_revenue'] = {
                'labels': list(grp.index.astype(str)),
                'values': [clean(round(float(v), 2)) for v in grp.values]
            }
        except Exception:
            pass
    if date and rev and date in df.columns:
        try:
            grp2 = df.groupby(date)[rev].sum()
            chart['trend'] = {
                'labels': list(grp2.index.astype(str)),
                'values': [clean(round(float(v), 2)) for v in grp2.values]
            }
        except Exception:
            pass

    preview = df.head(50).replace({float('nan'): None, float('inf'): None, float('-inf'): None}).to_dict(orient='records')

    return clean_dict({
        'job_id': job_id,
        'filename': filename,
        'db_type': DB_TYPE,
        'table_name': table_name,
        'shape': [len(df), len(df.columns)],
        'columns': df.columns.tolist(),
        'logs': logs,
        'queries': results,
        'stats': stats,
        'chart': chart,
        'preview': preview,
        'detected': {'revenue': rev, 'cost': cost, 'category': cat, 'date': date, 'qty': qty},
        'db_path': str(db_path) if DB_TYPE == 'sqlite' else None
    })

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    p = Path(__file__).parent / 'index.html'
    return p.read_text() if p.exists() else "SQL Engine running"

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file: return jsonify({'error': 'No file'}), 400
    allowed = {'.csv', '.xlsx', '.xls'}
    if not any(file.filename.lower().endswith(e) for e in allowed):
        return jsonify({'error': 'CSV or Excel only'}), 400
    try:
        result = run_pipeline(file.read(), file.filename)
        SESSIONS[result['job_id']] = result
        return jsonify({'job_id': result['job_id'], 'status': 'complete'})
    except Exception as e:
        return jsonify({'error': traceback.format_exc()}), 500

@app.route('/results/<job_id>')
def results(job_id):
    if job_id not in SESSIONS:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(SESSIONS[job_id])

@app.route('/query/<job_id>', methods=['POST'])
def run_custom(job_id):
    """Run a custom SQL query."""
    if job_id not in SESSIONS:
        return jsonify({'error': 'Session not found'}), 404
    sql = request.json.get('sql', '').strip()
    if not sql: return jsonify({'error': 'No SQL provided'}), 400
    # Safety: only SELECT allowed
    if not sql.upper().startswith('SELECT'):
        return jsonify({'error': 'Only SELECT queries allowed'}), 400
    try:
        db_path = SESSIONS[job_id].get('db_path')
        rows, cols = run_query(sql, db_path)
        return jsonify(clean_dict({'columns': cols, 'rows': rows[:200], 'row_count': len(rows)}))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/<job_id>')
def export(job_id):
    if job_id not in SESSIONS: return jsonify({'error': 'Not found'}), 404
    s = SESSIONS[job_id]
    lines = [f"-- SQL Analytics Report: {s['filename']}",
             f"-- Table: {s['table_name']} | DB: {s['db_type']}",
             f"-- Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n"]
    for key, q in s['queries'].items():
        if q['error']: continue
        lines.append(f"-- ═══ {q['title']} ═══")
        lines.append(f"-- {q['desc']}")
        lines.append(q['sql'].strip() + ";\n")
    content = "\n".join(lines)
    buf = io.BytesIO(content.encode())
    return send_file(buf, as_attachment=True,
                     download_name=f"queries_{job_id[:8]}.sql",
                     mimetype='text/plain')

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'db': DB_TYPE})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000, debug=False)
        
