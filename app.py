from flask import Flask, render_template, request, jsonify, Response
import psycopg
from psycopg.rows import dict_row
import os
from datetime import datetime
import csv
from io import StringIO

app = Flask(__name__)

# Database configuration - reads from environment variables
DB_CONFIG = {
    'host': os.getenv('DATABASE_HOST', 'your-rds-endpoint.rds.amazonaws.com'),
    'dbname': os.getenv('DATABASE_NAME', 'news_crawler'),
    'user': os.getenv('DATABASE_USER', 'crawler_user'),
    'password': os.getenv('DATABASE_PASSWORD', 'your_password'),
    'port': int(os.getenv('DATABASE_PORT', 5432))
}

def get_db_connection():
    """Create database connection with psycopg3"""
    return psycopg.connect(**DB_CONFIG, row_factory=dict_row)

@app.route('/')
def index():
    """Main dashboard page"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) as total FROM articles")
    total_articles = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(DISTINCT author) as total FROM articles WHERE author IS NOT NULL")
    total_authors = cursor.fetchone()['total']
    
    cursor.execute("""
        SELECT COUNT(*) as recent 
        FROM articles 
        WHERE crawled_at > NOW() - INTERVAL '24 hours'
    """)
    recent_24h = cursor.fetchone()['recent']
    
    cursor.execute("""
        SELECT MIN(crawled_at) as first, MAX(crawled_at) as last 
        FROM articles
    """)
    dates = cursor.fetchone()
    
    # Get top authors
    cursor.execute("""
        SELECT author, COUNT(*) as article_count 
        FROM articles 
        WHERE author IS NOT NULL 
        GROUP BY author 
        ORDER BY article_count DESC 
        LIMIT 10
    """)
    top_authors = cursor.fetchall()
    
    # Get recent articles
    cursor.execute("""
        SELECT title, author, url, crawled_at 
        FROM articles 
        ORDER BY crawled_at DESC 
        LIMIT 20
    """)
    recent_articles = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('index.html',
                         total_articles=total_articles,
                         total_authors=total_authors,
                         recent_24h=recent_24h,
                         first_crawl=dates['first'],
                         last_crawl=dates['last'],
                         top_authors=top_authors,
                         recent_articles=recent_articles)

@app.route('/articles')
def articles():
    """Paginated articles list"""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    search = request.args.get('search', '')
    author_filter = request.args.get('author', '')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build query
    where_clauses = []
    params = []
    
    if search:
        where_clauses.append("(title ILIKE %s OR author ILIKE %s)")
        params.extend([f'%{search}%', f'%{search}%'])
    
    if author_filter:
        where_clauses.append("author = %s")
        params.append(author_filter)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Get total count
    cursor.execute(f"SELECT COUNT(*) as total FROM articles WHERE {where_sql}", params)
    total = cursor.fetchone()['total']
    
    # Get articles
    offset = (page - 1) * per_page
    params.extend([per_page, offset])
    cursor.execute(f"""
        SELECT id, title, author, url, crawled_at 
        FROM articles 
        WHERE {where_sql}
        ORDER BY crawled_at DESC 
        LIMIT %s OFFSET %s
    """, params)
    articles_list = cursor.fetchall()
    
    # Get all authors for filter
    cursor.execute("""
        SELECT DISTINCT author 
        FROM articles 
        WHERE author IS NOT NULL 
        ORDER BY author
    """)
    authors = [row['author'] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    
    return render_template('articles.html',
                         articles=articles_list,
                         page=page,
                         total_pages=total_pages,
                         total=total,
                         search=search,
                         author_filter=author_filter,
                         authors=authors)

@app.route('/author/<author_name>')
def author_profile(author_name):
    """Author profile page"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) as article_count,
               MIN(crawled_at) as first_article,
               MAX(crawled_at) as latest_article
        FROM articles 
        WHERE author = %s
    """, (author_name,))
    author_stats = cursor.fetchone()
    
    cursor.execute("""
        SELECT title, url, crawled_at 
        FROM articles 
        WHERE author = %s 
        ORDER BY crawled_at DESC
    """, (author_name,))
    author_articles = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return render_template('author.html',
                         author_name=author_name,
                         stats=author_stats,
                         articles=author_articles)

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Articles per day for last 30 days
    cursor.execute("""
        SELECT DATE(crawled_at) as date, COUNT(*) as count
        FROM articles
        WHERE crawled_at > NOW() - INTERVAL '30 days'
        GROUP BY DATE(crawled_at)
        ORDER BY date
    """)
    daily_counts = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Convert rows to dicts for JSON
    result = []
    for row in daily_counts:
        result.append({
            'date': row['date'].strftime('%Y-%m-%d') if row['date'] else None,
            'count': row['count']
        })
    
    return jsonify({'daily_counts': result})

@app.route('/export')
def export():
    """Export options page"""
    return render_template('export.html')

@app.route('/api/export/csv')
def export_csv():
    """Export articles as CSV"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT title, author, url, crawled_at 
        FROM articles 
        ORDER BY crawled_at DESC
    """)
    articles_list = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Create CSV
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['Title', 'Author', 'URL', 'Crawled At'])
    
    for article in articles_list:
        crawled_at_str = ''
        if article['crawled_at']:
            crawled_at_str = article['crawled_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        writer.writerow([
            article['title'] or '',
            article['author'] or '',
            article['url'] or '',
            crawled_at_str
        ])
    
    output = si.getvalue()
    return Response(
        output,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=articles.csv'}
    )

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
