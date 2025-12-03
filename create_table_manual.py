from app import app, db
from models import EtlConnection

with app.app_context():
    print(f"DB URI: {app.config['SQLALCHEMY_DATABASE_URI']}")
    db.create_all()
    print("Tables created using SQLAlchemy create_all.")
    
    # Verify
    import sqlite3
    # Assuming default sqlite location for verification
    try:
        conn = sqlite3.connect('instance/etl_metadata.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables in instance/etl_metadata.db: {tables}")
        conn.close()
    except Exception as e:
        print(f"Verification failed: {e}")

