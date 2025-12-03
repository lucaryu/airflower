import sqlite3
import os

# Path to the database
db_path = os.path.join('instance', 'etl_metadata.db')

def fix_database():
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if column exists first to avoid error if run multiple times
        cursor.execute("PRAGMA table_info(etl_connection)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if 'role' not in columns:
            print("Adding 'role' column to 'etl_connection' table...")
            cursor.execute("ALTER TABLE etl_connection ADD COLUMN role VARCHAR(20) DEFAULT 'UNUSED' NOT NULL")
            conn.commit()
            print("Column 'role' added successfully.")
        else:
            print("Column 'role' already exists.")
            
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fix_database()
