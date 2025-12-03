import json
import os

CONFIG_FILE = 'config.json'

class ConfigManager:
    @staticmethod
    def load_config():
        if not os.path.exists(CONFIG_FILE):
            return {
                "db_type": "sqlite",
                "host": "",
                "port": "",
                "schema_db": "etl_metadata.db",
                "username": "",
                "password": ""
            }
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return ConfigManager.get_default_config()

    @staticmethod
    def get_default_config():
        return {
            "db_type": "sqlite",
            "host": "",
            "port": "",
            "schema_db": "etl_metadata.db",
            "username": "",
            "password": ""
        }

    @staticmethod
    def save_config(config_data):
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config_data, f, indent=4)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False

    @staticmethod
    def get_db_uri(config=None):
        if config is None:
            config = ConfigManager.load_config()
            
        db_type = config.get('db_type', 'sqlite').lower()
        
        if db_type == 'sqlite':
            # Use absolute path for SQLite to avoid issues
            basedir = os.path.abspath(os.path.dirname(__file__))
            db_name = config.get('schema_db', 'etl_metadata.db')
            # If it's just a filename, put it in instance folder, otherwise trust the path
            if not os.path.isabs(db_name):
                 db_path = os.path.join(basedir, 'instance', db_name)
            else:
                db_path = db_name
            return f'sqlite:///{db_path}'
            
        elif db_type == 'oracle':
            # oracle+cx_oracle://user:password@host:port/?service_name=sid
            return f"oracle+cx_oracle://{config['username']}:{config['password']}@{config['host']}:{config['port']}/?service_name={config['schema_db']}"
            
        elif db_type == 'mysql':
            # mysql+pymysql://user:password@host:port/dbname
            return f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['schema_db']}"
            
        elif db_type == 'postgresql':
            # postgresql://user:password@host:port/dbname
            return f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['schema_db']}"
            
        return 'sqlite:///instance/etl_metadata.db' # Fallback
