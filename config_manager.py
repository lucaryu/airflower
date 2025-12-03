import json
import os

CONFIG_FILE = 'config.json'

class ConfigManager:
    @staticmethod
    def load_config():
        """
        Loads the configuration.
        Migrates old flat config to new profile-based config if necessary.
        """
        if not os.path.exists(CONFIG_FILE):
            return ConfigManager.get_default_config_structure()
            
        try:
            with open(CONFIG_FILE, 'r') as f:
                data = json.load(f)
                
            # Migration check: if 'profiles' key is missing, it's the old format
            if 'profiles' not in data:
                print("Migrating old config to profile format...")
                new_config = {
                    "active_profile": "Default",
                    "profiles": {
                        "Default": data
                    }
                }
                ConfigManager.save_full_config(new_config)
                return new_config
                
            return data
        except Exception as e:
            print(f"Error loading config: {e}")
            return ConfigManager.get_default_config_structure()

    @staticmethod
    def get_default_config_structure():
        return {
            "active_profile": "Default",
            "profiles": {
                "Default": ConfigManager.get_default_connection_config()
            }
        }

    @staticmethod
    def get_default_connection_config():
        return {
            "db_type": "sqlite",
            "host": "",
            "port": "",
            "schema_db": "etl_metadata.db",
            "username": "",
            "password": ""
        }

    @staticmethod
    def save_full_config(config_data):
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config_data, f, indent=4)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False

    @staticmethod
    def get_active_config():
        data = ConfigManager.load_config()
        active_profile = data.get('active_profile', 'Default')
        return data.get('profiles', {}).get(active_profile, ConfigManager.get_default_connection_config())

    @staticmethod
    def get_profiles():
        data = ConfigManager.load_config()
        return data.get('profiles', {})

    @staticmethod
    def get_active_profile_name():
        data = ConfigManager.load_config()
        return data.get('active_profile', 'Default')

    @staticmethod
    def save_profile(name, config_data):
        data = ConfigManager.load_config()
        if 'profiles' not in data:
            data['profiles'] = {}
            
        data['profiles'][name] = config_data
        
        # If this is the first profile or currently active, ensure consistency
        if not data.get('active_profile'):
            data['active_profile'] = name
            
        return ConfigManager.save_full_config(data)

    @staticmethod
    def set_active_profile(name):
        data = ConfigManager.load_config()
        if name in data.get('profiles', {}):
            data['active_profile'] = name
            return ConfigManager.save_full_config(data)
        return False

    @staticmethod
    def delete_profile(name):
        data = ConfigManager.load_config()
        if name in data.get('profiles', {}):
            # Prevent deleting the last profile or the active one?
            # For now, let's just delete. If active is deleted, fallback to another or Default.
            del data['profiles'][name]
            
            if data['active_profile'] == name:
                # Fallback to Default or first available
                if 'Default' in data['profiles']:
                    data['active_profile'] = 'Default'
                elif len(data['profiles']) > 0:
                    data['active_profile'] = list(data['profiles'].keys())[0]
                else:
                    # Re-create Default if all deleted
                    data['profiles']['Default'] = ConfigManager.get_default_connection_config()
                    data['active_profile'] = 'Default'
            
            return ConfigManager.save_full_config(data)
        return False

    # Deprecated but kept for compatibility if needed, redirects to save_profile for active
    @staticmethod
    def save_config(config_data):
        # This method was used to save the single config. 
        # Now it should update the ACTIVE profile.
        active_name = ConfigManager.get_active_profile_name()
        return ConfigManager.save_profile(active_name, config_data)

    @staticmethod
    def get_db_uri(config=None):
        if config is None:
            config = ConfigManager.get_active_config()
            
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
            # oracle+oracledb://user:password@host:port/?service_name=sid
            return f"oracle+oracledb://{config['username']}:{config['password']}@{config['host']}:{config['port']}/?service_name={config['schema_db']}"
            
        elif db_type == 'mysql':
            # mysql+pymysql://user:password@host:port/dbname
            return f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['schema_db']}"
            
        elif db_type == 'postgresql':
            # postgresql://user:password@host:port/dbname
            return f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['schema_db']}"
            
        return 'sqlite:///instance/etl_metadata.db' # Fallback
