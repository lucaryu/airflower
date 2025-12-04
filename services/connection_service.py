from models import db, EtlConnection

class ConnectionService:
    def get_all_connections(self):
        return EtlConnection.query.order_by(EtlConnection.id.desc()).all()

    def save_connection(self, data):
        conn = EtlConnection(
            name=data['name'],
            role=data.get('role', 'UNUSED'),
            type=data['type'],
            host=data['host'],
            port=int(data['port']),
            schema_db=data['schema_db'],
            username=data['username'],
            password=data['password']
        )
        db.session.add(conn)
        db.session.commit()
        return conn

    def update_connection(self, id, data):
        conn = EtlConnection.query.get(id)
        if conn:
            conn.name = data['name']
            conn.role = data.get('role', 'UNUSED')
            conn.type = data['type']
            conn.host = data['host']
            conn.port = int(data['port'])
            conn.schema_db = data['schema_db']
            conn.username = data['username']
            if data.get('password'): # Only update password if provided
                conn.password = data['password']
            
            db.session.commit()
            return conn
        return None

    def delete_connection(self, id):
        conn = EtlConnection.query.get(id)
        if conn:
            db.session.delete(conn)
            db.session.commit()
            return True
        return False

    def test_connection(self, data):
        from sqlalchemy import create_engine, text
        
        try:
            # Construct URI based on type
            db_type = data.get('type', '').upper()
            uri = ""
            
            if db_type == 'ORACLE':
                # oracle+oracledb://user:password@host:port/?service_name=sid
                uri = f"oracle+oracledb://{data['username']}:{data['password']}@{data['host']}:{data['port']}/?service_name={data['schema_db']}"
            elif db_type == 'POSTGRES':
                # postgresql://user:password@host:port/dbname
                uri = f"postgresql://{data['username']}:{data['password']}@{data['host']}:{data['port']}/{data['schema_db']}"
            else:
                return False, f"Unsupported database type: {db_type}"
                
            engine = create_engine(uri)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            return True, "Connection Successful!"
            
        except Exception as e:
            return False, str(e)
