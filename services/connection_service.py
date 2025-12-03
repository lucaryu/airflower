from models import db, EtlConnection

class ConnectionService:
    def get_all_connections(self):
        return EtlConnection.query.order_by(EtlConnection.id.desc()).all()

    def save_connection(self, data):
        conn = EtlConnection(
            name=data['name'],
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

    def delete_connection(self, id):
        conn = EtlConnection.query.get(id)
        if conn:
            db.session.delete(conn)
            db.session.commit()
            return True
        return False

    def test_connection(self, data):
        # Mock connection test
        # In real app, try connecting with cx_Oracle or psycopg2
        import time
        time.sleep(1) # Simulate network delay
        return True, "Connection Successful!"
