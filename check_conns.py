from app import app
from services.metadata_service import MetadataService

with app.app_context():
    service = MetadataService()
    print("Testing create_sample_tables...")
    service.create_sample_tables()
    print("Testing get_source_tables...")
    tables = service.get_source_tables()
    print(f"Found {len(tables)} source tables via Inspector.")
    
    # Test raw SQL
    conn_data = service._get_connection_by_role('SOURCE')
    engine = service._get_engine(conn_data)
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("SELECT table_name, owner FROM all_tables WHERE owner = 'SYSTEM' AND table_name IN ('EMP', 'DEPT')"))
        print("Raw SQL results:")
        for row in result:
            print(f" - {row[0]} (Owner: {row[1]})")
