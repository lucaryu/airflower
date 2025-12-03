import json
from datetime import datetime
from models import db, EtlMetadata

class MetadataService:
    def get_source_tables(self):
        # TODO: Connect to actual Oracle DB and fetch tables
        # Mock data for now
        return [
            {"table_name": "EMP", "columns": [
                {"name": "EMPNO", "type": "NUMBER(4)", "pk": True, "nullable": False},
                {"name": "ENAME", "type": "VARCHAR2(10)", "pk": False, "nullable": True},
                {"name": "JOB", "type": "VARCHAR2(9)", "pk": False, "nullable": True},
                {"name": "MGR", "type": "NUMBER(4)", "pk": False, "nullable": True},
                {"name": "HIREDATE", "type": "DATE", "pk": False, "nullable": True},
                {"name": "SAL", "type": "NUMBER(7,2)", "pk": False, "nullable": True},
                {"name": "COMM", "type": "NUMBER(7,2)", "pk": False, "nullable": True},
                {"name": "DEPTNO", "type": "NUMBER(2)", "pk": False, "nullable": True}
            ]},
            {"table_name": "DEPT", "columns": [
                {"name": "DEPTNO", "type": "NUMBER(2)", "pk": True, "nullable": False},
                {"name": "DNAME", "type": "VARCHAR2(14)", "pk": False, "nullable": True},
                {"name": "LOC", "type": "VARCHAR2(13)", "pk": False, "nullable": True}
            ]}
        ]

    def get_target_tables(self):
        # Fetch from local metadata DB (Postgres/Target definitions)
        # Order by ID desc to get latest first
        targets = EtlMetadata.query.filter_by(db_type='POSTGRES').order_by(EtlMetadata.id.desc()).all()
        
        unique_targets = {}
        for t in targets:
            if t.table_name not in unique_targets:
                unique_targets[t.table_name] = t
                
        return [{"id": t.id, "table_name": t.table_name, "schema_info": json.loads(t.schema_info)} for t in unique_targets.values()]

    def save_target_table(self, table_name, columns):
        # Check if exists
        existing = EtlMetadata.query.filter_by(table_name=table_name, db_type='POSTGRES').first()
        
        if existing:
            existing.schema_info = json.dumps(columns)
            existing.etl_cry_dtm = datetime.utcnow()
            db.session.commit()
            return existing
        
        new_target = EtlMetadata(
            table_name=table_name,
            db_type='POSTGRES',
            schema_info=json.dumps(columns),
            etl_cry_dtm=datetime.utcnow()
        )
        db.session.add(new_target)
        db.session.commit()
        return new_target

    def create_target_from_source(self, source_table_name, source_columns):
        # Convert Oracle types to Postgres types (Simple mapping)
        target_columns = []
        for col in source_columns:
            pg_type = self._map_oracle_to_postgres(col['type'])
            target_columns.append({
                "name": col['name'],
                "type": pg_type,
                "pk": col['pk'],
                "nullable": col['nullable']
            })
        
        return self.save_target_table(source_table_name, target_columns)

    def delete_target_table(self, table_name):
        # Delete all versions/duplicates of this target table
        print(f"DEBUG: Service deleting table: '{table_name}'")
        deleted_count = EtlMetadata.query.filter_by(table_name=table_name, db_type='POSTGRES').delete()
        db.session.commit()
        print(f"DEBUG: Deleted count: {deleted_count}")
        return deleted_count > 0

    def _map_oracle_to_postgres(self, oracle_type):
        oracle_type = oracle_type.upper()
        if 'NUMBER' in oracle_type:
            if ',' in oracle_type: return 'NUMERIC'
            return 'INTEGER'
        if 'VARCHAR2' in oracle_type: return 'VARCHAR'
        if 'DATE' in oracle_type: return 'TIMESTAMP'
        return 'TEXT' # Fallback
