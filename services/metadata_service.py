import json
from datetime import datetime
from models import db, EtlMetadata, EtlConnection
from sqlalchemy import create_engine, text, inspect

class MetadataService:
    def _get_connection_by_role(self, role):
        return EtlConnection.query.filter_by(role=role).order_by(EtlConnection.id.desc()).first()

    def get_active_connection_name(self, role):
        conn = self._get_connection_by_role(role)
        return conn.name if conn else "No Connection"

    def _get_engine(self, conn_data):
        if not conn_data: return None
        
        db_type = conn_data.type.upper()
        uri = ""
        if db_type == 'ORACLE':
            uri = f"oracle+oracledb://{conn_data.username}:{conn_data.password}@{conn_data.host}:{conn_data.port}/?service_name={conn_data.schema_db}"
        elif db_type == 'POSTGRES':
            uri = f"postgresql://{conn_data.username}:{conn_data.password}@{conn_data.host}:{conn_data.port}/{conn_data.schema_db}"
        
        if uri:
            return create_engine(uri)
        return None

    def create_sample_tables(self):
        conn_data = self._get_connection_by_role('SOURCE')
        if not conn_data or conn_data.type != 'ORACLE':
            print("DEBUG: No Oracle Source connection found for sample creation.")
            return

        try:
            engine = self._get_engine(conn_data)
            with engine.connect() as conn:
                # Check if EMP exists using SQL directly to be sure
                # Or just try to create and catch error
                try:
                    conn.execute(text("""
                        CREATE TABLE EMP (
                            EMPNO NUMBER(4) NOT NULL,
                            ENAME VARCHAR2(10),
                            JOB VARCHAR2(9),
                            MGR NUMBER(4),
                            HIREDATE DATE,
                            SAL NUMBER(7,2),
                            COMM NUMBER(7,2),
                            DEPTNO NUMBER(2),
                            CONSTRAINT PK_EMP PRIMARY KEY (EMPNO)
                        )
                    """))
                    conn.execute(text("INSERT INTO EMP VALUES (7369, 'SMITH', 'CLERK', 7902, TO_DATE('17-12-1980', 'DD-MM-YYYY'), 800, NULL, 20)"))
                    conn.execute(text("INSERT INTO EMP VALUES (7499, 'ALLEN', 'SALESMAN', 7698, TO_DATE('20-02-1981', 'DD-MM-YYYY'), 1600, 300, 30)"))
                    
                    # Add Comments
                    conn.execute(text("COMMENT ON TABLE EMP IS '사원정보'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.EMPNO IS '사원번호'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.ENAME IS '사원명'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.JOB IS '직무'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.MGR IS '관리자'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.HIREDATE IS '입사일'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.SAL IS '급여'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.COMM IS '성과급'"))
                    conn.execute(text("COMMENT ON COLUMN EMP.DEPTNO IS '부서번호'"))
                    
                    conn.commit()
                    print("DEBUG: Created EMP table with comments.")
                except Exception as e:
                    if 'ORA-00955' in str(e):
                        print("DEBUG: EMP table already exists.")
                        # Try to add comments even if table exists (in case they are missing)
                        try:
                            conn.execute(text("COMMENT ON TABLE EMP IS '사원정보'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.EMPNO IS '사원번호'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.ENAME IS '사원명'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.JOB IS '직무'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.MGR IS '관리자'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.HIREDATE IS '입사일'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.SAL IS '급여'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.COMM IS '성과급'"))
                            conn.execute(text("COMMENT ON COLUMN EMP.DEPTNO IS '부서번호'"))
                            conn.commit()
                        except:
                            pass
                    else:
                        print(f"ERROR: Failed to create EMP: {e}")

                try:
                    conn.execute(text("""
                        CREATE TABLE DEPT (
                            DEPTNO NUMBER(2) NOT NULL,
                            DNAME VARCHAR2(14),
                            LOC VARCHAR2(13),
                            CONSTRAINT PK_DEPT PRIMARY KEY (DEPTNO)
                        )
                    """))
                    conn.execute(text("INSERT INTO DEPT VALUES (10, 'ACCOUNTING', 'NEW YORK')"))
                    conn.execute(text("INSERT INTO DEPT VALUES (20, 'RESEARCH', 'DALLAS')"))
                    conn.execute(text("INSERT INTO DEPT VALUES (30, 'SALES', 'CHICAGO')"))
                    conn.execute(text("INSERT INTO DEPT VALUES (40, 'OPERATIONS', 'BOSTON')"))
                    conn.commit()
                    print("DEBUG: Created DEPT table.")
                except Exception as e:
                    if 'ORA-00955' in str(e):
                        print("DEBUG: DEPT table already exists.")
                    else:
                        print(f"ERROR: Failed to create DEPT: {e}")
                    
        except Exception as e:
            print(f"ERROR: Failed to connect for sample tables: {e}")

    def get_source_tables(self):
        conn_data = self._get_connection_by_role('SOURCE')
        if not conn_data: return []

        try:
            engine = self._get_engine(conn_data)
            # Use raw SQL for Oracle as Inspector is having issues
            if conn_data.type == 'ORACLE':
                return self._get_oracle_metadata(engine, conn_data.username.upper())
            
            # Fallback for others (or if we fix Inspector)
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            tables = []
            for t_name in table_names:
                columns = []
                for col in inspector.get_columns(t_name):
                    columns.append({
                        "name": col['name'],
                        "type": str(col['type']),
                        "pk": col.get('primary_key', False),
                        "nullable": col.get('nullable', True)
                    })
                tables.append({"table_name": t_name, "columns": columns})
            return tables
        except Exception as e:
            print(f"ERROR: Failed to fetch source tables: {e}")
            return []

    def _get_oracle_metadata(self, engine, schema):
        tables = []
        with engine.connect() as conn:
            # Get Tables with Comments
            t_result = conn.execute(text("""
                SELECT t.table_name, c.comments 
                FROM all_tables t
                LEFT JOIN all_tab_comments c ON t.owner = c.owner AND t.table_name = c.table_name
                WHERE t.owner = :schema 
                ORDER BY t.table_name
            """), {"schema": schema})
            
            table_rows = t_result.fetchall()
            
            for t_row in table_rows:
                t_name = t_row[0]
                t_comment = t_row[1]
                
                # Get Columns with Comments
                c_result = conn.execute(text("""
                    SELECT c.column_name, c.data_type, c.nullable, com.comments
                    FROM all_tab_columns c
                    LEFT JOIN all_col_comments com ON c.owner = com.owner 
                        AND c.table_name = com.table_name 
                        AND c.column_name = com.column_name
                    WHERE c.owner = :schema AND c.table_name = :table_name 
                    ORDER BY c.column_id
                """), {"schema": schema, "table_name": t_name})
                
                # Get PKs
                pk_result = conn.execute(text("""
                    SELECT cols.column_name
                    FROM all_constraints cons
                    JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
                    WHERE cons.owner = :schema 
                    AND cons.table_name = :table_name 
                    AND cons.constraint_type = 'P'
                """), {"schema": schema, "table_name": t_name})
                pks = [row[0] for row in pk_result]
                
                columns = []
                for row in c_result:
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "pk": row[0] in pks,
                        "nullable": row[2] == 'Y',
                        "comment": row[3]
                    })
                tables.append({
                    "table_name": t_name, 
                    "comment": t_comment,
                    "columns": columns
                })
        return tables

    def get_real_target_tables(self):
        conn_data = self._get_connection_by_role('TARGET')
        if not conn_data: return []

        try:
            engine = self._get_engine(conn_data)
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            
            tables = []
            for t_name in table_names:
                columns = []
                for col in inspector.get_columns(t_name):
                    columns.append({
                        "name": col['name'],
                        "type": str(col['type']),
                        "pk": col.get('primary_key', False),
                        "nullable": col.get('nullable', True)
                    })
                # Format to match template expectation (schema_info as list of cols)
                tables.append({"table_name": t_name, "schema_info": columns})
            return tables
        except Exception as e:
            print(f"ERROR: Failed to fetch target tables: {e}")
            return []

    def get_target_tables_metadata(self):
        # Renamed old method to distinguish from real DB fetch
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
