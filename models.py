from app import db
from datetime import datetime

class EtlMetadata(db.Model):
    __tablename__ = 'tb_etl_meta_mst'
    id = db.Column(db.Integer, primary_key=True)
    table_name = db.Column(db.String(100), nullable=False)
    db_type = db.Column(db.String(20), nullable=False) # ORACLE, POSTGRES
    schema_info = db.Column(db.Text, nullable=True) # JSON or Text representation of columns
    etl_cry_dtm = db.Column(db.DateTime, default=datetime.utcnow)

class EtlMapping(db.Model):
    __tablename__ = 'tb_etl_map_def'
    id = db.Column(db.Integer, primary_key=True)
    source_table_id = db.Column(db.Integer, db.ForeignKey('tb_etl_meta_mst.id'))
    target_table_id = db.Column(db.Integer, db.ForeignKey('tb_etl_meta_mst.id'))
    mapping_json = db.Column(db.Text, nullable=False) # JSON storing column mappings and rules
    etl_cry_dtm = db.Column(db.DateTime, default=datetime.utcnow)

class EtlTemplate(db.Model):
    __tablename__ = 'tb_etl_tmpl_mst'
    id = db.Column(db.Integer, primary_key=True)
    template_name = db.Column(db.String(100), nullable=False)
    template_type = db.Column(db.String(50), nullable=False) # ORACLE_S3_POSTGRES, etc.
    template_content = db.Column(db.Text, nullable=False) # Jinja2 template content
    
class EtlConnection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(20), nullable=False) # ORACLE, POSTGRES
    host = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    schema_db = db.Column(db.String(100), nullable=False) # Service Name or DB Name
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(100), nullable=False) # Plain text for demo
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class EtlDagHistory(db.Model):
    __tablename__ = 'tb_etl_dag_hist'
    id = db.Column(db.Integer, primary_key=True)
    dag_id = db.Column(db.String(100), nullable=False)
    mapping_id = db.Column(db.Integer, db.ForeignKey('tb_etl_map_def.id'))
    generated_code = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
