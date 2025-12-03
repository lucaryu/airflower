from models import db, EtlDagHistory, EtlMapping, EtlTemplate, EtlMetadata
from jinja2 import Template
from datetime import datetime
import json

class DagService:
    def get_history(self):
        return EtlDagHistory.query.order_by(EtlDagHistory.created_at.desc()).all()

    def generate_dag(self, mapping_id, template_id):
        mapping = EtlMapping.query.get(mapping_id)
        template = EtlTemplate.query.get(template_id)
        
        if not mapping or not template:
            raise ValueError("Invalid Mapping or Template ID")

        target_table = EtlMetadata.query.get(mapping.target_table_id)
        source_table = EtlMetadata.query.get(mapping.source_table_id)
        
        # Prepare context for Jinja2
        context = {
            "dag_id": f"etl_{source_table.table_name}_to_{target_table.table_name}_{datetime.now().strftime('%Y%m%d')}",
            "source_table": source_table.table_name,
            "target_table": target_table.table_name,
            "mappings": json.loads(mapping.mapping_json),
            "created_at": datetime.now().isoformat()
        }
        
        # Render template
        jinja_template = Template(template.template_content)
        generated_code = jinja_template.render(context)
        
        # Save history
        history = EtlDagHistory(
            dag_id=context['dag_id'],
            mapping_id=mapping.id,
            generated_code=generated_code,
            created_at=datetime.utcnow()
        )
        db.session.add(history)
        db.session.commit()
        
        return history

    def get_dag_code(self, history_id):
        return EtlDagHistory.query.get(history_id).generated_code
