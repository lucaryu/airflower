import json
from datetime import datetime
from models import db, EtlMapping, EtlMetadata

class MappingService:
    def get_mappings(self):
        return EtlMapping.query.all()

    def save_mapping(self, source_table_id, target_table_id, mapping_data):
        # mapping_data is a list of dicts: {source_col, target_col, rule, transformation}
        
        new_mapping = EtlMapping(
            source_table_id=source_table_id,
            target_table_id=target_table_id,
            mapping_json=json.dumps(mapping_data),
            etl_cry_dtm=datetime.utcnow()
        )
        db.session.add(new_mapping)
        db.session.commit()
        return new_mapping

    def get_mapping_by_ids(self, source_id, target_id):
        return EtlMapping.query.filter_by(source_table_id=source_table_id, target_table_id=target_table_id).first()
