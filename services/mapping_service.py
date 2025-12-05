import json
from datetime import datetime
from models import db, EtlMapping, EtlMetadata
from sqlalchemy.orm import aliased

class MappingService:
    def get_mappings(self):
        return EtlMapping.query.all()

    def get_mappings_with_names(self, source_filter=None, target_filter=None):
        SourceMeta = aliased(EtlMetadata)
        TargetMeta = aliased(EtlMetadata)
        
        query = db.session.query(EtlMapping, SourceMeta.table_name, TargetMeta.table_name)\
            .join(SourceMeta, EtlMapping.source_table_id == SourceMeta.id)\
            .join(TargetMeta, EtlMapping.target_table_id == TargetMeta.id)
            
        if source_filter:
            query = query.filter(SourceMeta.table_name.ilike(f'%{source_filter}%'))
        if target_filter:
            query = query.filter(TargetMeta.table_name.ilike(f'%{target_filter}%'))
            
        results = query.order_by(EtlMapping.id.desc()).all()
            
        mappings = []
        for m, s_name, t_name in results:
            mappings.append({
                "id": m.id,
                "source_table": s_name,
                "target_table": t_name,
                "created_at": m.etl_cry_dtm,
                "mapping_json": json.loads(m.mapping_json)
            })
        return mappings

    def save_mapping(self, source_table, target_table, mapping_data, mapping_id=None):
        # source_table and target_table can be IDs (int) or Names (str)
        
        source_id = self._resolve_metadata_id(source_table, 'ORACLE')
        target_id = self._resolve_metadata_id(target_table, 'POSTGRES')

        if not source_id or not target_id:
            raise Exception("Could not resolve Source or Target table metadata.")

        if mapping_id:
            # Update existing
            mapping = EtlMapping.query.get(mapping_id)
            if mapping:
                mapping.source_table_id = source_id
                mapping.target_table_id = target_id
                mapping.mapping_json = json.dumps(mapping_data)
                mapping.etl_cry_dtm = datetime.utcnow() # Update timestamp
                db.session.commit()
                return mapping

        # Create new
        new_mapping = EtlMapping(
            source_table_id=source_id,
            target_table_id=target_id,
            mapping_json=json.dumps(mapping_data),
            etl_cry_dtm=datetime.utcnow()
        )
        db.session.add(new_mapping)
        db.session.commit()
        return new_mapping

    def _resolve_metadata_id(self, identifier, default_type):
        # If it's already an ID (int), verify it exists
        try:
            id_val = int(identifier)
            if EtlMetadata.query.get(id_val):
                return id_val
        except:
            pass
            
        # It's a name, look it up
        name = str(identifier)
        meta = EtlMetadata.query.filter_by(table_name=name).first()
        if meta:
            return meta.id
            
        # Not found, create it
        # Note: We don't have schema_info here, but that's okay for just linking
        new_meta = EtlMetadata(
            table_name=name,
            db_type=default_type,
            schema_info='[]',
            etl_cry_dtm=datetime.utcnow()
        )
        db.session.add(new_meta)
        db.session.commit()
        return new_meta.id

    def get_mapping_by_ids(self, source_id, target_id):
        return EtlMapping.query.filter_by(source_table_id=source_id, target_table_id=target_id).first()

    def get_mapping(self, id):
        return EtlMapping.query.get(id)

    def delete_mapping(self, id):
        mapping = EtlMapping.query.get(id)
        if mapping:
            db.session.delete(mapping)
            db.session.commit()
            return True
        return False
