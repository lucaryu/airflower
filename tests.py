import unittest
import os
os.environ['DATABASE_URL'] = 'sqlite:///:memory:'

from app import app, db
from services.metadata_service import MetadataService
from services.mapping_service import MappingService
from services.template_service import TemplateService
from services.dag_service import DagService
import json

class TestEtlManager(unittest.TestCase):
    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        app.config['TESTING'] = True
        self.app = app.test_client()
        with app.app_context():
            db.drop_all()
            db.create_all()

    def test_metadata_flow(self):
        with app.app_context():
            service = MetadataService()
            # Test Source Tables (Mock)
            sources = service.get_source_tables()
            self.assertEqual(len(sources), 2)
            self.assertEqual(sources[0]['table_name'], 'EMP')
            
            # Test Target Creation
            target = service.create_target_from_source('EMP', sources[0]['columns'])
            self.assertEqual(target.table_name, 'EMP')
            self.assertEqual(target.db_type, 'POSTGRES')
            
            # Test Target List
            targets = service.get_target_tables()
            self.assertEqual(len(targets), 1)

    def test_mapping_flow(self):
        with app.app_context():
            meta_service = MetadataService()
            map_service = MappingService()
            
            # Setup Metadata
            sources = meta_service.get_source_tables()
            target = meta_service.create_target_from_source('EMP', sources[0]['columns'])
            
            # Create Mapping
            mappings = [
                {"source_column": "EMPNO", "target_column": "EMPNO", "rule_type": "DIRECT", "custom_sql": ""},
                {"source_column": "ENAME", "target_column": "ENAME", "rule_type": "NVL", "custom_sql": ""}
            ]
            
            saved_map = map_service.save_mapping(1, target.id, mappings) # 1 is dummy source id
            self.assertIsNotNone(saved_map.id)
            
            # Verify JSON storage
            stored_mappings = json.loads(saved_map.mapping_json)
            self.assertEqual(len(stored_mappings), 2)

    def test_dag_generation(self):
        with app.app_context():
            meta_service = MetadataService()
            map_service = MappingService()
            tmpl_service = TemplateService()
            dag_service = DagService()
            
            # Setup Data
            sources = meta_service.get_source_tables()
            target = meta_service.create_target_from_source('EMP', sources[0]['columns'])
            
            mappings = [{"source_column": "EMPNO", "target_column": "EMPNO", "rule_type": "DIRECT", "custom_sql": ""}]
            saved_map = map_service.save_mapping(1, target.id, mappings)
            
            template_content = "DAG_ID: {{ dag_id }}, SOURCE: {{ source_table }}, TARGET: {{ target_table }}"
            saved_tmpl = tmpl_service.save_template("Test Template", "TEST", template_content)
            
            # Generate DAG
            history = dag_service.generate_dag(saved_map.id, saved_tmpl.id)
            
            self.assertIn("DAG_ID: etl_EMP_to_EMP", history.generated_code)
            self.assertIn("SOURCE: EMP", history.generated_code)

if __name__ == '__main__':
    unittest.main()
