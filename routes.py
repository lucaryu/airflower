from app import app, db
from flask import render_template, request, jsonify, redirect, url_for
from models import EtlMetadata, EtlMapping, EtlTemplate, EtlDagHistory

from services.metadata_service import MetadataService

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/metadata', methods=['GET', 'POST'])
def metadata():
    service = MetadataService()
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'create_target':
            source_table = request.form.get('source_table')
            # In a real app, we'd fetch the specific source columns again or pass them hidden
            # For this demo, we'll just re-fetch from the mock service
            sources = service.get_source_tables()
            source_data = next((t for t in sources if t['table_name'] == source_table), None)
            if source_data:
                service.create_target_from_source(source_table, source_data['columns'])
        elif action == 'delete_target':
            target_table = request.form.get('target_table').strip()
            print(f"DEBUG: Deleting target table: '{target_table}'")
            service.delete_target_table(target_table)
        
        return redirect(url_for('metadata'))
                
    source_tables = service.get_source_tables()
    target_tables = service.get_target_tables()
    return render_template('metadata.html', source_tables=source_tables, target_tables=target_tables)

from services.mapping_service import MappingService

@app.route('/mapping', methods=['GET', 'POST'])
def mapping():
    meta_service = MetadataService()
    map_service = MappingService()
    
    if request.method == 'POST':
        data = request.json
        source_id = data.get('source_table_id')
        target_id = data.get('target_table_id')
        mappings = data.get('mappings')
        
        map_service.save_mapping(source_id, target_id, mappings)
        return jsonify({"status": "success"})

    # Fetch available source and target tables for the dropdowns
    # In a real scenario, we'd fetch IDs. For now, we'll pass the full objects.
    # We need to make sure we have IDs for source tables too, but our mock service returns dicts.
    # Let's assume for this demo that we select by Name.
    
    sources = meta_service.get_source_tables()
    targets = meta_service.get_target_tables()
    
    return render_template('mapping.html', sources=sources, targets=targets)

from services.template_service import TemplateService

@app.route('/templates', methods=['GET', 'POST'])
def templates():
    service = TemplateService()
    if request.method == 'POST':
        name = request.form.get('name')
        type = request.form.get('type')
        content = request.form.get('content')
        service.save_template(name, type, content)
    
    templates = service.get_all_templates()
    return render_template('templates.html', templates=templates)

from services.dag_service import DagService
from services.mapping_service import MappingService
from services.template_service import TemplateService

@app.route('/history', methods=['GET', 'POST'])
def history():
    dag_service = DagService()
    map_service = MappingService()
    tmpl_service = TemplateService()
    
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'generate':
            mapping_id = request.form.get('mapping_id')
            template_id = request.form.get('template_id')
            dag_service.generate_dag(mapping_id, template_id)
            
    history = dag_service.get_history()
    mappings = map_service.get_mappings()
    templates = tmpl_service.get_all_templates()
    
    return render_template('history.html', history=history, mappings=mappings, templates=templates)

@app.route('/dag/<code>/<id>')
def view_dag_code(id):
    dag_service = DagService()
    code = dag_service.get_dag_code(id)
    return render_template('view_code.html', code=code)

from services.connection_service import ConnectionService

@app.route('/connections', methods=['GET', 'POST'])
def connections():
    service = ConnectionService()
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'create':
            data = {
                'name': request.form.get('name'),
                'type': request.form.get('type'),
                'host': request.form.get('host'),
                'port': request.form.get('port'),
                'schema_db': request.form.get('schema_db'),
                'username': request.form.get('username'),
                'password': request.form.get('password')
            }
            service.save_connection(data)
        elif action == 'delete':
            id = request.form.get('id')
            service.delete_connection(id)
        elif action == 'test':
            # For AJAX test
            data = request.json
            success, message = service.test_connection(data)
            return jsonify({"success": success, "message": message})
            
        return redirect(url_for('connections'))

    connections = service.get_all_connections()
    return render_template('connections.html', connections=connections)

from config_manager import ConfigManager

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        db_type = request.form.get('db_type')
        
        config_data = {
            "db_type": db_type,
            "host": request.form.get('host'),
            "port": request.form.get('port'),
            "username": request.form.get('username'),
            "password": request.form.get('password')
        }
        
        if db_type == 'sqlite':
            config_data['schema_db'] = request.form.get('schema_db_file')
        else:
            config_data['schema_db'] = request.form.get('schema_db_net')
            
        if ConfigManager.save_config(config_data):
            # In a real app we might flash a message
            pass
            
        return redirect(url_for('settings'))

    config = ConfigManager.load_config()
    return render_template('settings.html', config=config)
