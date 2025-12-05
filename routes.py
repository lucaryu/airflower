from app import app, db
import json
from flask import render_template, request, jsonify, redirect, url_for, flash
from models import EtlMetadata, EtlMapping, EtlTemplate, EtlDagHistory, EtlConnection

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
        elif action == 'generate_ddl':
            source_table = request.form.get('source_table')
            sources = service.get_source_tables()
            source_data = next((t for t in sources if t['table_name'] == source_table), None)
            if source_data:
                # Convert to target columns first (to map types)
                target_columns = []
                for col in source_data['columns']:
                    pg_type = service._map_oracle_to_postgres(col['type'])
                    target_columns.append({
                        "name": col['name'].upper(),
                        "type": pg_type,
                        "pk": col['pk'],
                        "nullable": col['nullable'],
                        "comment": col.get('comment')
                    })
                
                ddl = service.generate_target_ddl(source_table, target_columns)
                return jsonify({"status": "success", "ddl": ddl})
            return jsonify({"status": "error", "message": "Source table not found"})
        elif action == 'generate_drop_ddl':
            target_table = request.form.get('target_table')
            if target_table:
                ddl = service.generate_drop_ddl(target_table)
                return jsonify({"status": "success", "ddl": ddl})
            return jsonify({"status": "error", "message": "Target table not specified"})
        elif action == 'delete_target':
            target_table = request.form.get('target_table').strip()
            print(f"DEBUG: Deleting target table: '{target_table}'")
            service.delete_target_table(target_table)
        
        return redirect(url_for('metadata'))
                
        return redirect(url_for('metadata'))
                
    # Create sample tables if needed (Source Oracle)
    service.create_sample_tables()
    
    source_tables = service.get_source_tables()
    # Use real target tables from DB, or keep using metadata?
    # User asked: "target DB from table search" -> implies real tables from Target DB.
    # But the UI expects "target_tables" to be the ones we manage/delete.
    # Actually, "Target Table" list in UI usually shows what we have defined/mapped.
    # But user said: "search table in target DB".
    # Let's show real tables from Target DB in the list, but maybe we need to distinguish 
    # between "managed targets" and "all targets".
    # For now, let's show ALL tables from Target DB as requested.
    target_tables = service.get_real_target_tables()
    
    # Check if source tables exist in target
    target_table_names = {t['table_name'] for t in target_tables}
    for st in source_tables:
        st['exists_in_target'] = st['table_name'] in target_table_names
    
    source_conn_name = service.get_active_connection_name('SOURCE')
    target_conn_name = service.get_active_connection_name('TARGET')

    return render_template('metadata.html', 
                         source_tables=source_tables, 
                         target_tables=target_tables,
                         source_conn_name=source_conn_name,
                         target_conn_name=target_conn_name)

from services.mapping_service import MappingService

@app.route('/mapping', methods=['GET', 'POST'])
def mapping():
    meta_service = MetadataService()
    map_service = MappingService()
    
    if request.method == 'POST':
        data = request.json
        source_id = data.get('source_table_id')
        target_id = data.get('target_table_id')
        mapping_type = data.get('mapping_type', '1:1')
        mappings = data.get('mappings')
        mapping_id = data.get('id') # Get ID if updating
        
        # Structure the data to include type
        mapping_data = {
            "type": mapping_type,
            "mappings": mappings
        }
        
        map_service.save_mapping(source_id, target_id, mapping_data, mapping_id)
        return jsonify({"status": "success"})

    # Fetch available source and target tables for the dropdowns
    # In a real scenario, we'd fetch IDs. For now, we'll pass the full objects.
    # We need to make sure we have IDs for source tables too, but our mock service returns dicts.
    # Let's assume for this demo that we select by Name.
    
    sources = meta_service.get_source_tables()
    targets = meta_service.get_real_target_tables()
    
    return render_template('mapping.html', sources=sources, targets=targets)

@app.route('/mappings')
def mapping_list():
    source_filter = request.args.get('source')
    target_filter = request.args.get('target')
    
    map_service = MappingService()
    mappings = map_service.get_mappings_with_names(source_filter, target_filter)
    return render_template('mapping_list.html', mappings=mappings, source_filter=source_filter, target_filter=target_filter)

@app.route('/mappings/delete/<int:id>', methods=['POST'])
def delete_mapping(id):
    map_service = MappingService()
    if map_service.delete_mapping(id):
        flash('Mapping deleted successfully.', 'success')
    else:
        flash('Error deleting mapping.', 'danger')
    return redirect(url_for('mapping_list'))

@app.route('/api/mappings/<int:id>')
def get_mapping_details(id):
    map_service = MappingService()
    meta_service = MetadataService()
    
    mapping = map_service.get_mapping(id)
    if mapping:
        # Fetch live metadata to get column details
        # Note: This might be slow if there are many tables. 
        # In a production app, we should fetch only specific table metadata.
        source_tables = meta_service.get_source_tables()
        target_tables = meta_service.get_real_target_tables()
        
        source_info = next((t for t in source_tables if t['table_name'] == mapping.source_table.table_name), None)
        target_info = next((t for t in target_tables if t['table_name'] == mapping.target_table.table_name), None)
        
        # Normalize column data
        source_cols = source_info['columns'] if source_info else []
        target_cols = target_info['schema_info'] if target_info else [] # get_real_target_tables uses schema_info
        
        return jsonify({
            "status": "success",
            "mapping": json.loads(mapping.mapping_json),
            "source_table_id": mapping.source_table_id,
            "target_table_id": mapping.target_table_id,
            "source_table_name": mapping.source_table.table_name,
            "target_table_name": mapping.target_table.table_name,
            "source_columns": source_cols,
            "target_columns": target_cols
        })
    return jsonify({"status": "error", "message": "Mapping not found"}), 404

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
        action = None
        if request.is_json:
            action = request.json.get('action')
        
        if not action:
            action = request.form.get('action')
        if action == 'create':
            data = {
                'name': request.form.get('name'),
                'role': request.form.get('role'),
                'type': request.form.get('type'),
                'host': request.form.get('host'),
                'port': request.form.get('port'),
                'schema_db': request.form.get('schema_db'),
                'username': request.form.get('username'),
                'password': request.form.get('password')
            }
            service.save_connection(data)
        elif action == 'update':
            id = request.form.get('id')
            data = {
                'name': request.form.get('name'),
                'role': request.form.get('role'),
                'type': request.form.get('type'),
                'host': request.form.get('host'),
                'port': request.form.get('port'),
                'schema_db': request.form.get('schema_db'),
                'username': request.form.get('username'),
                'password': request.form.get('password')
            }
            service.update_connection(id, data)
        elif action == 'delete':
            id = request.form.get('id')
            service.delete_connection(id)
        elif action == 'test':
            # For AJAX test
            data = request.json
            if 'id' in data:
                # Test existing connection
                conn = EtlConnection.query.get(data['id'])
                if conn:
                    test_data = {
                        'type': conn.type,
                        'host': conn.host,
                        'port': conn.port,
                        'schema_db': conn.schema_db,
                        'username': conn.username,
                        'password': conn.password
                    }
                    success, message = service.test_connection(test_data)
                else:
                    success, message = False, "Connection not found"
            else:
                # Test new connection data (if needed in future)
                success, message = service.test_connection(data)
                
            return jsonify({"success": success, "message": message})
            
        return redirect(url_for('connections'))

    connections = service.get_all_connections()
    return render_template('connections.html', connections=connections)

from config_manager import ConfigManager
from services.user_service import UserService

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        action = request.form.get('action')
        profile_name = request.form.get('profile_name')
        
        # Common config data collection
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

        if action == 'save':
            # Save to currently selected profile (passed as hidden field or inferred)
            # For simplicity, let's assume 'profile_name' is the target
            if ConfigManager.save_profile(profile_name, config_data):
                flash(f'Profile "{profile_name}" saved successfully!', 'success')
                
        elif action == 'save_as':
            new_name = request.form.get('new_profile_name')
            if new_name:
                if ConfigManager.save_profile(new_name, config_data):
                    flash(f'Profile "{new_name}" created successfully!', 'success')
                    # Optionally switch to it?
                    
        elif action == 'activate':
            if ConfigManager.set_active_profile(profile_name):
                # Hot reload logic
                new_uri = ConfigManager.get_db_uri() # Uses active profile
                app.config['SQLALCHEMY_DATABASE_URI'] = new_uri
                try:
                    db.engine.dispose()
                    print(f"DEBUG: Database connection reloaded to {new_uri}")
                except Exception as e:
                    print(f"ERROR: Failed to dispose engine: {e}")
                flash(f'Profile "{profile_name}" activated!', 'success')
                
        elif action == 'delete':
            if ConfigManager.delete_profile(profile_name):
                flash(f'Profile "{profile_name}" deleted.', 'warning')
                
        # User Management Actions
        elif action == 'create_user':
            user_service = UserService()
            first_name = request.form.get('first_name')
            last_name = request.form.get('last_name')
            department = request.form.get('department')
            if first_name and last_name and department:
                user_service.save_user(first_name, last_name, department)
                flash('User added successfully.', 'success')
            else:
                flash('All user fields are required.', 'danger')
                
        elif action == 'activate_user':
            user_service = UserService()
            user_id = request.form.get('user_id')
            if user_service.set_active_user(user_id):
                flash('User activated.', 'success')
                
        elif action == 'delete_user':
            user_service = UserService()
            user_id = request.form.get('user_id')
            if user_service.delete_user(user_id):
                flash('User deleted.', 'warning')

        return redirect(url_for('settings'))

    # GET request
    profiles = ConfigManager.get_profiles()
    active_profile_name = ConfigManager.get_active_profile_name()
    
    # User Data
    user_service = UserService()
    users = user_service.get_all_users()
    active_user = user_service.get_active_user()
    
    # We need to know which profile is "selected" for viewing/editing.
    # Default to active profile if not specified via query param.
    selected_profile_name = request.args.get('profile', active_profile_name)
    if selected_profile_name not in profiles:
        selected_profile_name = active_profile_name
        
    selected_config = profiles.get(selected_profile_name)
    
    return render_template('settings.html', 
                         profiles=profiles, 
                         active_profile_name=active_profile_name,
                         selected_profile_name=selected_profile_name,
                         config=selected_config,
                         users=users,
                         active_user=active_user)

@app.route('/settings/test', methods=['POST'])
def test_settings_connection():
    data = request.json
    uri = ConfigManager.get_db_uri(data)
    
    from sqlalchemy import create_engine, text
    try:
        engine = create_engine(uri)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"success": True, "message": "Connection successful!"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})
