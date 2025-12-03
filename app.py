from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
import os
import sys
import pypyodbc

# Patch pypyodbc to look like pyodbc for SQLAlchemy
sys.modules['pyodbc'] = pypyodbc

from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects import registry

# Register oracle.pypyodbc to use the oracle.pyodbc dialect
registry.register("oracle.pypyodbc", "sqlalchemy.dialects.oracle.pyodbc", "OracleDialect_pyodbc")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_key_secret'
from config_manager import ConfigManager

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev_key_secret'

# Load DB URI from config
app.config['SQLALCHEMY_DATABASE_URI'] = ConfigManager.get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Import routes after app/db initialization to avoid circular imports
from routes import *

if __name__ == '__main__':
    app.run(debug=True, port=5000)
