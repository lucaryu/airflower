from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
import os

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
