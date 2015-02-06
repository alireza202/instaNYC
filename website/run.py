#!/usr/bin/env python
from app import app
#from flask import Flask
#app.run('0.0.0.0', port=5000, debug=False)

#app = Flask(__name__)

if __name__ == "__main__":
    app.run('0.0.0.0', port=80, debug=False)