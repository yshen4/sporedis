from flask import Flask

app = Flask(__name__)
app.config.from_object('sporedis.settings')

from . import main

