# -*- coding: utf-8 -*-
"""bdl-2023-assignment1-main_app.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/19ImsH-y64-oZAFOavnyXfszjZ5Pc3xR_
"""

import flask

import main

app = flask.Flask(__name__)

@app.route("/")
def index():
    return main.return_fibonacci(flask.request)