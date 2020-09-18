from flask import Flask
from flask import render_template
from flask import request
from flask import send_from_directory
import subprocess

from kompass import *


app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def root():
    if request.method == 'GET' or 'POST':
        return render_template('home.html')

@app.route('/results')
def results():
    process = subprocess.Popen('python3 kompass.py', shell=True)
    process.wait()

    return render_template('result.html')



if __name__ == "__main__":
  app.run()