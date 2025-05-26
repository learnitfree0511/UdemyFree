
from flask import Flask, request
import subprocess
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Udemy Free Crawler API is running."

@app.route('/run', methods=['POST'])
def run_script():
    try:
        result = subprocess.run(["python3", "udemy_free.py"], capture_output=True, text=True)
        return {
            "status": result.returncode,
            "output": result.stdout[-1000:],
            "error": result.stderr[-1000:]
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
