import os
import webbrowser
import threading
import time

# Add the backend directory to Python path
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from app import app

def open_browser():
    """Wait a moment for the server to start, then open the browser"""
    time.sleep(2)
    webbrowser.open_new('http://127.0.0.1:5000/')

if __name__ == '__main__':
    print("🚀 Starting ReachIQ Server...")
    print("📁 Serving frontend from:", app.static_folder)
    print("🔗 API running on: http://127.0.0.1:5000")
    print("📊 Health check: http://127.0.0.1:5000/api/health")
    
    # Start browser in a separate thread
    threading.Thread(target=open_browser).start()
    
    # Run the Flask app
    app.run(debug=True, port=5000)