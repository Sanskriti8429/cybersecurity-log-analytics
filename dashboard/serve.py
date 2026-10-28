import json
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pymongo import MongoClient

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/alerts.json":
            client = MongoClient("mongodb://localhost:27018", serverSelectionTimeoutMS=3000)
            docs = list(client["security_logs"]["alerts"].find())
            for d in docs:
                d["_id"] = str(d["_id"])
            body = json.dumps(docs).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

if __name__ == "__main__":
    print("Serving at http://localhost:8000  (Ctrl+C to stop)")
    HTTPServer(("", 8000), Handler).serve_forever()