from flask import Flask
from routes.city import city_bp
from routes.district import district_bp
from routes.region import region_bp

app = Flask(__name__)

# register blueprints
app.register_blueprint(city_bp)
app.register_blueprint(district_bp)
app.register_blueprint(region_bp)

@app.route("/")
def home():
    return {"status": "OK", "message": "DB2 + Flask API Ready"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
