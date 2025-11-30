# app.py
from flask import Flask, render_template
from config import Config
from routes import register_routes


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # تسجيل الراوتات
    register_routes(app)

    @app.route("/")
    def index():
        return render_template("map.html")

    @app.route("/map")
    def map_view():
        return render_template("map.html")

    return app


if __name__ == "__main__":
    app = create_app()
    # شغّل على بورت 5001 زي ما كنت تستخدم
    app.run(host="0.0.0.0", port=5001, debug=True)
