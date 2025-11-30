# routes/__init__.py
from flask import Blueprint
from .region import region_bp
from .city import city_bp
from .district import district_bp


def register_routes(app):
    app.register_blueprint(region_bp, url_prefix="/api")
    app.register_blueprint(city_bp, url_prefix="/api")
    app.register_blueprint(district_bp, url_prefix="/api")
