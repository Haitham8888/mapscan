from flask import Flask, request, jsonify, render_template
import requests
import json
import urllib.parse
import os
try:
	import jaydebeapi
	import jpype
	_JDBC_AVAILABLE = True
except Exception:
	# jaydebeapi / jpype not installed or JVM not available; we'll fall back to sqlite when necessary
	_JDBC_AVAILABLE = False
import sqlite3

app = Flask(__name__)

# Default configuration - replace with the actual FeatureLayer URL and field names
# Example layer URL (replace with your Saudi neighborhoods feature layer):
DEFAULT_LAYER_URL = "https://sampleserver6.arcgisonline.com/arcgis/rest/services/Census/MapServer/3"
DEFAULT_NEIGHBOR_FIELD = "NAME"  # field that contains neighborhood name
DEFAULT_POP_FIELD = "POP2000"   # field that contains population (example)


# Sample GeoJSON source: prefer DB-driven SQL when configured via env vars, otherwise fallback to local file
# Environment variables:
#  SAMPLE_GEOJSON_SQL - SQL query that returns one or more rows where first column is a GeoJSON Feature or FeatureCollection string
#  SAMPLE_GEOJSON_JDBC_URL, SAMPLE_GEOJSON_JDBC_DRIVER, SAMPLE_GEOJSON_JDBC_JARS, SAMPLE_GEOJSON_JDBC_USER, SAMPLE_GEOJSON_JDBC_PASS
SAMPLE_GEOJSON_SQL = os.environ.get('SAMPLE_GEOJSON_SQL')
SAMPLE_GEOJSON_JDBC_URL = os.environ.get('SAMPLE_GEOJSON_JDBC_URL')
SAMPLE_GEOJSON_JDBC_DRIVER = os.environ.get('SAMPLE_GEOJSON_JDBC_DRIVER')
SAMPLE_GEOJSON_JDBC_JARS = os.environ.get('SAMPLE_GEOJSON_JDBC_JARS')
SAMPLE_GEOJSON_JDBC_USER = os.environ.get('SAMPLE_GEOJSON_JDBC_USER')
SAMPLE_GEOJSON_JDBC_PASS = os.environ.get('SAMPLE_GEOJSON_JDBC_PASS')

# Local file fallback
SAMPLE_GEOJSON_PATH = os.path.join(os.path.dirname(__file__), 'data', 'sample_neighborhoods.geojson')


_SAMPLE_CACHE = None


def load_sample_data():
	"""Load and cache sample GeoJSON file from the repository."""
	global _SAMPLE_CACHE
	if _SAMPLE_CACHE is not None:
		return _SAMPLE_CACHE

	# First attempt: if JDBC + SQL specified, try to fetch features from DB
	if SAMPLE_GEOJSON_SQL and (_JDBC_AVAILABLE and SAMPLE_GEOJSON_JDBC_URL and SAMPLE_GEOJSON_JDBC_DRIVER):
		try:
			# ensure JVM started if needed
			if not jpype.isJVMStarted():
				if SAMPLE_GEOJSON_JDBC_JARS:
					jars = SAMPLE_GEOJSON_JDBC_JARS.split(os.pathsep)
					jpype.startJVM(classpath=jars)
				else:
					# start default JVM (may fail if no jars provided)
					jpype.startJVM()
			conn = jaydebeapi.connect(SAMPLE_GEOJSON_JDBC_DRIVER, SAMPLE_GEOJSON_JDBC_URL, [SAMPLE_GEOJSON_JDBC_USER, SAMPLE_GEOJSON_JDBC_PASS] if SAMPLE_GEOJSON_JDBC_USER else None, jars=SAMPLE_GEOJSON_JDBC_JARS.split(os.pathsep) if SAMPLE_GEOJSON_JDBC_JARS else None)
			cur = conn.cursor()
			cur.execute(SAMPLE_GEOJSON_SQL)
			rows = cur.fetchall()
			features = []
			for r in rows:
				cell = r[0] if isinstance(r, (list, tuple)) else r
				if isinstance(cell, (bytes, bytearray)):
					cell = cell.decode('utf-8')
				try:
					obj = json.loads(cell)
					# If it's a FeatureCollection, extend
					if isinstance(obj, dict) and obj.get('type') == 'FeatureCollection' and isinstance(obj.get('features'), list):
						features.extend(obj.get('features'))
					elif isinstance(obj, dict) and obj.get('type') == 'Feature':
						features.append(obj)
					elif isinstance(obj, dict):
						# treat dict as properties-only feature
						features.append({'type': 'Feature', 'properties': obj})
					else:
						# ignore non-dict
					except Exception:
					# not JSON, maybe columns are individual properties: try to build a properties dict from row
					if isinstance(r, (list, tuple)):
						# convert cursor.description to keys
						try:
							keys = [c[0] for c in cur.description]
							props = {keys[i]: r[i] for i in range(min(len(keys), len(r)))}
							features.append({'type': 'Feature', 'properties': props})
						except Exception:
							continue
			cur.close()
			conn.close()
			_SAMPLE_CACHE = {'type': 'FeatureCollection', 'features': features}
			return _SAMPLE_CACHE
		except Exception as e:
			# log and fall back to file
			try:
				app.logger.exception('failed to load sample data from JDBC: %s', e)
			except Exception:
				pass

	# Fallback to local file
	if not os.path.isfile(SAMPLE_GEOJSON_PATH):
		_SAMPLE_CACHE = { 'features': [] }
		return _SAMPLE_CACHE
	with open(SAMPLE_GEOJSON_PATH, 'r', encoding='utf-8') as fh:
		_SAMPLE_CACHE = json.load(fh)
	return _SAMPLE_CACHE


def query_sample_sum(neighborhood_field, neighborhood_value, population_field):
	"""Query the local sample GeoJSON and sum population for matching neighborhood name."""
	data = load_sample_data()
	total = 0
	count = 0
	needle = str(neighborhood_value).strip().lower()
	for feat in data.get('features', []):
		props = feat.get('properties', {})
		# try multiple candidate name fields (case-insensitive)
		candidates = []
		for k in [neighborhood_field, neighborhood_field.upper(), neighborhood_field.lower(), 'NAME', 'name', 'EN_NAME', 'en_name']:
			v = props.get(k)
			if v:
				candidates.append(str(v).strip().lower())

		matched = False
		for cand in candidates:
			if cand == needle:
				matched = True
				break

		if not matched:
			# also allow looser match: spaces/hyphens normalized
			norm_needle = needle.replace('-', ' ').replace('_', ' ')
			for cand in candidates:
				if cand.replace('-', ' ').replace('_', ' ') == norm_needle:
					matched = True
					break

		if not matched:
			continue

		val = props.get(population_field) or props.get(population_field.upper()) or props.get('POP') or props.get('pop')
		try:
			if val is None:
				continue
			total += float(val)
			count += 1
		except Exception:
			continue
	return {
		'neighborhood': neighborhood_value,
		'feature_count': count,
		'population_sum': total
	}


def query_sample_feature(neighborhood_field, neighborhood_value):
	"""Return the first matching sample feature plus population breakdown by gender."""
	data = load_sample_data()
	needle = str(neighborhood_value).strip().lower()
	for feat in data.get('features', []):
		props = feat.get('properties', {})
		candidates = []
		for k in [neighborhood_field, neighborhood_field.upper(), neighborhood_field.lower(), 'NAME', 'name', 'EN_NAME', 'en_name']:
			v = props.get(k)
			if v:
				candidates.append(str(v).strip().lower())
		matched = False
		for cand in candidates:
			if cand == needle:
				matched = True
				break
		if not matched:
			norm_needle = needle.replace('-', ' ').replace('_', ' ')
			for cand in candidates:
				if cand.replace('-', ' ').replace('_', ' ') == norm_needle:
					matched = True
					break
		if not matched:
			continue

		# found a matching feature
		pop_m = props.get('POP_M') or props.get('pop_m') or 0
		pop_f = props.get('POP_F') or props.get('pop_f') or 0
		try:
			pop_m = float(pop_m)
		except Exception:
			pop_m = 0
		try:
			pop_f = float(pop_f)
		except Exception:
			pop_f = 0

		# return the feature and counts
		return {
			'neighborhood': props.get('EN_NAME') or props.get('NAME') or neighborhood_value,
			'population_male': pop_m,
			'population_female': pop_f,
			'population_total': pop_m + pop_f,
			'feature': feat
		}

	return None


def query_sample_group_sum(neighborhood_field, population_field):
	"""Return grouped sums from sample geojson keyed by neighborhood_field."""
	data = load_sample_data()
	groups = {}
	for feat in data.get('features', []):
		props = feat.get('properties', {})
		# prefer Arabic NAME, fall back to EN_NAME
		name = props.get(neighborhood_field) or props.get('NAME') or props.get('name') or props.get('EN_NAME')
		if not name:
			continue
		val = props.get(population_field) or props.get('POP') or props.get('pop')
		try:
			v = float(val) if val is not None else 0
		except Exception:
			v = 0
		groups[name] = groups.get(name, 0) + v
	stats = []
	for k, v in groups.items():
		stats.append({'neighborhood': k, 'population_sum': v})
	return sorted(stats, key=lambda x: x['population_sum'], reverse=True)


# Note: Removed use of `data/SA_regions.json` as requested. We'll search the
# allowed external files (geojson/ and json/) instead.


# Optional region population mapping (local test data)
REGION_POP_PATH = os.path.join(os.path.dirname(__file__), 'data', 'region_population.json')
_REGION_POP_CACHE = None


def load_region_populations():
	global _REGION_POP_CACHE
	if _REGION_POP_CACHE is not None:
		return _REGION_POP_CACHE
	if not os.path.isfile(REGION_POP_PATH):
		_REGION_POP_CACHE = {}
		return _REGION_POP_CACHE
	with open(REGION_POP_PATH, 'r', encoding='utf-8') as fh:
		_REGION_POP_CACHE = json.load(fh)
	return _REGION_POP_CACHE



# Population DB support: prefer JDBC when configured, otherwise fallback to sqlite
# Environment variables for JDBC:
#  POP_JDBC_URL, POP_JDBC_DRIVER, POP_JDBC_JARS, POP_JDBC_USER, POP_JDBC_PASS
POP_JDBC_URL = os.environ.get('POP_JDBC_URL')
POP_JDBC_DRIVER = os.environ.get('POP_JDBC_DRIVER')
POP_JDBC_JARS = os.environ.get('POP_JDBC_JARS')
POP_JDBC_USER = os.environ.get('POP_JDBC_USER')
POP_JDBC_PASS = os.environ.get('POP_JDBC_PASS')

# Fallback sqlite path (used when JDBC not configured)
POP_DB_PATH = os.environ.get('POP_DB') or os.path.join(os.path.dirname(__file__), 'data', 'population.db')


def _start_jvm_if_needed(jars_str):
	if not _JDBC_AVAILABLE:
		return
	if not jpype.isJVMStarted():
		if jars_str:
			jars = jars_str.split(os.pathsep)
			jpype.startJVM(classpath=jars)
		else:
			# try starting default JVM
			jpype.startJVM()


def _get_db_connection():
	"""Return a DB connection. If JDBC configured and available, returns a jaydebeapi connection.
	Otherwise returns sqlite3.Connection or None.
	"""
	# Try JDBC first
	if _JDBC_AVAILABLE and POP_JDBC_URL and POP_JDBC_DRIVER:
		try:
			_start_jvm_if_needed(POP_JDBC_JARS)
			jars = POP_JDBC_JARS.split(os.pathsep) if POP_JDBC_JARS else None
			conn = jaydebeapi.connect(POP_JDBC_DRIVER, POP_JDBC_URL, [POP_JDBC_USER, POP_JDBC_PASS] if POP_JDBC_USER else None, jars=jars)
			return conn
		except Exception as e:
			try:
				app.logger.exception('JDBC connection failed: %s', e)
			except Exception:
				pass

	# Fallback to sqlite
	try:
		if not POP_DB_PATH or not os.path.isfile(POP_DB_PATH):
			return None
		conn = sqlite3.connect(POP_DB_PATH)
		conn.row_factory = sqlite3.Row
		return conn
	except Exception:
		return None


def _row_to_dict(cursor, row):
	"""Convert a DB row to a dict using cursor.description when needed."""
	if row is None:
		return None
	# sqlite3.Row is dict-like
	try:
		if isinstance(row, sqlite3.Row):
			return dict(row)
	except Exception:
		pass
	# jaydebeapi returns tuples; use cursor.description
	try:
		cols = [c[0] for c in cursor.description]
		return {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
	except Exception:
		# last resort: try to coerce sequence to dict with numeric keys
		try:
			return {str(i): row[i] for i in range(len(row))}
		except Exception:
			return None


def query_population_by_ids(region_id=None, city_id=None, district_id=None):
	"""Query population table prioritizing district -> city -> region.

	Expected table `population` with columns: region_id, city_id, district_id, pop_m, pop_f, pop_total
	"""
	conn = _get_db_connection()
	if not conn:
		return None
	cur = None
	try:
		cur = conn.cursor()
		# helper to run a param query and coerce row to dict
		def run_query_single(q, params):
			try:
				cur.execute(q, params if params is not None else [])
				row = cur.fetchone()
				rowd = _row_to_dict(cur, row)
				if rowd:
					pop_m = float(rowd.get('pop_m') or rowd.get('POP_M') or 0)
					pop_f = float(rowd.get('pop_f') or rowd.get('POP_F') or 0)
					pop_total = float(rowd.get('pop_total') or rowd.get('POP_TOTAL') or (pop_m + pop_f))
					return {'population_male': pop_m, 'population_female': pop_f, 'population_total': pop_total}
			except Exception:
				return None
			return None

		if district_id:
			res = run_query_single('SELECT pop_m, pop_f, pop_total FROM population WHERE district_id = ? LIMIT 1', (district_id,))
			if res:
				return res
		if city_id:
			res = run_query_single('SELECT pop_m, pop_f, pop_total FROM population WHERE city_id = ? LIMIT 1', (city_id,))
			if res:
				return res
		if region_id:
			res = run_query_single('SELECT pop_m, pop_f, pop_total FROM population WHERE region_id = ? LIMIT 1', (region_id,))
			if res:
				return res
	finally:
		try:
			if cur:
				cur.close()
		except Exception:
			pass
		try:
			conn.close()
		except Exception:
			pass
	return None




# Allowed external files (keys -> relative path).
# Restrict to only the requested geojson files.
ALLOWED_FILES = {
	'geo_regions': os.path.join(os.path.dirname(__file__), 'geojson', 'regions.geojson'),
	'geo_districts': os.path.join(os.path.dirname(__file__), 'geojson', 'districts.geojson'),
	'geo_cities': os.path.join(os.path.dirname(__file__), 'geojson', 'cities.geojson'),
}


def load_external_file(key):
	"""Load a GeoJSON/JSON file from ALLOWED_FILES by key and cache it in memory."""
	path = ALLOWED_FILES.get(key)
	if not path or not os.path.isfile(path):
		return {'features': []}
	with open(path, 'r', encoding='utf-8') as fh:
		try:
			raw = json.load(fh)
		except Exception:
			return {'features': []}

	# If the file is a plain list of features
	if isinstance(raw, list):
		return {'features': raw}

	# If it's a dict already with GeoJSON FeatureCollection
	if isinstance(raw, dict):
		# Standard GeoJSON
		if raw.get('type') == 'FeatureCollection' and isinstance(raw.get('features'), list):
			return raw
		# Direct 'features' key
		if 'features' in raw and isinstance(raw['features'], list):
			return {'features': raw['features']}
		# Common alternative wrappers
		for keyname in ('data', 'rows', 'items'):
			if keyname in raw and isinstance(raw[keyname], list):
				return {'features': raw[keyname]}

		# Try to find the first value that's a list of dicts and treat it as features
		for v in raw.values():
			if isinstance(v, list) and v and all(isinstance(i, dict) for i in v):
				return {'features': v}

		# If the dict itself looks like a single feature (has 'properties' or 'geometry'), wrap it
		if 'properties' in raw or 'geometry' in raw:
			return {'features': [raw]}

	# Fallback: no recognizable features
	return {'features': []}


def query_file_feature(file_key, neighborhood_value, name_field_candidates=None):
	"""Generic search in an external GeoJSON/JSON file for a matching neighborhood name.

	name_field_candidates: list of candidate property names to try (defaults to common names)
	"""
	data = load_external_file(file_key)
	needle = str(neighborhood_value).strip().lower()
	if name_field_candidates is None:
		# include common English and Arabic name fields and variants
		name_field_candidates = [
			'name', 'NAME', 'Name',
			'name_en', 'NAME_EN', 'EN_NAME', 'nameEn', 'nameEn',
			'name_ar', 'NAME_AR', 'AR_NAME', 'nameAr',
			'region', 'region_id', 'code'
		]

	for feat in data.get('features', []):
		# features in allowed files may be plain dicts (with properties at top-level)
		props = {}
		if isinstance(feat, dict):
			if 'properties' in feat and isinstance(feat.get('properties'), dict):
				props = feat.get('properties')
			else:
				# treat the dict itself as properties (e.g., json/regions.json items)
				props = feat
		candidates = []
		for k in name_field_candidates:
			v = props.get(k)
			if v:
				candidates.append(str(v).strip().lower())

		matched = False
		for cand in candidates:
			if cand == needle:
				matched = True
				break
		if not matched:
			norm_needle = needle.replace('-', ' ').replace('_', ' ')
			for cand in candidates:
				if cand.replace('-', ' ').replace('_', ' ') == norm_needle:
					matched = True
					break

		if matched:
			return feat
	return None


def normalize_feature_to_geojson(feat):
	"""Return a GeoJSON Feature dict for various input shapes.

	Converts files that use `boundaries` (arrays of [lat,lon]) into a
	GeoJSON Polygon geometry with coordinates as [lon, lat].
	"""
	# If it's already a GeoJSON Feature
	if not isinstance(feat, dict):
		return None
	if feat.get('type') == 'Feature' and isinstance(feat.get('geometry'), dict):
		return feat

	props = feat.get('properties') if isinstance(feat.get('properties'), dict) else {k: v for k, v in feat.items() if k != 'geometry'}

	# If there's a geometry object already
	if 'geometry' in feat and isinstance(feat['geometry'], dict):
		return {'type': 'Feature', 'properties': props, 'geometry': feat['geometry']}

	# Common boundary field names that contain ring coordinates
	boundary_keys = ['boundaries', 'boundary', 'coordinates', 'coords', 'polygons', 'shape']
	for bk in boundary_keys:
		if bk in feat and isinstance(feat[bk], list) and feat[bk]:
			raw_bound = feat[bk]
			# raw_bound may be a list of rings or a single ring
			rings = []
			if isinstance(raw_bound[0], list) and raw_bound and isinstance(raw_bound[0][0], list):
				# already a list of rings: [[ [lat,lon], ... ], ...]
				for ring in raw_bound:
					# convert lat,lon -> lon,lat
					converted = [[pt[1], pt[0]] for pt in ring]
					rings.append(converted)
			else:
				# single ring of points [[lat,lon],...]
				converted = [[pt[1], pt[0]] for pt in raw_bound]
				rings.append(converted)

			geometry = {'type': 'Polygon', 'coordinates': rings}
			return {'type': 'Feature', 'properties': props, 'geometry': geometry}

	# If properties contain boundaries (some files store geometry inside properties)
	for bk in boundary_keys:
		if bk in props and isinstance(props[bk], list) and props[bk]:
			raw_bound = props[bk]
			rings = []
			if isinstance(raw_bound[0], list) and raw_bound and isinstance(raw_bound[0][0], list):
				for ring in raw_bound:
					converted = [[pt[1], pt[0]] for pt in ring]
					rings.append(converted)
			else:
				converted = [[pt[1], pt[0]] for pt in raw_bound]
				rings.append(converted)
			geometry = {'type': 'Polygon', 'coordinates': rings}
			return {'type': 'Feature', 'properties': props, 'geometry': geometry}

	# As a last resort, wrap dict as feature without geometry
	return {'type': 'Feature', 'properties': props}


def _feature_has_polygon(candidate):
	"""Return True if candidate feature appears to have polygon geometry or boundaries."""
	if not isinstance(candidate, dict):
		return False
	# GeoJSON geometry
	geom = candidate.get('geometry')
	if isinstance(geom, dict):
		t = geom.get('type')
		if t in ('Polygon', 'MultiPolygon'):
			return True
		if t == 'GeometryCollection' and any(g.get('type') in ('Polygon', 'MultiPolygon') for g in geom.get('geometries', []) if isinstance(g, dict)):
			return True
	# properties may include boundaries/boundary/coordinates
	props = candidate.get('properties') if isinstance(candidate.get('properties'), dict) else candidate
	for bk in ('boundaries', 'boundary', 'polygons', 'shape', 'coordinates'):
		if bk in props and isinstance(props[bk], list) and props[bk]:
			return True
	return False


def find_polygon_for_feature(props):
	"""Search allowed files for a feature that matches `props` by id or name and contains polygon geometry.

	Returns a GeoJSON feature or None.
	"""
	if not isinstance(props, dict):
		return None


	# Candidate keys to try for id and names
	id_keys = ['city_id', 'region_id', 'district_id', 'id', 'region_id']
	name_keys = ['name_en', 'name_ar', 'name', 'NAME', 'EN_NAME']

	# Collect values to match
	ids = set()
	names = set()
	for k in id_keys:
		v = props.get(k)
		if v is not None:
			ids.add(str(v))
	for k in name_keys:
		v = props.get(k)
		if v:
			names.add(str(v).strip().lower())

	# scan files for polygon candidates
	# Prefer searching city files first, then districts, then regions, to avoid matching a region polygon when looking for a city
	def _sort_key(k):
		k_lower = k.lower()
		if 'city' in k_lower:
			return 0
		if 'district' in k_lower:
			return 1
		if 'region' in k_lower:
			return 2
		return 3

	for key in sorted(ALLOWED_FILES.keys(), key=_sort_key):
		data = load_external_file(key)
		for cand in data.get('features', []):
			cprops = cand.get('properties') if isinstance(cand, dict) and 'properties' in cand else cand
			# check id match
			matched = False
			for k in id_keys:
				cv = cprops.get(k)
				if cv is not None and str(cv) in ids:
					matched = True
					break
			if not matched:
				# check name match
				for nk in name_keys:
					cv = cprops.get(nk)
					if cv and str(cv).strip().lower() in names:
						matched = True
						break
			if not matched:
				continue

			# if this candidate has polygon-like geometry, normalize and return
			if _feature_has_polygon(cand):
				return normalize_feature_to_geojson(cand)

	return None


@app.route('/stats_city')
def stats_city():
	"""Return a city feature (prefer polygon) by `city_id` or `city_name`.

	Query params:
	- city_id (optional): numeric id
	- city_name (optional): name string
	"""
	city_id = request.args.get('city_id')
	city_name = request.args.get('city_name')
	# Optional region context to disambiguate cities with same name
	region_id = request.args.get('region_id')
	region_name = request.args.get('region_name')

	# Normalize city_name: some UI display strings include an English suffix like
	# "الشرطة — Al Shurta" or "Name — EN". Prefer the left-side (Arabic) part
	# for matching. Also trim whitespace.
	if city_name and isinstance(city_name, str):
		# common separators used in our UI display
		for sep in (' — ', ' - ', ' – ', '—', '–'):
			if sep in city_name:
				city_name = city_name.split(sep, 1)[0].strip()
				break

	# Try city files first (but don't fail if none are configured; we'll fallback to searching all files)
	city_keys = [k for k in ALLOWED_FILES.keys() if 'city' in k]

	# helper to search a list of keys
	def _search_keys(keys):
		for key in keys:
			data = load_external_file(key)
			for feat in data.get('features', []):
				props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat
				if city_id:
					try:
						if str(props.get('city_id') or props.get('id') or '') == str(city_id):
							# respect region scoping if provided
							if region_id and str(props.get('region_id') or '') != str(region_id):
								continue
							if region_name and any(str(props.get(k,'')).strip().lower() == str(region_name).strip().lower() for k in ('region','region_name','name','NAME','name_en','name_ar')):
								# ok if region name matches
								pass
							return key, feat, props
					except Exception:
						pass
				if city_name:
					for namek in ('name_en','name_ar','name','NAME'):
						v = props.get(namek)
						if v and str(v).strip().lower() == str(city_name).strip().lower():
							# check region scoping
							if region_id and str(props.get('region_id') or '') != str(region_id):
								continue
							if region_name:
								# if props contains region fields, require match
								if any(k in props for k in ('region','region_name','region_id')):
									if not any(str(props.get(k,'')).strip().lower() == str(region_name).strip().lower() for k in ('region','region_name','name','NAME','name_en','name_ar')):
										continue
							return key, feat, props
		return None, None, None

	key, feat, props = _search_keys(city_keys)
	app.logger.info('stats_city called city_id=%s city_name=%s region_id=%s region_name=%s', city_id, city_name, region_id, region_name)

	# fallback: try all files if not found in city files
	if not feat:
		# Custom fallback: scan all allowed files but prefer results that represent a city
		needle = city_name or city_id
		potential = (None, None, None)
		found = (None, None, None)
		name_keys = ('name_en','name_ar','name','NAME')
		for k in ALLOWED_FILES.keys():
			data = load_external_file(k)
			for cand in data.get('features', []):
				cprops = cand.get('properties') if isinstance(cand, dict) and 'properties' in cand else cand
				# id match
				try:
					if city_id and str(cprops.get('city_id') or cprops.get('id') or '') == str(city_id):
						# respect region scoping if provided
						if region_id and str(cprops.get('region_id') or '') != str(region_id):
							continue
						# prefer city-level records: skip district records (they also carry city_id)
						if 'district_id' in cprops:
							# treat as potential match but don't prefer it
							if potential[0] is None:
								potential = (k, cand, cprops)
							continue
						found = (k, cand, cprops)
						break
				except Exception:
					pass
				# name match
				if city_name:
					dn = str(city_name).strip().lower()
					matched = False
					for nk in name_keys:
						v = cprops.get(nk)
						if v and str(v).strip().lower() == dn:
							matched = True
							break
					if not matched:
						for nk in name_keys:
							v = cprops.get(nk)
							if v and str(v).strip().lower().replace('-', ' ').replace('_',' ') == dn.replace('-', ' ').replace('_',' '):
								matched = True
								break
					if matched:
						# require region match if provided
						if region_id and str(cprops.get('region_id') or '') != str(region_id):
							continue
						# skip district-level features as preferred (they often contain city_id)
						if 'district_id' in cprops:
							if potential[0] is None:
								potential = (k, cand, cprops)
							continue
						# prefer candidates that include a city_id or are from city files
						if 'city_id' in cprops or 'city' in k:
							found = (k, cand, cprops)
							break
						# otherwise keep as potential if we don't yet have one
						if potential[0] is None:
							potential = (k, cand, cprops)
			if found[0] is not None:
				break
		if found[0] is not None:
			key, feat, props = found
			app.logger.info('stats_city fallback found (preferred) in %s with props id=%s name_en=%s name_ar=%s', key, props.get('city_id') or props.get('id'), props.get('name_en'), props.get('name_ar'))
		elif potential[0] is not None:
			key, feat, props = potential
			app.logger.info('stats_city fallback found (potential) in %s with props id=%s name_en=%s name_ar=%s', key, props.get('city_id') or props.get('id'), props.get('name_en'), props.get('name_ar'))

	if not feat:
		return jsonify({'error': 'city not found'}), 404

	gf = normalize_feature_to_geojson(feat)

	# If there's no geometry but the record has a `center` coordinate, convert it to a Point geometry.
	# Many of our JSON city records store `center` as [lat, lon]; normalize to GeoJSON [lon, lat].
	# NOTE: Do NOT attempt an automatic search for a polygon elsewhere when a city is a point-only
	# (this caused villages or point-only cities to be replaced with unrelated region/district polygons).
	if gf and (not gf.get('geometry') or not isinstance(gf.get('geometry'), dict)):
		center = None
		# props may be in gf['properties'] or already in props
		p = gf.get('properties') or props
		if isinstance(p, dict) and p.get('center') and isinstance(p.get('center'), (list, tuple)) and len(p.get('center')) >= 2:
			center = p.get('center')
		# also check for 'lon'/'lat' fields
		if not center and isinstance(p, dict) and ('latitude' in p and 'longitude' in p):
			center = [p.get('latitude'), p.get('longitude')]
		if center:
			try:
				lat = float(center[0])
				lon = float(center[1])
				gf['geometry'] = {'type': 'Point', 'coordinates': [lon, lat]}
			except Exception:
				pass

	# Do NOT attempt to find/replace the point with another polygon here. If a polygon exists
	# for the city it should be present in the matched record; otherwise we keep the Point.

	app.logger.info('stats_city returning file_key=%s neighborhood=%s props=%s', key, props.get('name_en') or props.get('name_ar') or city_name or city_id, {k: props.get(k) for k in ('city_id','region_id','name_en','name_ar')})
	response = {'file_key': key, 'neighborhood': props.get('name_en') or props.get('name_ar') or city_name or city_id, 'feature': gf, 'properties': props}
	return jsonify(response)


@app.route('/stats_district')
def stats_district():
	"""Return a district feature by `district_id`/`district_name` optionally scoped to a city (`city_id` or `city_name`)."""
	district_id = request.args.get('district_id')
	district_name = request.args.get('district_name')
	city_id = request.args.get('city_id')
	city_name = request.args.get('city_name')

	if not district_id and not district_name:
		return jsonify({'error': 'missing district identifier'}), 400

	# prefer district files first
	district_keys = [k for k in ALLOWED_FILES.keys() if 'district' in k]

	def _search_in_keys(keys):
		for key in keys:
			data = load_external_file(key)
			for feat in data.get('features', []):
				props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat
				if district_id:
					try:
						if str(props.get('district_id') or props.get('id') or '') == str(district_id):
							# if city context provided, ensure match
							if city_id and str(props.get('city_id') or '') != str(city_id):
								continue
							if city_name:
								# compare city name fields if available
								if not any(str(props.get(k,'')).strip().lower() == str(city_name).strip().lower() for k in ('city_name','name','NAME','name_en','name_ar')):
									# if district record doesn't contain city name, we'll still accept and rely on city_id above
									pass
							return key, feat, props
					except Exception:
						pass
				if district_name:
					dn = str(district_name).strip().lower()
					matched = False
					for nk in ('name_en','name_ar','name','NAME'):
						v = props.get(nk)
						if v and str(v).strip().lower() == dn:
							matched = True
							break
					if not matched:
						# loosened normalization
						for nk in ('name_en','name_ar','name','NAME'):
							v = props.get(nk)
							if v and str(v).strip().lower().replace('-', ' ').replace('_',' ') == dn.replace('-', ' ').replace('_',' '):
								matched = True
								break
					if matched:
						# check city scoping if provided
						if city_id and str(props.get('city_id') or '') != str(city_id):
							continue
						if city_name:
							# if props contains city_name fields, require match
							if any(k in props for k in ('city_name','city','region')):
								if not any(str(props.get(k,'')).strip().lower() == str(city_name).strip().lower() for k in ('city_name','city','region','name_en','name_ar')):
									continue
						return key, feat, props
		return None, None, None

	key, feat, props = _search_in_keys(district_keys)
	# fallback: search all files
	if not feat:
		key, feat = query_all_files(district_name or district_id)
		if feat:
			props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat

	if not feat:
		return jsonify({'error': 'district not found'}), 404

	gf = normalize_feature_to_geojson(feat)

	# if point, try find polygon for district
	if gf and gf.get('geometry') and gf.get('geometry').get('type') == 'Point':
		poly = find_polygon_for_feature(props)
		if poly:
			gf = poly

	response = {'file_key': key, 'neighborhood': props.get('name_en') or props.get('name_ar') or district_name or district_id, 'feature': gf, 'properties': props}
	return jsonify(response)


@app.route('/stats_region')
def stats_region():
	"""Return a region feature (prefer polygon) by `region_id` or `region_name`.

	Query params:
	- region_id (optional)
	- region_name (optional)
	"""
	region_id = request.args.get('region_id')
	region_name = request.args.get('region_name')

	# Try region-specific files first
	region_keys = [k for k in ALLOWED_FILES.keys() if 'region' in k]

	def _search_keys(keys):
		for key in keys:
			data = load_external_file(key)
			for feat in data.get('features', []):
				props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat
				if region_id:
					try:
						if str(props.get('region_id') or props.get('id') or '') == str(region_id):
							return key, feat, props
					except Exception:
						pass
				if region_name:
					for namek in ('name_en','name_ar','name','NAME'):
						v = props.get(namek)
						if v and str(v).strip().lower() == str(region_name).strip().lower():
							return key, feat, props
		return None, None, None

	key, feat, props = _search_keys(region_keys)

	# fallback: search all files
	if not feat:
		key, feat = query_all_files(region_name or region_id, None)
		if feat:
			props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat

	if not feat:
		return jsonify({'error': 'region not found'}), 404

	gf = normalize_feature_to_geojson(feat)

	# If returned geometry is Point, try to find polygon for this region specifically
	if gf and gf.get('geometry') and gf.get('geometry').get('type') == 'Point':
		poly = find_polygon_for_feature(props)
		if poly:
			gf = poly

	response = {'file_key': key, 'neighborhood': props.get('name_en') or props.get('name_ar') or region_name or region_id, 'feature': gf, 'properties': props}

	# attach any local population mapping if exists (match by region name)
	pop = load_region_populations()
	region_name_key = response.get('neighborhood')
	pop_entry = None
	if region_name_key:
		pop_entry = pop.get(region_name_key) or pop.get(region_name_key.strip().lower()) or pop.get(region_name_key.strip().upper())
	if pop_entry:
		try:
			male = float(pop_entry.get('POP_M', 0))
		except Exception:
			male = 0
		try:
			female = float(pop_entry.get('POP_F', 0))
		except Exception:
			female = 0
		response.update({'population_male': male, 'population_female': female, 'population_total': male + female})

	return jsonify(response)


@app.route('/city_districts')
def city_districts():
	"""Return a FeatureCollection of district features for a given city.

	Query params:
	- city_id (optional)
	- city_name (optional)
	"""
	city_id = request.args.get('city_id')
	city_name = request.args.get('city_name')
	if not city_id and not city_name:
		return jsonify({'error': 'missing city identifier'}), 400

	# load district files
	district_keys = [k for k in ALLOWED_FILES.keys() if 'district' in k]
	features = []

	def _match_props(props):
		if city_id:
			try:
				if str(props.get('city_id') or props.get('city') or '') == str(city_id):
					return True
			except Exception:
				pass
		if city_name:
			needle = str(city_name).strip().lower()
			for nk in ('name_en','name_ar','name','CITY','city'):
				v = props.get(nk)
				if v and str(v).strip().lower() == needle:
					return True
		return False

	for key in district_keys:
		data = load_external_file(key)
		for feat in data.get('features', []):
			props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat
			if _match_props(props):
				gf = normalize_feature_to_geojson(feat)
				features.append(gf)

	# If no districts found, return an empty FeatureCollection (200).
	# Client will fallback to drawing the city itself when collection is empty.
	return jsonify({'type': 'FeatureCollection', 'features': features})


def query_all_files(neighborhood_value, name_field_candidates=None):
	"""Search all allowed files in order and return the first matching feature.

	Returns a tuple (key, feature) or (None, None) if not found.
	"""
	for key in ALLOWED_FILES.keys():
		feat = query_file_feature(key, neighborhood_value, name_field_candidates)
		if feat:
			return key, feat
	return None, None


def query_all_matches(query_value, name_field_candidates=None, limit=50):
	"""Return a list of matching candidate features across all allowed files.

	Matching is performed by case-insensitive substring match against common name fields.
	Returns list of dicts: {file_key, id, name_en, name_ar, props}
	"""
	q = (query_value or '').strip().lower()
	if not q:
		return []
	if name_field_candidates is None:
		name_field_candidates = [
			'name', 'NAME', 'Name',
			'name_en', 'NAME_EN', 'EN_NAME', 'nameEn',
			'name_ar', 'NAME_AR', 'AR_NAME', 'nameAr'
		]

	results = []
	for key in ALLOWED_FILES.keys():
		data = load_external_file(key)
		for feat in data.get('features', []):
			props = feat.get('properties') if isinstance(feat, dict) and 'properties' in feat else feat
			candidate_names = []
			for nf in name_field_candidates:
				v = props.get(nf)
				if v:
					candidate_names.append(str(v).strip())
			# also include combined display name
			display_name = None
			if candidate_names:
				display_name = candidate_names[0]
			# perform substring match on available names
			match = False
			for cname in candidate_names:
				if q in cname.lower():
					match = True
					break
			if match:
				# id extraction
				id_val = None
				for ik in ('id','city_id','region_id','district_id'):
					if ik in props:
						id_val = props.get(ik)
						break
				results.append({'file_key': key, 'id': id_val, 'name_en': props.get('name_en') or props.get('NAME') or props.get('name'), 'name_ar': props.get('name_ar') or props.get('NAME') or props.get('name'), 'props': props})
				if len(results) >= limit:
					return results
	return results


@app.route('/stats_search_all')
def stats_search_all():
	"""Search across all provided geojson/json files for a neighborhood name.

	Query params:
	- neighborhood (required)
	- name_field (optional) comma-separated candidate property names
	"""
	neighborhood = request.args.get('neighborhood')
	if not neighborhood:
		return jsonify({'error': 'missing neighborhood parameter'}), 400

	name_field_param = request.args.get('name_field')
	candidates = None
	if name_field_param:
		candidates = [s.strip() for s in name_field_param.split(',') if s.strip()]

	key, feat = query_all_files(neighborhood, candidates)
	if not feat:
		return jsonify({'error': 'not found in allowed files'}), 404

	# normalize to GeoJSON feature where possible
	gf = normalize_feature_to_geojson(feat)
	props = gf.get('properties', {}) if isinstance(gf, dict) else {}
	region_name = props.get('name') or props.get('NAME') or props.get('EN_NAME') or props.get('name_en') or props.get('name_ar') or neighborhood
	pop = load_region_populations()
	pop_entry = None
	if region_name:
		pop_entry = pop.get(region_name) or pop.get(region_name.strip().lower()) or pop.get(region_name.strip().upper())

	# If the normalized feature is a Point (city center), try finding a polygon for it
	if isinstance(gf, dict) and gf.get('geometry') and gf.get('geometry').get('type') == 'Point':
		poly = find_polygon_for_feature(props)
		if poly:
			gf = poly

	response = {'file_key': key, 'neighborhood': region_name, 'feature': gf, 'properties': props}
	if pop_entry:
		try:
			male = float(pop_entry.get('POP_M', 0))
		except Exception:
			male = 0
		try:
			female = float(pop_entry.get('POP_F', 0))
		except Exception:
			female = 0
		response.update({'population_male': male, 'population_female': female, 'population_total': male + female})

	return jsonify(response)


@app.route('/search_suggest')
def search_suggest():
	"""Return a list of matching name suggestions across allowed files.

	Query params:
	- q: query string (required)
	- limit: max results (optional)
	"""
	q = request.args.get('q')
	if not q:
		return jsonify({'error': 'missing q parameter'}), 400
	try:
		limit = int(request.args.get('limit') or 50)
	except Exception:
		limit = 50

	matches = query_all_matches(q, limit=limit)
	# Prepare simple suggestion objects
	out = []
	for m in matches:
		out.append({'file_key': m.get('file_key'), 'id': m.get('id'), 'name_en': m.get('name_en'), 'name_ar': m.get('name_ar'), 'props': m.get('props')})
	return jsonify({'suggestions': out})


# --- Listing endpoints for cascading dropdowns ---
def _load_list_from_key(key, id_field_candidates=('id','region_id','city_id'), name_field_candidates=('name_en','name_ar','name','NAME')):
	"""Return list of dicts {id:, name_en:, name_ar:, props:...} from allowed file key."""
	data = load_external_file(key)
	out = []
	for item in data.get('features', []):
		props = item.get('properties') if isinstance(item, dict) and 'properties' in item else item
		# find id
		id_val = None
		for f in id_field_candidates:
			if f in props:
				id_val = props.get(f)
				break
		# find name variants
		name_en = props.get('name_en') or props.get('name_en'.upper()) or props.get('name_en'.lower()) or props.get('name_en')
		name_en = name_en or props.get('name_en')
		name_en = name_en or props.get('name_en')
		name_en = name_en or props.get('name_en')
		name_ar = props.get('name_ar') or props.get('name_ar'.upper()) or props.get('name_ar'.lower())
		if not name_en and 'name_en' in props:
			name_en = props.get('name_en')
		# fallback to common keys
		if not name_en:
			for k in ('name_en','name','NAME','name_en','name_en'):
				if k in props:
					name_en = props.get(k)
					break
		if not name_ar:
			for k in ('name_ar','NAME_AR','name','NAME'):
				if k in props:
					name_ar = props.get(k)
					break

		out.append({'id': id_val, 'name_en': name_en, 'name_ar': name_ar, 'props': props})
	return out


@app.route('/list_regions')
def list_regions():
	# prefer json/regions.json then geojson/regions.geojson
	regions = _load_list_from_key('json_regions') if 'json_regions' in ALLOWED_FILES else []
	if not regions and 'geo_regions' in ALLOWED_FILES:
		regions = _load_list_from_key('geo_regions')
	# remove duplicates and sort
	seen = set()
	out = []
	for r in regions:
		key = (str(r.get('id')), str(r.get('name_en') or r.get('name_ar') or ''))
		if key in seen:
			continue
		seen.add(key)
		out.append({'id': r.get('id'), 'name_en': r.get('name_en'), 'name_ar': r.get('name_ar')})
	return jsonify({'regions': out})


@app.route('/list_cities')
def list_cities():
	# Accept either region_id or region_name
	region_id = request.args.get('region_id')
	region_name = request.args.get('region_name')
	# load cities from json_cities or geo_cities
	cities = []
	if 'json_cities' in ALLOWED_FILES:
		cities = _load_list_from_key('json_cities', id_field_candidates=('city_id',), name_field_candidates=('name_en','name_ar'))
	elif 'geo_cities' in ALLOWED_FILES:
		cities = _load_list_from_key('geo_cities', id_field_candidates=('city_id',), name_field_candidates=('name_en','name_ar'))

	if region_id:
		try:
			rid = int(region_id)
			cities = [c for c in cities if c.get('props', {}).get('region_id') == rid or c.get('props', {}).get('region_id') == str(rid)]
		except Exception:
			pass
	elif region_name:
		# find region id by name
		regs = _load_list_from_key('json_regions') if 'json_regions' in ALLOWED_FILES else _load_list_from_key('geo_regions')
		target = None
		needle = (region_name or '').strip().lower()
		for r in regs:
			if r.get('name_en') and str(r.get('name_en')).strip().lower() == needle:
				target = r.get('id')
				break
			if r.get('name_ar') and str(r.get('name_ar')).strip().lower() == needle:
				target = r.get('id')
				break
		if target is not None:
			cities = [c for c in cities if c.get('props', {}).get('region_id') == target or c.get('props', {}).get('region_id') == str(target)]

	return jsonify({'cities': [{'id': c.get('id'), 'name_en': c.get('name_en'), 'name_ar': c.get('name_ar')} for c in cities]})


@app.route('/list_districts')
def list_districts():
	# Accept city_id or city_name
	city_id = request.args.get('city_id')
	city_name = request.args.get('city_name')
	districts = []
	if 'json_districts' in ALLOWED_FILES:
		districts = _load_list_from_key('json_districts', id_field_candidates=('district_id','id'), name_field_candidates=('name_en','name_ar'))
	elif 'geo_districts' in ALLOWED_FILES:
		districts = _load_list_from_key('geo_districts', id_field_candidates=('district_id','id'), name_field_candidates=('name_en','name_ar'))

	if city_id:
		try:
			cid = int(city_id)
			districts = [d for d in districts if d.get('props', {}).get('city_id') == cid or d.get('props', {}).get('city_id') == str(cid)]
		except Exception:
			pass
	elif city_name:
		# find city id by name
		cities = _load_list_from_key('json_cities') if 'json_cities' in ALLOWED_FILES else _load_list_from_key('geo_cities')
		needle = (city_name or '').strip().lower()
		target = None
		for c in cities:
			if c.get('name_en') and str(c.get('name_en')).strip().lower() == needle:
				target = c.get('id')
				break
			if c.get('name_ar') and str(c.get('name_ar')).strip().lower() == needle:
				target = c.get('id')
				break
		if target is not None:
			districts = [d for d in districts if d.get('props', {}).get('city_id') == target or d.get('props', {}).get('city_id') == str(target)]

	return jsonify({'districts': [{'id': d.get('id'), 'name_en': d.get('name_en'), 'name_ar': d.get('name_ar')} for d in districts]})


@app.route('/stats_file_feature')
def stats_file_feature():
	"""Return a matched feature from one of the allowed external files.

	Query params:
	- file: key of ALLOWED_FILES (required)
	- neighborhood: name to search (required)
	- name_field (optional): comma-separated list of property names to try
	"""
	file_key = request.args.get('file')
	neighborhood = request.args.get('neighborhood')
	if not file_key or file_key not in ALLOWED_FILES:
		return jsonify({'error': 'invalid or missing file parameter'}), 400
	if not neighborhood:
		return jsonify({'error': 'missing neighborhood parameter'}), 400

	name_field_param = request.args.get('name_field')
	candidates = None
	if name_field_param:
		candidates = [s.strip() for s in name_field_param.split(',') if s.strip()]

	feat = query_file_feature(file_key, neighborhood, candidates)
	if not feat:
		return jsonify({'error': 'feature not found'}), 404

	props = feat.get('properties', {})
	# attach any local population mapping if exists (match by neighborhood key)
	region_name = props.get('name') or props.get('NAME') or props.get('EN_NAME')
	pop = load_region_populations()
	pop_entry = None
	if region_name:
		pop_entry = pop.get(region_name) or pop.get(region_name.strip().lower()) or pop.get(region_name.strip().upper())

	response = {'neighborhood': region_name or neighborhood, 'feature': feat, 'properties': props}
	if pop_entry:
		try:
			male = float(pop_entry.get('POP_M', 0))
		except Exception:
			male = 0
		try:
			female = float(pop_entry.get('POP_F', 0))
		except Exception:
			female = 0
		response.update({'population_male': male, 'population_female': female, 'population_total': male + female})

	return jsonify(response)


# Note: `/stats_region_feature` and the SA_regions.json loader were removed.


def query_layer_sum(layer_url, neighborhood_field, neighborhood_value, population_field):
	"""Query the ArcGIS FeatureLayer for features matching a neighborhood and sum the population field."""
	# Build a safe where clause; ArcGIS expects single quotes around string values
	where = f"{neighborhood_field} = '{neighborhood_value.replace("'","''")}'"
	params = {
		'where': where,
		'outFields': population_field,
		'returnGeometry': 'false',
		'f': 'json'
	}
	resp = requests.get(f"{layer_url.rstrip('/')}/query", params=params, timeout=30)
	resp.raise_for_status()
	data = resp.json()
	features = data.get('features', [])
	total_pop = 0
	count = 0
	for feat in features:
		attrs = feat.get('attributes', {})
		val = attrs.get(population_field)
		try:
			if val is None:
				continue
			total_pop += float(val)
			count += 1
		except Exception:
			continue

	return {
		'neighborhood': neighborhood_value,
		'feature_count': count,
		'population_sum': total_pop
	}


def query_layer_group_sum(layer_url, neighborhood_field, population_field, where_clause='1=1'):
	"""Query the layer to get summed population grouped by neighborhood field.
	Uses the ArcGIS `outStatistics` + `groupByFieldsForStatistics` parameters."""
	# outStatistics must be JSON encoded
	out_statistics = json.dumps([
		{"statisticType": "sum", "onStatisticField": population_field, "outStatisticFieldName": "SUM_POP"}
	])

	params = {
		'where': where_clause,
		'f': 'json',
		'groupByFieldsForStatistics': neighborhood_field,
		'outStatistics': out_statistics,
		'returnGeometry': 'false'
	}
	resp = requests.get(f"{layer_url.rstrip('/')}/query", params=params, timeout=30)
	resp.raise_for_status()
	data = resp.json()
	stats = []
	for feat in data.get('features', []):
		attrs = feat.get('attributes', {})
		stats.append({
			'neighborhood': attrs.get(neighborhood_field),
			'population_sum': attrs.get('SUM_POP')
		})
	return stats


@app.route('/')
def index():
	# Render a simple page that loads ESRI map and calls this API for stats
	return render_template('map.html')


@app.route('/stats')
def stats():
	"""Return population statistics for a single neighborhood.

	Query params:
	- neighborhood (required): neighborhood name (string)
	- layer_url (optional): override default feature layer URL
	- neighborhood_field (optional): field name for neighborhood
	- population_field (optional): field name for population
	"""
	neighborhood = request.args.get('neighborhood')
	if not neighborhood:
		return jsonify({'error': 'missing neighborhood parameter'}), 400

	layer_url = request.args.get('layer_url', DEFAULT_LAYER_URL)
	neighborhood_field = request.args.get('neighborhood_field', DEFAULT_NEIGHBOR_FIELD)
	population_field = request.args.get('population_field', DEFAULT_POP_FIELD)

	try:
		result = query_layer_sum(layer_url, neighborhood_field, neighborhood, population_field)
	except requests.HTTPError as e:
		return jsonify({'error': 'failed to query layer', 'details': str(e)}), 500
	except Exception as e:
		return jsonify({'error': 'unexpected error', 'details': str(e)}), 500

	return jsonify(result)


@app.route('/stats_all')
def stats_all():
	"""Return grouped population sums for all neighborhoods.

	Query params (optional): layer_url, neighborhood_field, population_field
	"""
	layer_url = request.args.get('layer_url', DEFAULT_LAYER_URL)
	neighborhood_field = request.args.get('neighborhood_field', DEFAULT_NEIGHBOR_FIELD)
	population_field = request.args.get('population_field', DEFAULT_POP_FIELD)
	where_clause = request.args.get('where', '1=1')

	try:
		stats = query_layer_group_sum(layer_url, neighborhood_field, population_field, where_clause)
	except requests.HTTPError as e:
		return jsonify({'error': 'failed to query layer', 'details': str(e)}), 500
	except Exception as e:
		return jsonify({'error': 'unexpected error', 'details': str(e)}), 500

	# Optionally sort descending by population
	stats_sorted = sorted(stats, key=lambda x: (x['population_sum'] or 0), reverse=True)
	return jsonify({'stats': stats_sorted})


@app.route('/stats_sample')
def stats_sample():
	"""Return population statistics using the local sample GeoJSON file.

	Query params:
	- neighborhood (required)
	- neighborhood_field (optional) defaults to NAME
	- population_field (optional) defaults to POP
	"""
	neighborhood = request.args.get('neighborhood')
	if not neighborhood:
		return jsonify({'error': 'missing neighborhood parameter'}), 400
	neighborhood_field = request.args.get('neighborhood_field', 'NAME')
	population_field = request.args.get('population_field', 'POP')
	result = query_sample_sum(neighborhood_field, neighborhood, population_field)
	return jsonify(result)


@app.route('/stats_all_sample')
def stats_all_sample():
	"""Return grouped population sums for the sample GeoJSON."""
	neighborhood_field = request.args.get('neighborhood_field', 'NAME')
	population_field = request.args.get('population_field', 'POP')
	stats = query_sample_group_sum(neighborhood_field, population_field)
	return jsonify({'stats': stats})


@app.route('/stats_sample_feature')
def stats_sample_feature():
	"""Return a single matching feature with geometry and gender breakdown for the sample GeoJSON.

	Query params:
	- neighborhood (required)
	- neighborhood_field (optional)
	"""
	neighborhood = request.args.get('neighborhood')
	if not neighborhood:
		return jsonify({'error': 'missing neighborhood parameter'}), 400
	neighborhood_field = request.args.get('neighborhood_field', 'NAME')
	result = query_sample_feature(neighborhood_field, neighborhood)
	if not result:
		return jsonify({'error': 'neighborhood not found'}), 404
	# return the feature (as GeoJSON) and counts
	return jsonify({
		'neighborhood': result['neighborhood'],
		'population_male': result['population_male'],
		'population_female': result['population_female'],
		'population_total': result['population_total'],
		'feature': result['feature']
	})


if __name__ == '__main__':
	# Run in development mode. For production use a WSGI server.
	app.run(host='0.0.0.0', port=5001, debug=True)

