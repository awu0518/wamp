"""
This is the file containing all of the endpoints for our flask app.
The endpoint called `endpoints` will return all available endpoints.
"""
# from http import HTTPStatus

from flask import Flask, request
from flask_restx import Resource, Api, fields
from flask_cors import CORS
from datetime import datetime
from data import db_connect as dbc
import users.auth as auth
import users.queries as uq
import cities.queries as cq
import countries.queries as ctq
import states.queries as stq

# import werkzeug.exceptions as wz

app = Flask(__name__)
CORS(app)
api = Api(
    app,
    version='1.0',
    title='Geographic Database API',
    description='A comprehensive REST API for managing geographic data '
                'including countries, states, and cities',
    doc='/',  # Swagger UI will be available at /docs/
    contact_email='support@geodatabase.com',
    authorizations={
        'apikey': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'X-API-KEY'
        }
    }
)

ENDPOINT_EP = '/endpoints'
ENDPOINT_RESP = 'Available endpoints'
HELLO_EP = '/hello'
HELLO_RESP = 'hello'
MESSAGE = 'Message'
TIMESTAMP_EP = '/timestamp'
TIMESTAMP_RESP = 'timestamp'
HEALTH_EP = '/health'
HEALTH_RESP = 'status'
CITIES_EP = '/cities'
CITIES_RESP = 'cities'
CITIES_SEARCH_EP = '/cities/search'
COUNTRIES_EP = '/countries'
COUNTRIES_RESP = 'countries'
COUNTRIES_SEARCH_EP = '/countries/search'
STATES_EP = '/states'
STATES_RESP = 'states'
STATES_SEARCH_EP = '/states/search'

# Swagger Models for Documentation
city_model = api.model('City', {
    'name': fields.String(required=True, description='City name',
                          example='New York'),
    'state_code': fields.String(required=True, description='State code',
                                example='NY')
})

city_response = api.model('CityResponse', {
    'cities': fields.Raw(description='Dictionary of cities keyed by name'),
    'count': fields.Integer(description='Number of cities returned')
})

country_model = api.model('Country', {
    'name': fields.String(required=True, description='Country name',
                          example='United States'),
    'iso_code': fields.String(required=True, description='ISO country code',
                              example='US')
})

country_response = api.model('CountryResponse', {
    'countries': fields.Raw(description='Dictionary of countries '
                            'keyed by name'),
    'count': fields.Integer(description='Number of countries returned')
})

state_model = api.model('State', {
    'name': fields.String(required=True, description='State name',
                          example='New York'),
    'state_code': fields.String(required=True, description='State code',
                                example='NY'),
    'capital': fields.String(required=True, description='Capital city',
                             example='Albany')
})

state_response = api.model('StateResponse', {
    'states': fields.Raw(description='Dictionary of states keyed by name'),
    'count': fields.Integer(description='Number of states returned')
})

error_response = api.model('Error', {
    'error': fields.String(description='Error message',
                           example='Resource not found')
})

success_response = api.model('Success', {
    'Message': fields.String(description='Success message',
                             example='Resource created successfully'),
    'id': fields.String(description='Created resource ID',
                        example='507f1f77bcf86cd799439011')
})

pagination_response = api.model('PaginationMeta', {
    'page': fields.Integer(description='Current page number', example=1),
    'limit': fields.Integer(description='Items per page', example=50),
    'total': fields.Integer(description='Total number of items',
                            example=150),
    'pages': fields.Integer(description='Total number of pages', example=3),
    'has_next': fields.Boolean(description='Has next page', example=True),
    'has_prev': fields.Boolean(description='Has previous page',
                               example=False)
})

# Create namespaces for better organization
geographic_ns = api.namespace('geographic',
                              description='Geographic data operations')
utility_ns = api.namespace('utility', description='Utility endpoints')


@api.route(HELLO_EP)
class HelloWorld(Resource):
    """
    The purpose of the HelloWorld class is to have a simple test to see if the
    app is working at all.
    """

    @api.doc('hello_world')
    @api.response(200, 'Success - Server is running')
    def get(self):
        """
        Health check endpoint to verify server is running.

        Returns a simple 'hello world' message to confirm the API is accessible
        """
        return {HELLO_RESP: 'world'}


@api.route(ENDPOINT_EP)
class Endpoints(Resource):
    """
    This class will serve as live, fetchable documentation of what endpoints
    are available in the system.
    """

    @api.doc('list_endpoints')
    @api.response(200, 'Success - List of available endpoints')
    def get(self):
        """
        Get all available API endpoints.

        Returns a sorted list of all registered endpoints in the system.
        Useful for API discovery and documentation.
        """
        endpoints = sorted(rule.rule for rule in api.app.url_map.iter_rules())
        return {ENDPOINT_RESP: endpoints}


@api.route(TIMESTAMP_EP)
class Timestamp(Resource):
    """
    This class returns the current server timestamp.
    """

    @api.doc('get_timestamp')
    @api.response(200, 'Success - Current server timestamp')
    def get(self):
        """
        Get current server timestamp.

        Returns the current server time in both ISO format and Unix timestamp.
        Useful for synchronization and logging.
        """
        current_time = datetime.now().isoformat()
        return {
            TIMESTAMP_RESP: current_time,
            'unix': datetime.now().timestamp()
        }


@api.route(HEALTH_EP)
class Health(Resource):
    """
    Enhanced health check endpoint with database statistics.
    """

    @api.doc('health_check')
    @api.response(200, 'Success - System is healthy')
    @api.response(503, 'Service degraded - Database issues')
    def get(self):
        """
        Comprehensive health check.

        Returns server liveness, database health details, and collection
        statistics including:
        - Server timestamp and uptime
        - Database connectivity status
        - Collection counts (countries, states, cities)
        - Overall system health status
        """
        now = datetime.now()
        db_status = dbc.ping()
        overall = 'ok' if db_status.get('ok') else 'degraded'

        # Gather collection statistics
        collections_stats = {}
        db_stats = {}

        try:
            client = dbc.get_client()
            db = client[dbc.SE_DB]

            # Get collection counts
            collections_stats = {
                'countries': {
                    'count': db['countries'].count_documents({}),
                    'name': 'countries'
                },
                'states': {
                    'count': db['states'].count_documents({}),
                    'name': 'states'
                },
                'cities': {
                    'count': db['cities'].count_documents({}),
                    'name': 'cities'
                }
            }

            # Get database statistics
            db_stats_raw = db.command('dbStats')
            db_stats = {
                'database': db_stats_raw.get('db', 'seDB'),
                'collections': db_stats_raw.get('collections', 0),
                'data_size_bytes': db_stats_raw.get('dataSize', 0),
                'storage_size_bytes': db_stats_raw.get('storageSize', 0),
                'indexes': db_stats_raw.get('indexes', 0),
                'index_size_bytes': db_stats_raw.get('indexSize', 0),
            }

            # Calculate total documents
            total_docs = sum(c['count'] for c in collections_stats.values())

        except Exception as e:
            # If stats gathering fails, set empty stats and mark as degraded
            overall = 'degraded'
            collections_stats = {
                'error': f'Failed to gather collection stats: {str(e)}'
            }
            db_stats = {}
            total_docs = 0

        return {
            HEALTH_RESP: overall,
            'timestamp': now.isoformat(),
            'unix': now.timestamp(),
            'db': db_status,
            'collections': collections_stats,
            'database_stats': db_stats,
            'total_documents': total_docs
        }


@api.route(CITIES_EP)
class Cities(Resource):
    """
    This class handles operations on cities collection.
    """

    @api.doc('get_all_cities')
    @api.doc(params={
        'page': 'Page number for pagination (optional)',
        'limit': 'Number of items per page (optional)',
        'sort_by': 'Field to sort by (default: name)',
        'order': 'Sort order: asc or desc (default: asc)'
    })
    @api.marshal_with(city_response)
    @api.response(200, 'Success', city_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Get all cities with optional pagination and sorting.

        Returns a dictionary of cities keyed by city name, with optional
        pagination metadata.
        """
        try:
            # Optional pagination
            page = request.args.get('page')
            limit = request.args.get('limit')
            if page is not None or limit is not None:
                sort_by = request.args.get('sort_by', cq.NAME)
                order = request.args.get('order', 'asc')
                page_val = int(page) if page is not None else 1
                limit_val = int(limit) if limit is not None else 50
                data = cq.read_paginated(
                    page=page_val,
                    limit=limit_val,
                    sort_by=sort_by,
                    order=order
                )
                return {
                    CITIES_RESP: data['items'],
                    'count': len(data['items']),
                    'pagination': {
                        'page': data['page'],
                        'limit': data['limit'],
                        'total': data['total'],
                        'pages': data['pages'],
                        'has_next': data['has_next'],
                        'has_prev': data['has_prev'],
                    }
                }
            else:
                cities = cq.read()
                return {
                    CITIES_RESP: cities,
                    'count': len(cities)
                }
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('create_city')
    @api.expect(city_model)
    @api.response(201, 'City created successfully', success_response)
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Create a new city.

        Creates a new city with the provided name and state code.
        The city name should be unique within the state.
        """
        try:
            data = request.json
            new_id = cq.create(data)
            return {
                MESSAGE: 'City created successfully',
                'id': str(new_id)
            }, 201
        except ValueError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(f'{CITIES_EP}/<city_name>')
class CityByName(Resource):
    """
    This class handles operations on a specific city.
    """
    @api.doc('delete_city_by_name')
    @api.response(200, 'City deleted successfully')
    @api.response(400, 'Missing state_code parameter', error_response)
    @api.response(404, 'City not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self, city_name):
        """
        Delete a city by name.
        Requires state_code as query parameter.
        """
        try:
            state_code = request.args.get('state_code')
            if not state_code:
                return {'error': 'state_code query parameter is required'}, 400
            cq.delete(city_name, state_code)
            return {MESSAGE: f'City {city_name} deleted successfully'}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(CITIES_SEARCH_EP)
class CitiesSearch(Resource):
    """
    Search cities by name and/or state code.
    """

    @api.doc('search_cities')
    @api.doc(params={
        'name': 'City name to search for (substring match, optional)',
        'state_code': 'State code to filter by (exact match, optional)'
    })
    @api.response(200, 'Success - Cities found', city_response)
    @api.response(400, 'Bad Request - No search parameters provided',
                  error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Search cities by name and/or state code.

        Performs flexible search across cities using:
        - name: Substring matching (case-insensitive)
        - state_code: Exact matching

        At least one parameter must be provided.
        """
        try:
            name = request.args.get('name')
            state_code = request.args.get('state_code')
            if not name and not state_code:
                return {
                    'error': 'Provide at least one parameter: '
                             'name or state_code'
                }, 400
            results = cq.search(name=name, state_code=state_code)
            return {
                CITIES_RESP: results,
                'count': len(results)
            }
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(COUNTRIES_EP)
class Countries(Resource):
    """
    This class handles operations on countries collection.
    """

    @api.doc('get_all_countries')
    @api.doc(params={
        'iso_code': 'ISO country code to filter by (optional)',
        'page': 'Page number for pagination (optional)',
        'limit': 'Number of items per page (optional)',
        'sort_by': 'Field to sort by (default: name)',
        'order': 'Sort order: asc or desc (default: asc)'
    })
    @api.response(200, 'Success', country_response)
    @api.response(404, 'Country not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Get all countries with optional filtering and pagination.

        Returns a dictionary of countries keyed by name. Can be filtered
        by ISO code or paginated for large datasets.
        """
        try:
            iso_code = request.args.get('iso_code')
            if iso_code:
                country = ctq.find_by_iso_code(iso_code)
                if country:
                    return {COUNTRIES_RESP: country}
                else:
                    error_msg = f'Country with ISO code {iso_code} not found'
                    return {'error': error_msg}, 404
            # Optional pagination
            page = request.args.get('page')
            limit = request.args.get('limit')
            if page is not None or limit is not None:
                sort_by = request.args.get('sort_by', ctq.NAME)
                order = request.args.get('order', 'asc')
                page_val = int(page) if page is not None else 1
                limit_val = int(limit) if limit is not None else 50
                data = ctq.read_paginated(
                    page=page_val,
                    limit=limit_val,
                    sort_by=sort_by,
                    order=order
                )
                return {
                    COUNTRIES_RESP: data['items'],
                    'count': len(data['items']),
                    'pagination': {
                        'page': data['page'],
                        'limit': data['limit'],
                        'total': data['total'],
                        'pages': data['pages'],
                        'has_next': data['has_next'],
                        'has_prev': data['has_prev'],
                    }
                }
            else:
                countries = ctq.read()
                return {
                    COUNTRIES_RESP: countries,
                    'count': len(countries)
                }
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('create_country')
    @api.expect(country_model)
    @api.response(201, 'Country created successfully', success_response)
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Create a new country.

        Creates a new country with the provided name and ISO code.
        The ISO code should be unique (e.g., 'US', 'CA', 'FR').
        """
        try:
            data = request.json
            new_id = ctq.create(data)
            return {
                MESSAGE: 'Country created successfully',
                'id': str(new_id)
            }, 201
        except ValueError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(f'{COUNTRIES_EP}/<country_id>')
class CountryById(Resource):
    """
    This class handles operations on a specific country.
    """
    @api.doc('get_country_by_id')
    @api.response(200, 'Success', country_response)
    @api.response(404, 'Country not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self, country_id):
        """
        Get a specific country by ID (name).
        """
        try:
            country = ctq.read_one(country_id)
            return {COUNTRIES_RESP: country}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('update_country_by_id')
    @api.expect(country_model)
    @api.response(200, 'Country updated successfully')
    @api.response(404, 'Country not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def put(self, country_id):
        """
        Update a country by ID.
        Expected JSON body: fields to update
        """
        try:
            data = request.json
            ctq.update(country_id, data)
            return {MESSAGE: f'Country {country_id} updated successfully'}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('delete_country_by_id')
    @api.response(200, 'Country deleted successfully')
    @api.response(404, 'Country not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self, country_id):
        """
        Delete a country by ID.
        """
        try:
            ctq.delete(country_id)
            return {MESSAGE: f'Country {country_id} deleted successfully'}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(COUNTRIES_SEARCH_EP)
class CountriesSearch(Resource):
    """
    Search countries by name and/or ISO code.
    """

    @api.doc('search_countries')
    @api.doc(params={
        'name': 'Country name to search for (substring match, optional)',
        'iso_code': 'ISO country code (exact match, optional)'
    })
    @api.response(200, 'Success - Countries found', country_response)
    @api.response(400, 'Bad Request - No search parameters', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Search countries by name and/or ISO code.

        Performs flexible search across countries using:
        - name: Substring matching (case-insensitive)
        - iso_code: Exact matching

        At least one parameter must be provided.
        """
        try:
            name = request.args.get('name')
            iso_code = request.args.get('iso_code')
            if not name and not iso_code:
                err = 'Provide at least one parameter: name or iso_code'
                return {'error': err}, 400
            results = ctq.search(name=name, iso_code=iso_code)
            return {
                COUNTRIES_RESP: results,
                'count': len(results)
            }
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(STATES_EP)
class States(Resource):
    """
    This class handles operations on states collection.
    """
    @api.doc('get_all_states')
    @api.response(200, 'Success', state_response)
    @api.response(404, 'State not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Returns all states from the database.
        Optional query parameter: state_code (to filter by state code)
        """
        try:
            state_code = request.args.get('state_code')
            if state_code:
                state = stq.find_by_state_code(state_code)
                if state:
                    return {STATES_RESP: state}
                else:
                    error_msg = f'State with code {state_code} not found'
                    return {'error': error_msg}, 404
            # Optional pagination
            page = request.args.get('page')
            limit = request.args.get('limit')
            if page is not None or limit is not None:
                sort_by = request.args.get('sort_by', stq.NAME)
                order = request.args.get('order', 'asc')
                page_val = int(page) if page is not None else 1
                limit_val = int(limit) if limit is not None else 50
                data = stq.read_paginated(
                    page=page_val,
                    limit=limit_val,
                    sort_by=sort_by,
                    order=order
                )
                return {
                    STATES_RESP: data['items'],
                    'count': len(data['items']),
                    'pagination': {
                        'page': data['page'],
                        'limit': data['limit'],
                        'total': data['total'],
                        'pages': data['pages'],
                        'has_next': data['has_next'],
                        'has_prev': data['has_prev'],
                    }
                }
            else:
                states = stq.read()
                return {
                    STATES_RESP: states,
                    'count': len(states)
                }
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('create_state')
    @api.expect(state_model)
    @api.response(201, 'State created successfully')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Create a new state.
        Expected JSON body: {"name": "State Name", "state_code": "ST",
                            "capital": "Capital City"}
        """
        try:
            data = request.json
            new_id = stq.create(data)
            return {
                MESSAGE: 'State created successfully',
                'id': str(new_id)
            }, 201
        except ValueError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(f'{STATES_EP}/<state_id>')
class StateById(Resource):
    """
    This class handles operations on a specific state.
    """
    @api.doc('get_state_by_id')
    @api.response(200, 'Success', state_response)
    @api.response(404, 'State not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self, state_id):
        """
        Get a specific state by ID (name).
        """
        try:
            state = stq.read_one(state_id)
            return {STATES_RESP: state}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('update_state_by_id')
    @api.expect(state_model)
    @api.response(200, 'State updated successfully')
    @api.response(404, 'State not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def put(self, state_id):
        """
        Update a state by ID.
        Expected JSON body: fields to update
        """
        try:
            data = request.json
            stq.update(state_id, data)
            return {MESSAGE: f'State {state_id} updated successfully'}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('delete_state_by_id')
    @api.response(200, 'State deleted successfully')
    @api.response(404, 'State not found', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self, state_id):
        """
        Delete a state by ID.
        """
        try:
            stq.delete(state_id)
            return {MESSAGE: f'State {state_id} deleted successfully'}
        except ValueError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(STATES_SEARCH_EP)
class StatesSearch(Resource):
    """
    Search states by name, state code, and/or capital.
    """
    @api.doc('search_states')
    @api.response(200, 'Search results', state_response)
    @api.response(400, 'Missing search parameters', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def get(self):
        """
        Search states with optional filters.
        Query params: name (substring), state_code (exact), capital (substring)
        """
        try:
            name = request.args.get('name')
            state_code = request.args.get('state_code')
            capital = request.args.get('capital')
            if not name and not state_code and not capital:
                err = ('Provide at least one parameter: '
                       'name, state_code, or capital')
                return {'error': err}, 400
            results = stq.search(
                name=name, state_code=state_code, capital=capital
            )
            return {
                STATES_RESP: results,
                'count': len(results)
            }
        except Exception as e:
            return {'error': str(e)}, 500


# Bulk operations endpoints
COUNTRIES_BULK_EP = '/countries/bulk'
CITIES_BULK_EP = '/cities/bulk'
STATES_BULK_EP = '/states/bulk'


@api.route(COUNTRIES_BULK_EP)
class CountriesBulk(Resource):
    """
    Bulk operations for countries.
    """
    @api.doc('bulk_create_countries')
    @api.response(200, 'Bulk create success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Bulk create countries.
        Expects JSON array of country objects.
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = ctq.bulk_create(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_update_countries')
    @api.response(200, 'Bulk update success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def put(self):
        """
        Bulk update countries.
        Expects JSON array of update objects: [{"id": "name", "fields": {...}}]
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = ctq.bulk_update(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_delete_countries')
    @api.response(200, 'Bulk delete success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self):
        """
        Bulk delete countries.
        Expects JSON array of country names.
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = ctq.bulk_delete(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(CITIES_BULK_EP)
class CitiesBulk(Resource):
    """
    Bulk operations for cities.
    """
    @api.doc('bulk_create_cities')
    @api.response(200, 'Bulk create success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Bulk create cities.
        Expects JSON array of city objects.
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = cq.bulk_create(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_update_cities')
    @api.response(200, 'Bulk update success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def put(self):
        """
        Bulk update cities.
        Expects JSON array of update objects: [{"id": "name", "fields": {...}}]
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = cq.bulk_update(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_delete_cities')
    @api.response(200, 'Bulk delete success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self):
        """
        Bulk delete cities.
        Expects JSON array of delete objects:
        [{"name": "city", "state_code": "ST"}]
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = cq.bulk_delete(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500


@api.route(STATES_BULK_EP)
class StatesBulk(Resource):
    """
    Bulk operations for states.
    """
    @api.doc('bulk_create_states')
    @api.response(200, 'Bulk create success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def post(self):
        """
        Bulk create states.
        Expects JSON array of state objects.
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = stq.bulk_create(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_update_states')
    @api.response(200, 'Bulk update success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def put(self):
        """
        Bulk update states.
        Expects JSON array of update objects: [{"id": "name", "fields": {...}}]
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = stq.bulk_update(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500

    @api.doc('bulk_delete_states')
    @api.response(200, 'Bulk delete success')
    @api.response(400, 'Validation Error', error_response)
    @api.response(500, 'Internal Server Error', error_response)
    def delete(self):
        """
        Bulk delete states.
        Expects JSON array of state names.
        """
        try:
            data = request.json
            if not isinstance(data, list):
                return {'error': 'Request body must be a JSON array'}, 400
            result = stq.bulk_delete(data)
            return result, 200 if result['failed'] == 0 else 207
        except Exception as e:
            return {'error': str(e)}, 500


@api.route('/register')
class Register(Resource):
    def post(self):
        try:
            data = request.json

            email = data.get("email")
            username = data.get("username")
            password = data.get("password")

            # Validate
            valid, msg = uq.validate_email(email)
            if not valid:
                return {"error": msg}, 400

            valid, msg = uq.validate_username(username)
            if not valid:
                return {"error": msg}, 400

            valid, msg = uq.validate_password(password)
            if not valid:
                return {"error": msg}, 400

            password_hash = auth.hash_password(password)

            user = uq.create_user(email, username, password_hash)

            return {
                "message": "User created successfully",
                "user_id": user[uq.ID]
            }, 201

        except ValueError as e:
            return {"error": str(e)}, 400
        except Exception as e:
            return {"error": str(e)}, 500


@api.route('/login')
class Login(Resource):
    def post(self):
        try:
            data = request.json
            email = data.get("email")
            password = data.get("password")

            result = auth.authenticate_user(email, password)

            if not result:
                return {"error": "Invalid credentials"}, 401

            token, user_data = result

            return {
                "token": token,
                "user": user_data
            }

        except Exception as e:
            return {"error": str(e)}, 500


if __name__ == '__main__':
    app.run(debug=True)
