from functools import wraps

# import data.db_connect as dbc

"""
Our record format to meet our requirements (see security.md) will be:

{
    feature_name1: {
        create: {
            user_list: [],
            checks: {
                login: True,
                ip_address: False,
                dual_factor: False,
                # etc.
            },
        },
        read: {
            user_list: [],
            checks: {
                login: True,
                ip_address: False,
                dual_factor: False,
                # etc.
            },
        },
        update: {
            user_list: [],
            checks: {
                login: True,
                ip_address: False,
                dual_factor: False,
                # etc.
            },
        },
        delete: {
            user_list: [],
            checks: {
                login: True,
                ip_address: False,
                dual_factor: False,
                # etc.
            },
        },
    },
    feature_name2: # etc.
}
"""

COLLECT_NAME = 'security'
CREATE = 'create'
READ = 'read'
UPDATE = 'update'
DELETE = 'delete'
USER_LIST = 'user_list'
CHECKS = 'checks'
LOGIN = 'login'

# Features:
PEOPLE = 'people'
DEVELOPER_LOGS = 'developer_logs'

security_recs = None
# These will come from the DB soon:
temp_recs = {
    PEOPLE: {
        CREATE: {
            USER_LIST: ['ejc369@nyu.edu'],
            CHECKS: {
                LOGIN: True,
            },
        },
    },
    DEVELOPER_LOGS: {
        READ: {
            USER_LIST: ['ejc369@nyu.edu'],
            CHECKS: {
                LOGIN: True,
            },
        },
    },
}


def read() -> dict:
    global security_recs
    # dbc.read()
    security_recs = temp_recs
    return security_recs


def needs_recs(fn):
    """
    Should be used to decorate any function that directly accesses sec recs.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        global security_recs
        if not security_recs:
            security_recs = read()
        return fn(*args, **kwargs)
    return wrapper


@needs_recs
def read_feature(feature_name: str) -> dict:
    if feature_name in security_recs:
        return security_recs[feature_name]
    else:
        return None


@needs_recs
def read_operation(feature: str, operation: str) -> dict:
    feature_data = read_feature(feature)
    if not feature_data:
        return None
    return feature_data.get(operation)


@needs_recs
def has_permission(user_email: str, feature: str, operation: str) -> bool:

    # Check if a user has permission to perform an operation on a feature.

    # Returns True if user is authorized, False otherwise.
    feature_data = read_feature(feature)
    if not feature_data:
        return False
    op_data = feature_data.get(operation)
    if not op_data:
        return False
    user_list = op_data.get(USER_LIST, [])
    return user_email in user_list


@needs_recs
def check_required(feature: str, operation: str, check_name: str) -> bool:
    # Check if a specific security check is required for an operation.

    # Returns True if the check is required, False otherwise.
    feature_data = read_feature(feature)
    if not feature_data:
        return False
    op_data = feature_data.get(operation)
    if not op_data:
        return False
    checks = op_data.get(CHECKS, {})
    return checks.get(check_name, False)
