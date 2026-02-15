# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2026.                                 |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: IBM Application Development Team                                |
# +--------------------------------------------------------------------------+

"""
DB2 database backend for Django.
Requires: ibm_db_dbi (http://pypi.python.org/pypi/ibm_db) for python
"""
import sys
_IS_JYTHON = sys.platform.startswith( 'java' )

from django.core.exceptions import ImproperlyConfigured
from django.utils.functional import cached_property
# Importing class from base module of django.db.backends

try:
    from django.db.backends import BaseDatabaseFeatures
except ImportError:
    from django.db.backends.base.features import BaseDatabaseFeatures

try:
    from django.db.backends import BaseDatabaseWrapper
except ImportError:
    from django.db.backends.base.base import BaseDatabaseWrapper

try:
    from django.db.backends import BaseDatabaseValidation
except ImportError:
    from django.db.backends.base.validation import BaseDatabaseValidation

from django.db.backends.signals import connection_created

# Importing internal classes from ibm_db_django package.
from ibm_db_django.client import DatabaseClient
from ibm_db_django.creation import DatabaseCreation
from ibm_db_django.introspection import DatabaseIntrospection
from ibm_db_django.operations import DatabaseOperations
if not _IS_JYTHON:
    import ibm_db_django.pybase as Base
    import ibm_db_dbi as Database
else:
    import ibm_db_django.jybase as Base
    from com.ziclix.python.sql import zxJDBC as Database

# For checking django's version
from django import VERSION as djangoVersion
from django.db.utils import Error as dbuError
from contextlib import contextmanager

if ( djangoVersion[0:2] >= ( 1, 7 )):
    from ibm_db_django.schemaEditor import DB2SchemaEditor

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
if ( djangoVersion[0:2] >= ( 1, 6 )):
    Error = Database.Error
    InterfaceError = Database.InterfaceError
    DataError = Database.DataError
    OperationalError = Database.OperationalError
    InternalError = Database.InternalError
    ProgrammingError = Database.ProgrammingError
    NotSupportedError = Database.NotSupportedError


if _IS_JYTHON:
    dbms_name = 'dbname'
else:
    dbms_name = 'dbms_name'

import datetime
from django.db.models import DurationField, DateField, DateTimeField, TimeField, Value 
from ibm_db_django.expressions import DurationValue
from django.db.models.expressions import Combinable, CombinedExpression
from django.db.models.expressions import TemporalSubtraction
from django.db.models.expressions import DurationExpression

from django.db.models.constraints import CheckConstraint
from django.db.models.sql.query import Query
from django.core.exceptions import SynchronousOnlyOperation

def db2_safe_get_check_sql(self, model, schema_editor):
    query = Query(model=model, alias_cols=False)    
    where = query.build_where(self.condition)
    compiler = query.get_compiler(connection=schema_editor.connection)
    sql, params = where.as_sql(compiler, schema_editor.connection)
    if params:
        sql = sql % tuple(schema_editor.quote_value(p) for p in params)
    return sql

# Apply monkeypatch
CheckConstraint._get_check_sql = db2_safe_get_check_sql

# Backup original
original_combinedexpression_as_sql = CombinedExpression.as_sql

def db2_combinedexpression_as_sql(self, compiler, connection):
    if connection.vendor == "DB2":
        lhs_sql, lhs_params = compiler.compile(self.lhs)
        rhs_sql, rhs_params = compiler.compile(self.rhs)

        expressions = [lhs_sql, rhs_sql]
        expression_wrapper = "(%s)"

        # Let DB2-specific combine_expression handle it
        sql = connection.ops.combine_expression(self, self.connector, expressions)
        params = tuple(list(lhs_params) + list(rhs_params))
        return expression_wrapper % sql, params

    # Default fallback if not DB2
    return original_combinedexpression_as_sql(self, compiler, connection)

# Apply the monkey patch
CombinedExpression.as_sql = db2_combinedexpression_as_sql

original_durationexpression_as_sql = DurationExpression.as_sql

def db2_durationexpression_as_sql(self, compiler, connection):
    if connection.vendor == "DB2":
        lhs_sql, lhs_params = self.compile(self.lhs, compiler, connection)
        rhs_sql, rhs_params = self.compile(self.rhs, compiler, connection)        

        # Fallback to normal combine_expression logic for non-temporal operands
        expressions = [lhs_sql, rhs_sql]
        expression_wrapper = "(%s)"
        sql = connection.ops.combine_duration_expression(self, self.connector, expressions)
        return expression_wrapper % sql, lhs_params + rhs_params

    # Default fallback if not DB2
    return original_durationexpression_as_sql(self, compiler, connection)

# Apply the monkey patch
DurationExpression.as_sql = db2_durationexpression_as_sql

# Save the original to fallback
original_temporal_subtraction_as_sql = TemporalSubtraction.as_sql

def db2_temporal_subtraction_as_sql(self, compiler, connection):
    if connection.vendor == "DB2":
        # Compile once for NULL detection
        lhs_sql, lhs_params = compiler.compile(self.lhs)
        rhs_sql, rhs_params = compiler.compile(self.rhs)

        if lhs_sql.strip().upper() == "NULL" or rhs_sql.strip().upper() == "NULL":
            return "NULL", []

        # Helper to wrap in CAST
        def sql_part(func_name, expr):
            expr_sql, expr_params = compiler.compile(expr)
            casted = f"BIGINT({func_name}(CAST({expr_sql} AS TIMESTAMP)))"
            return casted, tuple(expr_params)

        lhs_field = getattr(self.lhs, "output_field", None)
        rhs_field = getattr(self.rhs, "output_field", None)
        
        # Special logic for TimeField - TimeField (NO CAST)
        if isinstance(lhs_field, TimeField) and isinstance(rhs_field, TimeField):
            lhs_micro = f"(BIGINT(MIDNIGHT_SECONDS({lhs_sql})) * 1000000)"
            rhs_micro = f"(BIGINT(MIDNIGHT_SECONDS(CAST({rhs_sql} AS TIME))) * 1000000)"
            sql = f"({lhs_micro} - {rhs_micro})"
            return sql, tuple(lhs_params) + tuple(rhs_params)   
        
        if isinstance(lhs_field, (DateField, DateTimeField, TimeField)) and \
           isinstance(rhs_field, (DateField, DateTimeField, TimeField)):

            lhs_days, lhs_days_params = sql_part("DAYS", self.lhs)
            rhs_days, rhs_days_params = sql_part("DAYS", self.rhs)

            lhs_secs, lhs_secs_params = sql_part("MIDNIGHT_SECONDS", self.lhs)
            rhs_secs, rhs_secs_params = sql_part("MIDNIGHT_SECONDS", self.rhs)

            lhs_micro, lhs_micro_params = sql_part("MICROSECOND", self.lhs)
            rhs_micro, rhs_micro_params = sql_part("MICROSECOND", self.rhs)

            sql = (
                f"((({lhs_days} - {rhs_days}) * 86400 + "
                f"({lhs_secs} - {rhs_secs})) * 1000000 + "
                f"({lhs_micro} - {rhs_micro}))"
            )

            params = (
                lhs_days_params + rhs_days_params +
                lhs_secs_params + rhs_secs_params +
                lhs_micro_params + rhs_micro_params
            )

            return sql, params

    # Fallback to default
    return original_temporal_subtraction_as_sql(self, compiler, connection)

# Apply the patch
TemporalSubtraction.as_sql = db2_temporal_subtraction_as_sql

# Save the original method
original_combine = Combinable._combine

def db2_safe_combine(self, other, connector, reversed):
    # Case 1: Raw datetime.timedelta object
    if not hasattr(other, 'resolve_expression'):
        if isinstance(other, datetime.timedelta):
            other = DurationValue(other, output_field=DurationField())
        else:
            other = Value(other)
    # Case 2: Its Value but wrapping a timedelta object
    elif isinstance(other, Value) and isinstance(other.value, datetime.timedelta):
        other = DurationValue(other.value, output_field=DurationField())
    
    if reversed:
        return CombinedExpression(other, connector, self)
    return CombinedExpression(self, connector, other)

Combinable._combine = db2_safe_combine

class DatabaseFeatures( BaseDatabaseFeatures ):
    can_use_chunked_reads = True

    #Save point is supported by DB2.
    uses_savepoints = True

    #Custom query class has been implemented
    #django.db.backends.db2.query.query_class.DB2QueryClass
    uses_custom_query_class = True

    #transaction is supported by DB2
    supports_transactions = True
    #Support for atomic migrations
    can_rollback_ddl = True

    supports_tablespaces = True

    uppercases_column_names = True
    interprets_empty_strings_as_nulls = False
    allows_primary_key_0 = True
    can_defer_constraint_checks = False
    supports_forward_references = False
    requires_rollback_on_dirty_transaction = True
    supports_regex_backreferencing = True
    supports_timezones = False
    has_bulk_insert = False
    has_select_for_update = True
    supports_long_model_names = False
    can_distinct_on_fields = False
    supports_paramstyle_pyformat = False
    supports_sequence_reset = True
    #DB2 doesn't take default values as parameter
    requires_literal_defaults = True
    has_case_insensitive_like = True
    can_introspect_big_integer_field = True
    can_introspect_boolean_field = False
    can_introspect_positive_integer_field = False
    can_introspect_small_integer_field = True
    can_introspect_null = True
    can_introspect_max_length = True
    can_introspect_ip_address_field = False
    can_introspect_time_field = True
    supports_subqueries_in_group_by = False
    bare_select_suffix = " FROM SYSIBM.DUAL"
    # What kind of error does the backend throw when accessing closed cursor?
    closed_cursor_error_class = dbuError
    supports_ignore_conflicts = False
    # Does the database have a copy of the zoneinfo database?
    has_zoneinfo_database = False
    #DB2 does not support partial indexes
    supports_partial_indexes = False
    can_introspect_duration_field = False
    supports_partially_nullable_unique_constraints = False
    supports_nullable_unique_constraints = False
    nulls_order_largest = True
    # DB2 doesn't ignore quoted identifiers case but the current DJango adapter
    # is designed to send all identifiers in uppercase.
    ignores_table_name_case = True
    allows_multiple_constraints_on_same_fields = False

    # Can it create foreign key constraints inline when adding columns?
    #In DB2, creating FK inline when adding columns is allowed, but we disable it purposefully
    #as at many situations deferring adding constraints is needed.
    can_create_inline_fk = False
    supports_json_field_contains = False
    supports_temporal_subtraction = True
    allows_group_by_select_index = False
    supports_json_field = False
    # Does the backend support stored generated columns?
    supports_stored_generated_columns = False

    # Does the backend support virtual generated columns?
    supports_virtual_generated_columns = True
    # Does the backend support column and table comments?
    supports_comments = True
    # Does the backend support column comments in ADD COLUMN statements?
    supports_comments_inline = False
    
    # Does the backend support column collations?
    supports_collation_on_charfield = False
    supports_collation_on_textfield = False
    # Does the backend support non-deterministic collations?
    supports_non_deterministic_collations = False

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            'AutoField': 'IntegerField',
            'BigAutoField': 'BigIntegerField',
            'SmallAutoField': 'SmallIntegerField',
            'DurationField': 'BigIntegerField',
            'GenericIPAddressField': 'CharField',
            'PositiveBigIntegerField': 'BigIntegerField',
            'PositiveIntegerField': 'IntegerField',
            'PositiveSmallIntegerField': 'SmallIntegerField',
        }

class DatabaseValidation( BaseDatabaseValidation ):
    #Need to do validation for DB2 and ibm_db version
    def validate_field( self, errors, opts, f ):
        pass

class DatabaseWrapper( BaseDatabaseWrapper ):

    """
    This is the base class for DB2 backend support for Django. The under lying
    wrapper is IBM_DB_DBI (latest version can be downloaded from http://code.google.com/p/ibm-db/ or
    http://pypi.python.org/pypi/ibm_db).
    """
    data_types={}
    data_types_suffix = {
        'AutoField':    'GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER)', # DB2 Specific
        'BigAutoField': 'GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER)', # DB2 Specific
        'SmallAutoField': 'GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER)', # DB2 Specific
    }
    vendor = 'DB2'
    display_name = 'DB2'
    operators = {
        "exact":        "= %s",
        "iexact":       "LIKE UPPER(%s) ESCAPE '\\'",
        "contains":     "LIKE %s ESCAPE '\\'",
        "icontains":    "LIKE UPPER(%s) ESCAPE '\\'",
        "gt":           "> %s",
        "gte":          ">= %s",
        "lt":           "< %s",
        "lte":          "<= %s",
        "startswith":   "LIKE %s ESCAPE '\\'",
        "endswith":     "LIKE %s ESCAPE '\\'",
        "istartswith":  "LIKE UPPER(%s) ESCAPE '\\'",
        "iendswith":    "LIKE UPPER(%s) ESCAPE '\\'",
    }

    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%', '\%'), '_', '\_')"
    pattern_ops = {
        'contains': r"LIKE '%' || {} || '%' ESCAPE '\'",
        'icontains': r"LIKE '%' || UPPER({}) || '%' ESCAPE '\'",
        'startswith': r"LIKE {} || '%' ESCAPE '\'",
        'istartswith': r"LIKE UPPER({}) || '%%' ESCAPE '\'",
        'endswith': r"LIKE '%%' || {} ESCAPE '\'",
        'iendswith': r"LIKE '%%' || UPPER({}) ESCAPE '\'",
    }

    if( djangoVersion[0:2] >= ( 1, 6 ) ):
        Database = Database

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation
    ops_class = DatabaseOperations

    # Constructor of DB2 backend support. Initializing all other classes.
    def __init__( self, *args ):
        super( DatabaseWrapper, self ).__init__( *args )
        self.ops = DatabaseOperations( self )
        if( djangoVersion[0:2] <= ( 1, 0 ) ):
            self.client = DatabaseClient()
        else:
            self.client = DatabaseClient( self )
        if( djangoVersion[0:2] <= ( 1, 2 ) ):
            self.features = DatabaseFeatures()
        else:
            self.features = DatabaseFeatures( self )
        self.creation = DatabaseCreation( self )

        if( djangoVersion[0:2] >= ( 1, 8 ) ):
            self.data_types=self.creation.data_types
            self.data_type_check_constraints=self.creation.data_type_check_constraints

        self.introspection = DatabaseIntrospection( self )
        if( djangoVersion[0:2] <= ( 1, 1 ) ):
            self.validation = DatabaseValidation()
        else:
            self.validation = DatabaseValidation( self )
        self.databaseWrapper = Base.DatabaseWrapper()
        
        # IBM DB2 version 11.1 suports natively LIMIT/OFFSET - see #112
        self._supports_limit_offset = None

    @property
    def supports_limit_offset(self):
        """
        Check if server supports LIMIT/OFFSET
        Lazily initialized to avoid sync operations in async contexts
        """
        if self._supports_limit_offset is None:
            try:
                version = self.get_server_version()
                self._supports_limit_offset = (version[:2] >= (11, 1))
            except SynchronousOnlyOperation:
                # In async context, assume modern version
                self._supports_limit_offset = True
            except Exception as e:
                # Fallback to safe default
                self._supports_limit_offset = True
        
        return self._supports_limit_offset
    
    # Method to check if connection is live or not.
    def __is_connection( self ):
        return self.connection is not None

    # To get dict of connection parameters
    def get_connection_params(self):
        if sys.version_info.major >= 3:
            strvar = str
        else:
            strvar = str
        kwargs = { }
        if ( djangoVersion[0:2] <= ( 1, 0 ) ):
            database_name = self.settings.DATABASE_NAME
            database_user = self.settings.DATABASE_USER
            database_pass = self.settings.DATABASE_PASSWORD
            database_host = self.settings.DATABASE_HOST
            database_port = self.settings.DATABASE_PORT
            database_options = self.settings.DATABASE_OPTIONS
        elif ( djangoVersion[0:2] <= ( 1, 1 ) ):
            settings_dict = self.settings_dict
            database_name = settings_dict['DATABASE_NAME']
            database_user = settings_dict['DATABASE_USER']
            database_pass = settings_dict['DATABASE_PASSWORD']
            database_host = settings_dict['DATABASE_HOST']
            database_port = settings_dict['DATABASE_PORT']
            database_options = settings_dict['DATABASE_OPTIONS']
        else:
            settings_dict = self.settings_dict
            database_name = settings_dict['NAME']
            database_user = settings_dict['USER']
            database_pass = settings_dict['PASSWORD']
            database_host = settings_dict['HOST']
            database_port = settings_dict['PORT']
            database_options = settings_dict['OPTIONS']

        if database_name != '' and isinstance( database_name, strvar ):
            kwargs['database'] = database_name
        else:
            raise ImproperlyConfigured( "Please specify the valid database Name to connect to" )

        if isinstance( database_user, strvar ):
            kwargs['user'] = database_user

        if isinstance( database_pass, strvar ):
            kwargs['password'] = database_pass

        if isinstance( database_host, strvar ):
            kwargs['host'] = database_host

        if isinstance( database_port, strvar ):
            kwargs['port'] = database_port

        if isinstance( database_host, strvar ):
            kwargs['host'] = database_host

        if isinstance( database_options, dict ):
            kwargs['options'] = database_options

        if ( djangoVersion[0:2] <= ( 1, 0 ) ):
           if( hasattr( settings, 'PCONNECT' ) ):
               kwargs['PCONNECT'] = settings.PCONNECT
        else:
            if ( list(settings_dict.keys()) ).__contains__( 'PCONNECT' ):
                kwargs['PCONNECT'] = settings_dict['PCONNECT']

        if('CURRENTSCHEMA' in settings_dict):
            database_schema = settings_dict['CURRENTSCHEMA']
            if isinstance( database_schema, str ):
                kwargs['currentschema'] = database_schema

        if('SECURITY'  in settings_dict):
            database_security = settings_dict['SECURITY']
            if isinstance( database_security, str ):
                kwargs['security'] = database_security

        if('SSLCLIENTKEYDB'  in settings_dict):
            database_sslclientkeydb = settings_dict['SSLCLIENTKEYDB']
            if isinstance( database_sslclientkeydb, str ):
                kwargs['sslclientkeydb'] = database_sslclientkeydb

        if('SSLCLIENTKEYSTOREDBPASSWORD'  in settings_dict):
            database_sslclientkeystoredbpassword = settings_dict['SSLCLIENTKEYSTOREDBPASSWORD']
            if isinstance( database_sslclientkeystoredbpassword, str ):
                kwargs['sslclientkeystoredbpassword'] = database_sslclientkeystoredbpassword

        if('SSLCLIENTKEYSTASH'  in settings_dict):
            database_sslclientkeystash =settings_dict['SSLCLIENTKEYSTASH']
            if isinstance( database_sslclientkeystash, str ):
                kwargs['sslclientkeystash'] = database_sslclientkeystash

        if('SSLSERVERCERTIFICATE'  in settings_dict):
            database_sslservercertificate =settings_dict['SSLSERVERCERTIFICATE']
            if isinstance( database_sslservercertificate, str ):
                kwargs['sslservercertificate'] = database_sslservercertificate

        return kwargs

    # To get new connection from Database
    def get_new_connection(self, conn_params):
        connection = self.databaseWrapper.get_new_connection(conn_params)
        if getattr(connection, dbms_name) == 'DB2':
            self.features.has_bulk_insert = False
        else:
            self.features.has_bulk_insert = True
        return connection

    # Over-riding _cursor method to return DB2 cursor.
    if ( djangoVersion[0:2] < ( 1, 6 )):
        def _cursor( self, settings = None ):
            if not self.__is_connection():
                if ( djangoVersion[0:2] <= ( 1, 0 ) ):
                    self.settings = settings

                self.connection = self.get_new_connection(self.get_connection_params())
                cursor = self.databaseWrapper._cursor(self.connection)

                if( djangoVersion[0:3] <= ( 1, 2, 2 ) ):
                    connection_created.send( sender = self.__class__ )
                else:
                    connection_created.send( sender = self.__class__, connection = self )
            else:
                cursor = self.databaseWrapper._cursor( self.connection )
            return cursor
    else:
        def create_cursor( self , name = None):
            return self.databaseWrapper._cursor( self.connection )

        def init_connection_state( self ):
            pass

        def is_usable(self):
            try:
                self.databaseWrapper.is_active( self.connection )
            except Exception as e:
                return False
            else:
                return True

    def _set_autocommit(self, autocommit):
        self.connection.set_autocommit( autocommit )

    def _close(self):
        if self.connection is not None:
            self.databaseWrapper.close( self.connection )
            self.connection = None

    def close( self ):
        if( djangoVersion[0:2] >= ( 1, 5 ) ):
            self.validate_thread_sharing()

        self.run_on_commit = []  # Clear all pending on_commit hooks

        # Don't call validate_no_atomic_block() to avoid making it difficult
        # to get rid of a connection in an invalid state. The next connect()
        # will reset the transaction state anyway.
        if self.closed_in_transaction or self.connection is None:
            return
        try:
            self._close()
        finally:
            if self.in_atomic_block:
                self.closed_in_transaction = True
                self.needs_rollback = True
            else:
                self.connection = None

    def get_server_version( self ):
        if not self.connection:
            self.cursor()
        return self.databaseWrapper.get_server_version( self.connection )

    def schema_editor(self, *args, **kwargs):
        return DB2SchemaEditor(self, *args, **kwargs)

    @contextmanager
    def constraint_checks_disabled(self, table_names=None):
        """
        Disable foreign key constraint checking.
        IF Table names not passed, FK will be disabled for whole DB, its time consuming in DB2.
        """
        disabled = self.disable_constraint_checking(table_names)
        try:
            yield
        finally:
            if disabled:
                self.enable_constraint_checking(table_names)

    def disable_constraint_checking(self, table_names=None):
        """
        IF Table names not passed, FK will be disabled for whole DB, its time consuming in DB2.
        """
        def execute_query_fetched(cursor):
            dis_fk_list = cursor.fetchall()
            for each_query in dis_fk_list:
                each_query_split = each_query[0].split()
                cursor.execute("select enforced from syscat.tabconst where tabname='%s' and constname='%s'" % (each_query_split[2].upper(), each_query_split[6].upper()))
                enforced = cursor.fetchall()
                if enforced and enforced[0][0] != 'N':
                    cursor.execute(each_query[0])


        cursor = self.cursor()

        if table_names is None:
            if not cursor.execute("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1;"):
                return False
            schema = cursor.fetchall()[0][0].strip()
            cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' NOT ENFORCED;' from syscat.references where tabschema='%s'" % schema)
            execute_query_fetched(cursor)
        else:
            for table_name in table_names:
                cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' NOT ENFORCED;' from syscat.references where tabname='%s'" % table_name.upper())
                execute_query_fetched(cursor)

        return True

    def enable_constraint_checking(self, table_names=None):
        """
        Re-enable foreign key checks after they have been disabled.
        """
        # Override needs_rollback in case constraint_checks_disabled is
        # nested inside transaction.atomic.
        self.needs_rollback, needs_rollback = False, self.needs_rollback

        try:
            def execute_query_fetched(cursor):
                en_fk_list = cursor.fetchall()
                for each_query in en_fk_list:
                    each_query_split = each_query[0].split()
                    cursor.execute("select enforced from syscat.tabconst where tabname='%s' and constname='%s'" % (each_query_split[2].upper(), each_query_split[6].upper()))
                    enforced = cursor.fetchall()
                    if enforced and enforced[0][0] != 'Y':
                        cursor.execute(each_query[0])

            cursor = self.cursor()
            if table_names is None:
                if not cursor.execute("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1;"):
                    return False
                schema = cursor.fetchall()[0][0].strip()
                cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' ENFORCED;' from syscat.references where tabschema='%s'" % schema)
                execute_query_fetched(cursor)
            else:
                for table_name in table_names:
                    cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' ENFORCED;' from syscat.references where tabname='%s'" % table_name.upper())
                    execute_query_fetched(cursor)
        finally:
            self.needs_rollback = needs_rollback

    def check_constraints(self, table_names=None):
        """
        Check each table name in `table_names` for rows with invalid foreign
        key references. This method is intended to be used in conjunction with
        `disable_constraint_checking()` and `enable_constraint_checking()`, to
        determine if rows with invalid references were entered while constraint
        checks were off.

        Raise an IntegrityError on the first invalid foreign key reference
        encountered (if any) and provide detailed information about the
        invalid reference in the error message.

        Backends can override this method if they can more directly apply
        constraint checking (e.g. via "SET CONSTRAINTS ALL IMMEDIATE")

        Note: IF Table names not passed, FK check is done for whole DB, its time consuming.
        """

        try:
            self.enable_constraint_checking(table_names)
        finally:
            pass
