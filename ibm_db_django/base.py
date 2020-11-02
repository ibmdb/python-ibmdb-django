# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2020.                                      |
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
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+

"""
DB2 database backend for Django.
Requires: ibm_db_dbi (http://pypi.python.org/pypi/ibm_db) for python
"""
import sys
_IS_JYTHON = sys.platform.startswith( 'java' )

from django.core.exceptions import ImproperlyConfigured

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
    
class DatabaseFeatures( BaseDatabaseFeatures ):    
    can_use_chunked_reads = True
    
    #Save point is supported by DB2.
    uses_savepoints = True
    
    #Custom query class has been implemented 
    #django.db.backends.db2.query.query_class.DB2QueryClass
    uses_custom_query_class = True
    
    #transaction is supported by DB2
    supports_transactions = True
    
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

    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': r"LIKE '%%' || {} || '%%' ESCAPE '\'",
        'icontains': r"LIKE '%%' || UPPER({}) || '%%' ESCAPE '\'",
        'startswith': r"LIKE {} || '%%' ESCAPE '\'",
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

        self._close()
        
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
                if enforced[0][0] != 'N':
                    cursor.execute(each_query[0])

        cursor = self.cursor()
        if table_names is None:
            cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' NOT ENFORCED;' from syscat.references")
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
                    if enforced[0][0] != 'Y':
                        cursor.execute(each_query[0])

            cursor = self.cursor()
            if table_names is None:
                cursor.execute("select 'ALTER TABLE ' || TRIM(tabname) || ' ALTER FOREIGN KEY ' || TRIM(constname) || ' ENFORCED;' from syscat.references")
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
