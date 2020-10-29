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
#from builtins import True
from _ast import Or

# Importing IBM_DB wrapper ibm_db_dbi
try:
    import ibm_db_dbi as Database
except ImportError as e:
    raise ImportError( "ibm_db module not found. Install ibm_db module from http://code.google.com/p/ibm-db/. Error: %s" % e )

from decimal import Decimal
import regex

import datetime
# For checking django's version
from django import VERSION as djangoVersion

if ( djangoVersion[0:2] > ( 1, 1 ) ):
    from django.db import utils
    import sys
if ( djangoVersion[0:2] >= ( 1, 4) ):
    from django.utils import timezone
    from django.conf import settings
    import warnings
if ( djangoVersion[0:2] >= ( 1, 5 )):
    from django.utils.encoding import force_bytes, force_text
    from django.utils import six
    import re
 
_IS_JYTHON = sys.platform.startswith( 'java' )
if _IS_JYTHON:
    dbms_name = 'dbname'
else:
    dbms_name = 'dbms_name'

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
    
class DatabaseWrapper( object ):
    # Get new database connection for non persistance connection 
    def get_new_connection(self, kwargs):
        kwargsKeys = list(kwargs.keys())
        if ( kwargsKeys.__contains__( 'port' ) and 
            kwargsKeys.__contains__( 'host' ) ):
            kwargs['dsn'] = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;" % ( 
                     kwargs.get( 'database' ),
                     kwargs.get( 'host' ),
                     kwargs.get( 'port' )
            )
        else:
            kwargs['dsn'] = kwargs.get( 'database' )

        if ( kwargsKeys.__contains__( 'currentschema' )):
            kwargs['dsn'] += "CurrentSchema=%s;" % (  kwargs.get( 'currentschema' ))
            del kwargs['currentschema']

        if ( kwargsKeys.__contains__( 'security' )):
            kwargs['dsn'] += "security=%s;" % (  kwargs.get( 'security' ))
            del kwargs['security']

        if ( kwargsKeys.__contains__( 'sslclientkeystoredb' )):
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDB=%s;" % (  kwargs.get( 'sslclientkeystoredb' ))
            del kwargs['sslclientkeystoredb']

        if ( kwargsKeys.__contains__( 'sslclientkeystoredbpassword' )):
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDBPASSWORD=%s;" % (  kwargs.get( 'sslclientkeystoredbpassword' ))
            del kwargs['sslclientkeystoredbpassword']

        if ( kwargsKeys.__contains__( 'sslclientkeystash' )):
            kwargs['dsn'] += "SSLCLIENTKEYSTASH=%s;" % (  kwargs.get( 'sslclientkeystash' ))
            del kwargs['sslclientkeystash']

        if ( kwargsKeys.__contains__( 'sslservercertificate' )):
            kwargs['dsn'] += "SSLSERVERCERTIFICATE=%s;" % (  kwargs.get( 'sslservercertificate' ))
            del kwargs['sslservercertificate']

        # Before Django 1.6, autocommit was turned OFF
        if ( djangoVersion[0:2] >= ( 1, 6 )):
            conn_options = {Database.SQL_ATTR_AUTOCOMMIT : Database.SQL_AUTOCOMMIT_ON}
        else:
            conn_options = {Database.SQL_ATTR_AUTOCOMMIT : Database.SQL_AUTOCOMMIT_OFF}
        kwargs['conn_options'] = conn_options
        if kwargsKeys.__contains__( 'options' ):
            kwargs.update( kwargs.get( 'options' ) )
            del kwargs['options']
        if kwargsKeys.__contains__( 'port' ):
            del kwargs['port']
        
        pconnect_flag = False
        if kwargsKeys.__contains__( 'PCONNECT' ):
            pconnect_flag = kwargs['PCONNECT']
            del kwargs['PCONNECT']
            
        if pconnect_flag:
            connection = Database.pconnect( **kwargs )
        else:
            connection = Database.connect( **kwargs )
        connection.autocommit = connection.set_autocommit
        
        return connection
    
    def is_active( self, connection = None ):
        return Database.ibm_db.active(connection.conn_handler)
        
    # Over-riding _cursor method to return DB2 cursor.
    def _cursor( self, connection ):
        return DB2CursorWrapper( connection )
                    
    def close( self, connection ):
        connection.close()
        
    def get_server_version( self, connection ):
        self.connection = connection
        if not self.connection:
            self.cursor()
        return tuple( int( version ) for version in self.connection.server_info()[1].split( "." ) )
    
class DB2CursorWrapper( Database.Cursor ):
        
    """
    This is the wrapper around IBM_DB_DBI in order to support format parameter style
    IBM_DB_DBI supports qmark, where as Django support format style, 
    hence this conversion is required. 
    """
    
    def __init__( self, connection ): 
        super( DB2CursorWrapper, self ).__init__( connection.conn_handler, connection )
        
    def __iter__( self ):
        return self
        
    def __next__( self ):
        row = self.fetchone()
        if row == None:
            raise StopIteration
        return row
    
    def _create_instance(self, connection):
        return DB2CursorWrapper(connection)
        
    #Ex: string = 'ababababababababab', sub = 'ab', wanted = 'CD', n = 5
    #outputs: ababababCDabababab
    def _replacenth( self, string, sub, wanted, index, need_quote):
        where = [m.start() for m in re.finditer(sub, string)][index]
        before = string[:where]
        after = string[where+2:]
        newString = before + need_quote + str(wanted) + need_quote + after
        return newString

    def _format_parameters( self, parameters, operation, return_only_param = False):
        select_update = False
        if re.match(r'^(SELECT|UPDATE) ', operation):
            select_update = True

        new_parameters = []
        parameters = list( parameters )
        for index in range( len( parameters ) ):
            # With raw SQL queries, datetimes can reach this function
            # without being converted by DateTimeField.get_db_prep_value.
            if settings.USE_TZ and isinstance( parameters[index], datetime.datetime ):
                param = parameters[index]
                if timezone.is_naive( param ):
                    warnings.warn("Received a naive datetime (%s)"
                              " while time zone support is active." % param,
                              RuntimeWarning)
                    default_timezone = timezone.get_default_timezone()
                    param = timezone.make_aware( param, default_timezone )
                param = param.astimezone(timezone.utc).replace(tzinfo=None)
                parameters[index] = param

            need_quote = ''
            if (select_update and isinstance(parameters[index], Decimal)):
                operation = self._replacenth(operation, '%s', parameters[index], len(new_parameters), need_quote)
            else:
                new_parameters.append(parameters[index])

        if return_only_param:
            return tuple( new_parameters )

        return tuple( new_parameters ), operation

    def _resolve_parameters_in_aggregator_func(self, parameters, operation):
        op_temp = ""
        op_temp_wParam = ""
        p_start = 0
        aggr_list = ['COUNT','AVG','MIN','MAX','SUM']
        res = any(ele in operation for ele in aggr_list)

        if res:
            for m in regex.finditer(r'(SUM|AVG|COUNT|MIN|MAX)\ *\(', operation):
                end = m.end()
                p_start = len(op_temp)
                prev_str = operation[p_start:end-1]
                op_temp = op_temp + prev_str
                op_temp_wParam = op_temp_wParam + prev_str
                next_str = operation[end-1:]
                parm_count = op_temp_wParam.count('%s')
                for item in regex.finditer(r'\((?>[^()]|(?R))*\)', next_str):
                    start = item.start()
                    end = item.end()
                    str_wp = next_str[start:end]
                    while (str_wp.count('%s') > 0):
                        if(isinstance(parameters[parm_count], str) and
                           (parameters[parm_count].find('DATE') != 0) and
                           (parameters[parm_count].find('TIMESTAMP') != 0)):
                            need_quote = "\'"
                        else:
                            need_quote = ''
                        str_wp = self._replacenth(str_wp, '%s', parameters[parm_count], 0, need_quote)
                        parameters = parameters[:parm_count] + parameters[(parm_count+1):]
                    op_temp_wParam = op_temp_wParam + str_wp
                    op_temp = op_temp + next_str[start:end]
                    remg = end
                    break

            p_start = len(op_temp)
            operation = op_temp_wParam + operation[p_start:]

        return parameters, operation

    def _resolve_parameters_in_expression_func(self, parameters, operation):
        prev_end = 0
        op_temp_wParam = ""
        p_start = 0

        if re.search(r'%s\ *\+|\+\ *%s|\ *THEN\ *%s|\ *\(?%s\)?\ *AS\ *|\ *ELSE\ *%s', operation):
            for m in re.finditer(r'%s\ *\+|\+\ *%s|\ *THEN\ *%s|\ *\(?%s\)?\ *AS\ *|\ *ELSE\ *%s', operation):
                p_start = m.start()
                op_temp_wParam = op_temp_wParam + operation[prev_end:p_start]
                parm_count = op_temp_wParam.count('%s')
                end = m.end()
                str_wp = operation[p_start:end]
                if((isinstance(parameters[parm_count], str) and
                   (parameters[parm_count].find('DATE') != 0) and
                   (parameters[parm_count].find('TIMESTAMP') != 0)) or
                    (isinstance(parameters[parm_count], datetime.date))):
                    need_quote = "\'"
                else:
                    need_quote = ''
                if(isinstance(parameters[parm_count], memoryview)):
                    replace_string = "BX\'%s\'" % parameters[parm_count].obj.hex()
                else:
                    replace_string = parameters[parm_count]
                str_wp = self._replacenth(str_wp, '%s', replace_string, 0, need_quote)
                parameters = parameters[:parm_count] + parameters[(parm_count+1):]
                op_temp_wParam = op_temp_wParam + str_wp
                prev_end = end

            operation = op_temp_wParam + operation[end:]

        return parameters, operation

    # Over-riding this method to modify SQLs which contains format parameter to qmark. 
    def execute( self, operation, parameters = () ):
        if( djangoVersion[0:2] >= (2 , 0)):
            operation = str(operation)
        try:
            if operation == "''":
                operation = "SELECT NULL FROM SYSIBM.DUAL FETCH FIRST 0 ROW ONLY"
            if operation.find('ALTER TABLE') == 0 and getattr(self.connection, dbms_name) != 'DB2':
                doReorg = 1
            else:
                doReorg = 0
            if operation.count("db2regexExtraField(%s)") > 0:
                operation = operation.replace("db2regexExtraField(%s)", "")
                operation = operation % parameters
                parameters = ()

            if operation.count( "%s" ) > 0 and parameters:
                parameters, operation = self._resolve_parameters_in_aggregator_func(parameters, operation)
                parameters, operation = self._format_parameters( parameters, operation )
                parameters, operation = self._resolve_parameters_in_expression_func( parameters, operation )
                if operation.count( "%s" ) > 0:
                    operation = operation.replace("%s", "?")
                
            if ( djangoVersion[0:2] <= ( 1, 1 ) ):
                if ( doReorg == 1 ):
                    super( DB2CursorWrapper, self ).execute( operation, parameters )
                    return self._reorg_tables()
                else:    
                    return super( DB2CursorWrapper, self ).execute( operation, parameters )
            else:
                try:
                    if ( doReorg == 1 ):
                        super( DB2CursorWrapper, self ).execute( operation, parameters )
                        return self._reorg_tables()
                    else:    
                        return super( DB2CursorWrapper, self ).execute( operation, parameters )
                except IntegrityError as e:
                    six.reraise(utils.IntegrityError, utils.IntegrityError( *tuple( six.PY3 and e.args or ( e._message, ) ) ), sys.exc_info()[2])
                    raise
                        
                except ProgrammingError as e:
                    six.reraise(utils.ProgrammingError, utils.ProgrammingError( *tuple( six.PY3 and e.args or ( e._message, ) ) ), sys.exc_info()[2])
                    raise

                except DatabaseError as e:
                    six.reraise(utils.DatabaseError, utils.DatabaseError( *tuple( six.PY3 and e.args or ( e._message, ) ) ), sys.exc_info()[2])
                    raise

        except ( TypeError ):
            return None
        
    # Over-riding this method to modify SQLs which contains format parameter to qmark.
    def executemany( self, operation, seq_parameters ):
        try:
            if operation.count("db2regexExtraField(%s)") > 0:
                 raise ValueError("Regex not supported in this operation")

            return_only_param = True
            seq_parameters = [ self._format_parameters( parameters, operation, return_only_param) for parameters in seq_parameters ]
            if operation.count( "%s" ) > 0:
                operation = operation % ( tuple( "?" * operation.count( "%s" ) ) )
                
            if ( djangoVersion[0:2] <= ( 1, 1 ) ):
                return super( DB2CursorWrapper, self ).executemany( operation, seq_parameters )
            else:
                try:
                    return super( DB2CursorWrapper, self ).executemany( operation, seq_parameters )
                except IntegrityError as e:
                    six.reraise(utils.IntegrityError, utils.IntegrityError( *tuple( six.PY3 and e.args or ( e._message, ) ) ), sys.exc_info()[2])
                    raise

                except DatabaseError as e:
                    six.reraise(utils.DatabaseError, utils.DatabaseError( *tuple( six.PY3 and e.args or ( e._message, ) ) ), sys.exc_info()[2])
                    raise

        except ( IndexError, TypeError ):
            return None
    
    # table reorganization method
    def _reorg_tables( self ):
        checkReorgSQL = "select TABSCHEMA, TABNAME from SYSIBMADM.ADMINTABINFO where REORG_PENDING = 'Y'"
        res = []
        reorgSQLs = []
        parameters = ()
        super( DB2CursorWrapper, self ).execute(checkReorgSQL, parameters)
        res = super( DB2CursorWrapper, self ).fetchall()
        if res:
            for sName, tName in res:
                reorgSQL = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {'sName': sName, 'tName': tName}
                reorgSQLs.append(reorgSQL)
            for sql in reorgSQLs:
                super( DB2CursorWrapper, self ).execute(sql)
    
    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchone( self ):
        row = super( DB2CursorWrapper, self ).fetchone()
        if row is None:
            return row
        else:
            return self._fix_return_data( row )
    
    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchmany( self, size=0 ):
        rows = super( DB2CursorWrapper, self ).fetchmany( size )
        if rows is None:
            return rows
        else:
            return [self._fix_return_data( row ) for row in rows]
    
    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchall( self ):
        rows = super( DB2CursorWrapper, self ).fetchall()
        if rows is None:
            return rows
        else:
            return [self._fix_return_data( row ) for row in rows]
        
    # This method to modify result set containing datetime and time zone support is active   
    def _fix_return_data( self, row ):
        row = list( row )
        index = -1
        if ( djangoVersion[0:2] >= ( 1, 4 ) ):
            for value, desc in zip( row, self.description ):
                index = index + 1
                if ( desc[1] == Database.DATETIME ):
                    if settings.USE_TZ and value is not None and timezone.is_naive( value ):
                        value = value.replace( tzinfo=timezone.utc )
                        row[index] = value
                elif ( djangoVersion[0:2] >= (1, 5 ) ):
                    if isinstance(value, six.string_types):
                        row[index] = re.sub(r'[\x00]', '', value)
        return tuple( row )
