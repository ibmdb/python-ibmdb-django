# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2021.                                      |
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
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi,             |
# | Hemlata Bhatt, Vyshakh A                                                 |
# +--------------------------------------------------------------------------+

try:
    from django.db.backends import BaseDatabaseOperations
except ImportError:
    from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.utils import split_tzname_delta
from django.utils.duration import duration_microseconds
from ibm_db_django import query
from django import VERSION as djangoVersion
import sys, datetime, uuid
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime, parse_time
try:
    import pytz
except ImportError:
    pytz = None
    
if ( djangoVersion[0:2] > ( 1, 1 ) ):
    from django.db import utils
    
_IS_JYTHON = sys.platform.startswith( 'java' )
if( djangoVersion[0:2] >= ( 1, 4 ) ):
    from django.conf import settings

if _IS_JYTHON:
    dbms_name = 'dbname'
else:
    dbms_name = 'dbms_name'
    
class DatabaseOperations ( BaseDatabaseOperations ):
    cast_char_field_without_max_length = 'varchar'
    def __init__( self, connection ):
        if( djangoVersion[0:2] >= ( 1, 4 ) ):
            super( DatabaseOperations, self ).__init__(self)
        else:
            super( DatabaseOperations, self ).__init__()
        self.connection = connection
        
    if( djangoVersion[0:2] >= ( 1, 2 ) ):
        compiler_module = "ibm_db_django.compiler"
    
    def cache_key_culling_sql(self):
        return '''SELECT cache_key 
                    FROM (SELECT cache_key, ( ROW_NUMBER() OVER() ) AS ROWNUM FROM %s ORDER BY cache_key)
                    WHERE ROWNUM = %%s + 1
        '''
    def check_aggregate_support( self, aggregate ):
        # In DB2 data type of the result is the same as the data type of the argument values for AVG aggregation
        # But Django aspect in Float regardless of data types of argument value
        # http://publib.boulder.ibm.com/infocenter/db2luw/v9r7/index.jsp?topic=/com.ibm.db2.luw.apdv.cli.doc/doc/c0007645.html
        if aggregate.sql_function == 'AVG':
            aggregate.sql_template = '%(function)s(DOUBLE(%(field)s))'
        #In DB2 equivalent sql function of STDDEV_POP is STDDEV
        elif aggregate.sql_function == 'STDDEV_POP':
            aggregate.sql_function = 'STDDEV'
        #In DB2 equivalent sql function of VAR_SAMP is VARIENCE
        elif aggregate.sql_function == 'VAR_POP':
            aggregate.sql_function = 'VARIANCE'
        #DB2 doesn't have sample standard deviation function
        elif aggregate.sql_function == 'STDDEV_SAMP':
            raise NotImplementedError("sample standard deviation function not supported")
        #DB2 doesn't have sample variance function
        elif aggregate.sql_function == 'VAR_SAMP':
            raise NotImplementedError("sample variance function not supported")

    def adapt_timefield_value(self, value):
        """
        Transform a time value to an object compatible with what is expected
        by the backend driver for time columns.
        """
        if value is None:
            return None

        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value
        
        if timezone.is_aware(value):
            raise ValueError("Django does not support timezone-aware times.")
        return "%02d:%02d:%02d" % (value.hour, value.minute, value.second)

    def get_db_converters(self, expression):
        converters = super(DatabaseOperations, self).get_db_converters(expression)
        
        field_type = expression.output_field.get_internal_type()
        if field_type in ( 'BinaryField',  ):
            converters.append(self.convert_binaryfield_value)
        elif field_type in ('NullBooleanField', 'BooleanField'):
            converters.append(self.convert_booleanfield_value)
        elif field_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif field_type == 'DateTimeField':
            converters.append(self.convert_datetimefield_value)
        elif field_type == 'DateField':
            converters.append(self.convert_datefield_value)
        elif field_type == 'TimeField':
            converters.append(self.convert_timefield_value)
        #  else:
        #   converters.append(self.convert_empty_values)
        """Get a list of functions needed to convert field data.

        Some field types on some backends do not provide data in the correct
        format, this is the hook for coverter functions.
        """
        return converters
    
    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                value = parse_datetime(value)
            if settings.USE_TZ and not timezone.is_aware(value):
                value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_datefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, datetime.date):
                value = parse_date(value)
        return value

    def convert_timefield_value(self, value, expression, connection):
        if value is not None:
            if not isinstance(value, datetime.time):
                value = parse_time(value)
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        return bool(value) if value in (1, 0) else value

    def convert_empty_values(self, value, expression, context):
        # Oracle stores empty strings as null. We need to undo this in
        # order to adhere to the Django convention of using the empty
        # string instead of null, but only if the field accepts the
        # empty string.
        field = expression.output_field
        if value is None and field.empty_strings_allowed:
            value = ''
            if field.get_internal_type() == 'BinaryField':
                value = b''
        return value
    
    def adapt_datetimefield_value(self, value):
        if value is None:
            return None

        # Expression values are adapted by the database.
        if hasattr(value, 'resolve_expression'):
            return value

        return 'TIMESTAMP(\'' + str(value) +'\')'

    def adapt_datefield_value(self, value):
        """
        Transform a date value to an object compatible with what is expected
        by the backend driver for date columns.
        """
        if value is None:
            return None

        return 'DATE(\'' + str(value) +'\')'

    def combine_expression( self, operator, sub_expressions ):
        if operator == '%%':
            return 'MOD(%s, %s)' % ( sub_expressions[0], sub_expressions[1] ) 
        elif operator == '&':
            return 'BITAND(%s, %s)' % ( sub_expressions[0], sub_expressions[1] )
        elif operator == '|': 
            return 'BITOR(%s, %s)' % ( sub_expressions[0], sub_expressions[1] )
        elif operator == '^':
            return 'POWER(%s, %s)' % ( sub_expressions[0], sub_expressions[1] )
        elif operator == '#':
            return 'BITXOR(%s, %s)' % ( sub_expressions[0], sub_expressions[1] )
        elif operator == '-':
            if( djangoVersion[0:2] >= ( 2 , 0) ):
                strr= str(sub_expressions[1])
                sub_expressions[1] = strr.replace('+', '-')
            else:
                sub_expressions[1] = str.replace('+', '-')
            return super( DatabaseOperations, self ).combine_expression( operator, sub_expressions )
        else:
            return super( DatabaseOperations, self ).combine_expression( operator, sub_expressions )
    
    def convert_binaryfield_value( self,value, expression,connections ):
        return value

    if( djangoVersion[0:2] >= ( 1, 8 ) ):
        def format_for_duration_arithmetic(self, sql):
            return ' %s MICROSECONDS' % sql
    
    # Function to extract day, month or year from the date.
    # Reference: http://publib.boulder.ibm.com/infocenter/db2luw/v9r5/topic/com.ibm.db2.luw.sql.ref.doc/doc/r0023457.html
    def date_extract_sql( self, lookup_type, sql, params ):
        if lookup_type.upper() == 'WEEK_DAY':
            return f" DAYOFWEEK({sql}) " , params
        elif lookup_type.upper() == 'ISO_YEAR':
            return f" TO_CHAR({sql}, 'IYYY')" , params
        elif lookup_type.upper() == 'WEEK':
            return f" WEEK_ISO({sql}) " , params
        elif lookup_type.upper() == 'ISO_WEEK_DAY':            
            return f"DAYOFWEEK_ISO({sql})" , params
        else:
            sql = f" %s({sql}) " % lookup_type.upper()
            return sql, params
    
    def _get_utcoffset(self, tzname):
        if pytz is None and tzname is not None:
            NotSupportedError("Not supported without pytz")
        else:
            hr = 0
            min = 0
            tz = pytz.timezone(tzname)
            td = tz.utcoffset(datetime.datetime(2012,1,1))
            if td.days == -1:
                min = (td.seconds % (60*60))/60 - 60
                if min:
                    hr = td.seconds/(60*60) - 23
                else:
                    hr = td.seconds/(60*60) - 24
            else:
                hr = td.seconds/(60*60)
                min = (td.seconds % (60*60))/60
            return hr, min
            
    # Function to extract time zone-aware day, month or day of week from timestamps   
    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return self.date_extract_sql(lookup_type, sql, params)

    # Truncating the date value on the basic of lookup type.
    # e.g If input is 2008-12-04 and month then output will be 2008-12-01 00:00:00
    # Reference: http://www.ibm.com/developerworks/data/library/samples/db2/0205udfs/index.html
    # Note: For zos we may need to modify this
    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"DATE_TRUNC(%s, {sql})", (lookup_type, *params)

    def _prepare_tzname_delta(self, tzname):
        tzname, sign, offset = split_tzname_delta(tzname)
        return f"{sign}{offset}" if offset else tzname

    def _convert_sql_to_tz(self, sql, params, tzname):
        if tzname and settings.USE_TZ and self.connection.timezone_name != tzname:
            return f"TIMEZONE_TZ({sql}, %s)", (
                *params,
                self._prepare_tzname_delta(tzname),
            )
        return sql, params
    
    # Truncating the time zone-aware timestamps value on the basic of lookup type
    # Note: For zos we may need to modify this
    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        return self.date_trunc_sql(lookup_type, sql, params, tzname)

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"DATE_TRUNC(%s, {sql})::time", (lookup_type, *params)

    def datetime_cast_date_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f'({sql})::date' , params

    def datetime_cast_time_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"TIME({sql})", params

    if( djangoVersion[0:2] >= ( 1, 8 ) ): 
        def date_interval_sql( self, timedelta ):
            if(timedelta.days and timedelta.seconds and timedelta.microseconds):
                return " %d days + %d seconds + %d microseconds" % (
                timedelta.days, timedelta.seconds, timedelta.microseconds)
            elif(timedelta.seconds or timedelta.microseconds):
                ms = duration_microseconds(timedelta)
                return ' %s MICROSECONDS' % ms
            else:
                return str( timedelta.days ) + " DAYS"
    else:
        def date_interval_sql( self, sql, connector, timedelta ):
            date_interval_token = []
            date_interval_token.append( sql )
            date_interval_token.append( str( timedelta.days ) + " DAYS" )
            if timedelta.seconds > 0:
                date_interval_token.append( str( timedelta.seconds ) + " SECONDS" )
            if timedelta.microseconds > 0:
                date_interval_token.append( str( timedelta.microseconds ) + " MICROSECONDS" )
            sql = "( %s )" % connector.join( date_interval_token )
            return sql
    
    #As casting is not required, so nothing is required to do in this function.
    def datetime_cast_sql( self ):
        return "%s"
        
    def deferrable_sql( self ):
        if getattr(self.connection.connection, dbms_name) == 'DB2':
            return "ON DELETE NO ACTION NOT ENFORCED"
        else:
            return ""
        
    # Function to return SQL from dropping foreign key.
    def drop_foreignkey_sql( self ):
        return "DROP FOREIGN KEY"
    
    # Dropping auto generated property of the identity column.
    def drop_sequence_sql( self, table ):
        return "ALTER TABLE %s ALTER COLUMN ID DROP IDENTITY" % ( self.quote_name( table ) )
    
    #This function casts the field and returns it for use in the where clause
    def field_cast_sql( self, db_type, internal_type=None ):
        if db_type == 'CLOB':
            return "VARCHAR(%s, 4096)"
        else:
            return " %s"
        
    def fulltext_search_sql( self, field_name ):
        sql = "WHERE %s = ?" % field_name
        return sql
    
    # Function to return value of auto-generated field of last executed insert query. 
    def last_insert_id( self, cursor, table_name, pk_name ):
        if not _IS_JYTHON:
            return cursor.last_identity_val
        else:
            operation = 'SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1'
            cursor.execute( operation )
            row = cursor.fetchone()
            last_identity_val = None
            if row is not None:
                last_identity_val = int( row[0] )
            return last_identity_val
    
    # In case of WHERE clause, if the search is required to be case insensitive then converting 
    # left hand side field to upper.
    def lookup_cast( self, lookup_type, internal_type=None ):
        if lookup_type in ( 'iexact', 'icontains', 'istartswith', 'iendswith' ):
            return "UPPER(%s)"
        return "%s"
    
    # As DB2 v91 specifications, 
    # Maximum length of a table name and Maximum length of a column name is 128
    # http://publib.boulder.ibm.com/infocenter/db2e/v9r1/index.jsp?topic=/com.ibm.db2e.doc/db2elimits.html
    def max_name_length( self ):
        return 128
    
    # As DB2 v97 specifications,
    # Maximum length of a database name is 8
    #http://publib.boulder.ibm.com/infocenter/db2luw/v9r7/topic/com.ibm.db2.luw.sql.ref.doc/doc/r0001029.html
    def max_db_name_length( self ):
        return 8
    
    def no_limit_value( self ):
        return None
    
    # Method to point custom query class implementation.
    def query_class( self, DefaultQueryClass ):
        return query.query_class( DefaultQueryClass )
        
    # Function to quote the name of schema, table and column.
    def quote_name( self, name = None):
        if name == None:
            return None
        name = name.upper()
        
        if( name.startswith( '""' ) & name.endswith( '""' ) ):
            return "\"%s\"" % name
        
        if( name.startswith( "\"" ) & name.endswith( "\"" ) ):
            return name
        
        if( name.startswith( "\"" ) ):
            return "%s\"" % name
        
        if( name.endswith( "\"" ) ) & ( name.count("\"") % 2 == 1 ):
            return "\"%s" % name

        return "\"%s\"" % name
    
    # SQL to return RANDOM number.
    # Reference: http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.ibm.db2.udb.doc/admin/r0000840.htm
    def random_function_sql( self ):
        return "SYSFUN.RAND()"
    
    
    def regex_lookup(self, lookup_type):
        if lookup_type == 'regex':
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s")' passing %s as "c") as varchar(5)) = 'true' db2regexExtraField(%s)'''
        else:
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s", "i")' passing %s as "c") as varchar(5)) = 'true' db2regexExtraField(%s)'''
        
    # As save-point is supported by DB2, following function will return SQL to create savepoint.
    def savepoint_create_sql( self, sid ):
        return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid
    
    # Function to commit savepoint.   
    def savepoint_commit_sql( self, sid ):
        return "RELEASE TO SAVEPOINT %s" % sid
    
    # Function to rollback savepoint.
    def savepoint_rollback_sql( self, sid ):
        return "ROLLBACK TO SAVEPOINT %s" % sid
    
    # Deleting all the rows from the list of tables provided and resetting all the
    # sequences.
    def sql_flush( self, style, tables, reset_sequences=False, allow_cascade=False ):
        curr_schema = self.connection.connection.get_current_schema().upper()
        sqls = []
        if tables:
            #check for zOS DB2 server
            if getattr(self.connection.connection, dbms_name) != 'DB2':
                fk_tab = 'TABNAME'
                fk_tabschema = 'TABSCHEMA'
                fk_const = 'CONSTNAME'
                fk_systab = 'SYSCAT.TABCONST'
                type_check_string = "type = 'F' and"
                sqls.append( '''CREATE PROCEDURE FKEY_ALT_CONST(django_tabname VARCHAR(128), curr_schema VARCHAR(128))
                    LANGUAGE SQL
                    P1: BEGIN
                        DECLARE fktable varchar(128);
                        DECLARE fkconst varchar(128);
                        DECLARE row_count integer;
                        DECLARE alter_fkey_sql varchar(350);
                        DECLARE cur1 CURSOR for SELECT %(fk_tab)s, %(fk_const)s FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tabschema)s = curr_schema and ENFORCED = 'N';
                        DECLARE cur2 CURSOR for SELECT %(fk_tab)s, %(fk_const)s FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tab)s = django_tabname and %(fk_tabschema)s = curr_schema and ENFORCED = 'Y';
                        IF ( django_tabname = '' ) THEN
                            SET row_count = 0;
                            SELECT count( * ) INTO row_count FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tabschema)s = curr_schema and ENFORCED = 'N';
                            IF ( row_count > 0 ) THEN
                                OPEN cur1;
                                WHILE( row_count > 0 ) DO 
                                    FETCH cur1 INTO fktable, fkconst;
                                    IF ( LOCATE( ' ', fktable ) > 0 ) THEN 
                                        SET alter_fkey_sql = 'ALTER TABLE ' || '\"' || fktable || '\"' ||' ALTER FOREIGN KEY ';
                                    ELSE
                                        SET alter_fkey_sql = 'ALTER TABLE ' || fktable || ' ALTER FOREIGN KEY ';
                                    END IF;
                                    IF ( LOCATE( ' ', fkconst ) > 0) THEN
                                        SET alter_fkey_sql = alter_fkey_sql || '\"' || fkconst || '\"' || ' ENFORCED';
                                    ELSE
                                        SET alter_fkey_sql = alter_fkey_sql || fkconst || ' ENFORCED';
                                    END IF;
                                    execute immediate alter_fkey_sql;
                                    SET row_count = row_count - 1;
                                END WHILE;
                                CLOSE cur1;
                            END IF;
                        ELSE
                            SET row_count = 0;
                            SELECT count( * ) INTO row_count FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tab)s = django_tabname and %(fk_tabschema)s = curr_schema and ENFORCED = 'Y';
                            IF ( row_count > 0 ) THEN
                                OPEN cur2;
                                WHILE( row_count > 0 ) DO 
                                    FETCH cur2 INTO fktable, fkconst;
                                    IF ( LOCATE( ' ', fktable ) > 0 ) THEN 
                                        SET alter_fkey_sql = 'ALTER TABLE ' || '\"' || fktable || '\"' ||' ALTER FOREIGN KEY ';
                                    ELSE
                                        SET alter_fkey_sql = 'ALTER TABLE ' || fktable || ' ALTER FOREIGN KEY ';
                                    END IF;
                                    IF ( LOCATE( ' ', fkconst ) > 0) THEN
                                        SET alter_fkey_sql = alter_fkey_sql || '\"' || fkconst || '\"' || ' NOT ENFORCED';
                                    ELSE
                                        SET alter_fkey_sql = alter_fkey_sql || fkconst || ' NOT ENFORCED';
                                    END IF;
                                    execute immediate alter_fkey_sql;
                                    SET row_count = row_count - 1;
                                END WHILE;
                                CLOSE cur2;
                            END IF;
                        END IF;
                    END P1''' % {'fk_tab':fk_tab, 'fk_tabschema':fk_tabschema, 'fk_const':fk_const, 'fk_systab':fk_systab, 'type_check_string':type_check_string} )  
            
            if getattr(self.connection.connection, dbms_name) != 'DB2':
                for table in tables:
                    sqls.append( "CALL FKEY_ALT_CONST( '%s', '%s' );" % ( table.upper(), curr_schema ) )
            else:
                sqls = []
                
            for table in tables:
                sqls.append( style.SQL_KEYWORD( "DELETE" ) + " " + 
                           style.SQL_KEYWORD( "FROM" ) + " " + 
                           style.SQL_TABLE( "%s" % self.quote_name( table ) ) )
                
            if getattr(self.connection.connection, dbms_name) != 'DB2':    
                sqls.append( "CALL FKEY_ALT_CONST( '' , '%s' );" % ( curr_schema, ) )
                sqls.append( "DROP PROCEDURE FKEY_ALT_CONST;" )  
                
            sequences = self.connection.introspection.sequence_list() if reset_sequences else ()
            for sequence in sequences:
                if( sequence['column'] != None ):
                    sqls.append( style.SQL_KEYWORD( "ALTER TABLE" ) + " " + 
                            style.SQL_TABLE( "%s" % self.quote_name( sequence['table'] ) ) + 
                            " " + style.SQL_KEYWORD( "ALTER COLUMN" ) + " %s "
                            % self.quote_name( sequence['column'] ) + 
                            style.SQL_KEYWORD( "RESTART WITH 1" ) )
            return sqls
        else:
            return []
    
    # Table many contains rows when this is get called, hence resetting sequence
    # to a large value (10000).
    def sequence_reset_sql( self, style, model_list ):
        from django.db import models
        cursor = self.connection.cursor()
        sqls = []
        for model in model_list:
            table = model._meta.db_table
            for field in model._meta.local_fields:
                if isinstance( field, models.AutoField ):
                    max_sql = "SELECT MAX(%s) FROM %s" % ( self.quote_name( field.column ), self.quote_name( table ) )
                    cursor.execute( max_sql )
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] == None:
                        max_id[0] = 0
                    sqls.append( style.SQL_KEYWORD( "ALTER TABLE" ) + " " + 
                        style.SQL_TABLE( "%s" % self.quote_name( table ) ) + 
                        " " + style.SQL_KEYWORD( "ALTER COLUMN" ) + " %s "
                        % self.quote_name( field.column ) + 
                        style.SQL_KEYWORD( "RESTART WITH %s" % ( max_id[0] + 1 ) ) )
                    break

            for field in model._meta.many_to_many:
                m2m_table = field.m2m_db_table()
                if( djangoVersion[0:2] < ( 1, 9 ) ):
                    if field.rel is not None and hasattr(field.rel,'through'):
                        flag = field.rel.through
                    else:
                        flag = False
                else:
                    if field.remote_field is not None and hasattr(field.remote_field,'through'):
                        flag= field.remote_field.through
                    else:
                        flag = False
                if not flag:
                    max_sql = "SELECT MAX(%s) FROM %s" % ( self.quote_name( 'ID' ), self.quote_name( table ) )
                    cursor.execute( max_sql )
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] == None:
                        max_id[0] = 0
                    sqls.append( style.SQL_KEYWORD( "ALTER TABLE" ) + " " + 
                        style.SQL_TABLE( "%s" % self.quote_name( m2m_table ) ) + 
                        " " + style.SQL_KEYWORD( "ALTER COLUMN" ) + " %s "
                        % self.quote_name( 'ID' ) + 
                        style.SQL_KEYWORD( "RESTART WITH %s" % ( max_id[0] + 1 ) ) )
        if cursor:
            cursor.close()
            
        return sqls
    
    # Returns sqls to reset the passed sequences
    def sequence_reset_by_name_sql(self, style, sequences):
        sqls = []
        for seq in sequences:
            sqls.append( style.SQL_KEYWORD( "ALTER TABLE" ) + " " + 
                         style.SQL_TABLE( "%s" % self.quote_name( seq.get('table') ) ) +
                         " " + style.SQL_KEYWORD( "ALTER COLUMN" ) + " %s " % self.quote_name( seq.get('column') ) + 
                         style.SQL_KEYWORD( "RESTART WITH %s" % ( 1 ) ))
        return sqls
    
    def tablespace_sql( self, tablespace, inline = False ):
        # inline is used for column indexes defined in-line with column definition, like:
        #   CREATE TABLE "TABLE1" ("ID_OTHER" VARCHAR(20) NOT NULL UNIQUE) IN "TABLESPACE1";
        # couldn't find support for this in create table 
        #   (http://publib.boulder.ibm.com/infocenter/db2luw/v9/topic/com.ibm.db2.udb.admin.doc/doc/r0000927.htm)
        if inline:
            sql = ""
        else:
            sql = "IN %s" % self.quote_name( tablespace )
        return sql
    
    def value_to_db_datetime( self, value ):
        if value is None:
            return None
        
        if( djangoVersion[0:2] <= ( 1, 3 ) ):
            #DB2 doesn't support time zone aware datetime
            if ( value.tzinfo is not None ):
                raise ValueError( "Timezone aware datetime not supported" )
            else:
                return value
        else:
            if timezone.is_aware(value):
                if settings.USE_TZ:
                    value = value.astimezone( utc ).replace( tzinfo=None )
                else:
                    raise ValueError( "Timezone aware datetime not supported" )
            return str( value )
        
    def value_to_db_time( self, value ):
        if value is None:
            return None
        
        if( djangoVersion[0:2] <= ( 1, 3 ) ):
            #DB2 doesn't support time zone aware time
            if ( value.tzinfo is not None ):
                raise ValueError( "Timezone aware time not supported" )
            else:
                return value
        else:
            if timezone.is_aware(value):
                raise ValueError( "Timezone aware time not supported" )
            else:
                return value
    
    def bulk_insert_sql(self, fields, num_values):
        placeholder_rows_sql = (", ".join(row) for row in num_values)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql
    
    def for_update_sql(self, nowait=False, skip_locked=False, of=(), no_key=False):
        #DB2 doesn't support nowait select for update
        if nowait:
            if ( djangoVersion[0:2] > ( 1, 1 ) ):
                raise utils.DatabaseError( "Nowait Select for update not supported " )
            else:
                raise ValueError( "Nowait Select for update not supported " )
        else:
            return 'WITH RS USE AND KEEP UPDATE LOCKS'

    def distinct_sql(self, fields, params):
        if fields:
            raise ValueError( "distinct_on_fields not supported" )
        else:
            return ['DISTINCT'], []

    def last_executed_query(self, cursor, sql, params):
        if params:
            if isinstance(params, list):
                params = tuple(params)
            
            if sql.count("db2regexExtraField(%s)") > 0:
                sql = sql.replace("db2regexExtraField(%s)", "")
                
            return sql % params
        else:
            return sql

    def limit_offset_sql(self, low_mark, high_mark):
        fetch, offset = self._get_limit_offset_params(low_mark, high_mark)
        return ' '.join(sql for sql in (
            ('OFFSET %d ROWS' % offset) if offset else None,
            ('FETCH FIRST %d ROWS ONLY' % fetch) if fetch else None,
        ) if sql)