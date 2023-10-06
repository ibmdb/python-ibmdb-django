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
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+
from collections import namedtuple
import sys
_IS_JYTHON = sys.platform.startswith( 'java' )

import regex
if not _IS_JYTHON:
    try:    
        # Import IBM_DB wrapper ibm_db_dbi
        import ibm_db_dbi as Database
        #from Database import DatabaseError
    except ImportError as e:
        raise ImportError( "ibm_db module not found. Install ibm_db module from http://code.google.com/p/ibm-db/. Error: %s" % e )
else:
    from com.ziclix.python.sql import zxJDBC

try:
    from django.db.backends import BaseDatabaseIntrospection, FieldInfo
except ImportError:
    from django.db.backends.base.introspection import BaseDatabaseIntrospection, FieldInfo

from django import VERSION as djangoVersion

class DatabaseIntrospection( BaseDatabaseIntrospection ):
    
    """
    This is the class where database metadata information can be generated.
    """

    if not _IS_JYTHON:
        data_types_reverse = {
            Database.STRING :           "CharField",
            Database.TEXT :             "TextField",
            Database.XML :              "XMLField",
            Database.NUMBER :           "IntegerField",
            Database.FLOAT :            "FloatField",
            Database.DECIMAL :          "DecimalField",
            Database.DATE :             "DateField",
            Database.TIME :             "TimeField",
            Database.DATETIME :         "DateTimeField",
            Database.BOOLEAN:           "BooleanField",
        }    
        if(djangoVersion[0:2] > (1, 1)):
            data_types_reverse[Database.BINARY] = "BinaryField"
            data_types_reverse[Database.BIGINT] = "BigIntegerField"
        else:
            data_types_reverse[Database.BIGINT] = "IntegerField"
    else:
        data_types_reverse = {
            zxJDBC.CHAR:                "CharField",
            zxJDBC.BIGINT:              "BigIntegerField",
            zxJDBC.BINARY:              "BinaryField",
            zxJDBC.BIT:                 "SmallIntegerField",
            zxJDBC.BLOB:                "BinaryField",
            zxJDBC.CLOB:                "TextField",
            zxJDBC.DATE:                "DateField",
            zxJDBC.DECIMAL:             "DecimalField",
            zxJDBC.DOUBLE:              "FloatField",
            zxJDBC.FLOAT:               "FloatField",
            zxJDBC.INTEGER:             "IntegerField",
            zxJDBC.LONGVARCHAR:         "TextField",
            zxJDBC.LONGVARBINARY:       "ImageField",
            zxJDBC.NUMERIC:             "DecimalField",
            zxJDBC.REAL:                "FloatField",
            zxJDBC.SMALLINT:            "SmallIntegerField",
            zxJDBC.VARCHAR:             "CharField",
            zxJDBC.TIMESTAMP:           "DateTimeField",
            zxJDBC.TIME:                "TimeField",
        }
     
    def get_field_type(self, data_type, description):
        if data_type == Database.NUMBER and not _IS_JYTHON:
            if description.precision == 5:
                return 'SmallIntegerField'
            elif description.name in ('bool_field', 'null_bool_field'):
                return 'BooleanField'
        
        if data_type == Database.TEXT and not _IS_JYTHON:
            if description.precision == 4194304:
                return 'JSONField'

        return super(DatabaseIntrospection, self).get_field_type(data_type, description)
    
    # Converting table name to lower case.
    def table_name_converter ( self, name ):        
        return name.lower()
    
    def identifier_converter(self, name):
        """
        Apply a conversion to the identifier for the purposes of comparison.

        The default identifier converter is for case sensitive comparison.
        """
        return name.lower()

    # Getting the list of all tables, which are present under current schema.
    def get_table_list ( self, cursor ):
        TableInfo = namedtuple('TableInfo', ['name', 'type'])
        table_list = []
        if not _IS_JYTHON:
            for table in cursor.connection.tables( cursor.connection.get_current_schema() ):
                if( djangoVersion[0:2] < ( 1, 8 ) ):
                    table_list.append( table['TABLE_NAME'].lower() )
                else:
                    table_list.append(TableInfo( table['TABLE_NAME'].lower(),'t' if table['TABLE_TYPE'] == 'TABLE' else 'v'))
        else:
            cursor.execute( "select current_schema from sysibm.sysdummy1" )
            schema = cursor.fetchone()[0]
            # tables(String catalog, String schemaPattern, String tableNamePattern, String[] types) gives a description of tables available in a catalog 
            cursor.tables( None, schema, None, ( "TABLE", ) )
            for table in cursor.fetchall():
                # table[2] is table name
                if( djangoVersion[0:2] < ( 1, 8 ) ):
                    table_list.append( table[2].lower() )
                else:
                    table_list.append(TableInfo(table[2].lower(),"t"))
                
        return table_list
    
    # Generating a dictionary for foreign key details, which are present under current schema.
    def get_relations( self, cursor, table_name ):
        relations = {}
        if not _IS_JYTHON:
            schema = cursor.connection.get_current_schema()
            for fk in cursor.connection.foreign_keys( True, schema, table_name ):
                relations[fk['FKCOLUMN_NAME'].lower()] = ( fk['PKCOLUMN_NAME'].lower() , fk['PKTABLE_NAME'].lower() )
        else:
            cursor.execute( "select current_schema from sysibm.sysdummy1" )
            schema = cursor.fetchone()[0]
            # foreign_keys(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable) 
            # gives a description of the foreign key columns in the foreign key table that reference the primary key columns 
            # of the primary key table (describe how one table imports another's key.) This should normally return a single foreign key/primary key pair 
            # (most tables only import a foreign key from a table once.) They are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ
            cursor.foreignkeys( None, schema, table_name, None, '%', '%' )
            for fk in cursor.fetchall():
                # fk[2] is primary key table name, fk[3] is primary key column name, fk[7] is foreign key column name being exported
                relations[self.__get_col_index( cursor, schema, table_name, fk[7] )] = ( self.__get_col_index( cursor, schema, fk[2], fk[3] ), fk[3], fk[2] )
        return relations
    
    # Private method. Getting Index position of column by its name
    def __get_col_index ( self, cursor, schema, table_name, col_name ):
        if not _IS_JYTHON:
            for col in cursor.connection.columns( schema, table_name, [col_name] ):
                return col['ORDINAL_POSITION'] - 1
        else:
            cursor.execute( "select current_schema from sysibm.sysdummy1" )
            schema = cursor.fetchone()[0]
            # columns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) gives a description of table columns available in the specified catalog
            cursor.columns( None, schema, table_name, col_name )
            for col in cursor.fetchall():
                #col[16] is index of column in table
                return col[16] - 1
    
    def get_key_columns(self, cursor, table_name):
        relations = []
        if not _IS_JYTHON:
            schema = cursor.connection.get_current_schema()
            for fk in cursor.connection.foreign_keys( True, schema, table_name ):
                relations.append( (fk['FKCOLUMN_NAME'].lower(), fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()) )
        else:
            cursor.execute( "select current_schema from sysibm.sysdummy1" )
            schema = cursor.fetchone()[0]
            # foreign_keys(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable) 
            # gives a description of the foreign key columns in the foreign key table that reference the primary key columns 
            # of the primary key table (describe how one table imports another's key.) This should normally return a single foreign key/primary key pair 
            # (most tables only import a foreign key from a table once.) They are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ
            cursor.foreignkeys( None, schema, table_name, None, '%', '%' )
            for fk in cursor.fetchall():
                # fk[2] is primary key table name, fk[3] is primary key column name, fk[7] is foreign key column name being exported
                relations.append( (fk[7], fk[2], fk[3]) )
        return relations
        
    # Getting list of indexes associated with the table provided.
    def get_indexes( self, cursor, table_name ):
        indexes = {}
        # To skip indexes across multiple fields
        multifield_indexSet = set()
        if not _IS_JYTHON:
            schema = cursor.connection.get_current_schema()
            all_indexes = cursor.connection.indexes( True, schema, table_name )
            for index in all_indexes:
                if (index['ORDINAL_POSITION'] is not None) and (index['ORDINAL_POSITION']== 2):
                    multifield_indexSet.add(index['INDEX_NAME'])
                    
            for index in all_indexes:
                temp = {}
                if index['INDEX_NAME'] in multifield_indexSet:
                    continue
                
                if ( index['NON_UNIQUE'] ):
                    temp['unique'] = False
                else:
                    temp['unique'] = True
                temp['primary_key'] = False
                indexes[index['COLUMN_NAME'].lower()] = temp
            
            for index in cursor.connection.primary_keys( True, schema, table_name ):
                indexes[index['COLUMN_NAME'].lower()]['primary_key'] = True
        else:
            cursor.execute( "select current_schema from sysibm.sysdummy1" )
            schema = cursor.fetchone()[0]
            # statistics(String catalog, String schema, String table, boolean unique, boolean approximate) returns a description of a table's indices and statistics. 
            cursor.statistics( None, schema, table_name, 0, 0 )
            all_indexes = cursor.fetchall()
            for index in all_indexes:
                #index[7] indicate ORDINAL_POSITION within index, and index[5] is index name
                if index[7] == 2:
                    multifield_indexSet.add(index[5])
                    
            for index in all_indexes:
                temp = {}
                if index[5] in multifield_indexSet:
                    continue
                
                # index[3] indicate non-uniqueness of column
                if ( index[3] != None ):
                    if ( index[3] ) == 1:
                        temp['unique'] = False
                    else:
                        temp['unique'] = True
                    temp['primary_key'] = False
                    # index[8] is column name
                    indexes[index[8].lower()] = temp
            
            # primarykeys(String catalog, String schema, String table) gives a description of a table's primary key columns
            cursor.primarykeys( None, schema, table_name )
            for index in cursor.fetchall():
                #index[3] is column name
                indexes[index[3].lower()]['primary_key'] = True
        return indexes
    
    # Getting the description of the table.
    def get_table_description( self, cursor, table_name ):
        qn = self.connection.ops.quote_name
        description = []
        table_type = 'T'

        if not _IS_JYTHON:
            dbms_name='dbms_name'
            schema = cursor.connection.get_current_schema()

            if (getattr(cursor.connection, dbms_name) == 'AS'):
                 sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='%(schema)s' AND TABLE_NAME='%(table)s'" % {'schema': schema.upper(), 'table': table_name.upper()}
            elif ( getattr(cursor.connection, dbms_name) != 'DB2'):
                 sql = "SELECT TYPE FROM SYSCAT.TABLES WHERE TABSCHEMA='%(schema)s' AND TABNAME='%(table)s'" % {'schema': schema.upper(), 'table': table_name.upper()}
            else:
                sql = "SELECT TYPE FROM SYSIBM.SYSTABLES WHERE CREATOR='%(schema)s' AND NAME='%(table)s'" % {'schema': schema.upper(), 'table': table_name.upper()}
            cursor.execute(sql)
            table_type = cursor.fetchone()
            if table_type != None:
                table_type = table_type[0]

        if table_type != 'X':
            cursor.execute( "SELECT * FROM %s FETCH FIRST 1 ROWS ONLY" % qn( table_name ) )
            return [
                FieldInfo(
                    desc[0].lower(), #name
                    desc[1], #type_code
                    desc[2], #display_size
                    desc[3], #internal_size
                    desc[4], #precision
                    desc[5], #scale
                    desc[6], #null_ok
                    None,    #default
                    None,    #collation
                    ) for desc in cursor.description ]

        return description

    def _get_foreign_key_constraints(self, cursor, table_name):
        constraints = {}      
        
        table_t = table_name.replace('""', '\"') if table_name.count("\"") > 0 else table_name
        schema = cursor.connection.get_current_schema().upper()
        for fk in cursor.connection.foreign_keys( True, schema, table_t ):
            fk['FK_NAME'] = fk['FK_NAME'].lower()
            if fk['FK_NAME'] not in constraints:
                constraints[fk['FK_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
            if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)
                
        return constraints
    
    def get_constraints(self, cursor, table_name):
        constraints = {}
        if not _IS_JYTHON:
            schema = cursor.connection.get_current_schema().upper()   
            table_name = table_name.upper()         
            dbms_name='dbms_name'

            if (getattr(cursor.connection, dbms_name) == 'AS'):
                sql = "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM QSYS2.SYSCSTCOL WHERE TABLE_SCHEMA='%(schema)s' AND TABLE_NAME='%(table)s'" % {'schema': schema, 'table': table_name}
            elif ( getattr(cursor.connection, dbms_name) != 'DB2'):
                sql = "SELECT CONSTNAME, COLNAME FROM SYSCAT.COLCHECKS WHERE TABSCHEMA='%(schema)s' AND TABNAME='%(table)s'" % {'schema': schema, 'table': table_name}
            else:
                sql = "SELECT CHECKNAME, COLNAME FROM SYSIBM.SYSCHECKDEP WHERE TBOWNER='%(schema)s' AND TBNAME='%(table)s'" % {'schema': schema, 'table': table_name}
            cursor.execute(sql)
            for constname, colname in cursor.fetchall():
                constname = constname.lower()
                if constname not in constraints:
                    constraints[constname] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': None,
                        'check': True,
                        'index': False
                    }
                constraints[constname]['columns'].append(colname.lower())
                
            if getattr(cursor.connection, dbms_name) == 'AS':
                sql = "SELECT KEYCOL.CONSTRAINT_NAME, KEYCOL.COLUMN_NAME FROM QSYS2.SYSKEYCST KEYCOL INNER JOIN QSYS2.SYSCST TABCONST ON KEYCOL.CONSTRAINT_NAME=TABCONST.CONSTRAINT_NAME WHERE TABCONST.TABLE_SCHEMA='%(schema)s' and TABCONST.TABLE_NAME='%(table)s' and TABCONST.TYPE='U'" % {'schema': schema, 'table': table_name}
            elif ( getattr(cursor.connection, dbms_name) != 'DB2'):
                sql = "SELECT KEYCOL.CONSTNAME, KEYCOL.COLNAME FROM SYSCAT.KEYCOLUSE KEYCOL INNER JOIN SYSCAT.TABCONST TABCONST ON KEYCOL.CONSTNAME=TABCONST.CONSTNAME WHERE TABCONST.TABSCHEMA='%(schema)s' and TABCONST.TABNAME='%(table)s' and TABCONST.TYPE='U'" % {'schema': schema, 'table': table_name}
            else:
                sql = "SELECT KEYCOL.CONSTNAME, KEYCOL.COLNAME FROM SYSIBM.SYSKEYCOLUSE KEYCOL INNER JOIN SYSIBM.SYSTABCONST TABCONST ON KEYCOL.CONSTNAME=TABCONST.CONSTNAME WHERE TABCONST.TBCREATOR='%(schema)s' AND TABCONST.TBNAME='%(table)s' AND TABCONST.TYPE='U'" % {'schema': schema, 'table': table_name}
            cursor.execute(sql)
            for constname, colname in cursor.fetchall():
                constname = constname.lower()
                if constname not in constraints:
                    constraints[constname] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': True,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                constraints[constname]['columns'].append(colname.lower())
            
            table_t = table_name.replace('""', '\"') if table_name.count("\"") > 0 else table_name
            for pkey in cursor.connection.primary_keys(None, schema, table_t):
                pkey['PK_NAME'] = pkey['PK_NAME'].lower()
                if pkey['PK_NAME'] not in constraints:
                    constraints[pkey['PK_NAME']] = {
                        'columns': [],
                        'primary_key': True,
                        'unique': False,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                constraints[pkey['PK_NAME']]['columns'].append(pkey['COLUMN_NAME'].lower())    
            
            for fk in cursor.connection.foreign_keys( True, schema, table_t ):
                fk['FK_NAME'] = fk['FK_NAME'].lower()
                if fk['FK_NAME'] not in constraints:
                    constraints[fk['FK_NAME']] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                        'check': False,
                        'index': False
                    }
                constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
                if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                    fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                    fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                    constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)

            sql = "SELECT ind.INDNAME as INDNAME, ind.COLNAMES as COLNAMES, ind.UNIQUERULE as UNIQUERULE, ind.INDEXTYPE as INDEXTYPE, cols.COLORDER as COLORDER from SYSCAT.INDEXES ind  JOIN syscat.indexcoluse cols on ind.indname = cols.indname and ind.indschema = cols.indschema where ind.TABNAME = '%s'" % table_name
            cursor.execute(sql)            
            for INDEX_NAME, COLUMN_NAME, UNIQUE_RULE, INDEX_TYPE, COL_ORDER in cursor.fetchall():
                INDEX_NAME = INDEX_NAME.lower()
                COLUMN_NAME = COLUMN_NAME.lower()
                if COL_ORDER == 'D':
                    COLORDER = 'DESC'
                elif COL_ORDER == 'A':
                    COLORDER = 'ASC'
                else:
                    COLORDER = 'IGN'  #IGN=Ignored
                if INDEX_NAME not in constraints:
                    constraints[INDEX_NAME] = {
                        'columns': [],
                        'primary_key': True if UNIQUE_RULE == 'P' else False,
                        'unique': True if UNIQUE_RULE == 'U' else False,
                        'foreign_key': None,
                        'check': False,
                        'index': True,
                        'type': 'idx' if INDEX_TYPE == 'REG ' else INDEX_TYPE,
                        'orders': []                        
                    }
                elif constraints[INDEX_NAME]['unique'] :
                    continue
                elif constraints[INDEX_NAME]['primary_key']:
                    continue
                if COLUMN_NAME.startswith(('+', '-')):
                    COLUMN_NAME = regex.split('[+-]+', COLUMN_NAME)
                    COLUMN_NAME.pop(0)
                constraints[INDEX_NAME]['columns'] = COLUMN_NAME
                constraints[INDEX_NAME]['orders'].append(COLORDER)

            return constraints

    def get_sequences(self,cursor, table_name,table_fields=()):
        from django.apps import apps
        from django.db import models

        seq_list=[]
        for f in table_fields:
            if(isinstance(f,models.AutoField)):
               seq_list.append({'table':table_name, 'column': f.column})
               break
        return seq_list

    def sequence_list(self):
        """
        Return a list of information about all DB sequences for all models in
        all apps.
        """
        from django.apps import apps
        from django.db import router

        sequence_list = []
        with self.connection.cursor() as cursor:
            for app_config in apps.get_app_configs():
                for model in router.get_migratable_models(app_config, self.connection.alias):
                    if not model._meta.managed:
                        continue
                    if model._meta.swapped:
                        continue
                    sequence_list.extend(self.get_sequences(cursor, model._meta.db_table, model._meta.local_fields))
                    for f in model._meta.local_many_to_many:
                        # If this is an m2m using an intermediate table,
                        # we don't need to reset the sequence.
                        if f.remote_field.through._meta.auto_created:
                            sequence = self.get_sequences(cursor, f.m2m_db_table(), f.remote_field.through._meta.concrete_fields)
                            sequence_list.extend(sequence or [{'table': f.m2m_db_table(), 'column': None}])
        return sequence_list