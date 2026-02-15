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
Custom Query class for DB2.
Derives from: django.db.models.sql.query.Query
"""

def query_class( QueryClass ):
    
    class DB2QueryClass( QueryClass ):
        __rownum = 'Z.__ROWNUM'
        
        # To get ride of LIMIT/OFFSET problem in DB2, this method has been implemented.
        def as_sql( self, with_limits = True, with_col_aliases = False ):
            if not ( with_limits and ( self.high_mark is not None or self.low_mark ) ):
                return super( DB2QueryClass, self ).as_sql( False, with_col_aliases )
            else:
                if self.high_mark == self.low_mark:
                    return '', ()
                
                sql_ori, params = super( DB2QueryClass, self ).as_sql( False, with_col_aliases )
                sql_split = sql_ori.split( " FROM " )
                
                sql_sec = ""
                if len( sql_split ) > 2:
                    for i in range( 1, len( sql_split ) ):
                        sql_sec = " %s FROM %s " % ( sql_sec, sql_split[i] )
                else:
                    sql_sec = " FROM %s " % ( sql_split[1] )
                
                dummyVal = "Z.__db2_"
                sql_pri = ""
                
                sql_sel = "SELECT"
                if self.distinct:
                    sql_sel = "SELECT DISTINCT"

                sql_select_token = sql_split[0].split( "," )
                
                # rejoin items that use comma in a db function
                new_sql_select_token = []
                paren_count = 0
                column_fragment = None
                for column in sql_select_token:
                    paren_count += column.count('(') - column.count(')')
                    if paren_count > 0:
                        if column_fragment:
                            column_fragment = ', '.join([column_fragment, column])
                        else:
                            column_fragment = column
                    elif paren_count == 0 and column_fragment:
                        new_sql_select_token.append(', '.join([column_fragment, column]))
                        column_fragment = None
                    else:
                        new_sql_select_token.append(column)
                sql_select_token = new_sql_select_token
                
                i = 0
                while ( i < len( sql_select_token ) ):
                    if sql_select_token[i].count( "TIMESTAMP(DATE(SUBSTR(CHAR(" ) == 1:
                        sql_sel = "%s \"%s%d\"," % ( sql_sel, dummyVal, i + 1 )
                        sql_pri = '%s %s,%s,%s,%s AS "%s%d",' % ( 
                                        sql_pri,
                                        sql_select_token[i],
                                        sql_select_token[i + 1],
                                        sql_select_token[i + 2],
                                        sql_select_token[i + 3],
                                        dummyVal, i + 1 )
                        i = i + 4
                        continue
                                        
                    token = sql_select_token[i]

                    # 1) Quoted alias first: ... AS "alias"
                    if ' AS "' in token:
                        expr, alias_part = token.rsplit(' AS "', 1)
                        alias = alias_part.strip()
                        if alias.endswith('"'):
                            alias = alias[:-1].strip()
                        if alias:
                            sql_pri = '%s %s,' % (sql_pri, token)
                            sql_sel = '%s "%s",' % (sql_sel, alias)
                            i = i + 1
                            continue
                    
                    # 2) Unquoted alias fallback: ... AS alias                
                    if " AS " in token:
                        expr, alias = token.rsplit(" AS ", 1)
                        alias = alias.strip()
                        if alias and not "(" in alias:
                            sql_pri = '%s %s,' % (sql_pri, token)
                            sql_sel = "%s %s," % (sql_sel, alias)
                            i = i + 1
                            continue
                
                    sql_pri = '%s %s AS "%s%d",' % ( sql_pri, sql_select_token[i], dummyVal, i + 1 )
                    sql_sel = "%s \"%s%d\"," % ( sql_sel, dummyVal, i + 1 )
                    i = i + 1

                sql_pri = sql_pri[:len( sql_pri ) - 1]
                sql_pri = "%s%s" % ( sql_pri, sql_sec )
                sql_sel = sql_sel[:len( sql_sel ) - 1]
                sql = '%s, ( ROW_NUMBER() OVER() ) AS "%s" FROM ( %s ) AS M' % ( sql_sel, self.__rownum, sql_pri )
                sql = '%s FROM ( %s ) Z WHERE' % ( sql_sel, sql )
                
                if self.low_mark != 0:
                    sql = '%s "%s" > %d' % ( sql, self.__rownum, self.low_mark )
                    
                if self.low_mark != 0 and self.high_mark is not None:
                    sql = '%s AND ' % ( sql )

                if self.high_mark is not None:
                    sql = '%s "%s" <= %d' % ( sql, self.__rownum, self.high_mark )

            return sql, params
            
        # http://www.python.org/dev/peps/pep-0307/
        # See Extended __reduce__ API
        def __reduce__( self ):
            return ( __newobj__, ( QueryClass, ) )
        
        # For case insensitive search, converting parameter value to upper case.
        # The right hand side will get converted to upper case in the SQL itself.
        from django.db.models.sql.where import AND
        def add_filter( self, filter_expr, connector = AND, negate = False, trim = False,
                       can_reuse = None, process_extras = True ):
            if len( filter_expr ) != 0 and filter_expr is not None:
                filter_expr = list( filter_expr )
                if filter_expr[0].find( "__iendswith" ) != -1 or \
                    filter_expr[0].find( "__istartswith" ) != -1 or \
                    filter_expr[0].find( "__icontains" ) != -1 or \
                    filter_expr[0].find( "__iexact" ) != -1:
                    filter_expr[1] = filter_expr[1].upper()
                    
                filter_expr = tuple( filter_expr )                    
            return super( DB2QueryClass, self ).add_filter( filter_expr, connector,
                        negate, trim, can_reuse, process_extras )
            
    return DB2QueryClass

# Method to make DB2QueryClass picklable
def __newobj__( QueryClass ):
    # http://www.python.org/dev/peps/pep-0307/
    # The __newobj__ unpickling function
    DB2QueryClass = query_class( QueryClass )
    return DB2QueryClass.__new__( DB2QueryClass )
