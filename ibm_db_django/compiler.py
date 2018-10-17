# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2018.                                      |
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

from django.db.models.sql import compiler
import sys
if sys.version_info >= (3, ):
    try:
        from itertools import zip_longest
    except ImportError:
        from itertools import izip_longest as zip_longest
# For checking django's version
from django import VERSION as djangoVersion


class SQLCompiler( compiler.SQLCompiler ):

    def __map23(self, value, field):
        if sys.version_info >= (3, ):
            return zip_longest(value, field)
        else:
            return map(None, value, field)
        
    #This function  convert 0/1 to boolean type for BooleanField/NullBooleanField
    def resolve_columns( self, row, fields = () ):
        values = []
        index_extra_select = len( self.query.extra_select.keys() )
        for value, field in self.__map23( row[index_extra_select:], fields ):
            if ( field and field.get_internal_type() in ( "BooleanField", "NullBooleanField" ) and value in ( 0, 1 ) ):
                value = bool( value )
            values.append( value )
        return row[:index_extra_select] + tuple( values )
    
    # For case insensitive search, converting parameter value to upper case.
    # The right hand side will get converted to upper case in the SQL itself.
    def __do_filter( self, children ):
        for index in range( len( children ) ):
            if not isinstance( children[index], ( tuple, list ) ):
                if hasattr( children[index], 'children' ):
                    self.__do_filter( children[index].children )
            elif isinstance( children[index], tuple ):
                node = list( children[index] )
                if node[1].find( "iexact" ) != -1 or \
                    node[1].find( "icontains" ) != -1 or \
                    node[1].find( "istartswith" ) != -1 or \
                    node[1].find( "iendswith" ) != -1:
                    if node[2] == True:
                        node[3] = node[3].upper()
                        children[index] = tuple( node )

class SQLInsertCompiler( compiler.SQLInsertCompiler, SQLCompiler ):
    pass

class SQLDeleteCompiler( compiler.SQLDeleteCompiler, SQLCompiler ):
    pass

class SQLUpdateCompiler( compiler.SQLUpdateCompiler, SQLCompiler ):
    pass

class SQLAggregateCompiler( compiler.SQLAggregateCompiler, SQLCompiler ):
    pass

if djangoVersion[0:2] < ( 1, 8 ):
    class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
        pass

if djangoVersion[0:2] >= ( 1, 6 ) and djangoVersion[0:2] < ( 1, 8 ):
    class SQLDateTimeCompiler(compiler.SQLDateTimeCompiler, SQLCompiler):
        pass
