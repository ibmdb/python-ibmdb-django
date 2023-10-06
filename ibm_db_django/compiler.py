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

from django.db.models.sql import compiler
import sys
from django.db.models.functions.comparison import JSONObject

if sys.version_info >= (3, ):
    try:
        from itertools import zip_longest
    except ImportError:
        from itertools import zip_longest as zip_longest
# For checking django's version
from django import VERSION as djangoVersion

import datetime
import math
from django.db.models.sql.query import get_order_dir
from django.db.models.sql.constants import ORDER_DIR
from django.db.models.expressions import OrderBy, RawSQL, Ref, Value, F, Func
from django.utils.hashable import make_hashable
from django.db.utils import DatabaseError
from django.db.models.functions import Cast, Random, Pi
from django import VERSION as djangoVersion

FORCE = object()

class PiDB2(Pi):
    def as_sql(self, compiler, connection, **extra_context):
        return super().as_sql(
            compiler, connection, template=str(math.pi), **extra_context
        )

class FuncDB2(Func):
    def __init__(self, *expressions, output_field=None, **extra):
        super().__init__(output_field=output_field)

    def as_sql(self, compiler, connection, function=None, template=None, arg_joiner=None, **extra_context):
        connection.ops.check_expression_support(self)
        sql_parts = []
        params = []
        for arg in self.source_expressions:
            arg_sql, arg_params = compiler.compile(arg)
            sql_parts.append(arg_sql)
            params.extend(arg_params)
        data = {**self.extra, **extra_context}
        # Use the first supplied value in this order: the parameter to this
        # method, a value supplied in __init__()'s **extra (the value in
        # `data`), or the value defined on the class.
        if function is not None:
            data['function'] = function
        else:
            data.setdefault('function', self.function)
        template = template or data.get('template', self.template)
        arg_joiner = arg_joiner or data.get('arg_joiner', self.arg_joiner)
        arg_sql = ""
        for i in range(0, len(sql_parts), 2):
            if i > 0:
                arg_sql += ','
            arg_sql += " KEY '%s' VALUE %s " % (sql_parts[i], sql_parts[i+1])
        data['expressions'] = data['field'] = arg_sql
        sql = template % data
        return sql % tuple(params), []

class SQLCompiler( compiler.SQLCompiler ):
    __rownum = 'Z.__ROWNUM'

    def compile(self, node):
        vendor_impl = getattr(node, 'as_' + self.connection.vendor, None)
        if vendor_impl:
            sql, params = vendor_impl(self, self.connection)
        else:
            if isinstance(node, JSONObject):
                funcDb2 = FuncDB2()
                funcDb2.function = node.function
                funcDb2.source_expressions = node.source_expressions
                sql, params = funcDb2.as_sql(self, self.connection )
            elif isinstance(node, Pi):
                piDb2 = PiDB2()
                piDb2.function = node.function
                piDb2.source_expressions = node.source_expressions
                sql, params = piDb2.as_sql(self, self.connection )
            else:
                sql, params = node.as_sql(self, self.connection)
        return sql, params

    # To get ride of LIMIT/OFFSET problem in DB2, this method has been implemented.
    def as_sql( self, with_limits=True, with_col_aliases=False, subquery=False ):
        self.subquery = subquery
        self.__do_filter( self.query.where.children )

        if self.query.distinct:
            if ((list(self.connection.settings_dict.keys())).__contains__('FETCH_DISTINCT_ON_TEXT')) and not self.connection.settings_dict['FETCH_DISTINCT_ON_TEXT']:
                out_cols = self.get_columns(False)
                for col in out_cols:
                    col = col.split(".")[1].replace('"', '').lower()
                    field = self.query.model._meta.get_field_by_name(col)[0]
                    fieldType = field.get_internal_type()
                    if fieldType == 'TextField':
                        self.query.distinct = False
                        break
        if not ( with_limits and ( self.query.high_mark is not None or self.query.low_mark ) ):
            sql, params = super( SQLCompiler, self ).as_sql( False, with_col_aliases )
            if sql.find(' WITH RS USE AND KEEP UPDATE LOCKS') != -1:
                sql = sql.replace(' WITH RS USE AND KEEP UPDATE LOCKS','')
                sql = sql + (' WITH RS USE AND KEEP UPDATE LOCKS')
            return sql, params
        else:
            sql_ori, params = super( SQLCompiler, self ).as_sql( False, with_col_aliases )
            if sql_ori.find(' WITH RS USE AND KEEP UPDATE LOCKS') != -1:
                sql_ori = sql_ori.replace(' WITH RS USE AND KEEP UPDATE LOCKS','')
                sql_ori = sql_ori + (' WITH RS USE AND KEEP UPDATE LOCKS')
            if sql_ori.count( "%%s" ) > 0:
                sql_ori = sql_ori.replace("%%s", "%s")
            if self.query.low_mark == 0:
                return sql_ori + " FETCH FIRST %s ROWS ONLY" % ( self.query.high_mark ), params
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
            if self.query.distinct:
                sql_sel = "SELECT DISTINCT"

            sql_select_token = sql_split[0].split( "," )
            i = 0
            first_field_no = 0
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

                if sql_select_token[i].count( " AS " ) == 1:
                    temp_col_alias = sql_select_token[i].split( " AS " )
                    sql_pri = '%s %s,' % ( sql_pri, sql_select_token[i] )
                    sql_sel = "%s %s," % ( sql_sel, temp_col_alias[1] )
                    i = i + 1
                    continue

                sql_pri = '%s %s AS "%s%d",' % ( sql_pri, sql_select_token[i], dummyVal, i + 1 )
                sql_sel = "%s \"%s%d\"," % ( sql_sel, dummyVal, i + 1 )
                i = i + 1
                if first_field_no == 0:
                    first_field_no = i

            sql_pri = sql_pri[:len( sql_pri ) - 1]
            sql_pri = "%s%s" % ( sql_pri, sql_sec )
            sql_sel = sql_sel[:len( sql_sel ) - 1]
            if sql_pri.endswith("DESC ") or sql_pri.endswith("ASC ") or sql_pri.endswith("WITH RS USE AND KEEP UPDATE LOCKS "):
                sql = '%s, ( ROW_NUMBER() OVER() ) AS "%s" FROM ( %s ) AS M' % ( sql_sel, self.__rownum, sql_pri )
            else:
                sql_field = "\"%s%d\"" % (dummyVal, first_field_no)
                sql = '%s, ( ROW_NUMBER() OVER() ) AS "%s" FROM ( %s ORDER BY %s ASC ) AS M' % ( sql_sel, self.__rownum, sql_pri, sql_field)
            sql = '%s FROM ( %s ) Z WHERE' % ( sql_sel, sql )

            if self.query.low_mark != 0:
                sql = '%s "%s" > %d' % ( sql, self.__rownum, self.query.low_mark )

            if self.query.low_mark != 0 and self.query.high_mark is not None:
                sql = '%s AND ' % ( sql )

            if self.query.high_mark is not None:
                sql = '%s "%s" <= %d' % ( sql, self.__rownum, self.query.high_mark )

        return sql, params

    def pre_sql_setup(self, with_col_aliases=False):
        """
        Do any necessary class setup immediately prior to producing SQL. This
        is for things that can't necessarily be done in __init__ because we
        might not have all the pieces in place at that time.
        """

        # In Django 4.2, pre_sql_setup() sprouted an optional
        # parameter, which should probably be passed along to its
        # super class. Check the Django version here to maintain
        # backwards compatibility with older Django versions
        #
        # See: https://github.com/django/django/commit/8c3046daade8d9b019928f96e53629b03060fe73
        if djangoVersion[0:2] >= (4, 2):
            extra_select, order_by, group_by = super().pre_sql_setup(with_col_aliases=with_col_aliases)
        else:
            extra_select, order_by, group_by = super().pre_sql_setup()

        if group_by:
            group_by_list = []
            for (sql, params) in group_by:
                group_by_list.append([sql, params])

            group_by = []
            found_positional_param = False
            for (sql, params) in group_by_list:
                if (sql.count("%s") > 0) and params:

                    for parm in params:
                        if(isinstance(parm, memoryview)):
                            replace_string = "BX\'%s\'" % parm.obj.hex()
                        else:
                            replace_string = parm

                        if((isinstance(parm, str) and
                           (parm.find('DATE') == -1) and
                           (parm.find('TIMESTAMP') == -1)) or
                            (isinstance(parm, datetime.date))):
                            replace_string = "'%s'" % replace_string
                        else:
                            replace_string = str(replace_string)

                        sql = sql.replace("%s", replace_string, 1)

                    #sql = sql % tuple(params)
                    params = []
                    found_positional_param = True
                group_by.append((sql, params))

            if found_positional_param:
                self.select = self.get_updated_select(self.select)

        return extra_select, order_by, group_by

    def get_updated_select(self, select):
        """
        Return three values:
        - a list of 3-tuples of (expression, (sql, params), alias)

        The (sql, params) is what the expression will produce, and alias is the
        "AS alias" for the column (possibly None).
        """

        ret = []
        for col, (sql, params), alias in select:
            #Db2 doesnt accept positional parameters in Group By clause.
            if (sql.count("%s") > 0) and params:

                for parm in params:
                    if(isinstance(parm, memoryview)):
                        replace_string = "BX\'%s\'" % parm.obj.hex()
                    else:
                        replace_string = parm

                    if((isinstance(parm, str) and
                       (parm.find('DATE') == -1) and
                       (parm.find('TIMESTAMP') == -1)) or
                        (isinstance(parm, datetime.date))):
                        replace_string = "'%s'" % replace_string
                    else:
                        replace_string = str(replace_string)
                    sql = sql.replace("%s", replace_string, 1)

                params = []
            ret.append((col, (sql, params), alias))
        return ret

    def __map23(self, value, field):
        if sys.version_info >= (3, ):
            return zip_longest(value, field)
        else:
            return map(None, value, field)

    #This function  convert 0/1 to boolean type for BooleanField/NullBooleanField
    def resolve_columns( self, row, fields = () ):
        values = []
        index_extra_select = len( list(self.query.extra_select.keys()) )
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
