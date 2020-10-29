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

from django.db.models.sql import compiler
import sys

if sys.version_info >= (3, ):
    try:
        from itertools import zip_longest
    except ImportError:
        from itertools import zip_longest as zip_longest
# For checking django's version
from django import VERSION as djangoVersion

import datetime
from django.db.models.sql.query import get_order_dir
from django.db.models.sql.constants import ORDER_DIR
from django.db.models.expressions import OrderBy, Random, RawSQL, Ref
from django.utils.hashable import make_hashable
from django.db.utils import DatabaseError
FORCE = object()

class SQLCompiler( compiler.SQLCompiler ):
    __rownum = 'Z.__ROWNUM'

    def compile_order_by(self, node, select_format=False):
        template = None
        if node.nulls_last:
            template = '%(expression)s IS NULL, %(expression)s %(ordering)s'
        elif node.nulls_first:
            template = '%(expression)s IS NOT NULL, %(expression)s %(ordering)s'

        sql, params = node.as_sql(self, self.connection, template=template)
        if select_format is FORCE or (select_format and not self.query.subquery):
            return node.output_field.select_format(self, sql, params)
        return sql, params

    def get_order_by(self):
        """
        Return a list of 2-tuples of form (expr, (sql, params, is_ref)) for the
        ORDER BY clause.

        The order_by clause can alter the select clause (for example it
        can add aliases to clauses that do not yet have one, or it can
        add totally new select clauses).
        """
        if self.query.extra_order_by:
            ordering = self.query.extra_order_by
        elif not self.query.default_ordering:
            ordering = self.query.order_by
        elif self.query.order_by:
            ordering = self.query.order_by
        elif self.query.get_meta().ordering:
            ordering = self.query.get_meta().ordering
            self._meta_ordering = ordering
        else:
            ordering = []
        if self.query.standard_ordering:
            asc, desc = ORDER_DIR['ASC']
        else:
            asc, desc = ORDER_DIR['DESC']

        order_by = []
        for field in ordering:
            if hasattr(field, 'resolve_expression'):
                if not isinstance(field, OrderBy):
                    field = field.asc()
                if not self.query.standard_ordering:
                    field.reverse_ordering()
                    order_by.append((field, True))
                else:
                    order_by.append((field, False))
                continue
            if field == '?':  # random
                order_by.append((OrderBy(Random()), False))
                continue

            col, order = get_order_dir(field, asc)
            descending = order == 'DESC'

            if col in self.query.annotation_select:
                # Reference to expression in SELECT clause
                order_by.append((
                    OrderBy(Ref(col, self.query.annotation_select[col]), descending=descending),
                    True))
                continue
            if col in self.query.annotations:
                # References to an expression which is masked out of the SELECT clause
                order_by.append((
                    OrderBy(self.query.annotations[col], descending=descending),
                    False))
                continue

            if '.' in field:
                # This came in through an extra(order_by=...) addition. Pass it
                # on verbatim.
                table, col = col.split('.', 1)
                order_by.append((
                    OrderBy(
                        RawSQL('%s.%s' % (self.quote_name_unless_alias(table), col), []),
                        descending=descending
                    ), False))
                continue

            if not self.query._extra or col not in self.query._extra:
                # 'col' is of the form 'field' or 'field1__field2' or
                # '-field1__field2__field', etc.
                order_by.extend(self.find_ordering_name(
                    field, self.query.get_meta(), default_order=asc))
            else:
                if col not in self.query.extra_select:
                    order_by.append((
                        OrderBy(RawSQL(*self.query.extra[col]), descending=descending),
                        False))
                else:
                    order_by.append((
                        OrderBy(Ref(col, RawSQL(*self.query.extra[col])), descending=descending),
                        True))
        result = []
        seen = set()

        for expr, is_ref in order_by:
            resolved = expr.resolve_expression(self.query, allow_joins=True, reuse=None)
            if self.query.combinator:
                src = resolved.get_source_expressions()[0]
                # Relabel order by columns to raw numbers if this is a combined
                # query; necessary since the columns can't be referenced by the
                # fully qualified name and the simple column names may collide.
                for idx, (sel_expr, _, col_alias) in enumerate(self.select):
                    if is_ref and col_alias == src.refs:
                        src = src.source
                    elif col_alias:
                        continue
                    if src == sel_expr:
                        resolved.set_source_expressions([RawSQL('%d' % (idx + 1), ())])
                        break
                else:
                    raise DatabaseError('ORDER BY term does not match any column in the result set.')
            sql, params = self.compile_order_by(resolved)
            # Don't add the same column twice, but the order direction is
            # not taken into account so we strip it. When this entire method
            # is refactored into expressions, then we can check each part as we
            # generate it.
            without_ordering = self.ordering_parts.search(sql).group(1)
            params_hash = make_hashable(params)
            if (without_ordering, params_hash) in seen:
                continue
            seen.add((without_ordering, params_hash))
            result.append((resolved, (sql, params, is_ref)))
        return result


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
            if self.query.low_mark is 0:
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
            
            if self.query.low_mark is not 0:
                sql = '%s "%s" > %d' % ( sql, self.__rownum, self.query.low_mark )
                
            if self.query.low_mark is not 0 and self.query.high_mark is not None:
                sql = '%s AND ' % ( sql )

            if self.query.high_mark is not None:
                sql = '%s "%s" <= %d' % ( sql, self.__rownum, self.query.high_mark )

        return sql, params

    def pre_sql_setup(self):
        """
        Do any necessary class setup immediately prior to producing SQL. This
        is for things that can't necessarily be done in __init__ because we
        might not have all the pieces in place at that time.
        """
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
