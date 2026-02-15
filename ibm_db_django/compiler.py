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

from django.db.models.sql import compiler
import sys
from django.db.models.functions.json import JSONObject

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

from django.db.models.fields.tuple_lookups import TupleIn, TupleExact, Tuple
from django.db.models.expressions import Subquery, ColPairs, Col
from django.db.models.sql.query import Query

from django.db.models.functions import Log, Ln
from django.db.models.expressions import ExpressionWrapper, F
from django.db.models import FloatField
from django.db.models.functions import MD5
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

    def handle_tuple_in(self, node):
        """
        Convert TupleIn with ColPairs or Tuple(lhs) into DB2-compatible OR clauses.
        Handles both literal RHS lists and subqueries, plus PK/FK heuristics.
        """
        alias = getattr(node.lhs, 'alias', None)
        fields = getattr(node.lhs, 'targets', None)  # ColPairs case
        values = node.rhs
    
        # Heuristic: check if LHS fields look like PK or FK
        def looks_like_pk_or_fk(field):
            col = getattr(field, "attname", "") or getattr(field, "column", "")
            return col == "id" or col.endswith("_id")
    
        all_fields_look_like_keys = bool(fields) and all(looks_like_pk_or_fk(f) for f in fields)
    
        # Compile LHS into parts
        def compile_lhs_parts():
            if fields and alias:
                # ColPairs case
                parts_sql = [
                    f"{self.quote_name_unless_alias(alias)}."
                    f"{self.connection.ops.quote_name(field.column)}"
                    for field in fields
                ]
                return parts_sql, []
            # Generic Tuple(lhs) case
            source_exprs = getattr(node.lhs, 'source_expressions', None)
            if source_exprs:
                parts_sql, parts_params = [], []
                for expr in source_exprs:
                    s, p = self.compile(expr)
                    parts_sql.append(s)
                    parts_params.extend(p)
                return parts_sql, parts_params
            # Fallback: treat lhs as single col
            s, p = self.compile(node.lhs)
            return [s], p
    
        # 1) Empty RHS → always false (special case: PK/FK fields)
        if isinstance(values, (list, tuple)) and not values:
            return "1=0", []
    
        # 2) Subquery RHS: keep tuple form
        if isinstance(values, (Subquery, Query)):
            lhs_parts_sql, lhs_parts_params = compile_lhs_parts()
            lhs_sql = "(" + ", ".join(lhs_parts_sql) + ")"
            subquery_sql, subquery_params = self.compile(values)
            return f"{lhs_sql} IN ({subquery_sql})", lhs_parts_params + list(subquery_params)
    
        # 3) Literal list/tuple RHS
        if isinstance(values, (list, tuple)):
            # Filter out tuples containing NULL — DB2 can't match NULL with '='
            valid_values = [tup for tup in values if not (isinstance(tup, (list, tuple)) and None in tup)]
            if not valid_values:
                # All tuples have NULL and these look like pk/fk columns
                if all_fields_look_like_keys:
                    sql, params = node.as_sql(self, self.connection)
                    return sql, params
                else:
                    return "1=0", []  # Still safe default
    
            lhs_parts_sql, lhs_parts_params = compile_lhs_parts()
            arity = len(lhs_parts_sql)
    
            # Build OR-of-AND comparisons
            conditions = []
            params = []
            for val_tuple in valid_values:
                if not isinstance(val_tuple, (list, tuple)):
                    val_tuple = (val_tuple,)
                if len(val_tuple) != arity:
                    return "1=0", []
                sub_cond = " AND ".join(f"{lhs_parts_sql[i]} = %s" for i in range(arity))
                conditions.append("(" + sub_cond + ")")
                params.extend(val_tuple)
    
            sql = "(" + " OR ".join(conditions) + ")"
            return sql, lhs_parts_params + params
    
        # 4) Unsupported RHS
        return "1=0", []

    def handle_tuple_exact_subquery(self, node):
        alias = node.lhs.alias
        fields = node.lhs.targets
        subquery = node.rhs
    
        if not isinstance(subquery, (Subquery, Query)):
            raise TypeError(f"TupleExact: unsupported rhs type {type(subquery)}")
    
        q = subquery.query if isinstance(subquery, Subquery) else subquery
        select_list = q.select
    
        # Expecting a single ColPairs object
        if len(select_list) != 1 or not hasattr(select_list[0], 'targets'):
            raise ValueError("TupleExact: expected single ColPairs in subquery SELECT")
    
        rhs_expressions = select_list[0].targets  # This gives list of Col()s
    
        if len(rhs_expressions) != len(fields):
            raise ValueError(
                f"TupleExact: subquery returns {len(rhs_expressions)} columns, "
                f"but {len(fields)} fields are being compared."
            )
    
        conditions = []
        params = []
    
        for field, rhs_field in zip(fields, rhs_expressions):
            # LHS: compile the local field
            lhs_expr = Col(alias, field)
            lhs_sql, lhs_params = self.compile(lhs_expr)
        
            # RHS: build a Col pointing to the subquery's alias and field
            rhs_alias = q.alias_map.keys().__iter__().__next__()  # usually 'U0'
            rhs_col_expr = Col(rhs_alias, rhs_field)
        
            # Create a new subquery selecting only the right column
            q_clone = q.clone()
            q_clone.select = (rhs_col_expr,)
            q_clone.select_related = False
            q_clone.group_by = None
            q_clone.order_by = ()
            q_clone.low_mark = 0
            q_clone.high_mark = 1
        
            subq_sql, subq_params = self.compile(q_clone)
        
            conditions.append(f"{lhs_sql} = ({subq_sql})")
            params.extend(lhs_params)
            params.extend(subq_params)
    
        return " AND ".join(conditions), params


    def compile(self, node):
        if isinstance(node, Log):
            # Rewrite LOG(x, base) -> LN(x) / LN(base)
            base = node.source_expressions[0]
            lhs = node.source_expressions[1]
    
            ln_x = Ln(lhs)
            ln_base = Ln(base)
            expr = ExpressionWrapper(ln_x / ln_base, output_field=FloatField())
            return self.compile(expr)
    
        if isinstance(node, MD5):
            sql, params = self.compile(node.source_expressions[0])
            return f'HEX(HASH_SHA256({sql}))', params

        # Handle TupleIn: rewrite (col1, col2) IN ((1,1),(1,2)) into DB2-safe OR conditions
        if isinstance(node, TupleIn):
            return self.handle_tuple_in(node)
    
        if isinstance(node, TupleExact) and isinstance(node.rhs, Query):
            return self.handle_tuple_exact_subquery(node)
    
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

    def _move_for_update_sql_to_end(self, sql):
        if sql.find(' WITH RS USE AND KEEP UPDATE LOCKS') != -1:
            sql = sql.replace(' WITH RS USE AND KEEP UPDATE LOCKS','')
            sql = sql + (' WITH RS USE AND KEEP UPDATE LOCKS')
        if sql.count( "%%s" ) > 0:
            sql = sql.replace("%%s", "%s")
        return sql
    
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
        
        if self.connection.supports_limit_offset:
            # IBM DB2 version 11.1 supports natively LIMIT/OFFSET (see #112), 
            # thus no need for special logic -> use standard django sql construction logic 
            sql, params = super( SQLCompiler, self ).as_sql( with_limits=with_limits, with_col_aliases=with_col_aliases )
            sql = self._move_for_update_sql_to_end(sql)
        elif not ( with_limits and ( self.query.high_mark is not None or self.query.low_mark ) ):
            sql, params = super( SQLCompiler, self ).as_sql( with_limits=False, with_col_aliases=with_col_aliases )
            sql = self._move_for_update_sql_to_end(sql)            
            return sql, params
        else:
            sql_ori, params = super( SQLCompiler, self ).as_sql( with_limits=False, with_col_aliases=with_col_aliases )
            sql_ori = self._move_for_update_sql_to_end(sql_ori)            
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
    def prepare_value(self, field, value):
        if value is None and not field.null and field.default is None:
            # Field is NOT NULL, no default in Python, but we got None → must fallback to DB2 DEFAULT
            return self.connection.ops.insert_default_sql()
        return super().prepare_value(field, value)

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
