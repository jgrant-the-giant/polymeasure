from __future__ import annotations
from inspect import signature
from types import FunctionType
from typing import Any, List, Union
from copy import copy
import sqlparse


def make_as_list(maybe_vector: Any, filter_out_null=True, keep_none=False):
    if maybe_vector is None and not keep_none:
        maybe_null_vector = []
    elif maybe_vector is None and keep_none:
        return None
    elif isinstance(maybe_vector, str) and len(maybe_vector) == 0:
        maybe_null_vector = []
    elif isinstance(maybe_vector, list):
        maybe_null_vector = maybe_vector
    else:
        maybe_null_vector = [maybe_vector]
    return [x for x in maybe_null_vector if x is not None or not filter_out_null]


def make_statement(prefix, elements, separator=', \n', brackets=False, if_blank=''):
    """
    Returns a string expression like "prefix a1, a2, a3" with optional prefix, optional brackets, etc
    Args:
        prefix: prefix like select, where, etc)
        elements: things to concatenate together
        separator: concatenation separator
        brackets: whether to encase the whole thing in brackets
        if_blank: expression to use if there are no elements passed
    Returns:
    String expression
    """

    elements = make_as_list(elements)
    elements = [e for e in elements if e is not None]
    if len(elements) == 0:
        return if_blank

    encapsulator = ('(', ')') if brackets else ('', '')
    prefix_snippet = '' if prefix is None or len(prefix) == 0 else f"{prefix} "
    return f"""{encapsulator[0]}{prefix_snippet}{separator.join(elements)}{encapsulator[1]}"""


def return_if_elemental(measure_object):
    if isinstance(measure_object, str):
        return measure_object
    elif isinstance(measure_object, PolyMeasure):
        return None
    elif isinstance(measure_object, FunctionType):
        return measure_object
    elif isinstance(measure_object, tuple):
        return False
    elif isinstance(measure_object, list):
        return False
    elif measure_object is None:
        # No outer measurements supplied, just a dimensional groupby
        return None
    else:
        raise Exception


class FilterExpression:
    def __init__(self, expression, lineage=None, source=None, auxiliary=None):
        """
        Represents a filter action, expression, (optional) on columns with lineage over a source.
        Supplying lineage allows the evaluator to identify and alias column names.
        Dimensional joins create a filter expression with an expression function, producing something like:

        inner_alias.column_name {comparator} outer_alias.column_name

        The outer_alias and the column name are fixed, and a source is assigned. If evaluation occurs over a matching
        source inner, then the filter expression becomes active and is passed inner_alias to complete the expression.
        Args:
            expression: String expression like "qcategory = 'BPA'" or a function that admits positional arguments
            matching the source and auxiliary measures, and returns a SQL expression string. The resulting object has an expression method.
            lineage: String or list of strings identifying the columns
            source: Primitive or measure
            auxiliary: List of auxiliary measures that also participate in the filter
        """

        # Ensure that the expression is a function of one or more arguments
        if isinstance(expression, str):
            self.expression = lambda inner: expression
        elif isinstance(expression, FunctionType) or callable(expression):
            self.expression = expression
        else:
            raise Exception

        # Retrieve the parameter list and check that it matches source + auxiliary in length
        self.expression_parameter_list = list(signature(self.expression).parameters.keys())
        assert len(self.expression_parameter_list) == 1 + len(auxiliary)

        # At the moment lineage is a set of column names targeted by the expression.
        # In the future, column name + source + database directory would give more information
        # about which joins can be inferred for other measures
        self.lineage = make_as_list(lineage)
        self.auxiliary = make_as_list(auxiliary)
        self.source = source
        self.source_name = source if isinstance(source, str) else source.name
        self.evaluation_contexts = []

    def test_keep_filter(self, include, exclude, source=None):
        """
        Check to see if the filter is still relevant based on the supplied include / exclude
        and source checks.

        If a filter has no lineage, it won't interact with include/exclude
        If a filter has no source, it won't be passed to subsequent queries.
        If a filter has a source, and source is not None, then the filter will be excluded unless
        the provided source is found in self.source list.

        Args:
            include: list of strings
            exclude: list of strings

        Returns: boolean

        """
        if include is None:
            include_all = True
            include = []
        else:
            include_all = False

        if self.source is not None and source is not None and not source in self.source:
            return False

        if len(self.lineage) == 0 and len(self.group_lineage) == 0:
            return True

        for column in self.lineage + self.group_lineage:
            if column not in exclude and (column in include or include_all):
                return True

        return False

    def fix_outer(self, view_alias):
        # Fixes the outer component of a filter expression,
        # if it has one, and returns the result as a new FilterExpression
        # This allows the filter object to encode the correct alias / sub query level through the process of evaluation.
        if self.two_parameter_join:
            def new_expression_function(inner_alias):
                return self.expression(inner_alias, view_alias)

            new_filter = FilterExpression(
                new_expression_function,
                self.lineage,
                self.source,
                self.group_lineage
            )
            return new_filter
        else:
            return self


class Dimension:
    """
    Rows is a special dimension which returns (*) from the view.
    Rows measures should be row context primitive expressions.
    The star keyword argument should be a single string star expression
    """

    def __init__(self, dimensions=None, rowset=False, star=None):
        """
        Represents a set of dimensions to group an inner query.
        Can be any granularity from [] to infinity, or None which defaults to the parent measures dimensions.
        Args:
            groupby: list column names or None
            rowset: list column names or None
            star: A single string star expression or None
        """
        self.dimensions = make_as_list(dimensions, keep_none=True)
        self.rowset = rowset

        # Wildcard dimensions take on
        if dimensions is None and not rowset:
            self.wildcard = True
        else:
            self.wildcard = False

        if star == True:
            # default to star
            self.star = "*"
        elif star is None:
            self.star = False
        else:
            self.star = star

        if self.star:
            self.rowset = True



    def check_null(self):
        return (self.dimensions is None or len(self.dimensions) == 0) and not self.rowset

    def absorb_new_dimension(self, new_dimension):
        # Absorb a new dimension and return the results as the union of their effects

        if new_dimension.dimensions is None:
            expanded_dimensions = self.dimensions
        elif self.dimensions is None:
            expanded_dimensions = new_dimension.dimensions
        else:
            expanded_dimensions = \
                self.dimensions + list(set(new_dimension.dimensions) - set(self.dimensions))
        rowset = self.rowset or new_dimension.rowset
        star = self.star if self.star is not None else new_dimension.star
        return Dimension(
            dimensions=expanded_dimensions,
            rowset=rowset,
            star=star
        )


class Rows(Dimension):

    def __init__(self, dimensions=None, star=None):
        """
        Represents a rowset-level (infinity) granularity.
        A Rows object also specifies a number of columns to return, or a star expression to evaluate.
        No group by operation will be performed in that evaluation.
        Args:
            *args: Columns to return
            rows: Columns to return (defaults to args)
            star: Star string expression
        """

        if star is not None:
            dimensions = None

        # Null dimension defaults to star expression
        if dimensions is None:
            star = True

        super(Rows, self).__init__(dimensions=dimensions, rowset=True, star=star)


class EvaluateContext:

    def __init__(self, input_where, prior_context=None):
        """
        :param input_where: List of FilterExpression objects being passed into the context
        :param prior_context: Optional connection to the previous evaluation context
        """

        """
    Collects the ViewContexts of a chain of inner measure evaluations, ending with a single primitive outer measure.
    The most general sequence of evaluations is, in pseudo-SQL:

    ====================================================================

    { with ( auxiliary SQL table i << orphan ) aux_i }...i

    [ with ( grouping SQL table << orphan, aux_i ) _grouping ]

    select dim,

    {
        (
            { with ( auxiliary SQL table j << orphan, aux_i, grouping ) g_aux_j }...j

            [ with ( target SQL table T << orphan, aux_i, grouping, aux_j ) target ]

            select ( primitive_outer_k << orphan, aux_i, grouping, aux_j )
            from target
        )
    }...k

    from _grouping
    group by dim

    ====================================================================

    A few components are optional and or repeatable - auxiliary tables exist to provide filters to the target query, as
    do grouping tables, and these filter each other in sequence. There are special cases for evaluation when there is
    no grouping or primitive stacks (but not both of these situations simultaneously.
    """

        # The input primitive stack yields the following information:
        """
        1) The incoming / existing where filters including potentially orphaned filters and dimensions
        2) A hierarchy of sub queries and their corresponding EvaluateContexts
        """

        # Start with context containing only the incoming evaluation filters
        # For record keeping
        self.input_where: List[FilterExpression] = input_where
        self.where: List[FilterExpression] = input_where.copy()

        self.prior_context: Union[None, PolyMeasure] = prior_context

        # The context will process into one of two distinct types:

        # 1) a primitive context just containing a single primitive outer measure with no grouping
        self.outer_primitive: Union[str, FunctionType] = None

        # 2) a complex context containing zero or more outer contexts and zero or more dimensions
        self.outer_contexts: List[EvaluateContext] = []
        self.dim: Dimension = Dimension()

        # In both cases there are zero or more auxiliary contexts
        self.auxiliary = []

        # When dim exists you can apply outer where expressions
        self.outer_where: List[FilterExpression]

        # The context tracks inner and source objects:
        self.inner: Union[PolyMeasure, None] = None
        self.source: Union[str, None] = None

        # Name and postfix modifiers
        self.name: Union[str, None] = None
        self.orderby = None
        self.having = None
        self.postfix = None


    def outgoing_filters(self):
        current_primitive = self.get_current_primitive()
        return current_primitive.current_filters()


    def evaluate(self, update_columns=False):
        """
        Returns a SQL string for the entire context (orphan -> aux -> group -> aux -> target)
        :return: SQL String
        """


        # if not self.primitive or evaluate_as_source:
        if evaluate_as_source or (is_null_query and not self.primitive):
            with_expression = ''
            from_expression = f"from {evaluate_inner_string} {view_alias}"
        else:
            with_expression = f"with {view_name} as ({evaluate_inner_string})"
            from_expression = f"from {view_name} {view_alias}"

        select_expression = make_statement('select', groupby_dimensions + outer_primitive_expressions)

        # evaluate where and dimensional where are treated the same, any of them could be dimensional joins, so:

        def process_where_filter(filter_object: FilterExpression):
            return f"({filter_object.expression(view_alias)})"

        evaluate_where_expressions = [
            process_where_filter(filter_object) for filter_object in evaluate_where]
        where_expression = make_statement('where', evaluate_where_expressions, separator='and \n')

        if len(evaluate_dimensions) > 0 and not self.outer_dimension.rowset:
            groupby_expression = make_statement('group by', groupby_dimensions)
            having_expression = make_statement('having', self.having)
        else:
            groupby_expression = ''
            having_expression = ''

        orderby_expression = make_statement('order by', self.order_by)
        postfix_expression = '' if self.postfix is None or len(self.postfix) == 0 else self.postfix

        if update_columns and alias_type != 'set':
            update_expression = f"update {evaluate_inner_string} {view_alias} \nset"
            update_list = make_statement(prefix='', elements=outer_primitive_expressions)
            evaluate_sql = f"""
        {update_expression} {update_list}
        {where_expression}"""
        else:
            # the from clause can be supressed, especially if a dynamic outer measure is used
            if evaluate_inner is None or self.suppress_from or (is_null_query and not self.primitive):
                post_select = ''
            else:
                post_select = f"""{from_expression} {where_expression} {groupby_expression}
        {having_expression} {orderby_expression} {postfix_expression}"""
            evaluate_sql = f"""
        {with_expression} 
        {select_expression}
        {post_select}"""
        if aliased:
            if alias_type == 'set':
                evaluate_sql = f"{override_name} = ({evaluate_sql})"
            else:
                # regular aliasing
                evaluate_sql = f"({evaluate_sql}) {override_name}"

        return evaluate_sql


class PolyMeasure:


    def __init__(
            self,
            name=None,
            outer=None,
            dim=None,
            inner=None,
            where=None, having=None, order_by=None, postfix=None, outer_where=None,
            include=None, exclude=None, join_nulls=True,
            acquire_dimensions=False, redirect=None,
            suppress_from=False
    ):
        """
        Args:
            name: The name of the measure.

            outer: List of PolyMeasures or primitives to evaluate over inner, grouping by dim.

            dim: Dimensions to group the view by. A (possibly empty) list of SQL column strings or string
            callable functions, or a Rows dummy object which will not group. Where dim = None
            the measure will group by the dimensions of the enclosing measure, or by no dimensions if [] is specified.

            inner: A single string or primitive, or None. This expression is grouped by dim, and aggregates in outer
            are applied. If inner is not None, the measure is said to be "bound" to inner.
            If None is supplied, the measure is "free" and will instead query the inner argument of the
            nearest bound parent measure.

            A bound outer measure will evaluate for each row in the inner evaluation,
            but query the bound view, whereas a free measure evaluates and queries over the bound view.

            where: Where statements to apply to the evaluation of the interior.

            having: Having statement to apply to the aggregation.
            order_by: Ordering statement to apply to the aggregation.
            postfix: Postfix statement to apply to the aggregation instead of having, order.

            include: Remove any dimensional filters not in this list.
            exclude: Remove any dimensional filters in this list.
            join_nulls: Treat null = null when evaluating joins (usually desired).
            acquire_dimensions: If true, the dimension for this measure will be the union of all the dimensions
            contained in the outer measures with a matching inner expression. Those measures maintain the granularity of
            their dimensions if defined.



        A PolyMeasure (or measure) represents a SQL "group by one view, evaluate over another" statement, with options
        for supplying the usual SQL operators or modifying the evaluation in other programmatic ways.
        The class provides a framework for creating analysis expressions over SQL database systems, in particular the
        duckdb SQL engine.

        PolyMeasures have three main components

        - an inner view, also a PolyMeasure
        - a set of outer PolyMeasures to evaluate over that view, and
        - a set of dimensions to group the inner view by when evaluating the outer view(s).

        The function evaluate returns a SQL statement, which will be syntactically valid only subject to
        sensible construction of the "PolyMeasure Tree".
        This evaluation may occur inside the evaluation contexts of other PolyMeasures, so that SQL expressions are
        built up recursively to achieve some complex calculation.

        In the following notation we drop the name parameter and write

        `M( Outer * Dim; Inner: Where, Context )`

        when describing a PolyMeasure, in line with the order of the other parameters of importance.
        The context can be parameters like having or order by clauses.
        The where parameter specifies additional filters to apply to the

        There are two classes of PolyMeasures - those with a free view, where Outer = None, or a bound view
        which already exists in the database.
        A bound measure with the usual arguments will be denoted `B( Inner * Dim; Outer: Where )`
        and a free measure denoted `F( Inner * Dim; Outer: Where )`. Free views become handy expressions like
        "count(*) of any inner table" or "count(*) over dimensions of the inner table".

        Free measures take on the context of the inner query in a PolyMeasure, rather than referencing a basic
        view in the schema. This allows a single PolyMeasure object to perform a double aggregation over a given
        view, where we group once, then group the resulting rowset again and perform some aggregation.

        If a free measure is supplied to the outer measure list, it will query the corresponding inner measure list.
        Free measures cannot be supplied as the inner measure of a PolyMeasure.

        Bound measures should have a default string value for the view, appropriate to the schema.
        Using the supplied factory method BoundedMeasure will return a super-classed PolyMeasure
        keyed to any views required.

        In the following examples, we assume one unique bounded measure exists,
        B = B(dim=Rows(), inner=main_view), which acts as the ultimate "leaf" measure.
        The class Free is also provided, which defaults the measure view to Free instead, and is aliased F.

        The Outer and Inner arguments are PolyMeasure vectors, which each can return tables of any dimension.
        When different dimensions are supplied in the outer measures, self.dim is used to group the inner view,
        but each measure still evaluates by matching only on its own dimension, if specified.
        No attempt is made to reconcile the dimensions between inner and outer arguments - if a measure requests
        a column that doesn't exist in dim(M) (the inner dimension specified by M), then the SQL parser
        will let you know :)

        When supplied as an argument, a tuple is interpreted as a "Dummy PolyMeasure" (name, inner, [dim]),
        where dim can be omitted. These measures are always free.

        When defining dimensions, [] means group by the whole table,
        RowSet means do not group the view. None is a wildcard, which means the measure will default
        to the dimensions of the enclosing measure after dimension promotion.

        The view and where parameters are applied to the inner query, Inner * Dim.
        For example if you want to apply a filter context to Outer directly (which is the final output aggregation),
        use M( Inner * Dim; F(Outer: Where_Outer): View, Where)

        Here are the things you can build with a single PolyMeasure

        1) M( Outer ) - Outer measure with no specified dimension - will take on the dimension of the enclosing measure.
        2) M( * Dim ) - A summary table of the dimensions Dim.
        3) M( Outer * Dim ) - A summary table of the dimensions with Outer measures calculated over each group.
        4) M( ; Inner ) - Defaults to M( Inner )
        5) M( Outer * Dim ; Inner ) - A set of aggregated calculations (Outer) grouped by Dim over Inner.

        ==========================================

        Invalid measures fail on evaluation, not instantiation. Test with where 0 = 1 to validate.....

        ==========================================

        Bound measures are "inner idempotent", meaning passing them as the sole argument to an inner parameter
        of a PolyMeasure constructor creates an object with the same evaluate() properties.

        Free measures don't have this property.
        The free context of F is overwritten by the context of the nearest enclosing bound measure B.

        In summary:
    ```
          B( B( Outer * Dim; Inner: Where) )
        = F( B( Outer * Dim; Inner: Where) )
        = B( F( Outer * Dim; Inner: Where) )
        =    B( Outer * Dim; Inner: Where)
    ```
        all hold. (Note: Inner=None implicitly in the expression F( Outer * Dim; Inner: Where))

        ==========================================

        In pseudo-sql M( Inner * Dim; Outer; Where, Context ) evaluates as:

        with __VIEW__ as ( [select [Inner] from view([Inner])] )

        select [(
            select [Outer] from (view(Outer) OR __VIEW__) dynamic_view
            where dynamic_view.dim(Outer) = __VIEW__.dim(Outer)
        )], Dim

        from __VIEW__
        where Where
        group by Dim
        <<context>>

        There is a simpler presentation when Dim = Rows(G):

        with __VIEW__ as ( [select [Inner] from view([Inner])] )

        select [Outer], __VIEW__.G

        from __VIEW__
        where Where
        <<context>>

        ==========================================

        Every outer measure needs to have dimensions that match the inner
        query. Use aliases or the rename function to achieve this.
        """

        # The name isn't usually present in evaluation unless it becomes a visible column
        # Pass aliased=True to evaluate the select in the form "(select..) as name"
        self.name = name

        # Convert the dimensional argument to a Dimension object
        self.dim = self._parse_dim_argument(dim)

        self.having = having
        self.order_by = order_by
        self.postfix = postfix

        self.include = make_as_list(include, keep_none=True)
        self.exclude = make_as_list(exclude)
        self.join_nulls = join_nulls
        self.acquire_dimensions = acquire_dimensions
        self.redirect = redirect
        self.suppress_from = suppress_from

        """
        The function _to_polymeasure processes dummy measure entities into true PolyMeasure objects.
        _primitive indicates that _to_polymeasure isn't needed to interpret the field outer, which stops the recursion.
        In the primitive case outer is a string, or string a function which is passed parent and root as parameters
        Primitives are always free measures
        The outer_dimension is the final dimension set of the measure, which is determined recursively by both self.dim and the 
        outer_dimensions of the outer measures. This closure process forms a union of the dimensions, with Rows being
        the most granular, and [] being the least. dim = None is a wildcard that assumes the dimension of the 
        (nearest dimension- defined) parent, or [] if that is not defined.
        """

        self.primitive = self._check_primitive(outer)

        if inner is None:
            # Free measure
            self.free = True
            self.source = False
        elif self._check_primitive(inner):
            # The inner query is a source - just a string or string evaluable that can be directly compared
            # and must be unique to the sql schema.
            self.free = False
            self.source = True
        elif isinstance(inner, PolyMeasure):
            self.free = False
            self.source = False
        else:
            raise Exception

        self.inner = inner

        # An outer measure can only aggregate one inner query, which is the final query mentioned.
        # Other inner queries can only be reached through tailored FilterExpressions
        # These are called auxiliary inner sub queries

        # The inner query is a complex measure
        if not self.free and not self.source:
            self.complex = True
        else:
            self.complex = False

        # Create where FilterExpression objects
        self.where = self._process_where_argument(where)
        self.outer_where = self._process_where_argument(outer_where)


        if self.primitive:
            self.outer = make_as_list(outer, filter_out_null=True)
        else:
            # The program would rather expire than put an invalid object inside outer and outer_dimension.
            self.outer = [self._to_polymeasure(measure) for measure in make_as_list(outer)]


    @staticmethod
    def dimensional_join_primitive(column, outer_alias, inner, join_nulls):
        """
        Returns a string function returning join operators to connect inner measurements together.
        Args:
            column: What dimensions to connect on
            outer_alias: Outer alias to encode into this join primitive - future measures know to connect to this source
            using this alias, which is generated on evaluation.
            inner: Inner source(s), string or PolyMeasure.
            join_nulls: Whether to treat null = null
        Returns:
        List of FilterExpressions with dynamic expression methods.
        """

        if join_nulls:
            comparator = 'is not distinct from'
        else:
            comparator = '='

        def expression_function(inner_alias):
            return f"{inner_alias}.{column} {comparator} {outer_alias}.{column}"

        return FilterExpression(expression_function, column, inner)

    @staticmethod
    def _parse_dim_argument(maybe_dimension):
        if isinstance(maybe_dimension, (tuple, list)):
            return_dim = Dimension(dimensions=maybe_dimension, rowset=False, star=False)
        elif isinstance(maybe_dimension, Dimension):
            return_dim = maybe_dimension
        elif isinstance(maybe_dimension, str):
            return_dim = Dimension(dimensions=[maybe_dimension], rowset=False, star=False)
        else:
            # wildcard dimension
            return_dim = Dimension(dimensions=[], rowset=False, star=False)

        return return_dim

    def _get_primitive_expression(self, inner_alias=None, override_name=None, update_columns=False):

        override_name = override_name if override_name is not None else self.name
        if len(self.outer) == 0:
            outer_primitive_expressions = []
        elif isinstance(self.outer[0], FunctionType):
            if update_columns:
                outer_primitive_expressions = [
                    override_name + ' = ' + self.outer[0](self=self, inner_alias=inner_alias)]
            else:
                outer_primitive_expressions = [self.outer[0](self=self, inner_alias=inner_alias) + ' ' + override_name]
        else:
            if update_columns:
                outer_primitive_expressions = [override_name + ' = ' + self.outer[0]]
            else:
                outer_primitive_expressions = [self.outer[0] + ' ' + override_name]
        return outer_primitive_expressions

    def get_final_columnset(self):
        # Returns a list of columns including dimensions and outer measure names
        outer_dimension_list = self.outer_dimension.dimensions if self.outer_dimension.dimensions else []
        return outer_dimension_list + self.final_outer_columns

    def _process_where_argument(self, where):
        where_list = [self._get_filter(expression) for expression in make_as_list(where)]
        where_list = [filter_object for filter_object in where_list if filter_object is not None]
        return where_list

    def rename(self, name):
        measure_clone = copy(self)
        measure_clone.name = name
        return measure_clone

    def addwhere(self, where):
        where = self._process_where_argument(where)
        measure_clone = copy(self)
        measure_clone.where = measure_clone.where + where
        return measure_clone

    def rewhere(self, where):
        where = self._process_where_argument(where)
        measure_clone = copy(self)
        measure_clone.where = where
        return measure_clone

    @staticmethod
    def _to_polymeasure(measure_object):
        """
        Converts a measure object of flexible type into a polymeasure.
        Args:
            measure_object:

        Returns:
        Internal dummy class with necessary properties
        """

        if isinstance(measure_object, str):
            measure = PolyMeasure(None, measure_object)
            return measure
        elif isinstance(measure_object, PolyMeasure):
            # Here we allow evaluation to work recursively
            # PolyMeasures are callable.
            # So we check for PolyMeasures first in the elif
            return measure_object
        elif callable(measure_object):
            measure = PolyMeasure(None, measure_object)
            return measure_object
        elif isinstance(measure_object, (tuple, list)) and len(measure_object) > 0:
            if len(measure_object) == 1:
                measure = PolyMeasure(None, measure_object[0])
            elif len(measure_object) == 2:
                measure = PolyMeasure(measure_object[0], measure_object[1])
            else:
                measure = PolyMeasure(measure_object[0], measure_object[1], measure_object[2])
            return measure
        else:
            # There's no way to form a query so raise an error
            raise Exception

    def _get_filter(self, filter_object):
        """
        Converts an object of flexible type into a FilterExpression
        Args:
            filter_object: string or tuple

        Returns: FilterExpression
        """

        if isinstance(filter_object, str):
            return FilterExpression(filter_object, source=self.primary_inner)
        elif isinstance(filter_object, FilterExpression):
            return filter_object
        elif isinstance(filter_object, (tuple, list)) and len(filter_object) > 0:
            if len(filter_object) == 1:
                return FilterExpression(filter_object[0], lineage=None, source=self.primary_inner)
            else:
                return FilterExpression(*filter_object[:3])
        else:
            # Improperly formatted filter
            return None


    def evaluate(
            self, where=None, pretty=False, update_columns=False):

        evaluate_where = self._process_where_argument(where)

        # Start with context containing only the evaluation filters
        context = EvaluateContext(where)

        # Beginning with self, iterate through measures to build the full branching context
        # The context object is passed through each measure and built up within the process_context method
        context = self.process_context(context)

        # Now evaluate..
        evaluate_sql = context.evaluate(update_columns=update_columns)

        if pretty:
            evaluate_sql = sqlparse.format(evaluate_sql, reindent=True, keyword_case='lower')

        return evaluate_sql


    def process_context(self, context: EvaluateContext) -> EvaluateContext:
        """
        Updates the evaluation context using the polymeasure properties.

        The updates are created in a new MeasureContext and a reference to the updated object is returned.
        Names and dimensions are ledgered if defined.

        :param context: SQL Evaluation context
        :return: EvaluateContext
        """

        # Filter incoming where expressions using self.include / exclude properties,
        # pass these to the new MeasureContext

        new_context = MeasureContext(self, context)

        # Pass clones of this context to any auxiliary measures provided by the filters, and evaluate
        for expression in self.where:
            pass
            # a primitive context needs to be created for any new auxiliary objects

            # Evaluate and attach M.where, including auxiliary where objects


            # List of new common table expressions (Auxiliary measures that are newly evaluated)



        # If there is an inner measure evaluate and promote to source


        # Determine dimensionality from self.dim and source evaluate dimensions if needed
        # If grouping: flag groupby or raise alert if there's already a grouping operation
        # When grouping begins, migrate primitive stack to grouping_stack


        # Pass a clone of the context to each outer measure for processing
        # Then merge the outer primitive stacks together

        # Create the evaluation SQL text before returning

        return context


def bound_objects(source):
    class BoundMeasure(PolyMeasure):

        def __init__(
                self,
                name=None, outer=None, dim=None, inner=None,
                where=None, having=None, order_by=None, postfix=None,
                include=None, exclude=None, join_nulls=True, acquire_dimensions=False,
                redirect=None, suppress_from=False, outer_where=None
        ):
            super().__init__(
                name=name, outer=outer, dim=dim, inner=source, where=where, having=having,
                order_by=order_by, postfix=postfix, include=include, exclude=exclude, join_nulls=join_nulls,
                acquire_dimensions=acquire_dimensions, redirect=redirect, suppress_from=suppress_from, outer_where=outer_where
            )

    class BoundFilter(FilterExpression):

        def __init__(self, expression, lineage=None, group_lineage=None):
            super().__init__(expression, lineage=lineage, source=source, group_lineage=group_lineage)

    return BoundMeasure, BoundFilter
