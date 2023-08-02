from inspect import signature
from types import FunctionType
from copy import copy
import sqlparse


def constant(value):
    def func():
        return value

    return func


def make_as_list(maybe_vector, filter_out_null=True, keep_none=False):
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


class FilterExpression:
    def __init__(self, expression, lineage=None, source=None, group_lineage=None):
        """
        Represents a filter action, expression, (optional) on columns with lineage over a source.
        Supplying lineage allows the evaluator to identify and alias column names.
        Dimensional joins create a filter expression with an expression function, producing something like:

        inner_alias.column_name {comparator} outer_alias.column_name

        The outer_alias and the column name are fixed, and a source is assigned. If evaluation occurs over a matching
        source inner, then the filter expression becomes active and is passed inner_alias to complete the expression.
        Args:
            expression: String expression like "qcategory = 'BPA'" or a function that admits two positional arguments,
            inner and outer aliases, and returns a SQL expression string. The resulting object has an expression method.
            lineage: String or list of strings identifying the columns
            source: String or list of strings identifying the source
            group_lineage: String or list of strings aliasing collections of columns.
        """

        # Expression is a function of one or two arguments - make it so
        if isinstance(expression, str):
            self.expression = lambda inner: expression
        elif isinstance(expression, FunctionType) or callable(expression):
            self.expression = expression
        else:
            raise Exception

        self.expression_parameter_list = list(signature(self.expression).parameters.keys())

        # Detects if the expression needs to be supplied an outer argument.
        # Not really a good way to do this, maybe demand as param in init or superclass not sure
        self.two_parameter_join = len(self.expression_parameter_list) > 1
        self.lineage = make_as_list(lineage)
        self.group_lineage = make_as_list(group_lineage)
        self.source = make_as_list(source, keep_none=True)
        if self.two_parameter_join:
            self.dummy_expression = self.expression('??INNER??', '??OUTER??')
        else:
            self.dummy_expression = self.expression('??INNER??')

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
    Rowset is a special dimension which returns (*) from the view.
    Rowset measures should be row context primitive expressions.
    The star keyword argument should be a single string star expression
    """

    def __init__(self, dimensions=None, rowset=False, star=None):
        """
        Represents a set of dimensions to group an inner query.
        Can be any granularity from [] to ∞, or None which defaults to the parent measures dimensions.
        Args:
            groupby: list column names or None
            rowset: list column names or None
            star: A single string star expression or None
        """
        self.dimensions = make_as_list(dimensions, keep_none=True)
        self.rowset = rowset

        if star == True:
            # default to star
            self.star = "*"
        else:
            self.star = star

        if self.star:
            self.rowset = True

        # if self.dimensions is None and self.rowset is False:
        if self.dimensions is None:
            self.wildcard = True
        else:
            self.wildcard = False

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


class Rowset(Dimension):

    def __init__(self, dimensions=None, star=None):
        """
        Represents a rowset-level (infinity) granularity.
        A Rowset object also specifies a number of columns to return, or a star expression to evaluate.
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

        super(Rowset, self).__init__(dimensions=dimensions, rowset=True, star=star)


class PolyMeasure:

    @staticmethod
    def _do_nothing(self, *args, **kwargs):
        return None

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

    def _get_final_columnset(self):
        # Returns a list of columns including dimensions and outer measure names
        outer_dimension_list = self.outer_dimension.dimensions if self.outer_dimension.dimensions else []
        return outer_dimension_list + self.final_outer_columns

    def _process_where_argument(self, where):
        where_list = [self._get_filter(expression) for expression in make_as_list(where)]
        where_list = [filter_object for filter_object in where_list if filter_object is not None]
        return where_list

    @staticmethod
    def _check_primitive(measure_object):
        if isinstance(measure_object, str):
            return True
        elif isinstance(measure_object, PolyMeasure):
            return False
        elif isinstance(measure_object, FunctionType):
            return True
        elif isinstance(measure_object, tuple):
            return False
        elif isinstance(measure_object, list):
            return False
        elif measure_object is None:
            # No outer measurements supplied, just a dimensional groupby
            return True
        else:
            raise Exception

    def rename(self, name):
        measure_clone = copy(self)
        measure_clone.name = name
        return measure_clone

    def add_where(self, where):
        where = self._process_where_argument(where)
        measure_clone = copy(self)
        measure_clone.where = measure_clone.where + where
        return measure_clone

    @staticmethod
    def _to_polymeasure(measure_object):
        """
        Converts an object of flexible type into name and query data
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
            return FilterExpression(filter_object, source=self.inner)
        elif isinstance(filter_object, FilterExpression):
            return filter_object
        elif isinstance(filter_object, (tuple, list)) and len(filter_object) > 0:
            if len(filter_object) == 1:
                return FilterExpression(filter_object[0], lineage=None, source=self.inner)
            else:
                return FilterExpression(*filter_object[:3])
        else:
            # Improperly formatted filter
            return None

    def __init__(
            self,
            name=None,
            outer=None,
            dim=None,
            inner=None,
            where=None, having=None, order_by=None, postfix=None,
            include=None, exclude=None, join_nulls=True,
            acquire_dimensions=False, redirect=None,
            suppress_from=False
    ):
        """
        Args:
            name: The name of the measure.

            outer: List of PolyMeasures or primitives to evaluate over inner, grouping by dim.

            dim: Dimensions to group the view by. A (possibly empty) list of SQL column strings or string
            callable functions, or a Rowset dummy object which will not group. Where dim = None
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
        B = B(dim=Rowset(), inner=main_view), which acts as the ultimate "leaf" measure.
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

        There is a simpler presentation when Dim = Rowset(G):

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
        outer_dimensions of the outer measures. This closure process forms a union of the dimensions, with Rowset being
        the most granular, and [] being the least. dim = None is a wildcard that assumes the dimension of the 
        (nearest dimension- defined) parent, or [] if that is not defined.
        """

        self.primitive = self._check_primitive(outer)
        if inner is None:
            # Free measure
            self.inner = None
            self.free = True
            self.source = None
        elif self._check_primitive(inner):
            # The inner query is a source - just a string or string evaluable that can be directly compared
            # and must be unique to the sql schema.
            self.inner = inner
            self.free = False
            self.source = True
        elif isinstance(inner, PolyMeasure):
            self.inner = inner
            self.free = False
            self.source = False
        else:
            raise Exception

        # Create where FilterExpression objects (now that self.inner is defined)
        self.where = self._process_where_argument(where)

        # Determine the outer dimension

        if self.primitive:
            self.outer = make_as_list(outer, filter_out_null=True)
            self.outer_dimension = self.dim
        else:
            # The program would rather expire than put an invalid object inside outer and outer_dimension.
            self.outer = [self._to_polymeasure(measure) for measure in make_as_list(outer)]
            self.outer_dimension = self.dim

            if self.acquire_dimensions:
                for measure in self.outer:
                    # For each outer measure collect the dimensions as named
                    # (the designer should ensure they are unique strings or duckdb will mangle)
                    # All the objects involved in this step should involve Dimenion objects which can handle the None
                    # and rowset silliness.
                    self.outer_dimension = self.outer_dimension.absorb_new_dimension(measure.outer_dimension)

        # This is quite rare - acquire the inner dimension if none was supplied from the left
        # (outer dimensions + self.dim)
        if not self.source and self.outer_dimension.rowset \
                and self.outer_dimension.wildcard \
                and self.inner is not None \
                and self.inner.outer_dimension is not None:
            # self.outer_dimension = self.outer_dimension.absorb_new_dimension(self.inner.outer_dimension)
            # self.outer_dimension = self.outer_dimension.absorb_new_dimension(
            #     Dimension(
            #         [measure.name for measure in self.inner.outer], rowset=False
            #     )
            # )
            self.outer_dimension = self.outer_dimension.absorb_new_dimension(
                Dimension(self.inner._get_final_columnset())
            )

        self.final_outer_columns = self._get_primitive_sql_names()

        pass

    def evaluate(
            self, where=None, pretty=False, update_columns=False):

        evaluate_where = self._process_where_argument(where)

        # promote where expressions:
        evaluate_where = [
            filter_expression.fix_outer('__ALIAS_0_0') for filter_expression in evaluate_where
        ]

        evaluate_sql = self._evaluate(
            evaluate_where, 0, 0, self.inner, self.outer_dimension,
            update_columns=update_columns
        )

        if pretty:
            evaluate_sql = sqlparse.format(evaluate_sql, reindent=True, keyword_case='lower')

        return evaluate_sql

    def _evaluate(
            self, where, depth, breadth, wildcard_inner,
            wildcard_dimensions, aliased=False, allow_grouping=True,
            override_name=None, update_columns=False, alias_type=None
    ):
        """
        Internal
        Args:
            where: List of where FilterExpression objects that apply through the expression
            depth: Internal variable tracking how many subqueries down
            wildcard_inner: Default inner measure for free measures
            wildcard_dimensions: Default dimensions for wildcard dimension measures.

        Returns: SQL query string
        """

        """
        --target query:

        with __VIEW__i as ( [select [Inner] from view([Inner])] )

        select [(
            select [Outer] from (view(Outer) OR __VIEW__) dynamic_view
            where dynamic_view.dim(Outer) = __VIEW__i.dim(Outer)
        )], outer_dimension

        from __VIEW__i
        where where
        group by outer_dimension
        --   + <<context>>

        """

        view_name = f"__VIEW__{depth}_{breadth}"
        view_alias = f"__ALIAS__{depth}_{breadth}"
        override_name = self.name if override_name is None else override_name
        wildcard_dimensions = Rowset() if wildcard_dimensions is None else wildcard_dimensions

        evaluate_inner = self.inner if not self.free else wildcard_inner

        if self._check_primitive(evaluate_inner):
            # This is when self.outer_dimension is a rowset operator but the targeted view has no dimensional data
            # like select * from master - what is in master?
            backup_rowset_dimension = []
        else:
            backup_rowset_dimension = evaluate_inner._get_final_columnset()

        # evaluate_dimenions are the dimensional joins necessary at this level of the evaluation
        # The eventual where clauses are built in evaluate_where

        if self.outer_dimension.wildcard and not wildcard_dimensions.wildcard:
            evaluate_dimensions = wildcard_dimensions.dimensions
        elif self.outer_dimension.rowset and self.outer_dimension.dimensions is None:
            evaluate_dimensions = backup_rowset_dimension
        else:
            evaluate_dimensions = (
                self.outer_dimension.dimensions
                if not self.outer_dimension.wildcard
                else wildcard_dimensions.dimensions
            )

        # If evaluate_dimensions is None, swap to [] now
        evaluate_dimensions = [] if evaluate_dimensions is None else evaluate_dimensions

        # And there's a second trick with the dimensions - they don't need to be applied as filters
        # unless they are joining. Hence a second (!) set of dimensions which cut
        # parts of the process off if the measure is being evaluated by a parent that is already grouping.
        # If outer_dimension is a rowset operator then also blank any grouping.

        star_expression = self.outer_dimension.star
        if star_expression is not None and star_expression:
            groupby_dimensions = [star_expression]
        else:
            groupby_dimensions = evaluate_dimensions if allow_grouping else []

        # null queries have well defined inner measures but they do not have a select statement from them..
        is_null_query = (wildcard_dimensions.dimensions is None and not wildcard_dimensions.rowset) and (
            self.outer_dimension.wildcard and not self.outer_dimension.rowset)

        # Treat this query as a source query, do not alias the final from statement
        evaluate_as_source = self._check_primitive(evaluate_inner) or is_null_query

        # filter these where statements using include/exclude and dimensionality
        # These where statements are given to outer and inner measures
        # self.where are added in afterwards..
        passthrough_where = [
            expression for expression in where
            if expression.test_keep_filter(
                self.include, self.exclude
            )
        ] + self.where

        # Fix any flexible filter expressions with the current view_alias as a fixed outer_alias (above)
        # fix_outer knows to leave the filter alone if there is only one parameter available.
        passthrough_where = [filter_expression.fix_outer(view_alias) for filter_expression in passthrough_where]

        # If the expression source matches the inner expression, only include matching lineages
        passthrough_where = [
            expression for expression in passthrough_where
            if expression.source != self.inner or bool(set(expression.lineage) & set(evaluate_dimensions))
        ]

        # Finally, only actually evaluate these filters if their inner statements are matched
        evaluate_where = [
            expression for expression in passthrough_where if (
                expression.test_keep_filter(
                    None, [], evaluate_inner
                )
            )
        ]

        # Calculate the dimensional join objects to be supplied to sub measures
        # This is done inside _evaluate, rather than _generate_primitive_sql, as only one group by is performed no matter
        # how many nested measures are created
        evaluate_redirect_inner = evaluate_inner if self.redirect is None else self.redirect

        dimensional_where = [
            self.dimensional_join_primitive(column, view_alias, evaluate_redirect_inner, self.join_nulls)
            for column in evaluate_dimensions
        ]

        # Build the primitive SQL select expression
        # _get_primitive acts recursively here

        if self.primitive:
            # If the primitive is a function, it is evaluated now with self as its argument
            outer_primitive_expressions = self._get_primitive_expression(
                inner_alias=view_name,
                override_name=override_name
                # , update_columns=update_columns
            )
        else:
            outer_primitive_expressions = self._get_primitive_sql_list(
                passthrough_where + dimensional_where,
                depth,
                breadth,
                self.inner,
                self.outer_dimension,
                original_alias=view_alias,
                update_columns=update_columns
            )

        if evaluate_as_source:
            evaluate_inner_string = evaluate_inner
            with_expression = ''
        else:
            evaluate_inner_string = evaluate_inner._evaluate(
                passthrough_where, 0, breadth + 1,
                None, None, aliased=False
            )
            with_expression = f"with {view_name} as ({evaluate_inner_string})"

        select_expression = make_statement('select', groupby_dimensions + outer_primitive_expressions)

        if evaluate_as_source:
            from_expression = f"from {evaluate_inner_string} {view_alias}"
        else:
            from_expression = f"from {view_name} {view_alias}"

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
            if evaluate_inner is None or self.suppress_from or is_null_query:
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

    def _get_primitive_sql_list(self, where, depth, breadth, wildcard_inner, wildcard_dimensions, original_alias=None,
                                update_columns=False):
        """
        Returns a list of SQL select expressions built recursively from each primitive measure in self.outer.

        Args:
            where: The where filter clause context inherited from the parent measure, to be combined with self.where
            depth: The current integer depth of the subquery
            wildcard_inner: The inner clause inherited from the parent measure
            wildcard_dimensions: The dimension object inherited from the parent measure

        Returns: List of string SQL expressions aliased by their names.
        """

        primitive_sql_list = []

        # append measure where statements
        # new_where = where + self.where
        # append dimensional filters

        wildcard_inner = self.inner if not self.free else wildcard_inner
        wildcard_dimensions = self.outer_dimension if not self.outer_dimension.wildcard else wildcard_dimensions

        passthrough_where = [
            expression for expression in where
            if expression.test_keep_filter(
                self.include, self.exclude
            )
        ]

        if len(self.outer) == 1:
            override_name = self.name
        else:
            override_name = None

        for measure in self.outer:

            # if measure.primitive or (not measure.free and self.source):
            #     use_alias = False
            # else:
            #     use_alias = True

            if measure.primitive and (len(measure.outer) == 1) or not measure.source:

                # The recursion now has a primitive measure which can be evaluated directly.
                # Lock the passthrough wheres to the current original alias - this is the right place to do it

                locked_where = [
                    expression.fix_outer(original_alias) for expression in passthrough_where
                ]

                # if measure.free and measure.outer_dimension.wildcard and self.outer_dimension.rowset:
                if measure.free and wildcard_inner is None:
                    # Generated when a rowset query doesn't need a subquery expression
                    primitive_sql_list = primitive_sql_list + measure._get_primitive_expression(
                        inner_alias=original_alias, override_name=override_name, update_columns=update_columns)
                else:
                    if update_columns:
                        alias_type = 'set'
                    else:
                        alias_type = None
                    primitive_sql_list = primitive_sql_list + [measure._evaluate(
                        locked_where, depth + 1, breadth, wildcard_inner,
                        wildcard_dimensions, aliased=True, allow_grouping=False,
                        override_name=override_name, update_columns=update_columns, alias_type=alias_type
                    )]


            else:

                primitive_sql_list = primitive_sql_list + measure._get_primitive_sql_list(
                    # This is a non-primitive measure that has no opportunity to evaluate its
                    # own where clauses, so they are appended here.
                    passthrough_where + measure.where,
                    depth, breadth, wildcard_inner, wildcard_dimensions,
                    original_alias=original_alias, update_columns=update_columns
                )

        return primitive_sql_list

    def _get_primitive_sql_names(self):
        """
        Returns a list of names of descendent outer measure names, used on initialisation.
        """

        primitive_sql_names = []

        if self.primitive:
            primitive_sql_names = [self.name]
        else:
            for measure in self.outer:

                if len(measure.outer) == 1:
                    primitive_sql_names = primitive_sql_names + [measure.name]
                else:
                    primitive_sql_names = primitive_sql_names + measure._get_primitive_sql_names()

        return primitive_sql_names


def bound_objects(source):
    class BoundMeasure(PolyMeasure):

        def __init__(
                self,
                name=None, outer=None, dim=None, inner=None,
                where=None, having=None, order_by=None, postfix=None,
                include=None, exclude=None, join_nulls=True, acquire_dimensions=False,
                redirect=None, suppress_from=False
        ):
            super().__init__(
                name=name, outer=outer, dim=dim, inner=source, where=where, having=having,
                order_by=order_by, postfix=postfix, include=include, exclude=exclude, join_nulls=join_nulls,
                acquire_dimensions=acquire_dimensions, redirect=redirect, suppress_from=suppress_from
            )

    class BoundFilter(FilterExpression):

        def __init__(self, expression, lineage=None, group_lineage=None):
            super().__init__(expression, lineage=lineage, source=source, group_lineage=group_lineage)

    return BoundMeasure, BoundFilter
