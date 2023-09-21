from dataclasses import dataclass

from .core import PolyMeasure as M, FilterExpression, Rowset

count_all = M('countall', outer='count(*)', dim=[], include=[])
size = M('size', outer='count(*)')


def constant(c):
    """
    Args:
        c: Numeric or string literal, or object with str accessor.
    Returns: A polymeasure which always evaluates as c.
    """
    if not isinstance(c, str):
        c = str(c)
    return M('__a_constant', c, suppress_from=True)


def combine_scalars(name, outer, inner_measures):
    return M(name, outer, inner=M(outer=inner_measures))


def dot_product(name, measures, weights=None):
    """
    Returns a polymeasure that represents the dot product of measures and weights.
    https://en.wikipedia.org/wiki/Dot_product
    :param name: Name of the measure
    :param measures: A list of polymeasures [p1, p2 .. pN]
    :param weights: A list of weights of equal length, [c1, c2 .. cN]
    :return: Polymeasure representing the linear sum c1 * p1 + c2 * p2 + .. + cN * pN
    """
    measure_counts = len(measures)
    if weights is not None:
        if measure_counts != len(weights):
            raise Exception
        terms = [f"__input{i} * {weights[i]}" for i in range(measure_counts)]
    else:
        terms = [f"__input{i}" for i in range(measure_counts)]
    linear_combination_expression = ' + \n'.join(terms)
    return combine_scalars(name, linear_combination_expression, [
        measures[i].rename(f"__input{i}") for i in range(measure_counts)
    ])


def exp_dot_product(name, measures, weights=None):
    """
    Returns a polymeasure that represents the product of the exponents of the measures by index weights.
    Similar to a dot product, but exponentiate the terms and multiply them together.
    :param name: Name of the measure
    :param measures: A list of polymeasures [p1, p2 .. pN]
    :param weights: A list of weights of equal length, [c1, c2 .. cN]
    :return: Polymeasure representing the linear sum c1 ^ p1 * c2 ^ p2 * .. * cN ^ pN
    """
    measure_counts = len(measures)
    if weights is not None:
        if measure_counts != len(weights):
            raise Exception
        terms = [f"__input{i} ^ {weights[i]}" for i in range(measure_counts)]
    else:
        terms = [f"__input{i}" for i in range(measure_counts)]
    linear_combination_expression = ' * \n'.join(terms)
    return combine_scalars(name, linear_combination_expression, [
        measures[i].rename(f"__input{i}") for i in range(measure_counts)
    ])


def nulled_dot_product(name, measures, weights):

    measures_count = len(measures)

    def values_clause(self, inner_alias):
        values_terms = [f"({weights[i]}, (select __input{i} from {inner_alias}))" for i in range(measures_count)]
        comma_newline = ', \n'
        return f"""* from (values {comma_newline.join(values_terms)})"""

    kpi_values_summary = M(
        "kpis(weight, value)", values_clause,
        inner=M(
            outer=[measures[i].rename(f"__input{i}") for i in range(measures_count)])
        ,
        suppress_from=True)
    kpi_polymeasure = M(
        name, 'sum(weight * value) / sum(weight)',
        inner=kpi_values_summary)
    return kpi_polymeasure


def string_column_sample(
        name, columns, inner, sample_size=5, sort_by=size):
    """
    A composite measure that returns a string joined sample over multiple columns.
    Args:
        name: Name of the measure
        columns: Columns to join with ", "
        inner: View to target
        sample_size: Number of samples
        sort_by: Measure to evaluate to sort the results

    Returns: A PolyMeasure
    """

    def string_agg(self: M = None, inner_alias = None):
        # columns = ', '.join(parent.outer_dimension.dimensions)
        if self is not None:
            columns = self.outer_dimension.dimensions
        else:
            columns = []
        column_string = ', '.join(columns)
        return f"concat_ws(', ', {column_string})"

    return M(
        name, "string_agg(__the__words, ' --- ')", [],
        M(
            'the_words',
            [M('__the__words', string_agg, dim=columns), sort_by.rename('__string_sample_order')],
            dim=columns,
            inner=inner,
            postfix=f"order by __string_sample_order desc limit {sample_size}"
        )
    )


def count_distinct(columns, inner=None):
    if isinstance(columns, str):
        columns = [columns]
    name_column_string = '_'.join(columns)
    return M(
        f"count_distinct_{name_column_string}",
        size,
        inner=M('xyz', dim=columns, inner=inner)
    )


def double_outer(second_outer, first_outer, inner):
    """
    Evaluates the secound_outer measure over the rowset of the first_outer measures over the inner measure.
    Assumes the first_outer measures have the same inner measure, and the secound_outer measure is free.
    Args:
        second_outer: outermost free measure to evaluate over the result of M(first_outer; inner)
        first_outer: inner measures to evaluate over inner
        inner: Specific inner source polymeasure.

    Returns: The polymeasure representing M(second_outer; M(first_outer; inner))
    """
    if not isinstance(first_outer, (tuple, list)):
        first_outer = [first_outer]

    first_measure_name = first_outer[0].name
    first_measure_redirect = first_outer[0].inner
    evaluate_first_outer = M(
        first_measure_name,
        first_outer,
        Rowset(star=True),
        inner,
        redirect=first_measure_redirect
    )

    evaluate_double_outer = M(
        f"double_outer",
        second_outer,
        [],
        evaluate_first_outer
    )

    return evaluate_double_outer


def bound_objects(source):
    class BoundMeasure(M):

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

    class BoundLibrary:
        """
        Collection of standard measures and measure functions keyed to a specific inner source
        """
        count_all = M('countall', outer='count(*)', dim=[], include=[])
        size = M('size', outer='count(*)')

        def __init__(self, source):
            self.source = source

        # There should be a smarter way to declare this once..
        def count(self, col):
            return M(f'count_{col}', f"count({col})", inner=self.source)

        @staticmethod
        def constant(self, c):
            """
                Args:
                    c: Numeric or string literal, or object with str accessor.
                Returns: A polymeasure which always evaluates as c.
                """
            if not isinstance(c, str):
                c = str(c)
            return M('__a_constant', c, suppress_from=True)

        @staticmethod
        def combine_scalars(self, name, outer, inner_measures):
            return M(name, outer, inner=M(outer=inner_measures))

        @staticmethod
        def dot_product(self, name, measures, weights=None):
            """
                Returns a polymeasure that represents the dot product of measures and weights.
                https://en.wikipedia.org/wiki/Dot_product
                :param name: Name of the measure
                :param measures: A list of polymeasures [p1, p2 .. pN]
                :param weights: A list of weights of equal length, [c1, c2 .. cN]
                :return: Polymeasure representing the linear sum c1 * p1 + c2 * p2 + .. + cN * pN
                """
            measure_counts = len(measures)
            if weights is not None:
                if measure_counts != len(weights):
                    raise Exception
                terms = [f"__input{i} * {weights[i]}" for i in range(measure_counts)]
            else:
                terms = [f"__input{i}" for i in range(measure_counts)]
            linear_combination_expression = ' + \n'.join(terms)
            return combine_scalars(name, linear_combination_expression, [
                measures[i].rename(f"__input{i}") for i in range(measure_counts)
            ])

        @staticmethod
        def exp_dot_product(self, name, measures, weights=None):
            """
                Returns a polymeasure that represents the product of the exponents of the measures by index weights.
                Similar to a dot product, but exponentiate the terms and multiply them together.
                :param name: Name of the measure
                :param measures: A list of polymeasures [p1, p2 .. pN]
                :param weights: A list of weights of equal length, [c1, c2 .. cN]
                :return: Polymeasure representing the linear sum c1 ^ p1 * c2 ^ p2 * .. * cN ^ pN
                """
            measure_counts = len(measures)
            if weights is not None:
                if measure_counts != len(weights):
                    raise Exception
                terms = [f"__input{i} ^ {weights[i]}" for i in range(measure_counts)]
            else:
                terms = [f"__input{i}" for i in range(measure_counts)]
            linear_combination_expression = ' * \n'.join(terms)
            return combine_scalars(name, linear_combination_expression, [
                measures[i].rename(f"__input{i}") for i in range(measure_counts)
            ])

        @staticmethod
        def nulled_dot_product(self, name, measures, weights):
            measures_count = len(measures)

            def values_clause(self, inner_alias):
                values_terms = [f"({weights[i]}, (select __input{i} from {inner_alias}))" for i in
                                range(measures_count)]
                comma_newline = ', \n'
                return f"""* from (values {comma_newline.join(values_terms)})"""

            kpi_values_summary = M(
                "kpis(weight, value)", values_clause,
                inner=M(
                    outer=[measures[i].rename(f"__input{i}") for i in range(measures_count)])
                ,
                suppress_from=True)
            kpi_polymeasure = M(
                name, 'sum(weight * value) / sum(weight)',
                inner=kpi_values_summary)
            return kpi_polymeasure

        def string_column_sample(self, name, columns, sample_size=5, sort_by=size):
            """
                A composite measure that returns a string joined sample over multiple columns.
                Args:
                    name: Name of the measure
                    columns: Columns to join with ", "
                    inner: View to target
                    sample_size: Number of samples
                    sort_by: Measure to evaluate to sort the results

                Returns: A PolyMeasure
                """

            def string_agg(self: M = None, inner_alias=None):
                # columns = ', '.join(parent.outer_dimension.dimensions)
                if self is not None:
                    columns = self.outer_dimension.dimensions
                else:
                    columns = []
                column_string = ', '.join(columns)
                return f"concat_ws(', ', {column_string})"

            return M(
                name, "string_agg(__the__words, ' --- ')", [],
                M(
                    'the_words',
                    [M('__the__words', string_agg, dim=columns), sort_by.rename('__string_sample_order')],
                    dim=columns,
                    inner=self.source,
                    postfix=f"order by __string_sample_order desc limit {sample_size}"
                )
            )

        def count_distinct(self, columns):
            if isinstance(columns, str):
                columns = [columns]
            name_column_string = '_'.join(columns)
            return M(
                f"count_distinct_{name_column_string}",
                size,
                inner=M('xyz', dim=columns, inner=self.source)
            )

        def double_outer(self, second_outer, first_outer):
            """
                Evaluates the secound_outer measure over the rowset of the first_outer measures over the inner measure.
                Assumes the first_outer measures have the same inner measure, and the secound_outer measure is free.
                Args:
                    second_outer: outermost free measure to evaluate over the result of M(first_outer; inner)
                    first_outer: inner measures to evaluate over inner
                    inner: Specific inner source polymeasure.

                Returns: The polymeasure representing M(second_outer; M(first_outer; inner))
                """
            if not isinstance(first_outer, (tuple, list)):
                first_outer = [first_outer]

            first_measure_name = first_outer[0].name
            first_measure_redirect = first_outer[0].inner
            evaluate_first_outer = M(
                first_measure_name,
                first_outer,
                Rowset(star=True),
                inner=self.source,
                redirect=first_measure_redirect
            )

            evaluate_double_outer = M(
                f"double_outer",
                second_outer,
                [],
                evaluate_first_outer
            )

            return evaluate_double_outer

    class BoundFilter(FilterExpression):

        def __init__(self, expression, lineage=None, group_lineage=None):
            super().__init__(expression, lineage=lineage, source=source, group_lineage=group_lineage)

    return BoundMeasure, BoundLibrary(source), BoundFilter

