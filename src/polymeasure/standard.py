from .core import PolyMeasure as M, FilterExpression, Rowset

count_all = M('countall', outer='count(*)', dim=[], include=[])
count_rows = M('countrows', outer='count(*)')


def constant(c):
    """
    Args:
        c: Numeric or string literal.
    Returns: A polymeasure which always evaluates as c.
    """
    return M('__a_constant', c, dim=[], suppress_from=True)


def string_column_sample(name, columns, inner, sample_size=5, sort_by=count_rows):
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
        count_all,
        inner=M('xyz', dim=columns, inner=inner)
    )


def double_outer(second_outer, first_outer, inner):
    """
    Evaluates the secound_outer measure over the rowset of the first_outer measures over the inner measure.
    Assumes the first_outer measures have the same inner measure, and the secound_outer measure is free.
    Args:
        second_outer: outermost free measure to evaluate over the result of M(first_outer; inner)
        first_outer: inner measures to evaluate over inner
        inner: Specific inner polymeasure.

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
