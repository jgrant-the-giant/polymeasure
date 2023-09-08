# polymeasure

> [!WARNING]
> This repo is pre-alpha, I am currently refactoring the codebase so it isn't insane, and there are a few bugs for more complex cases that aren't going away until that's done :D

The current "release" can still do cool things but don't do anything practical with it just yet..

## A python analysis package for building SQL expressions

A PolyMeasure (or measure) represents a SQL "group by one view, evaluate over another" statement, with options
for supplying the usual SQL operators or modifying the evaluation in other programmatic ways.
The class provides a framework for creating analysis expressions over SQL database systems, in particular the
duckdb SQL engine.

A PolyMeasure has four main components

- an inner SQL view, also a PolyMeasure
- a list of outer PolyMeasures to evaluate over that view,
- a list of dimensions to group the inner view by, and
- a list of FilterExpression objects representing filters

The function `evaluate()` returns a SQL statement, built recursively from those components. Passing this SQL expression
to a capable SQL engine allows the user to rapidly compute very complex calculations, for use in processing or visualisation.
Post-aggregation options like order by.. and having.. can be supplied as arguments to the PolyMeasure or evaluate() call.
Where clauses are created by abstract python `FilterExpression` objects that create other auxiliary views.

In this documentation we drop the name parameter and write

`M( Outer * Dim; Inner: Where )`

when describing a PolyMeasure, in line with the order of the other parameters of importance.

## How it works in a nutshell

In pseudo-sql, `M( Inner * Dim; Outer; Where )` evaluates as:

```
  with __VIEW__ as ( [select [Inner] from view([Inner])] )
  
  select [(
      select [Outer] from (view(Outer) OR __VIEW__) dynamic_view
      where dynamic_view.dim(Outer) = __VIEW__.dim(Outer)
  )], Dim
  
  from __VIEW__
  where Where
  group by Dim
```

There is a simpler presentation when Dim = Rowset(G):

```
  with __VIEW__ as ( [select [Inner] from view([Inner])] )
  
  select [Outer], __VIEW__.G
  
  from __VIEW__
  where Where
```

## How it works


There are two classes of PolyMeasures - those with a free view, where Outer = None, or a bound view
which already exists in the database.
A bound measure with the usual arguments will be denoted `B( Inner * Dim; Outer: Where )`
and a free measure denoted `F( Inner * Dim; Outer: Where )`. Free views become handy expressions like
"count(*) of any inner table" or "count(*) over dimensions of the inner table".

Free measures take on the context of the inner query in a PolyMeasure, rather than referencing a basic
view in the schema. This allows a single PolyMeasure object to perform a double aggregation over a given
view, where we group once, then group the resulting row set again and perform some aggregation.

If a free measure is supplied to the outer measure list, it will query the corresponding inner measure list.
Free measures should not be supplied as the inner measure of a PolyMeasure, unless the outer measures can supply
their own inner bindings.

Bound measures should have a default string value for the view, appropriate to the schema.
Using the supplied factory method bound_objects will return a super-classed PolyMeasure and FilterExpression objects
keyed to a specific view, for convenience.

In the following examples, we assume one unique bounded measure exists,
B = B(dim=Rowset(), inner=main_view), which acts as the ultimate "leaf" measure.
The class Free is also provided, which defaults the measure view to Free instead, and is aliased F.

The Outer and Inner arguments are PolyMeasure vectors, which each can return tables of any dimension.
When different dimensions are supplied in the outer measures, self.dim is used to group the inner view,
but each measure still evaluates by matching only on its own dimension, if specified.
No attempt is made to reconcile the dimensions between inner and outer arguments - if a measure requests
a column that doesn't exist in dim(M) (the inner dimension specified by M), then the SQL parser
will let you know :)

When supplied as an argument, a tuple is interpreted as a "Dummy PolyMeasure" (name, outer, [dim]),
where dim can be omitted. These measures are always free.

When defining dimensions, [] means group by the whole table,
RowSet means do not group the view at all. None is a wildcard, which means the measure will default
to the dimensions of the enclosing measure after dimension promotion. None and [] will behave identically in most cases.

The view and where parameters are applied to the inner query, Inner * Dim.
For example if you want to apply a filter context to Outer directly (which is the final output aggregation),
use M( Inner * Dim; F(Outer: Where_Outer): View, Where)

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
