# polymeasure

> [!WARNING]
> This repo is pre-alpha, I am currently refactoring the codebase so it isn't insane, and there are a few bugs for more complex cases that aren't going away until that's done :D

The current "release" can still do cool things but don't do anything practical with it just yet..

> [!WARNING]
> This documentation reflects future updates to the code that do not exist in core.py in this branch.
> core_experiment.py is work in progress and the patient there is missing vital organs.

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
Where clauses are created by abstract python `FilterExpression` objects that can also leverage their own subexpressions.

## Installation

`python -m pip install polymeasure` with a dependency on sqlparse if you want to prettify the resulting SQL

## Why code this

Partly because Malloy's marketing didn't reach me in time, and partly because I'm a fan of doing things myself, and writing SQL myself.
PolyMeasures began as a wrapper class to help me filter views so that I could create user-driven analytical apps in Streamlit for the biz.

As a PowerBI developer working in the DAX language I nurtured an appreciation for functionally-programmed metrics that can

 - computer advanced, multi-aggregation-step analytics..
 - over arbitrary user-defined filter contexts.

What I did not enjoy was being severed from my code repository and my nice neat IDE, comments, version control, code hyperlinking linting and
probably most of all, debugging. And Streamlit, Plotly and co. can produce astonishing visualisations (I like dense visuals), 
inside bootstap-templated, gorgeous and mostly very responsive data applications (being very selective with the cross-filtering functionality).

But performance is important and nested pandas transformations won't fly - even 100k rows will trip up a double (or single) call to pandas.apply() if the logic is heavy enough.

So what I thought might be better is creating a shell python class (`PolyMeasure`) (or 2 or 5 or 3 or..) to stitch SQL together in a rudimentary way, with filter-like objects
(`FilterExpression`) that will mimic the functionality of "context-control" that features so heavily in DAX. 
Then the steps to build a report locally are 

1. Draw a chunk of your precious business schema into parquets. Do this incrementally.
2. Load this information into a DuckDB in-memory instance `connection`
3. Write the analysis logic using the PolyMeasures, including building other views and calculated columns.
   A single PolyMeasure (say org_kpi_A_results) may represent a table, which is a pivot whose values are other (outer) PolyMeasures
5. Create an interface that allows the user to define filters over the columns in the view, which generates a list of FilterExpressions `user_filters`
6. Graph, tabulate or transmit the results of:
```
connection.sql(
  org_kpi_A_results.evaluate(
    where=user_filters))
```

The PolyMeasure then folds the business kpi logic and filters into SQL in a naive but sufficiently performant way.

What I discovered was that it worked very well even in prototype form, and that it allowed for some very powerful expressions, especially when leveraging DuckDB's analytics functions.
I was soon able to replace more intricate, dataframe-based transformation pipelines with far more expressive PolyMeasures that were ultimately just very fancy wrappers around nested SQL queries. 

## How it works in a nutshell

As a simple example, start in Python with an SQL connection `sqlcon` to a schema containing a single view or table named 'core'.
Import some things and "bind" three helper objects to this view:

```
from polymeasure import PolyMeasure as M, FilterExpression, Rowset, bound_objects
MCore, LibCore, WCore = bound_objects('core')
```

The three objects are a measure object, a library of common measures and a filter object (all targeted to the named view, core)

Here is a summary table over some `dimensions`:

```
measure1 = MCore(dim=[dimensions])
materialised_table1 = sqlcon.sql(table1.evaluate()).df()
```

With cardinality and a distinct count of "something"
```
measure2 = MCore(
 outer=[LibCore.size, ("a_distinct_count_of_something", "count(distinct something)")],
 dim=[dimensions]
)
materialised_table2 = sqlcon.sql(measure2.evaluate()).df()
```

A measure that returns the maximum distinct count of something over measure2:

```
measure3 = M(
 'max_distinct_something_over_dimensions'
 outer='max(a_distinct_count_of_something)',
 inner=measure2
)
```

Finally, a measure that evaluates measure3 over a new grouping set `dimensions2`, after applying a filter on column_a:
```
measure4 = M(
 outer=measure3,
 dim=dimensions2,
 where=WCore("column_a <> 'ignore_this_record'")
)
```

For a more detailed implementation see [test](demo/test.py).

> [!WARNING]
> Work In Progress, some of the below is not relevant now..

## How it builds sql

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