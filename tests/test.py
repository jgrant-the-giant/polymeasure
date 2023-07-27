from polymeasure import PolyMeasure as M, bound_objects, FilterExpression, Rowset

B, core_where = bound_objects('core')

import duckdb as ddb

con = ddb.connect()
con.sql("create or replace table core as select * from core.parquet")

df = con.sql(
    B('count_q', 'count(*)', 'qcategory').evaluate()
).df()
df2 = con.sql(
    B('count_q', M('c', 'count(*)'), 'qcategory').evaluate()
).df()