from polymeasure import PolyMeasure as M, bound_objects, FilterExpression, Rowset

import duckdb as ddb
import xlwings as XL
from itertools import chain


con = ddb.connect()
# Can download from the interface at https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PSCompPars
con.sql("create or replace table core as select * from 'NASA_Exoplanet_Composite.csv'")
MCore, LibCore, WCore = bound_objects('core')

def to_df(m):
    if isinstance(m, M):
        return con.sql(m.evaluate()).df()
    else:
        return con.sql(m).df()

def compile_column(m, type):
    con.sql(f"alter table {m.inner} drop column if exists {m.name}")
    con.sql(f"alter table {m.inner} add column {m.name} {type}")
    con.sql(m.evaluate(update_columns=True))

description = to_df("describe core")

all_results = to_df(
    MCore(dim=Rowset())
)

facilities = LibCore.count_distinct('disc_facility')
facilities_by_year = to_df(MCore(
    'byclass',
    outer=facilities,
    dim='disc_year'
))

spectral_class = MCore(
    'spec_class_group',
    r"regexp_extract(st_spectype, '([[:alpha:]])')",
    Rowset()
)
classes = to_df(spectral_class)
compile_column(spectral_class, 'varchar')


main_group = 'disc_facility'
subgroups = ['spec_class_group']


def get_prizes_measure(main_group, subgroups) -> (M, M):

    def summary_statistics(column):
        return [
            MCore(f'{fn}_{column}', f"{fn}({column})") for fn in ('count', 'min', 'median', 'max', 'mean', 'var_pop')
        ]

    all_groups = [main_group] + subgroups

    first_summary = MCore(
        outer=[
            M(f"count_within_{main_group}", LibCore.size, include=[main_group])
        ] + summary_statistics('disc_year') +
        summary_statistics('pl_orbper') +
        summary_statistics('pl_bmassj'),
        dim=all_groups,
        order_by=main_group
    )

    second_summary = M(
        '_frequency', f"count_disc_year / count_within_{main_group}", Rowset(), inner=first_summary)

    prize_winners = M(
        'prizes',
        outer=list(chain(*zip(*[
            (
                (f'{x}_with_most_observations', f"arg_max( {x}, _frequency )"),
                (f'{x}_observation_frequency', f"max( _frequency )"),
                (f"{x}_with_biggest_maximal_orbital_period", f"arg_max( {x}, max_pl_orbper )"),
                (f"{x}_with_smallest_avg_mass", f"arg_min( {x}, mean_pl_bmassj )")
            )
            for x in subgroups
        ]))),
        dim=main_group,
        inner=second_summary
    )

    return second_summary, prize_winners


summary_table, prize_winners = get_prizes_measure(main_group, subgroups)
summary_data = to_df(summary_table)
final_prizes = to_df(prize_winners)

competitive_summary_table, more_competitive_competition_prize_winners = get_prizes_measure('disc_year', ['disc_facility'])
more_competitive_competition_prize_winners_table = to_df(more_competitive_competition_prize_winners)
