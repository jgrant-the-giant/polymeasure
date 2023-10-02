from polymeasure import PolyMeasure as M, bound_experimental as bound_objects, FilterExpression, Rowset, Dimension
from polymeasure.core import make_statement, make_as_list

import duckdb as ddb
from itertools import chain

# Create a new duck db connector and import csv data for analysis as a single table called core

# The source file comes from the NASA interface at
# https://exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=PSCompPars
# More generally one can work with a view, or a constellation of views, in memory or over parquets

con = ddb.connect()
con.sql("create or replace table core as select * from 'NASA_Exoplanet_Composite.csv'")

# Bind three objects to the core table/view -
# Core.M is a PolyMeasure with inner = core
# Core is a container class library of standard PolyMeasures with inner = core (where appropriate)
# Core.W is a FilterExpression class with inner = core
Core = bound_objects('core')

# Create a method to simplify converting strings or PolyMeasures into frames
def to_df(m):
    if isinstance(m, M):
        return con.sql(m.evaluate()).df()
    else:
        return con.sql(m).df()

# A measure evaluation can be overloaded to generate an update statement instead
# This method makes new columns by specifying a measure and a type string
def compile_column(measure: M, type: str):
    con.sql(f"alter table {measure.inner} drop column if exists {measure.name}")
    con.sql(f"alter table {measure.inner} add column {measure.name} {type}")
    con.sql(measure.evaluate(update_columns=True))

# So much astronomy I totally know what these columns do
description = to_df("describe core")

# A simple PolyMeasure with dim = Rowset() gives the whole dataset
all_results = to_df(
    Core.M(dim=Rowset())
)

# Let's use regex to grab the spectral class group from the spectral type column
# And materialise that column in our core table
# We use regex here because it makes you look pro
spectral_class = Core.M(
    'spec_class_group',
    r"upper(regexp_extract(st_spectype, '([[:alpha:]])'))",
    Rowset()
)
# We have to specify the data type like peasants - schema integration is going to have to wait
compile_column(spectral_class, 'varchar')

# Sample some rows and columns
some_results = to_df(
    Core.M(
        dim=Rowset(['st_spectype', 'spec_class_group'] + list(description.column_name.values[:30])),
        postfix="using sample 50"
    )
)

# Normal analysis calls for grouping by dimensions and applying calculations
# Core has usual suspects with a twist, like count distinct over many dimensions:
discovery_subtypes = Core.count_distinct(['discoverymethod', 'disc_locale'])
total_co_discovered = (
    Core.size
    .add_where("disc_facility in ('Multiple Observatories', 'Multiple Facilities')")
    .rename('total_co_discovered')
)
discoveries_by_year = to_df(Core.M(
    'byclass',
    outer=[
        discovery_subtypes,
        total_co_discovered
    ],
    dim='disc_year'
))

# Some kind of worldwide astronomy body wants the observatories of the world to hand out some prizes to the
# spectral classes of the host stars for the various exoplanets they've been discovering.

# Grouping by facility..
main_group = 'disc_facility'
# Do some calculations over the spectral class and return the winners
subgroups = ['spec_class_group']

# Now we will create a function that returns two PolyMeasures based on these groups:

def get_prizes_measure(main_group, subgroups, where=None) -> (M, M):
    """
    Given main and subgroup, return PolyMeasures representing a pre-calculations table and prizes summary table
    :param main_group:
    :param subgroups:
    :return: (pre-calculations, prize summary)
    """

    # Synthesise some measures. This should be in Core that's a great idea.
    # These measures are called similar to "min_orbital_period"
    # and represent min(orbital_period) in SQL
    def summary_statistics(column) -> [M]:
        return [
            Core.M(f'{fn}_{column}', f"{fn}({column})") for fn in ('count', 'min', 'median', 'max', 'mean', 'var_pop')
        ]

    all_groups = [main_group] + subgroups

    pre_calc_1 = Core.M(
        outer=(
            # In addition to the summary statistics add in a calculation of total observations for the main_group
            # So we can calculate frequency at the row level in the next step
            # The include parameter filters the joins generated by dim=all_groups (it filters the filters)
            [M(f"count_within_{main_group}", Core.size, include=[main_group])] +
            summary_statistics('disc_year') +
            summary_statistics('pl_orbper') +
            summary_statistics('pl_bmassj')
        ),
        dim=all_groups,
        order_by=main_group,
        where=where
    )

    pre_calc_final = M(
        '_frequency', f"count_disc_year / count_within_{main_group}", Rowset(), inner=pre_calc_1)

    # Using the summary statistics, retrieve the winning subgroups:
    # Most Frequently Discovered Exoplanet, Biggest Maximal Period and Smallest Average Mass Awards

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
        inner=pre_calc_final
    )

    return pre_calc_final, prize_winners


summary_table, prize_winners = get_prizes_measure(main_group, subgroups)
summary_data = to_df(summary_table)
final_prizes = to_df(prize_winners)

# Afterwards it is suggested that we should be grouping by year and assigning the awards to the facilities instead
# Restricting to ground observatories and avoiding co-discovered planets:
by_year_data, by_year_prizes = get_prizes_measure(
    'disc_year', ['disc_facility'],
    where=["disc_locale='Ground'", "disc_facility not in ('Multiple Observatories', 'Multiple Facilities')"])
yearly_prizes = to_df(by_year_prizes)

# At the end of it all is a SQL string that can be given to another template:
print(by_year_prizes.evaluate(pretty=True))
# And by_year_prizes can be passed to other PolyMeasures for further aggregations

# This demo needs to be expanded to include:
# FilterExpressions that generate auxiliary sub queries (bonkers powerful)
# Transfer of column lineage between inner views (making a measure look like a column to join on)
# But that has to wait for the refactoring v0.2.0
p = Core.size
print(Core.M(outer=Core.size, dim=['disc_facility']).add_where(Core.W("disc_year=2023")).evaluate())
to_df(Core.M(
    outer=Core.size,
    dim=['disc_facility'],
    where=[
        Core.W("disc_year=2023"), "disc_locale='Ground'", "disc_facility not in ('Multiple Observatories', 'Multiple Facilities')"],
    order_by='size desc'))