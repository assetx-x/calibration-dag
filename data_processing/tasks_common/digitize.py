import sqlalchemy as sa
import numpy as np
from collections import defaultdict
from luigi import Task, DictParameter, BoolParameter

from scipy.stats import norm as normal

from base import DCMTaskParams

_meta = sa.MetaData()


class DigitizeTask(DCMTaskParams, Task):
    group_mapping = DictParameter(
        description="Group mapping")
    use_mad_for_std = BoolParameter(
        description="Use MAD for calculating STDDev", default=True)

    def run(self):
        with self.input()["data"].engine() as engine:
            self.add_digitized_columns_given_input_filter(
                self.input()["data"].table,
                engine,
                self.output().table,
                dict(self.group_mapping),
                bool(self.use_mad_for_std)
            )
        self.output().touch()

    def add_digitized_columns_given_input_filter(self,
                                                 source_table,
                                                 engine,
                                                 output_table,
                                                 groups=None,
                                                 use_mad_for_std=False
                                                 ):
        source = table_for(source_table, engine).alias("source")
        group_mapping = prepare_group_mapping(groups)

        self.logger.info("Building query plan")
        query_plan = (
            # Median step (in case of MAD)
            [
                sa.func.median(source.c[column]).over(**mapping["window"]).label(
                    label_for(mapping["digitized_name"], "_median"))
                for column, mappings in group_mapping.items()
                for mapping in mappings
            ] if use_mad_for_std else None,

            # Avg/Stddev step
            [
                sa.func.avg(source.c[column]).over(**mapping["window"]).label(
                    label_for(mapping["digitized_name"], "_avg"))
                for column, mappings in group_mapping.items()
                for mapping in mappings
            ] +
            ([
                 sa.func.median(
                     sa.func.abs(
                         source.c[column] - sa.column(label_for(mapping["digitized_name"], "_median"))) / as_float(
                         normal.ppf(3 / 4.))
                 ).over(**mapping["window"]).label(label_for(mapping["digitized_name"], "_stddev"))
                for column, mappings in group_mapping.items()
                for mapping in mappings
            ] if use_mad_for_std else [
                sa.func.stddev_pop(source.c[column]).over(**mapping["window"]).label(
                    label_for(mapping["digitized_name"], "_stddev"))
                for column, mappings in group_mapping.items()
                for mapping in mappings
            ]),

            # Digitization step
            [source.c[x.name] for x in source.c] +
            [
                sa.case(
                    [(sa.and_(
                        (
                            sa.column(label_for(mapping["digitized_name"], "_avg")) +
                            as_float(normal.ppf(lower)) * sa.column(label_for(mapping["digitized_name"], "_stddev")) <=
                            source.c[column]
                        ),
                        (
                            sa.column(label_for(mapping["digitized_name"], "_avg")) +
                            as_float(normal.ppf(upper)) * sa.column(label_for(mapping["digitized_name"], "_stddev")) >
                            source.c[column]
                        ),
                    ), as_float(i))
                    for i, (lower, upper) in enumerate(mapping["bins"])]
                ).label(mapping["digitized_name"])
                for column, mappings in group_mapping.items()
                for mapping in mappings
            ]
        )

        phase_query = None
        prev_phase_query = None
        for phase_num, phase in enumerate(query_plan):
            if not phase:
                continue
            self.logger.info("Performing digitization phase %d" % phase_num)
            # Psql dialect has an internal 'ctid' column identifying internal order of rows
            # So we select ctid for each of intermediate phases and using it as key to join with next phase
            phase_query = sa.select((["source.id as id", ] if phase_num != len(query_plan) - 1 else []) + phase)

            if prev_phase_query is not None:
                phase_query = phase_query.select_from(
                    source.join(prev_phase_query,
                                sa.literal_column("%s.id" % label_for(phase_num - 1, "phase")) == sa.literal_column(
                                    "source.id"))
                )
            prev_phase_query = phase_query.alias(label_for(phase_num, "phase"))

        sql = str(phase_query.alias("q").compile(compile_kwargs={"literal_binds": True}))

        self.logger.info("Executing 'drop table' query")
        engine.execute("DROP TABLE IF EXISTS \"{}\"".format(output_table))
        create_table_sql = "CREATE TABLE \"{}\" AS {}".format(output_table, sql)

        self.logger.info("Executing 'create table' query")
        return engine.execute(create_table_sql)


def table_for(name, engine):
    """
        :type name:object
        :type engine:sqlalchemy.Engine
    """
    return sa.Table(name, _meta, autoload=True, autoload_with=engine)


def label_for(name, predicate):
    return "%s_%s" % (predicate, name)


def bins_for_cut_points(cut_points):
    np.sort(cut_points)

    return [[cut_points[x], cut_points[x + 1]] for x in range(0, len(cut_points) - 1)]


def window_for(group_columns):
    if group_columns:
        window_kwargs = {
            "partition_by": group_columns,
        }
    else:
        window_kwargs = {}

    return window_kwargs


def digitized_name_for(column_name, group_columns=None, suffix=None):
    if not group_columns:
        group_columns = []

    if suffix is None:
        suffix = '*'.join(['_Digital'] + group_columns)

    return column_name + suffix


def prepare_group_mapping(groups):
    mapping = defaultdict(list)

    for group in groups.values():
        for column in group["columns"]:

            mapping[column].append({
                "digitized_name": digitized_name_for(column, group["group_by"], group["suffix"]),
                "bins": bins_for_cut_points(group["quantiles"]),
                "window": window_for(group["group_by"])
            })

    return dict(mapping)


def as_float(value):
    if value == float("inf"):
        return sa.literal_column("(FLOAT8 '+infinity')")
    elif value == float("-inf"):
        return sa.literal_column("(FLOAT8 '-infinity')")
    else:
        return sa.literal_column(str(value))
