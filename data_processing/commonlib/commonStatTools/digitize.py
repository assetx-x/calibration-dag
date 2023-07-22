import sqlalchemy as sa
import numpy as np

from scipy.stats import norm as normal

_meta = sa.MetaData()


def add_digitized_columns_given_input_filter(
        source_table,
        engine,
        output_table,
        groups=None,
        use_mad_for_std=False
):
    """
    Digitizes a dataset by binning the selected columns based on defined cut points.  Useful for feeding
        deepImpact results to points further downstream (see TEC-89 and TEC-90).

    The results will be saved into another table which includes all columns of the original, plus the new digitized
    columns, which will by default follow the naming convention of sourceColumnName_Digital*group1*group2*groupN...
    :param source_table: Name of source table providing the data to be digitized.
    :param engine: The SQLAlchemy engine object representing the database connection in which source can be found, and
    output_table will be created
    :param output_table: Name of output table to save results into
    :param groups: Grouping schema i.e: {
       "<desc>": (["<column1>", "<column2>, ...], [<q1> ,<q2>, ...], ["<group_column1>", "<group_columns2>, ...]),
       ...
    }
    :param use_mad_for_std: use MAD as stddev value if True, stddev_pop otherwise
    :return: The result of an engine.execute on a generated CREATE TABLE AS ... query.  The actual results can be
    retrieved separately by reading the output_table.  e.g. pandas.read_sql("SELECT * from output_table", engine).
    This is not done implicitly to avoid unnecessary transfer time and overhead.
    """
    source = table_for(source_table, engine).alias("source")
    group_mapping = prepare_group_mapping(groups)

    query_plan = (
        # Median step (in case of MAD)
        [
            sa.func.median(source.c[column]).over(**mapping["window"]).label(label_for(column, "_median"))
            for column, mapping in group_mapping.items()
        ] if use_mad_for_std else None,

        # Avg/Stddev step
        [
            sa.func.avg(source.c[column]).over(**mapping["window"]).label(
                label_for(column, "_avg"))
            for column, mapping in group_mapping.items()
        ] +
        ([
            sa.func.median(
                sa.func.abs(source.c[column] - sa.column(label_for(column, "_median"))) / as_float(normal.ppf(3/4.))
            ).over(**mapping["window"]).label(label_for(column, "_stddev"))
            for column, mapping in group_mapping.items()
        ] if use_mad_for_std else [
            sa.func.stddev_pop(source.c[column]).over(**mapping["window"]).label(label_for(column, "_stddev"))
            for column, mapping in group_mapping.items()
        ]),

        # Digitization step
        [source.c[x.name] for x in source.c] +
        [
            sa.case(
                [(sa.and_(
                    (
                        sa.column(label_for(column, "_avg")) +
                        as_float(normal.ppf(lower)) * sa.column(label_for(column, "_stddev")) <=
                        source.c[column]
                    ),
                    (
                        sa.column(label_for(column, "_avg")) +
                        as_float(normal.ppf(upper)) * sa.column(label_for(column, "_stddev")) >
                        source.c[column]
                    ),
                ), as_float(i))
                for i, (lower, upper) in enumerate(mapping["bins"])]
            ).label(mapping["digitized_name"])
            for column, mapping in group_mapping.items()
        ]
    )

    phase_query = None
    prev_phase_query = None
    for phase_num, phase in enumerate(query_plan):
        if not phase:
            continue
        # Psql dialect has an internal 'ctid' column identifying internal order of rows
        # So we select ctid for each of intermediate phases and using it as key to join with next phase
        phase_query = sa.select((["ctid", ] if phase_num != len(query_plan) - 1 else []) + phase)

        if prev_phase_query is not None:
            phase_query = phase_query.select_from(
                source.join(prev_phase_query, prev_phase_query.c.ctid == sa.literal_column("source.ctid"))
            )
        prev_phase_query = phase_query.alias(label_for(phase_num, "phase"))

    sql = str(phase_query.order_by(sa.text("source.ctid")).alias("q").compile(compile_kwargs={"literal_binds": True}))
    create_table_sql = "CREATE TABLE {} AS {}".format(output_table, sql)

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
            "order_by": group_columns
        }
    else:
        window_kwargs = {}

    return window_kwargs


def digitized_name_for(column_name, group_columns=None):
    if not group_columns:
        group_columns = []

    return column_name + '*'.join(['_Digital'] + group_columns)


def prepare_group_mapping(groups):
    mapping = {}

    for (columns, cut_points, group_columns) in groups.values():
        for column in columns:
            mapping[column] = {
                "digitized_name": digitized_name_for(column, group_columns),
                "bins": bins_for_cut_points(cut_points),
                "window": window_for(group_columns)
            }

    return mapping


def as_float(value):
    if value == float("inf"):
        return sa.literal_column("(FLOAT8 '+infinity')")
    elif value == float("-inf"):
        return sa.literal_column("(FLOAT8 '-infinity')")
    else:
        return sa.literal_column(str(value))
