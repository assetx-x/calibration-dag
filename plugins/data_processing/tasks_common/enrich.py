import dateutil.parser
from bisect import bisect_left
from collections import OrderedDict

from datetime import timedelta, date, datetime
from pyspark.sql.types import StructType

from base import DCMTaskParams, PySparkTask


def ensure_datetime(value):
    # TODO: think about unification of date/datetime
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        return dateutil.parser.parse(value)
    else:
        return value


class EnrichTask(DCMTaskParams, PySparkTask):
    def run_spark(self):
        input_df = self.input()["data"].to_spark_dataframe(self.sql_context).cache()

        source_dfs = {
            key: self.input()["sources"][key].to_spark_dataframe(self.sql_context).cache()
            for key in self.input()["sources"]
        }

        enriched_df = input_df
        for enrichment_step in self.plan:
            enriched_df = self._enrich_step(
                enriched_df,
                enrichment_step,
                source_dfs[enrichment_step["source"]],
                enrichment_step["left_on"],
                enrichment_step["right_on"],
                enrichment_step.get("approximate_left_on"),
                enrichment_step.get("approximate_right_on"),
                enrichment_step.get("format", "short")
            )

        self.output().from_spark_dataframe(enriched_df)

    def _enrich_step(self, input_df, enrichment_step, source_df, left_on, right_on,
                     approximate_left_on=None, approximate_right_on=None, format="short"):
        if "filter" in enrichment_step:
            source_df = source_df \
                .filter(enrichment_step["filter"])
        if format == "long":
            # Pivot data
            source_df = source_df \
                .groupBy(enrichment_step["pivot_group_column"]) \
                .pivot(enrichment_step["pivot_column"]) \
                .min(enrichment_step["pivot_value_column"])

        join_name_prefix = "%s_" % enrichment_step["source"]
        input_fields = input_df.schema.fields
        input_fields_names = [field.name for field in input_fields]
        source_fields = source_df.schema.fields
        if "filter_fields" in enrichment_step:
            source_fields = filter(lambda field: field.name in enrichment_step["filter_fields"], source_fields)
        # Rename all conflicting field names
        source_fields_names_map = OrderedDict(
            (source_field.name, (
                join_name_prefix + source_field.name
                if source_field.name.lower() in [f.lower() for f in input_fields_names]
                    else source_field.name
            ))
            for source_field in source_fields
        )
        for source_field in source_fields:
            source_field.name = source_fields_names_map[source_field.name]

        # This is the final schema
        enriched_schema = StructType(input_fields + source_fields)
        print(enriched_schema)

        input_fields_names = self.spark_context.broadcast(input_fields_names)
        source_field_names_map = self.spark_context.broadcast(source_fields_names_map)
        left_on = self.spark_context.broadcast(left_on)
        right_on = self.spark_context.broadcast(right_on)
        approximate_left_on = self.spark_context.broadcast(approximate_left_on)
        approximate_right_on = self.spark_context.broadcast(approximate_right_on)

        if approximate_right_on.value:
            grouped_source_rdd = source_df.rdd \
                .map(lambda row: (
                    tuple(getattr(row, column) for column in right_on.value),
                    (ensure_datetime(getattr(row, approximate_right_on.value)), row)
                )) \
                .groupByKey() \
                .map(lambda (right_values, right_rows): (right_values, sorted(right_rows)))
        else:
            grouped_source_rdd = source_df.rdd \
                .map(lambda row: (tuple(getattr(row, column) for column in right_on.value), row)) \
                .groupByKey()

        grouped_source = self.spark_context.broadcast(grouped_source_rdd.collectAsMap())

        def join(left_row):
            left_values = tuple(getattr(left_row, column) for column in left_on.value)
            right_rows = grouped_source.value.get(left_values, tuple())

            if approximate_right_on.value:
                left_approximate_value = ensure_datetime(getattr(left_row, approximate_left_on.value))
                nearest_right_index = bisect_left(right_rows, (left_approximate_value,)) - 1

                if (nearest_right_index >= 0 and
                    left_approximate_value - right_rows[nearest_right_index][0] < timedelta(days=180)):
                    print("has value")
                    return ((left_row, right_rows[nearest_right_index][1]),)
                else:
                    print("empty value")
                    return ((left_row, None),)
            else:
                return (
                    (left_row, right_row)
                    for right_row in (right_rows or (None,))
                )

        def normalize_rows((left_row, right_row)):
            return tuple(
                getattr(left_row, field) for field in input_fields_names.value
            ) + tuple(
                getattr(right_row, field) if right_row else None
                for field in source_field_names_map.value
            )

        enriched_rdd = input_df.rdd \
            .flatMap(join) \
            .map(normalize_rows)

        return self.sql_context.createDataFrame(enriched_rdd, enriched_schema)