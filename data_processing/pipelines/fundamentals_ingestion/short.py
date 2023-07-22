from uuid import uuid4

import pandas as pd
import sqlalchemy as sa
from luigi import Task
from sqlalchemy.schema import CreateTable

# this import looks like not being used but it is needed for monkey patching pandas DataFrame
# noinspection PyUnresolvedReferences
import to_redshift
from base import DCMTaskParams, credentials_conf


class FundamentalsShortTask(DCMTaskParams, Task):
    def run(self):
        with self.input()["sector_mapping"].engine() as engine:
            sector_mapping_df = pd.read_sql(self.input()["sector_mapping"].table, engine)
        with self.input()["long"].engine() as engine:
            long_df = pd.read_sql(self.input()["long"].table, engine)

        self.logger.info("Performing pivot on fundamentals_long data frame")
        short_df = pd.pivot_table(long_df, values="value", columns="metric", index=["ticker", "date"], dropna=False).reset_index()
        # short_df = long_df[long_df.as_of_end.isnull()].pivot(columns="metric", values="value",
        #                                                      index=["ticker", "date"]).reset_index()

        self.logger.info("Merging short data frame with sector mapping")
        short_df = pd.merge(short_df, sector_mapping_df, how="left", on="ticker")
        self.logger.info("Applying quarter column")
        short_df["quarter"] = short_df.apply(lambda row: "%d-%d" % (row["date"].year, row["date"].quarter), axis=1)
        self.logger.info("Applying id column")
        short_df["id"] = short_df.apply(lambda row: str(uuid4()), axis=1)

        self.logger.info("Saving the resulting short data frame to database")
        with self.output().engine() as engine:
            short_df.to_redshift(
                self.output().table,
                engine,
                "dcm-data-temp",
                index=False,
                if_exists="replace",
                aws_access_key_id=credentials_conf["s3"]["access_key"],
                aws_secret_access_key=credentials_conf["s3"]["secret_key"]
            )
        self.output().touch()


class FundamentalsShortDigitizedJoinTask(DCMTaskParams, Task):
    def run(self):
        with self.output().engine() as engine:
            date_table = self.input()['date'].table
            sector_date_table = self.input()['sector_date'].table

            self.logger.info("Reflecting input tables")
            meta = sa.MetaData()
            meta.reflect(engine, only=[date_table, sector_date_table])

            command_queue = []
            out_table_name = self.output().table
            backup_table = out_table_name + '_backup'
            # command_queue.append(("Dropping backup table", ""'drop table if exists %s' % backup_table))
            # command_queue.append(
            #     ("Backing up output table", 'create table %s as select * from %s' % (backup_table, out_table_name)))
            command_queue.append(("Dropping output table", ""'drop table if exists %s' % out_table_name))

            in_columns1 = [c for c in meta.tables[date_table].columns]
            in_columns2 = [c for c in meta.tables[sector_date_table].columns if c.name.endswith('_sector_date')]
            all_columns = [sa.Column(c.name, c.type) for c in in_columns1 + in_columns2]

            # take all columns from input tables, names and data types, and apply to the new table
            out_table = sa.Table(self.output().table, sa.MetaData(bind=None), *all_columns, schema=None)
            command_queue.append(("(Re)creating output table", str(CreateTable(out_table).compile(engine))))

            out_columns = [c.name for c in all_columns]
            in_columns = map(lambda x: 't2.' + x if x.endswith('_sector_date') else 't1.' + x, out_columns)
            out_columns = ', '.join(out_columns)
            in_columns = ', '.join(in_columns)

            join_cmd = '''
                insert into {out_table} ({out_columns})
                ( select {in_columns}
                  from {in_table1} as t1 inner join {in_table2} as t2
                  on t1.ticker = t2.ticker and t1.date = t2.date
                )
            '''.format(out_columns=out_columns, in_columns=in_columns,
                       out_table=out_table_name, in_table1=date_table, in_table2=sector_date_table)
            command_queue.append(("Joining input tables into output", join_cmd))

            with engine.begin() as connection:
                for statement in command_queue:
                    self.logger.info(statement[0])
                    connection.execute(statement[1])

        self.output().touch()
