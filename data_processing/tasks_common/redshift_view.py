from luigi import Task, Parameter


class RedshiftViewTask(Task):
    query = Parameter(description="View query")

    def run(self):
        with self.output().engine() as engine:
            engine.execute("DROP TABLE IF EXISTS %s" % self.output().table)
            engine.execute("CREATE TABLE %s AS %s" % (
                self.output().table,
                self.query.format(**{
                    id: input.table
                    for id, input in self.input().items()
                })
            ))
        self.output().touch()
