from base import credentials_conf, TaskWithStatus
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


class RavenpackFromS3ToRedshiftTask(TaskWithStatus):

    def __init__(self, *args, **kwargs):
        super(RavenpackFromS3ToRedshiftTask, self).__init__(*args, **kwargs)
        self.pg_engine = sa.create_engine(
            "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                credentials_conf["postgres"]["username"],
                credentials_conf["postgres"]["password"],
                credentials_conf["postgres"]["host"],
                credentials_conf["postgres"]["port"],
                credentials_conf["postgres"]["database"]
            ), echo=True)

        self.redshift_engine = sa.create_engine(
            "redshift+psycopg2://%s:%s@%s:%s/%s" % (
                credentials_conf["redshift"]["username"],
                credentials_conf["redshift"]["password"],
                credentials_conf["redshift"]["host"],
                credentials_conf["redshift"]["port"],
                credentials_conf["redshift"]["database"]
            ), echo=True)

    def collect_keys(self):
        Session = sessionmaker(bind=self.redshift_engine)
        session = Session()
        try:
            session.execute("""
                CREATE TEMPORARY TABLE ravenpack_import
                (LIKE ravenpack_equities);""")

            session.execute("""
                COPY ravenpack_import
                FROM '%s'
                WITH CREDENTIALS AS
                'aws_access_key_id=%s;aws_secret_access_key=%s'
                dateformat 'auto'
                timeformat 'auto'
                ignoreheader 1
                delimiter ','
                acceptinvchars
                blanksasnull
                emptyasnull
                csv;""" % (self.input()["s3_file"]["s3_file"].path,
                           credentials_conf["s3"]["access_key"],
                           credentials_conf["s3"]["secret_key"]))

            session.execute("""
                DELETE FROM ravenpack_equities
                USING ravenpack_import
                WHERE
                ravenpack_equities.rp_story_id=ravenpack_import.rp_story_id
                AND
                ravenpack_equities.rp_entity_id=ravenpack_import.rp_entity_id;
                """)

            session.execute("""
                INSERT INTO ravenpack_equities
                SELECT * FROM ravenpack_import;""")

            session.query("DROP TABLE ravenpack_import;")

            session.commit()
        except:
            session.rollback()
            raise
