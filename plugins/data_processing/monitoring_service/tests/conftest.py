import pandas as pd
import sqlalchemy as sa


def create_fake_engine(files_for_tables=None):
    if files_for_tables is None:
        files_for_tables = {}

    engine = get_fake_engine()
    for table_name in files_for_tables:
        file_name = files_for_tables[table_name]
        upload_csv_to_db(file_name, table_name, engine)

    return engine


def get_fake_engine():
    return sa.engine.strategies.strategies["plain"].create("sqlite:///")


def upload_csv_to_db(file_name, table_name, engine):
    data_frame = pd.read_csv(file_name, parse_dates=True)
    data_frame.to_sql(table_name, engine, index=False)


def pytest_namespace():
    return {"create_fake_engine": create_fake_engine}
