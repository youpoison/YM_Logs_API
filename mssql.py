from loguru import logger
import pyodbc
import bd_config
from tqdm import tqdm
import query
import pandas as pd
import sqlalchemy
import datetime


def connect(server, bd, login, password):
    try:
        conn = pyodbc.connect(f'DRIVER={{SQL Server}};'
                              f'SERVER={server};'
                              f'DATABASE={bd};'
                              f'UID={login};'
                              f'PWD={password}')
        return conn
    except pyodbc.Error as err:
        logger.error(f'Ошибка при подключении к БД MSSQL с помощью ракета pyodbc. {err}')
        logger.warning(f'Скрипт завершил работу')
        exit()


def create_engine(login: str, password: str, server: str, database: str):
    """
    Создает объект для ваизмодействия с БД
    :param login:
    :param password:
    :param server:
    :param database:
    :return:
    """
    '''engine = sqlalchemy.create_engine(f'mssql+pyodbc://{login}:'
                                      f'{password}@{server}/{database}'
                                      f'?driver=ODBC+Driver+17+for+SQL+Server') # , echo=True'''
    engine = create_engine(f"mysql+pymysql://{login}:{password}@{server}/{database}?charset=utf8mb4")

    return engine


def chunker(seq, size):
    try:
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    except ValueError as err:
        logger.warning(f'DataFrame is empty. Script exit. chunksize = {size}')
        logger.info("Done!")
        exit()


def insert_with_progress(cursor, engine, conn, database, shema, table, df):
    try:
        cursor.execute(f'TRUNCATE TABLE {database}.{shema}.{table}')
        conn.commit()
        logger.info('Connect close')
        chunksize = int(len(df) / 10)
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df, chunksize)):
                cdf.to_sql(table,
                           con=engine,
                           schema='dbo',
                           if_exists='append',
                           index=False,
                           chunksize=20,
                           method='multi')
                pbar.update(chunksize)
                tqdm._instances.clear()
        logger.info(f'Dataframe is written to the table {table}')
    except Exception as err:
        logger.error(f'{err}')
        logger.warning(f'Скрипт завершил работу')
        exit()


def get_max_date(conn, database, shema, table, column, default_date):
    try:
        mssql_max_date = pd.read_sql(
            query.mssql_max_date(database,
                                 shema,
                                 table,
                                 column),
            conn)
        if mssql_max_date.iloc[0, 0] is None:
            mssql_max_date = pd.DataFrame([default_date])
            logger.warning(f'Max date empty in {table}. The date is set by default. Date = {mssql_max_date.iloc[0, 0]}')
        logger.info(f'Max date was received successfully from {table}. Date = {mssql_max_date.iloc[0, 0]}')
        return mssql_max_date.iloc[0, 0]
    except pd.io.sql.DatabaseError as err:
        logger.error(f'Ошибка при получении максимальной даты с помощью пакета sqlalchemy. {err}')
        logger.warning(f'Скрипт завершил работу')
        exit()


def get_max_date_log(query, log_depth):
    conn = connect(bd_config.SERVER,
                   bd_config.DATABASE_MyTracker,
                   bd_config.USERNAME,
                   bd_config.PASSWORD)
    max_date = pd.read_sql(query, conn)
    logger.info(f'Get max date log {max_date.iloc[0, 0]}')
    if max_date.iloc[0, 0] is None:
        max_date = pd.DataFrame([datetime.datetime.now() - datetime.timedelta(days=log_depth)])
        logger.info(f'Get max date log {max_date.iloc[0, 0]}')
        return max_date.iloc[0, 0]
    return max_date.iloc[0, 0]


def insert_data(engine, conn, cursor, database, shema, table, dataframe):
    cursor.execute(f'TRUNCATE TABLE {database}.{shema}.{table}')
    conn.commit()
    dataframe.to_sql(table, con=engine, schema=shema, if_exists='append', index=False)


def run_proc(cursor, conn, database, schema, proc):
    try:
        cursor.execute(f'exec {database}.{schema}.{proc}')
        logger.info(f'Proc {proc} run')
        conn.commit()
        logger.info('Connect close')
    except Exception as err:
        logger.error(f'{err}')
        logger.warning(f'Скрипт завершил работу')
        exit()


def write_log(dataframe):
    log_engine = create_engine(bd_config.USERNAME, bd_config.PASSWORD, bd_config.SERVER,
                               bd_config.DATABASE_LOG)
    dataframe.to_sql(bd_config.LOG_TABLE,
                     con=log_engine, schema=bd_config.DATABASE_SHEMA, if_exists='append', index=False)


def get_data(conn, query):
    data = pd.read_sql(query, conn)
    return data


def update_log(cursor, conn, query):
    cursor.execute(query)
    conn.commit()
