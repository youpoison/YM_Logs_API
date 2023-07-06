from loguru import logger
import pyodbc
from tqdm import tqdm
import sqlalchemy


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


def create_engine(login: str, password: str, server: str, port: int, database: str):
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
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{login}:{password}@{server}:{port}/{database}?charset=utf8mb4")

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

