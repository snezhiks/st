#pip freeze > requirements.txt    #запись окружения
#pip install -r requirements.txt    #восстановление окружения

import zipfile    #распаковка
import os    #териминальные команды
import logging    #для логирования процессов
from datetime import datetime

## 0. Логирование процессоов
LOG_DIR = 'logs'  # папка для логов
os.makedirs(LOG_DIR, exist_ok=True)  # создаём папку, если её нет

# Формат имени файла лога: logs/main_2023-10-15.log
log_filename = os.path.join(LOG_DIR, f'main_{datetime.now().strftime("%Y-%m-%d")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(funcName)s | %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),  # запись в файл
        logging.StreamHandler()  # вывод в консоль
    ]
)
logger = logging.getLogger(__name__)

## 1. распаковываем архив
def unzip_file(zip_path, extract_to=None):
    '''
    Распаковывает ZIP-архив в указанную папку.
    Если extract_to не указан, создается папка c имененм архива.
    '''

    if extract_to is None:
        extract_to = os.path.splitext(zip_path)[0]    # имя без .zip

    # Создаем папку для распаковки, если её нет
    os.makedirs(extract_to, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        logger.info(f'Архив {zip_path} распакован в {extract_to}') 

#если надо распаковать
#unzip_file('data.zip')

## 2. устанавливаем подключение к базе
# pip install python-dotenv
# pip install psycopg2
from dotenv import load_dotenv    #установка файла
import psycopg2    #для подключения к базе
load_dotenv()    #прогрузка параметров из файла

def connection_postgres():
    '''
    Функция устанавливает подключение к базе:
    conn, cursor = connection_postgres()
    '''
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user=os.getenv('database_user'),
            password=os.getenv('database_password'),
            port=5432
        )
        cursor = conn.cursor()
        cursor.execute('select version()')
        record = cursor.fetchone()
        #logger.info(f'Вы подключились к: {record} ')

        return conn, cursor

    except Exception as e:
        logger.error(f'Ошибка подключения: {e}')

def close_connection_postgres(conn, cursor):
    '''
    Функция закрывает подключение к базе
    '''
    if cursor:
        cursor.close()
    if conn and not conn.closed:
        conn.close()
    #logger.info('Соединение с БД закрыто')

#close_connection_postgres(conn, cursor)


## 3. Создание схемы
def create_cschema(schema_name='new_schema'):
    '''
    Создание схемы для последующей загрузки таблиц.
    Специально нет проверки на наличие схемы,
    чтобы не было перемешивание с другими таблицами (не из проекта).
    В случае ошибки, выберите другое имя схемы.
    '''
    conn, cursor = None, None

    try:
        conn, cursor = connection_postgres()
        cursor.execute(f'CREATE SCHEMA {schema_name}')
        conn.commit()
        logger.info(f'Схема {schema_name} успешно создана')
    
    except Exception as e:
        logger.error(f'{e}')

    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

## 4. Загрузка таблиц из файла ddl_dml.sql
def sql2sql(file_name, schema_name):
    '''
    Функция для первоначальной прогрузки в postgres
    '''
    conn, cursor = None, None

    try:
        conn, cursor = connection_postgres()
        with open(file_name, 'r', encoding='utf-8') as file:
            sql_script = file.read()

        cursor.execute(f'SET search_path TO {schema_name}')
        cursor.execute(sql_script)
        conn.commit()
        logger.info(f'Файл {file_name} успешно выполнен')
        
    except Exception as e:
        logger.error(f'Ошибка выполнения sql-файла {file_name}: {e}')
        conn.rollback()
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

"""
### 4.1. создание представления, для удобства
def create_v_():
    '''
    Создание/перезапись представления
    '''
    conn, cursor = None, None

    try:
        conn, cursor = connection_postgres()
        cursor.execute(f'''
        create or replace view banking.view_clients_accounts_cards as
        select 
            c.card_num,
            c.account,
            a.valid_to as account_valid_to,
            c.create_dt,
            c.update_dt,
            a.client,
            concat(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) as full_name,
            cl.date_of_birth,
            cl.passport_num,
            cl.passport_valid_to,
            cl.phone
        from banking.cards c 
        full join banking.accounts a
        on c.account = a.account
        full join banking.clients cl
        on a.client = cl.client_id;
        ''')
        conn.commit()
        print(f'Представление создано или перезаписано')
    
    except Exception as e:
        print(f'{e}')

    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor) """

## 5. Загрузка таблиц из .xlsx
# pip install pandas
# pip install sqlalchemy
# pip install openpyxl

import pandas as pd
import openpyxl
from sqlalchemy import create_engine


dsn = "postgresql://{user}:{password}@localhost:5432/postgres".format(
    user=os.getenv('database_user'),
    password=os.getenv('database_password'))

schema_name = 'banking'

def safe_load_to_sql(df, table_name, schema_name='banking', if_exists='replace'):
    '''
    Функция для прогрузки df в sql с явным зарытием соединения.
    '''
    engine = create_engine(dsn)

    try:
        with engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists=if_exists, index=False)
        logger.info(f'Успешно загружено: {schema_name}.{table_name}')
    
    except Exception as e:
        logger.error(f'Ошибка прогрузки {schema_name}.{table_name}: {e}')
        raise
    finally:
        engine.dispose()

def xlsx2sql(path, table_name='new_table', schema_name='banking'):
    df = pd.read_excel(path)
    safe_load_to_sql(df, table_name, schema_name)
    move_to_archive(path)
    return path

def txt2sql(path, table_name='new_table', schema_name='banking', if_exists='replace') :
    dtype_mapping = {
        'transaction_id': str,
        'card_num': str,
        'oper_type': str,
        'oper_result': str,
        'terminal': str
    }
    df = pd.read_csv(
        path,
        sep=';',
        dtype=dtype_mapping,
        parse_dates=['transaction_date'],
        # Обрабатываем amount отдельно: заменяем запятые на точки
        converters={'amount': lambda x: float(x.replace(',', '.'))})

    df['transaction_date'] = pd.to_datetime(
        df['transaction_date'],
        #format='%Y-%m-%d %H:%M:%S',
        errors='coerce')

    safe_load_to_sql(df, table_name, schema_name, if_exists)
    move_to_archive(path)
    return path

## 6. архивация файлов:
import shutil

def move_to_archive(file_path, archive_folder='archive'):
    # если нет, создаем папку
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    
    # Новое имя: исходный_путь.backup
    filename = os.path.basename(file_path)
    new_name = f'{filename}.backup'
    destination = os.path.join(archive_folder, new_name)

    # Перемещаем файл
    shutil.move(file_path, destination)
    logger.info(f'Файл перемещен в архив: {destination}')

from pathlib import Path

def clear_archive_folder(folder_path):
    """
    Полностью очищает папку (включая подпапки).
    """
    path = Path(folder_path)
    if not path.exists():
        logger.info("Папка не существует")
        return

    for item in path.iterdir():
        if item.is_file():
            item.unlink()  # Удаляем файл
        elif item.is_dir():
            shutil.rmtree(item)  # Удаляем папку с содержимым
    logger.info(f"Папка {folder_path} очищена")

def clear_and_remove_archive_folder(folder_path):
    """
    Удаляет папку целиком (включая все содержимое и саму папку).
    """
    path = Path(folder_path)
    if not path.exists():
        logger.info("Папка не существует")
        return

    try:
        shutil.rmtree(path)  # Удаляем папку целиком с содержимым
        logger.info(f"Папка {folder_path} и всё её содержимое удалены")
    except PermissionError:
        logger.error(f"Ошибка: нет прав для удаления папки {folder_path}")
    except Exception as e:
        logger.error(f"Произошла ошибка при удалении папки: {e}")

## 4.9 Создание table_hist
"""
def create_passport_hist():
    '''
    Создание table_hist + перезапись v_table
    '''
    conn, cursor = None, None

    try:
        conn, cursor = connection_postgres()
        cursor.execute('''
            CREATE TABLE if not exists banking.passport_blacklist_hist(
                id serial primary key,
                date date not null,
                passport varchar(128),
                deleted_flg integer default 0,
                start_dttm timestamp default current_timestamp,
                end_dttm timestamp default ('5999-12-31 23:59:59'::timestamp))
        ''')
        
        cursor.execute(''' 
            create or replace view banking.v_passport_blacklist as
                select
                    *
                from banking.passport_blacklist_hist
                where deleted_flg = 0
                and current_timestamp between start_dttm and end_dttm
        ''')
        conn.commit()

    except Exception as e:
        print(f'{e}')

    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def create_terminals_hist():
    '''
    Создание table_hist + перезапись v_table
    '''
    conn, cursor = None, None

    try:
        conn, cursor = connection_postgres()
        cursor.execute('''
            CREATE TABLE if not exists banking.terminals_hist(
                id serial primary key,
                terminal_id varchar(128),
                terminal_type varchar(128),
                terminal_address varchar(128),
                deleted_flg integer default 0,
                start_dttm timestamp default current_timestamp,
                end_dttm timestamp default ('5999-12-31 23:59:59'::timestamp))
        ''')
        
        cursor.execute(''' 
            create or replace view banking.v_terminals as
                select
                    *
                from banking.terminals_hist
                where deleted_flg = 0
                and current_timestamp between start_dttm and end_dttm
        ''')
        conn.commit()

    except Exception as e:
        print(f'{e}')

    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

#create_terminals_hist()
"""

def create_scd2_structure(table_name, schema, columns_definition):
    """
    Универсальная функция для создания SCD2 структуры
    table_name: имя базовой таблицы (без _hist)
    schema: схема БД
    columns_definition: SQL‑описание полей (кроме системных)
    """
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()

        # Создание исторической таблицы
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name}_hist (
                id SERIAL PRIMARY KEY,
                {columns_definition},
                deleted_flg INTEGER DEFAULT 0,
                start_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_dttm TIMESTAMP DEFAULT ('5999-12-31 23:59:59'::TIMESTAMP)
            )
        """
        )

        # Создание представления
        cursor.execute(f"""
            CREATE OR REPLACE VIEW {schema}.v_{table_name} AS
            SELECT *
            FROM {schema}.{table_name}_hist
            WHERE deleted_flg = 0
                AND CURRENT_TIMESTAMP >= start_dttm
                AND CURRENT_TIMESTAMP < end_dttm
        """
        )

        conn.commit()
        logger.info(f"SCD2 структура для {table_name} создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании SCD2 для {table_name}: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

# 5.1 Обновление таблиц

def create_new_rows(table_name, schema_name='banking'):
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()
        query_columns = ',\n'.join([f't1.{column}' for column in COLUMNS])
        join_conditions = ' and '.join([f't1.{key} = t2.{key}' for key in KEY_COLUMNS])

        cursor.execute(f'''
            create table {schema_name}.tmp_{table_name}_new_rows as
                select 
                    {query_columns}
                from {schema_name}.tmp_{table_name} t1
                left join {schema_name}.v_{table_name} t2
                on {join_conditions}
                where t2.id is null
        ''')
        conn.commit()
        logger.info(f"{schema_name}.tmp_{table_name}_new_rows создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании {schema_name}.tmp_{table_name}_new_rows: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def create_deleted_rows(table_name, schema_name='banking'):
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()
        query_columns = ',\n'.join([f't1.{column}' for column in COLUMNS])
        join_conditions = '\n and '.join([f't1.{key} = t2.{key}' for key in KEY_COLUMNS])
        where_conditions = '\n or '.join([f't2.{key} is null' for key in KEY_COLUMNS])

        cursor.execute(f'''
            create table {schema_name}.tmp_{table_name}_deleted_rows as
                select 
                    {query_columns}
                from {schema_name}.v_{table_name} t1
                left join {schema_name}.tmp_{table_name} t2
                on {join_conditions}
                where {where_conditions}
        ''')
        conn.commit()
        logger.info(f"{schema_name}.tmp_{table_name}_deleted_rows создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании {schema_name}.tmp_{table_name}_deleted_rows: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def create_updated_rows(table_name, schema_name='banking'):
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()
        query_columns = ',\n'.join([f't1.{column}' for column in COLUMNS])
        join_conditions = '\n and '.join([f't1.{key} = t2.{key}' for key in KEY_COLUMNS])
        where_conditions = '\n or '.join([f't1.{column} <> t2.{column}' for column in COLUMNS])

        cursor.execute(f'''
            create table {schema_name}.tmp_{table_name}_updated_rows as
                select 
                    {query_columns}
                from {schema_name}.tmp_{table_name} t1
                inner join {schema_name}.v_{table_name} t2
                on {join_conditions}
                where {where_conditions}
        ''')
        conn.commit()
        logger.info(f"{schema_name}.tmp_{table_name}_updated_rows создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании {schema_name}.tmp_{table_name}_updated_rows: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def update_table_hist(table_name, schema_name='banking'):
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()
        new_rows_insert_columns = ',\n'.join([f'{column}' for column in COLUMNS])

        # вставляем новые строки
        cursor.execute(f'''
            insert into {schema_name}.{table_name}_hist ({new_rows_insert_columns})
            select 
                {new_rows_insert_columns}
            from {schema_name}.tmp_{table_name}_new_rows
        ''')
        
        # заканчиваем время актуальности строчки для удаленных
        cursor.execute(f'''
            update {schema_name}.{table_name}_hist
            set end_dttm = current_timestamp - interval '1 second'
            where {KEY_COLUMNS[0]} in (select {KEY_COLUMNS[0]} from {schema_name}.tmp_{table_name}_deleted_rows)
            and end_dttm = '5999-12-31 23:59:59'::timestamp
        ''')

        # заканчиваем время актуальности строчки для обновленных
        cursor.execute(f'''
            update {schema_name}.{table_name}_hist
            set end_dttm = current_timestamp - interval '1 second'
            where {KEY_COLUMNS[0]} in (select {KEY_COLUMNS[0]} from {schema_name}.tmp_{table_name}_updated_rows)
            and end_dttm = '5999-12-31 23:59:59'::timestamp
        ''')

        # вставляем обновленные данные
        cursor.execute(f'''
            insert into {schema_name}.{table_name}_hist ({new_rows_insert_columns})
            select 
                {new_rows_insert_columns}
            from {schema_name}.tmp_{table_name}_updated_rows
        ''')

        # вставляем удаленные данные с флагом удаления
        cursor.execute(f'''
            insert into {schema_name}.{table_name}_hist ({new_rows_insert_columns}, deleted_flg)
            select 
                {new_rows_insert_columns},
                1
            from {schema_name}.tmp_{table_name}_deleted_rows
        ''')

        conn.commit()
        logger.info(f" В {schema_name}.{table_name}_hist добавлены строчки")
    except Exception as e:
        logger.error(f" В {schema_name}.{table_name}_hist ошбка при добавлении: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def drop_tmp_tables(schema_name='banking'):
    '''
    Функция универсальна, удаляет все таблицы tmp_
    указанной схемы
    '''
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()
        cursor.execute(f"""
            select
                table_name 
            from information_schema.tables
            where table_schema  = '{schema_name}'
            and table_name like 'tmp_%';
        """)

        for table in cursor.fetchall():
            cursor.execute(f"drop table if exists {schema_name}.{table[0]}")

        #cursor.execute("drop table if exists my_schema.tmp_auto")
        #cursor.execute("drop table if exists my_schema.tmp_new_rows")
        #cursor.execute("drop table if exists my_schema.tmp_updated_rows")
        #cursor.execute("drop table if exists my_schema.tmp_deleted_rows")

        conn.commit()
        logger.info(f"Таблицы tmp_ успешно удалены")

    except Exception as e:
        logger.error(f"Ошибка при удалении tmp_ таблиц: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def create_scd1_structure(table_name, schema, columns_definition):
    """
    Универсальная функция для создания SCD1 структуры
    table_name: имя базовой таблицы
    schema: схема БД
    columns_definition: SQL‑описание полей (кроме системных)
    """
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()

        # Создание исторической таблицы
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                id SERIAL PRIMARY KEY,
                {columns_definition},
                create_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_dttm TIMESTAMP DEFAULT ('5999-12-31 23:59:59'::TIMESTAMP),
                fraud_processed BOOLEAN DEFAULT false
            )
        """
        )

        conn.commit()
        logger.info(f"SCD1 структура для {table_name} создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании SCD1 для {table_name}: {e}")
    finally:
        if conn or cursor:
            close_connection_postgres(conn, cursor)

def transfer_data_to_main_table(table_name, schema_name='banking'):
    """
    Переносит данные из временной таблицы в основную
    с проверкой на изменение прошлых транзакций
    """
    conn, cursor = None, None
    try:
        conn, cursor = connection_postgres()

        # Обновляем существующие записи: закрываем период для старых версий
        cursor.execute(f"""
            UPDATE {schema_name}.{table_name}
            SET update_dttm = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE transaction_id IN (
                SELECT transaction_id
                FROM {schema_name}.tmp_{table_name}
            )
            AND update_dttm = '5999-12-31 23:59:59'::TIMESTAMP
        """)

        # Вставляем данные из временной таблицы в основную
        cursor.execute(f"""
            INSERT INTO {schema_name}.{table_name} (
                transaction_id, transaction_date, amount, card_num,
                oper_type, oper_result, terminal
            )
            SELECT
                transaction_id, transaction_date, amount, card_num,
                oper_type, oper_result, terminal
            FROM {schema_name}.tmp_{table_name}
        """)
        affected_rows = cursor.rowcount
        conn.commit()
        logger.info(f"Перенесено {affected_rows} строк из tmp_{table_name} в {table_name}")
    except Exception as e:
        logger.error(f"Ошибка при переносе данных из tmp_{table_name} в {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection_postgres(conn, None)



#clear_and_remove_archive_folder('archive')
unzip_file('data.zip')
create_cschema('banking')
sql2sql('ddl_dml.sql', 'banking')

# Для passport_blacklist
create_scd2_structure("passport_blacklist", "banking", "date DATE NOT NULL, passport VARCHAR(128)")
xlsx2sql(r'data\passport_blacklist_01032021.xlsx', table_name='tmp_passport_blacklist')
COLUMNS = ['date', 'passport']    # для таблицы blacklist
KEY_COLUMNS = ['passport']
create_new_rows('passport_blacklist')
create_deleted_rows('passport_blacklist')
create_updated_rows('passport_blacklist')
update_table_hist('passport_blacklist')

# Для terminals
create_scd2_structure("terminals", "banking", "terminal_id VARCHAR(128), terminal_type VARCHAR(128), terminal_city VARCHAR(128), terminal_address VARCHAR(128)")
xlsx2sql(r'data\terminals_01032021.xlsx', table_name='tmp_terminals')
COLUMNS = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'] 
KEY_COLUMNS = ['terminal_id'] 
create_new_rows('terminals')
create_deleted_rows('terminals')
create_updated_rows('terminals')
update_table_hist('terminals')

# Для transactions
create_scd1_structure("transactions", "banking", "transaction_id VARCHAR(128), transaction_date TIMESTAMP not null, amount numeric, card_num varchar(128), oper_type varchar(128), oper_result varchar(128), terminal varchar(128)")
txt2sql(r'data\transactions_01032021.txt', 'tmp_transactions', 'banking', if_exists='append')
transfer_data_to_main_table('transactions', 'banking')

# Отчет
sql2sql('fraud_report.sql', 'banking')
# Удаление временных таблиц
drop_tmp_tables()

# Для passport_blacklist
create_scd2_structure("passport_blacklist", "banking", "date DATE NOT NULL, passport VARCHAR(128)")
xlsx2sql(r'data\passport_blacklist_02032021.xlsx', table_name='tmp_passport_blacklist')
COLUMNS = ['date', 'passport']    # для таблицы blacklist
KEY_COLUMNS = ['passport']
create_new_rows('passport_blacklist')
create_deleted_rows('passport_blacklist')
create_updated_rows('passport_blacklist')
update_table_hist('passport_blacklist')

# Для terminals
create_scd2_structure("terminals", "banking", "terminal_id VARCHAR(128), terminal_type VARCHAR(128), terminal_city VARCHAR(128), terminal_address VARCHAR(128)")
xlsx2sql(r'data\terminals_02032021.xlsx', table_name='tmp_terminals')
COLUMNS = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'] 
KEY_COLUMNS = ['terminal_id'] 
create_new_rows('terminals')
create_deleted_rows('terminals')
create_updated_rows('terminals')
update_table_hist('terminals')

# Для transactions
create_scd1_structure("transactions", "banking", "transaction_id VARCHAR(128), transaction_date TIMESTAMP not null, amount numeric, card_num varchar(128), oper_type varchar(128), oper_result varchar(128), terminal varchar(128)")
txt2sql(r'data\transactions_02032021.txt', 'tmp_transactions', 'banking', if_exists='append')
transfer_data_to_main_table('transactions', 'banking')

# Отчет
sql2sql('fraud_report.sql', 'banking')
# Удаление временных таблиц
drop_tmp_tables()

# Для passport_blacklist
create_scd2_structure("passport_blacklist", "banking", "date DATE NOT NULL, passport VARCHAR(128)")
xlsx2sql(r'data\passport_blacklist_03032021.xlsx', table_name='tmp_passport_blacklist')
COLUMNS = ['date', 'passport']    # для таблицы blacklist
KEY_COLUMNS = ['passport']
create_new_rows('passport_blacklist')
create_deleted_rows('passport_blacklist')
create_updated_rows('passport_blacklist')
update_table_hist('passport_blacklist')

# Для terminals
create_scd2_structure("terminals", "banking", "terminal_id VARCHAR(128), terminal_type VARCHAR(128), terminal_city VARCHAR(128), terminal_address VARCHAR(128)")
xlsx2sql(r'data\terminals_03032021.xlsx', table_name='tmp_terminals')
COLUMNS = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'] 
KEY_COLUMNS = ['terminal_id'] 
create_new_rows('terminals')
create_deleted_rows('terminals')
create_updated_rows('terminals')
update_table_hist('terminals')

# Для transactions
create_scd1_structure("transactions", "banking", "transaction_id VARCHAR(128), transaction_date TIMESTAMP not null, amount numeric, card_num varchar(128), oper_type varchar(128), oper_result varchar(128), terminal varchar(128)")
txt2sql(r'data\transactions_03032021.txt', 'tmp_transactions', 'banking', if_exists='append')
transfer_data_to_main_table('transactions', 'banking')

# Отчет
sql2sql('fraud_report.sql', 'banking')
# Удаление временных таблиц
drop_tmp_tables()