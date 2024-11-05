import argparse
import configparser
import psycopg2

def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def get_table_dependencies(cursor, table_name):
    """获取表的依赖关系"""
    cursor.execute(f"""
        SELECT
            ccu.table_name AS dependent_table
        FROM
            information_schema.table_constraints AS tc
        JOIN
            information_schema.constraint_column_usage AS ccu
        ON
            tc.constraint_name = ccu.constraint_name
        WHERE
            constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{table_name}';
    """)
    return [row[0] for row in cursor.fetchall()]

def backup_table_to_sql(host, port, database, user, password, table_name, output_file):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        column_names = [row[0] for row in cursor.fetchall()]
        
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        
        with open(output_file, 'a') as f:
            for row in rows:
                sql_insert = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(repr(value) for value in row)});\n"
                f.write(sql_insert)
        
        print(f"成功备份表 '{table_name}' 到文件 '{output_file}'")
        
    except Exception as e:
        print(f"备份过程中发生错误: {e}")
    finally:
        cursor.close()
        connection.close()

def backup_database_tables(host, port, database, user, password, table_names, output_file):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        backed_up_tables = set()

        def backup_table(table):
            if table in backed_up_tables:
                return
            
            dependencies = get_table_dependencies(cursor, table)
            for dep_table in dependencies:
                backup_table(dep_table)

            backup_table_to_sql(host, port, database, user, password, table, output_file)
            backed_up_tables.add(table)

        for table_name in table_names:
            backup_table(table_name)

    finally:
        cursor.close()
        connection.close()

def restore_table_from_sql(host, port, database, user, password, input_file):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        
        with open(input_file, 'r') as f:
            sql_content = f.read()
            cursor.execute(sql_content)
        
        connection.commit()
        print(f"成功从文件 '{input_file}' 恢复数据")
        
    except Exception as e:
        print(f"恢复过程中发生错误: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def restore_database_tables(host, port, database, user, password, table_names, input_file):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        restored_tables = set()

        def restore_table(table):
            if table in restored_tables:
                return
            
            dependencies = get_table_dependencies(cursor, table)
            for dep_table in dependencies:
                restore_table(dep_table)

            restore_table_from_sql(host, port, database, user, password, input_file)
            restored_tables.add(table)

        for table_name in table_names:
            restore_table(table_name)

    finally:
        cursor.close()
        connection.close()

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL 数据库备份与恢复工具')
    parser.add_argument('--config', required=True, help='配置文件路径')
    parser.add_argument('command', choices=['backup', 'restore'], help='执行备份或恢复操作')

    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config['database']

    host = db_config['host']
    port = db_config['port']
    database = db_config['database']
    user = db_config['user']
    password = db_config['password']

    if args.command == 'backup':
        backup_config = config['backup']
        table_names = [table.strip() for table in backup_config['tables'].split(',')]
        output_file = backup_config['output']
        backup_database_tables(host, port, database, user, password, table_names, output_file)

    elif args.command == 'restore':
        restore_config = config['restore']
        table_names = [table.strip() for table in restore_config['tables'].split(',')]
        input_file = restore_config['input']
        restore_database_tables(host, port, database, user, password, table_names, input_file)

if __name__ == "__main__":
    main()
