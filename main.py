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
            AND ccu.table_name = '{table_name}';
    """)
    return [row[0] for row in cursor.fetchall()]

def backup_table_to_sql(host, database, user, password, table_name, output_file):
    try:
        # 连接到 PostgreSQL 数据库
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        # 创建一个游标对象
        cursor = connection.cursor()
        
        # 获取表的列名
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        column_names = [row[0] for row in cursor.fetchall()]
        
        # 执行查询以获取指定表的数据
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)
        
        # 获取查询结果
        rows = cursor.fetchall()
        
        # 打开输出文件
        with open(output_file, 'a') as f:
            # 遍历每一行数据并生成 INSERT 语句
            for row in rows:
                # 创建插入语句
                sql_insert = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(repr(value) for value in row)});\n"
                f.write(sql_insert)
        
        print(f"成功备份表 '{table_name}' 到文件 '{output_file}'")
        
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 关闭游标和连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def backup_database_tables(host, database, user, password, table_names, output_file):
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        # 维护一个已经备份的表集
        backed_up_tables = set()

        # 递归备份表及其依赖
        def backup_table(table):
            if table in backed_up_tables:
                return
            
            # 先备份依赖的表
            dependencies = get_table_dependencies(cursor, table)
            for dep_table in dependencies:
                backup_table(dep_table)

            # 备份当前表
            backup_table_to_sql(host, database, user, password, table, output_file)
            backed_up_tables.add(table)

        for table_name in table_names:
            backup_table(table_name)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def restore_table_from_sql(host, database, user, password, table_name, input_file):
    try:
        # 连接到 PostgreSQL 数据库
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        # 创建一个游标对象
        cursor = connection.cursor()
        
        # 读取 SQL 文件并执行每个 INSERT 语句
        with open(input_file, 'r') as f:
            for line in f:
                if line.strip():  # 忽略空行
                    cursor.execute(line.strip())
        
        # 提交更改
        connection.commit()
        print(f"成功从文件 '{input_file}' 恢复表 '{table_name}' 的数据")
        
    except Exception as e:
        print(f"恢复过程中发生错误: {e}")
        if connection:
            connection.rollback()  # 回滚事务以确保数据一致性
    finally:
        # 关闭游标和连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def restore_database_tables(host, database, user, password, table_names, input_file):
    """从 SQL 文件恢复数据库中的表及其依赖"""
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        # 维护一个已经恢复的表集
        restored_tables = set()

        def restore_table(table):
            if table in restored_tables:
                return
            
            # 先恢复依赖的表
            dependencies = get_table_dependencies(cursor, table)
            for dep_table in dependencies:
                restore_table(dep_table)

            # 恢复当前表
            restore_table_from_sql(host, database, user, password, table, input_file)
            restored_tables.add(table)

        for table_name in table_names:
            restore_table(table_name)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL 数据库备份与恢复工具')
    parser.add_argument('--config', required=True, help='配置文件路径')

    subparsers = parser.add_subparsers(dest='command', required=True)

    # 创建备份命令
    backup_parser = subparsers.add_parser('backup', help='备份指定表的数据')
    backup_parser.add_argument('--table', nargs='+', required=True, help='要备份的表名，可以指定多个表')
    backup_parser.add_argument('--output', required=True, help='输出 SQL 文件名')
    backup_parser.add_argument('--host', help='数据库主机')
    backup_parser.add_argument('--database', help='数据库名称')
    backup_parser.add_argument('--user', help='用户名')
    backup_parser.add_argument('--password', help='密码')

    # 创建恢复命令
    restore_parser = subparsers.add_parser('restore', help='从 SQL 文件恢复指定表的数据')
    restore_parser.add_argument('--table', nargs='+', required=True, help='要恢复的表名，可以指定多个表')
    restore_parser.add_argument('--input', required=True, help='输入 SQL 文件名')
    restore_parser.add_argument('--host', help='数据库主机')
    restore_parser.add_argument('--database', help='数据库名称')
    restore_parser.add_argument('--user', help='用户名')
    restore_parser.add_argument('--password', help='密码')

    args = parser.parse_args()

    # 从配置文件加载配置
    config = load_config(args.config)
    db_config = config['database']

    # 设置备份时的数据库连接信息
    backup_host = args.host if args.host else db_config['host']
    backup_database = args.database if args.database else db_config['database']
    backup_user = args.user if args.user else db_config['user']
    backup_password = args.password if args.password else db_config['password']

    # 执行备份操作
    if args.command == 'backup':
        backup_database_tables(
            host=backup_host,
            database=backup_database,
            user=backup_user,
            password=backup_password,
            table_names=args.table,
            output_file=args.output
        )

    # 设置恢复时的数据库连接信息
    restore_host = args.host if args.host else db_config['host']
    restore_database = args.database if args.database else db_config['database']
    restore_user = args.user if args.user else db_config['user']
    restore_password = args.password if args.password else db_config['password']

    # 执行恢复操作
    elif args.command == 'restore':
        restore_database_tables(
            host=restore_host,
            database=restore_database,
            user=restore_user,
            password=restore_password,
            table_names=args.table,
            input_file=args.input
        )

if __name__ == "__main__":
    main()
