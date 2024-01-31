from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import uuid

# Configurando o logging
logging.basicConfig(filename='arvore_dag.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Função para conectar ao Azure SQL Database
def connect_azure():
    # Definindo as credenciais de conexão
    server = 'adventureworks-arvore.database.windows.net'
    database = 'AdventureWorks'
    username = 'administrador'
    password = '123Admin'
    # Construindo a string de conexão
    connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
    # Retornando a conexão
    try:
        engine = create_engine(connection_string)
        conn = engine.raw_connection()
        print("Conexão com Azure SQL Database estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Azure SQL Database: {str(e)}")
        return None

# Função para conectar ao Redshift
def connect_redshift():
    # Definindo as credenciais de conexão
    dbname = 'arvore'
    user = 'u_arvore'
    password = 'u_Arvore123'
    port = '5439'
    host = 'redshift-cluster-arvore.cs12mzkyke5b.sa-east-1.redshift.amazonaws.com'
    # Retornando a conexão
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, port=port, host=host)
        print("Conexão com Redshift estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Redshift: {str(e)}")
        return None

# Definindo as funções para a carga inicial e carga incremental
def carga_total(table_name, dag):
    try:
        # Lendo os dados do Azure SQL Database
        con_azure = connect_azure()
        azure_cursor = con_azure.cursor()
        redshift_conn = connect_redshift()
        redshift_cursor = redshift_conn.cursor()
        azure_cursor.execute(f"SELECT * FROM SalesLT.{table_name}")
        dado = azure_cursor.fetchall()

        # Obtendo a estrutura da tabela do Azure SQL Database
        azure_cursor.execute(f"""SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                                   FROM INFORMATION_SCHEMA.COLUMNS
                                  WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'SalesLT'""")
        estrutura = azure_cursor.fetchall()
        print(estrutura)

        # Criando a tabela no Redshift com a mesma estrutura
        cria_tabela = f"CREATE TABLE IF NOT EXISTS dados.{table_name} ("
        for column in estrutura:
            if "unsigned" in column[1]:
                cria_tabela += f"{column[0]} INTEGER, "
            elif "bit" in column[1]:
                cria_tabela += f"{column[0]} BOOLEAN, "
            elif "xml" in column[1]:
                cria_tabela += f"{column[0]} VARCHAR, "
            elif "uniqueidentifier" in column[1]:
                cria_tabela += f"{column[0]} VARCHAR, "
            elif "money" in column[1]:
                cria_tabela += f"{column[0]} DECIMAL, "
            elif "tinyint" in column[1]:
                cria_tabela += f"{column[0]} SMALLINT, "
            else:
                cria_tabela += f"{column[0]} {column[1]}, "
        cria_tabela = cria_tabela.rstrip(", ") + ")"
        redshift_cursor.execute(cria_tabela)
        redshift_conn.commit()  # Confirma a transação

        # Transferindo os dados para o Redshift
        for row in dado:
            formatted_row = ['NULL' if item is None else f"'{str(item).replace("'", "''")}'" if isinstance(item, datetime) or isinstance(item, str) or isinstance(item, uuid.UUID) else item for item in row]
            insere = f"INSERT INTO dados.{table_name} VALUES ({','.join([str(elem) for elem in formatted_row])})"
            print(insere)  # Imprime a instrução INSERT
            redshift_cursor.execute(insere)
        redshift_conn.commit()  # Confirma a transação
        logging.info(f'Carga total para a tabela {table_name} concluída com sucesso.')
    except Exception as e:
        logging.error(f'Erro na carga total para a tabela {table_name}: {str(e)}')

def carga_incremental(table_name, dag):
    try:
        con_azure = connect_azure()
        if con_azure is None:
            return
        azure_cursor = con_azure.cursor()
        redshift_conn = connect_redshift()
        if redshift_conn is None:
            return
        redshift_cursor = redshift_conn.cursor()

        # Obtendo a data da última atualização no Redshift
        redshift_cursor.execute(f"SELECT MAX(updated_at) FROM dados.{table_name}")
        ultima_atualizacao = redshift_cursor.fetchone()[0]

        # Lendo os dados atualizados do Azure
        ultima_atualizacao_str = ultima_atualizacao.strftime('%Y-%m-%d %H:%M:%S')
        azure_cursor.execute(f"SELECT * FROM SalesLT.{table_name} WHERE updated_at > '{ultima_atualizacao_str}'")
        dado = azure_cursor.fetchall()

        # Atualizando os dados no Redshift
        for row in dado:
            # Verificando se o registro existe no Redshift
            redshift_cursor.execute(f"SELECT COUNT(*) FROM dados.{table_name} WHERE id = {row[0]}")
            count = redshift_cursor.fetchone()[0]
            if count > 0:
                # Se o registro existir, apague-o
                exclusao = f"DELETE FROM dados.{table_name} WHERE id = {row[0]}"
                redshift_cursor.execute(exclusao)

            # Insira o registro
            formatted_row = ['NULL' if item is None else str(item) if isinstance(item, datetime) else str(item) for item in row]
            formatted_row = [f"'{item}'" if isinstance(item, uuid.UUID) else item for item in formatted_row]
            insere = f"INSERT INTO dados.{table_name} VALUES {tuple(formatted_row)}"
            print(insere)  # Imprime a instrução INSERT
            redshift_cursor.execute(insere)
        redshift_conn.commit()  # Confirma a transação
        logging.info(f'Carga incremental para a tabela {table_name} concluída com sucesso.')
    except Exception as e:
        logging.error(f'Erro na carga incremental para a tabela {table_name}: {str(e)}')

# Definindo a função para a task de decisão
def decisao(*args, **kwargs):
    try:
        con_azure = connect_azure()
        if con_azure is None:
            return
        azure_cursor = con_azure.cursor()
        redshift_conn = connect_redshift()
        if redshift_conn is None:
            return
        redshift_cursor = redshift_conn.cursor()
        azure_cursor.execute("""SELECT table_name FROM information_schema.tables where table_schema = 'SalesLT'""")
        tabelas_azure = [t[0] for t in azure_cursor.fetchall()]
        print(tabelas_azure)

        redshift_cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'dados'""")
        redshift_tables = [t[0] for t in redshift_cursor.fetchall()]
        print(redshift_tables)

        # Verificando se as tabelas existem no Redshift
        for table_name in tabelas_azure:
            print(table_name)
            if table_name not in redshift_tables:
                tarefa_carga_total = PythonOperator(task_id=f'carga_total_{table_name}', python_callable=carga_total, op_kwargs={'table_name': table_name, 'dag': kwargs['dag']}, dag=kwargs['dag'])
                tarefa_carga_total.execute(context=kwargs)
            else:
                tarefa_carga_incremental = PythonOperator(task_id=f'carga_incremental_{table_name}', python_callable=carga_incremental, op_kwargs={'table_name': table_name, 'dag': kwargs['dag']}, dag=kwargs['dag'])
                tarefa_carga_incremental.execute(context=kwargs)
        logging.info('Decisão executada com sucesso.')
    except Exception as e:
        logging.error(f'Erro na execução da decisão: {str(e)}')

# Definindo o DAG
arvore_dag = DAG('arvore_dag', description='DAG para ingestao de dados do teste pratico Arvore',
          schedule_interval=None,  # Alterado para None
          start_date=datetime.now(), catchup=False)  # Alterado para a data atual

# Definindo as tasks
tarefa_decisao = PythonOperator(task_id='decisao', python_callable=decisao, op_kwargs={'dag': arvore_dag}, dag=arvore_dag, retries=0)

tarefa_decisao
