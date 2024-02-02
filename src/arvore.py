import mysql.connector
import psycopg2
from datetime import datetime
#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
import logging

# Configurando o logging
logging.basicConfig(filename='arvore_dag.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Função para conectar ao MySQL
def connect_mysql():
    return mysql.connector.connect(
      host="my-sql.mysql.database.azure.com",
      user="my_sql",
      password="Admin123",
      database="classicmodels"
    )

# Função para conectar ao Redshift
def connect_redshift():
    return psycopg2.connect(
        dbname='arvore', 
        user='redshift', 
        password='Redshift123', 
        port='5439', 
        host='redshift-cluster-arvore.cs12mzkyke5b.sa-east-1.redshift.amazonaws.com'
    )

# Definindo as funções para a carga inicial e carga incremental
def carga_total(table_name, dag):
    try:
        # Lendo os dados do MySQL
        con_mysql = connect_mysql()
        mysql_cursor = con_mysql.cursor()
        redshift_conn = connect_redshift()
        redshift_cursor = redshift_conn.cursor()
        mysql_cursor.execute(f"SELECT * FROM {table_name}")
        dado = mysql_cursor.fetchall()

        # Obtendo a estrutura da tabela do MySQL
        mysql_cursor.execute(f"DESCRIBE {table_name}")
        estrutura = mysql_cursor.fetchall()

        # Criando a tabela no Redshift com a mesma estrutura
        cria_tabela = f"CREATE TABLE dados.{table_name} ("
        for column in estrutura:
            if "unsigned" in column[1]:
                cria_tabela += f"{column[0]} INTEGER, "
            else:
                cria_tabela += f"{column[0]} {column[1]}, "
        cria_tabela = cria_tabela.rstrip(", ") + ")"
        redshift_cursor.execute(cria_tabela)
        redshift_conn.commit()  # Confirma a transação

        # Transferindo os dados para o Redshift
        for row in dado:
            formatted_row = [str(item) if isinstance(item, datetime) else item for item in row]
            insere = f"INSERT INTO dados.{table_name} VALUES {tuple(formatted_row)}"
            print(insere)  # Imprime a instrução INSERT
            redshift_cursor.execute(insere)
        redshift_conn.commit()  # Confirma a transação
        logging.info(f'Carga total para a tabela {table_name} concluída com sucesso.')
    except Exception as e:
        logging.error(f'Erro na carga total para a tabela {table_name}: {str(e)}')

def carga_incremental(table_name, dag):
    try:
        con_mysql = connect_mysql()
        mysql_cursor = con_mysql.cursor()
        redshift_conn = connect_redshift()
        redshift_cursor = redshift_conn.cursor()
        # Obtendo a data da última atualização no Redshift
        redshift_cursor.execute(f"SELECT MAX(updated_at) FROM dados.{table_name}")
        ultima_atualizacao = redshift_cursor.fetchone()[0]

        # Lendo os dados atualizados do MySQL
        ultima_atualizacao_str = ultima_atualizacao.strftime('%Y-%m-%d %H:%M:%S')
        mysql_cursor.execute(f"SELECT * FROM {table_name} WHERE updated_at > '{ultima_atualizacao_str}'")
        dado = mysql_cursor.fetchall()

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
            formatted_row = [str(item) if isinstance(item, datetime) else item for item in row]
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
        con_mysql = connect_mysql()
        mysql_cursor = con_mysql.cursor()
        redshift_conn = connect_redshift()
        redshift_cursor = redshift_conn.cursor()
        mysql_cursor.execute("SHOW TABLES")
        tabelas_mysql = [t[0] for t in mysql_cursor.fetchall()]
        print(tabelas_mysql)

        redshift_cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'dados'""")
        redshift_tables = [t[0] for t in redshift_cursor.fetchall()]
        print(redshift_tables)

        # Verificando se as tabelas existem no Redshift
        for table_name in tabelas_mysql:
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

decisao()
# # Definindo o DAG
# arvore_dag = DAG('arvore_dag', description='DAG para ingestao de dados do teste pratico Arvore',
#           schedule_interval='0 0 * * 0',
#           start_date=datetime(2024, 1, 22), catchup=False)

# # Definindo as tasks
# tarefa_decisao = PythonOperator(task_id='decisao', python_callable=decisao, op_kwargs={'dag': arvore_dag}, dag=arvore_dag, retries=0)

# tarefa_decisao
