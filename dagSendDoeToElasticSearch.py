import requests
import base64
import os
import PyPDF2
import hashlib
import logging
import re
import mysql.connector
from mysql.connector import Error
from urllib.parse import urlparse

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


ELASTIC_USER = Variable.get('elastic_chat_user')
ELASTIC_PASSWORD = Variable.get('elastic_chat_password')
ELASTIC_INDEX_ENDPOINT = Variable.get('elastic_doe_index_endpoint')

DATABASE = Variable.get('doe_database')
DATABASE_USER = Variable.get('doe_database_user')
DATABASE_SERVER = Variable.get('doe_database_server')
DATABASE_PASSWORD = Variable.get('doe_database_password')
DATABASE_PORT = Variable.get('doe_database_port')

POSTS_SQL = '''
    select id,post_date, post_title, guid   
        from wp_posts 
    where post_type = 'attachment' 
      and post_mime_type = 'application/pdf' 
      and post_date  >= %s
    order by post_date
'''


def runQuery(sql_query, params):
    try:
        # Conectar ao banco de dados MariaDB
        conexao = mysql.connector.connect(
            host=DATABASE_SERVER,       
            database=DATABASE,          
            user=DATABASE_USER,         
            password=DATABASE_PASSWORD, 
            port=DATABASE_PORT          
        )

        cursor = conexao.cursor()

        # Executar a consulta SQL com os parâmetros
        cursor.execute(sql_query, params)

        # Obter os resultados como uma lista de tuplas
        resultado_tuplas = cursor.fetchall()

        # Commit para garantir que alterações sejam salvas (necessário para operações de escrita)
        conexao.commit()

        # Fechar o cursor e a conexão
        cursor.close()
        conexao.close()

        return resultado_tuplas

    except Error as e:
        print(f"Erro ao executar a consulta SQL: {e}")
        return None



def downloadTextoDoe(urlPdfDoe):
    
    # Realiza a requisição REST para obter o PDF
    response = requests.get(urlPdfDoe)
    #response = requests.get(f"{URL_SPED_PDF}{idEscrito}")

    # Verifica se a requisição foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        # Salva o conteúdo do PDF em um arquivo temporário
        with open("temp.pdf", "wb") as pdf_file:
            pdf_file.write(response.content)

        # Extrai o texto do PDF usando PyMuPDF
        texto_do_pdf = extract_text_from_pdf("temp.pdf")

        # Remover o arquivo temporário após a extração
        remove_temp_file("temp.pdf")

        return texto_do_pdf
    else:
        # Se a requisição não for bem-sucedida, imprime o código de status
        raise Exception(            f"Error: {response.status_code}\nResponse content: {response.content} ")

#remover o pdf temporário
def remove_temp_file(nome_do_arquivo):    
    os.remove(nome_do_arquivo)


# Configuração básica do logging
logging.basicConfig(level=logging.INFO)

def existsInElastic(idDoc):

    authorization = base64.b64encode((ELASTIC_USER+":"+ELASTIC_PASSWORD).encode(encoding = "utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {authorization}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.head(f"{ELASTIC_INDEX_ENDPOINT}{idDoc}", headers=headers)
        return 200 <= response.status_code <= 299
    except requests.RequestException as e:
        logging.error(f"Erro ao verificar documento no Elasticsearch: {e}")
        return False

def sendToElastic(idDoc, data={}):


    authorization = base64.b64encode((ELASTIC_USER+":"+ELASTIC_PASSWORD).encode(encoding = "utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {authorization}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.post(f"{ELASTIC_INDEX_ENDPOINT}{idDoc}", json=data, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Erro ao enviar documento para o Elasticsearch: {e}")
        raise

#Extrair o texto a partir do pdf
def extract_text_from_pdf(pdf_path):

    if not os.path.exists(pdf_path):
        return None

    # Open PDF file
    pdf_file = open(pdf_path, 'rb')
    reader = PyPDF2.PdfReader(pdf_file)

    # Extract text from all pages
    text = ""
    for page in reader.pages:
        page_content = page.extract_text()
        text += page_content.strip() + "\n"

    # Close PDF file
    pdf_file.close()

    return text.strip()

def getDataInicialEnvio():
    return datetime.now() - timedelta(days=7)

def extrair_nome_arquivo_pdf(url):
    # Parse a URL para obter o caminho
    parsed_url = urlparse(url)
    # Extrair o nome do arquivo do caminho
    nome_arquivo = os.path.basename(parsed_url.path)
    return nome_arquivo

def extrair_numero_edicao(texto):
    # Expressão regular para encontrar a palavra "Edição" ou "Edicao" seguida por um número
    padrao = r"(Edição|Edicao)[^\d]*(\d+)"
    resultado = re.search(padrao, texto, re.IGNORECASE)
    
    # Verifica se encontrou o padrão e retorna o número
    if resultado:
        return int(resultado.group(2))
    else:
        return None
    
def processar():

    dtInicioEnvio = getDataInicialEnvio()
    tuplasPost = runQuery(POSTS_SQL,(dtInicioEnvio,))

    for post in tuplasPost:
        idPost    =  post[0]
        postDate  =  post[1]
        postTitle =  post[2]
        urlPdfDoe =  post[3]
        textoDoe = downloadTextoDoe(urlPdfDoe=urlPdfDoe)

        nome_arquivo = extrair_nome_arquivo_pdf(urlPdfDoe)
    
        # Extrair o número da edição a partir do nome do arquivo
        numDoe = extrair_numero_edicao(nome_arquivo)

        #ignorar os documentos que já existem no elastic
        id_documento_elastic = idPost
        if(existsInElastic(id_documento_elastic)):
            print("já existe " , id_documento_elastic, "Edição: ", numDoe)
            continue
        
        print("Enviando post:" , id_documento_elastic, "Edição: ", numDoe)

        documento = {
            "metadados": {
                "idPost": idPost,
                "postDate": postDate.strftime('%Y-%m-%d %H:%M:%S'),
                "postTitle": postTitle,
                "numDoe": numDoe,
                "urlPdfDoe": urlPdfDoe,
            },
            "texto_doe": textoDoe
        }

        sendToElastic(idDoc = idPost, data=documento)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1)
}

# Definir o objeto DAG com o intervalo desejado
dag = DAG(
    'dagSendDoeToElasticSearch',
    default_args=default_args,
    description='Envia novos arquivos do diário ofical para o elastic search usado pela Busca Avançada do DOE',
    schedule_interval='*/30 14-18 * * *',  # Executa a cada 30 minutos das 12h às 18h
    max_active_runs=1
)

# Definir a tarefa PythonOperator
dagSendDoeToElasticSearch = PythonOperator(
    task_id='dagSendDoeToElasticSearch',
    python_callable=processar,
    dag=dag
)

dagSendDoeToElasticSearch