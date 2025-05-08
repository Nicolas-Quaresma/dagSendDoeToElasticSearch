Descrição do Projeto
Este projeto é uma DAG do Apache Airflow que tem como objetivo enviar novos arquivos do Diário Oficial (DOE) para o Elasticsearch. Ele extrai os arquivos PDF diariamente do DIARIO OFICIAL DOS MUNICIPIOS DO ESTADO DO AMAZONAS, processa e reparte em ATOS os arquivos dos diários, extrai o texto, e os envia para o Elasticsearch, onde ficam indexados para uma busca posterior.

A DAG é configurada para ser executada automaticamente em intervalos de 30 minutos, entre 14h e 18h. O fluxo inclui as seguintes etapas:

Conectar ao banco de dados para verificar a existencia do pdf a ser baixado.

Baixar o PDF referente ao dia atual.

extrai e divide em atos os arquivos extraídos dos pdfs.

Verificar se o documento já foi indexado no Elasticsearch.

Enviar o documento e seus metadados para o Elasticsearch, caso ele ainda não tenha sido indexado.

Tecnologias
Airflow: Orquestração de workflows para automação de tarefas.

Python: Linguagem de programação para o desenvolvimento do código.

MySQL: Banco de dados utilizado para armazenar os registros de arquivos PDF.

Elasticsearch:banco não relacional de busca utilizado para indexar os documentos.

PyPDF2: Biblioteca para extrair texto de arquivos PDF.

Requests: Biblioteca para realizar requisições HTTP.

Base64: Codificação para autenticação básica no Elasticsearch.

