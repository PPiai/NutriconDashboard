import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# --- PASSO 1: LER AS CREDENCIAIS DO AMBIENTE ---
# As mesmas variáveis de ambiente do outro script serão usadas aqui
FB_ACCESS_TOKEN = os.environ.get('FB_ACCESS_TOKEN')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

# --- PASSO 2: AJUSTE NA URL ---
# Adicionei os nomes do anúncio/campanha para dar contexto a cada ação
# e removi o token de acesso para ser lido da variável de ambiente.
fields_solicitados = "ad_name,adset_name,campaign_name,actions"
url = f"https://graph.facebook.com/v19.0/act_439991594112145/insights?access_token={FB_ACCESS_TOKEN}&level=ad&fields={fields_solicitados}&date_preset=last_90d&time_increment=1&limit=1000"

print("Iniciando busca de dados de AÇÕES na API do Meta...")
all_data = []
page_count = 1

# O loop de paginação continua igual, está correto.
while url:
    try:
        print(f"Buscando dados da página {page_count}...")
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        
        if 'data' in json_data and json_data['data']:
            all_data.extend(json_data['data'])
            
        if 'paging' in json_data and 'next' in json_data['paging']:
            url = json_data['paging']['next']
            page_count += 1
        else:
            url = None
            
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição: {e}")
        url = None

print("Busca de dados finalizada.")

# Verifica se algum dado foi coletado
if all_data:
    print("Dados recebidos. Processando e achatando a estrutura...")
    # --- PASSO 3: AJUSTE NO PROCESSAMENTO (JSON_NORMALIZE) ---
    # Agora o 'meta' corresponde aos campos que realmente pedimos na URL
    df_results = pd.json_normalize(all_data,
                                   record_path=['actions'],
                                   meta=['ad_name', 'adset_name', 'campaign_name', 'date_start', 'date_stop'])

    print("DataFrame de Resultados criado com sucesso.")
    
    # --- PASSO 4: SALVAR NO BANCO DE DADOS (EM UMA NOVA TABELA) ---
    print("Iniciando a importação para o banco de dados PostgreSQL...")
    try:
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        
        # Salvaremos em uma nova tabela para não misturar com os dados de performance
        table_name = 'insights_meta_actions'
        
        df_results.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"SUCESSO: Dados importados para a nova tabela '{table_name}'.")
        
    except Exception as e:
        print(f"ERRO: Falha ao importar para o PostgreSQL: {e}")
        
else:
    print("Nenhum dado foi retornado pela API.")
