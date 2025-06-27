import requests
import pandas as pd
from sqlalchemy import create_engine
import os # Biblioteca para ler variáveis de ambiente

# --- PASSO 1: LER AS CREDENCIAIS DO AMBIENTE ---
# A plataforma de nuvem irá injetar esses valores no script
FB_ACCESS_TOKEN = os.environ.get('FB_ACCESS_TOKEN')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

# Monta a URL da API com o token lido
url = f"https://graph.facebook.com/v19.0/act_439991594112145/insights?access_token={FB_ACCESS_TOKEN}&level=ad&fields=ad_name,adset_name,campaign_name,clicks,impressions,reach,spend&date_preset=last_90d&time_increment=1&limit=1000"

print("Iniciando busca de dados na API do Meta...")
# ... (o resto do seu código de busca com o loop while continua exatamente o mesmo) ...
all_data = []
# ...
print("Busca de dados finalizada.")

if all_data:
    df_performance = pd.json_normalize(all_data)
    
    print("Iniciando a importação para o PostgreSQL...")
    try:
        # Monta a string de conexão com os dados lidos do ambiente
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        table_name = 'insights_meta_ads'
        
        df_performance.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"SUCESSO: Dados importados para a tabela '{table_name}'.")
    except Exception as e:
        print(f"ERRO: Falha ao importar para o PostgreSQL: {e}")
else:
    print("Nenhum dado retornado pela API.")
