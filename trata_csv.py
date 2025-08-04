import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from typing import Dict, List
import logging
from dotenv import load_dotenv
import csv

# Carrega variáveis do arquivo .env
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self):
        """
        Inicializa o processador de CSV com configurações do .env
        """
        self.input_dir = os.getenv('INPUT_DIR', './entrada')
        self.output_dir = os.getenv('OUTPUT_DIR', './saida')
        
        # Configurações do PostgreSQL
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Verifica/Cria diretórios
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Cache para dados do banco (opcional)
        self.db_cache = {}

    def get_db_connection(self):
        """Cria uma conexão com o banco PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port']
            )
            return conn
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def fetch_reference_data(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Busca dados de referência do banco PostgreSQL.
        
        :param query: Query SQL para executar
        :param params: Parâmetros para a query (opcional)
        :return: Lista de dicionários com os resultados
        """
        cache_key = f"{query}{params}"
        if cache_key in self.db_cache:
            return self.db_cache[cache_key]
            
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                self.db_cache[cache_key] = results
                return results
        except Exception as e:
            logger.error(f"Erro ao buscar dados de referência: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Percorre cada coluna, analisa seu conteúdo e aplica tratamentos manuais
        
        :param df: DataFrame com os dados originais
        :return: DataFrame com os dados processados
        """
        for coluna in df.columns:
            print(f"\n=== Processando coluna: {coluna} ===")
               
            if 'int' in str(df[coluna].dtype):
                # Tratamentos para colunas numéricas inteiras
                df[coluna] = self._tratar_inteiros(df[coluna])
                
            elif 'float' in str(df[coluna].dtype):
                # Tratamentos para colunas numéricas decimais
                df[coluna] = self._tratar_decimais(df[coluna])
                                
            #print(f"Tipo após tratamento: {df[coluna].dtype}")
            
            # Tratamentos específicos por nome de coluna (exemplos)
            if 'referência' in coluna.lower():
                print(df[coluna])
                #df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
                                
        return df

    def _tratar_texto(self, serie):
        """Tratamentos genéricos para colunas de texto"""
        serie = serie.astype(str)
        
        # Remover espaços extras
        serie = serie.str.strip()
        
        # Substituir valores vazios por NA
        serie = serie.replace(['', 'null', 'NULL', 'nan', 'NaN', 'NA', 'N/A'], pd.NA)
        
        # Converter para maiúsculas se parecer ser um código
        if serie.str.isupper().mean() > 0.7:  # Se 70% dos valores são uppercase
            serie = serie.str.upper()
        elif serie.str.istitle().mean() > 0.7:  # Se 70% estão em title case
            serie = serie.str.title()
        
        return serie

    def _tratar_inteiros(self, serie):
        """Tratamentos para colunas numéricas inteiras"""
        # Preencher NAs com 0 se for menos de 10% dos valores
        if serie.isna().mean() < 0.1:
            serie = serie.fillna(0)
        
        # Converter para Int64 (que suporta NAs)
        try:
            return serie.astype('Int64')
        except:
            return serie
        
    def _tratar_decimais(self, serie):
        """Tratamentos para colunas numéricas decimais"""
        # Converter para numérico
        serie = pd.to_numeric(serie, errors='coerce')
        
        # Arredondar para 2 casas decimais se tiver muitas casas
        if (serie.dropna() % 0.01 != 0).mean() > 0.5:
            serie = serie.round(2)
            
        return serie

    def _tratar_datas(self, serie):
        """Tentativa de parse automático de datas"""
        # Primeiro tenta o formato mais comum no Brasil
        data_br = pd.to_datetime(serie, format='%d/%m/%Y', errors='coerce')
        
        # Se não funcionar, tenta outros formatos
        if data_br.isna().mean() > 0.5:
            data_br = pd.to_datetime(serie, errors='coerce')
            
        return data_br
        
    def process_file(self, filename: str):
        """
        Processa um único arquivo CSV, tratando BOM se existir.
        """
        try:
            input_path = os.path.join(self.input_dir, filename)
            output_path = os.path.join(self.output_dir, filename)
            
            logger.info(f"Processando arquivo: {filename}")
            
            # Detecta automaticamente o BOM
            with open(input_path, 'rb') as f:
                raw = f.read(4)
                encoding = 'utf-8-sig' if raw.startswith(b'\xef\xbb\xbf') else 'utf-8'
            
            # Configurações de leitura do CSV
            csv_config = {
                'delimiter': os.getenv('CSV_DELIMITER', ';'),
                'encoding': encoding,  # Usa a codificação detectada
                'na_values': os.getenv('CSV_NA_VALUES', 'NA,N/A,NULL,null').split(',')
            }
            
            # Ler CSV com tratamento de BOM
            df = pd.read_csv(
                input_path,
                delimiter=csv_config['delimiter'],
                encoding=csv_config['encoding'],
                dtype=str,
                na_values=csv_config['na_values']
            )
            # Processar dados
            processed_df = self.process_data(df)
            # Configurações de escrita (sem BOM na saída)
            output_config = {
                'encoding': 'utf-8',  # Sem BOM na saída
                'quoting': int(os.getenv('OUTPUT_QUOTING', str(csv.QUOTE_NONNUMERIC))),
                'delimiter': ';',  # Adiciona o delimitador ponto-e-vírgula
            }
            
            # Salvar arquivo processado
            processed_df.to_csv(
                output_path,
                index=False,
                sep=output_config['delimiter'],  # Usa o delimitador configurado
                encoding=output_config['encoding'],
                #quoting=output_config['quoting'],
            )
            
            logger.info(f"Arquivo processado salvo em: {output_path}")
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {filename}: {e}")
            raise
            
    def process_all_files(self):
        """Processa todos os arquivos CSV no diretório de entrada."""
        try:
            # Listar arquivos CSV no diretório de entrada
            csv_files = [f for f in os.listdir(self.input_dir) if f.lower().endswith('.csv')]
            
            if not csv_files:
                logger.warning("Nenhum arquivo CSV encontrado no diretório de entrada.")
                return
            
            logger.info(f"Encontrados {len(csv_files)} arquivos para processar.")
            
            for filename in csv_files:
                self.process_file(filename)
                
            logger.info("Processamento concluído com sucesso!")
            
        except Exception as e:
            logger.error(f"Erro durante o processamento: {e}")
            raise


# Exemplo de uso
if __name__ == "__main__":
    # Criar e executar processador
    processor = CSVProcessor()
    
    # Processar todos os arquivos
    processor.process_all_files()