import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from typing import Dict, List
import logging
from dotenv import load_dotenv
import csv
import locale
import pandas as pd
from contextlib import contextmanager

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

    def fetch_reference_data(self, query: str, key_field: str = None, params: tuple = None) -> dict:
        """
        Busca dados de referência e retorna como dicionário indexado por key_field
        
        Args:
            query: Query SQL para executar
            key_field: Campo para usar como chave do dicionário (opcional)
            params: Parâmetros para a query (opcional)
            
        Returns:
            dict: Dicionário com os resultados indexados pela chave especificada
                Se key_field não for especificado, retorna lista tradicional
        """
        cache_key = f"{query}{params}{key_field}"
        if cache_key in self.db_cache:
            return self.db_cache[cache_key]
            
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                # Transforma em dicionário se key_field for especificado
                if key_field:
                    result_dict = {}
                    for item in results:
                        key = item.get(key_field)
                        if key is not None:
                            result_dict[key] = item
                    self.db_cache[cache_key] = result_dict
                    return result_dict
                    
                self.db_cache[cache_key] = results
                return results
                
        except Exception as e:
            logger.error(f"Erro ao buscar dados de referência: {e}")
            raise
        finally:
            if conn:
                conn.close()
                
    def _convert_br_to_en_us(self, series):
        """
        Converte valores numéricos no formato brasileiro para EN-US
        Ex: "75.000,00" → 75000.00 (float)
            "78,57" → 78.57 (float)
        """
        def convert_single(value):
            if pd.isna(value):
                return value
                
            # Se já for numérico, não precisa converter
            if isinstance(value, (int, float)):
                return value
                
            # Converte para string e limpa
            str_value = str(value).strip()
            
            try:
                # Remove pontos de milhar e substitui vírgula decimal por ponto
                clean_num = str_value.replace('.', '').replace(',', '.')
                return float(clean_num)
            except ValueError:
                return value  # Mantém original se falhar a conversão
        
        # Aplica a conversão para cada valor da Series
        return series.apply(convert_single)    

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Percorre cada coluna, analisa seu conteúdo e aplica tratamentos manuais
        
        :param df: DataFrame com os dados originais
        :return: DataFrame com os dados processados
        """
        query = "select * from depara.codigo_pais"
        estado_para_pais = self.fetch_reference_data(query,'estado')

        tratamentos_padronizados = {
             'CAPITAL SEGURADO (MOEDA ORIGEM)': self._convert_br_to_en_us
            ,'VALOR ESTIMADO (US$ )': self._convert_br_to_en_us
            ,'VALOR ESTIMADO (R$)': self._convert_br_to_en_us
            ,'FEE (US$)': self._convert_br_to_en_us
            ,'FEE (BRL)': self._convert_br_to_en_us
            ,'TAXA DE CÂMBIO': self._convert_br_to_en_us
        }
 
        # 3. Processar cada coluna
        for coluna in df.columns:
            # Aplicar tratamento específico se a coluna estiver no dicionário
            if coluna in tratamentos_padronizados:
                df[coluna] = tratamentos_padronizados[coluna](df[coluna])
            else:
                # Tratamento genérico para colunas não mapeadas
                df[coluna] = self._tratar_generico(df[coluna])
            
            # Tratamento especial para coluna de ESTADO
            if 'ESTADO' in coluna.upper():
                df['ID_PAIS'] = df[coluna].apply(
                    lambda estado: estado_para_pais.get(str(estado).strip(), {}).get('id') 
                    if pd.notna(estado) else None
                )
        
        return df


    def _tratar_generico(self, series):
        """
        Limpeza genérica para colunas não mapeadas
        """
        if series.dtype == 'object':
            return series.astype(str).str.strip()
        return series
                
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