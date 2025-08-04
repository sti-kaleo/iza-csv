# Usa uma imagem base do Python
FROM python:3.10-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos necessários
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Cria os diretórios de entrada e saída
RUN mkdir -p /app/entrada /app/saida

