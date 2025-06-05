# Desafio Spotify

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) para processar dados da API do Spotify utilizando **Apache Spark**.

## Pré-requisitos  
- Python 3.10+  
- Java JDK 17 ([Download](https://adoptium.net/temurin/releases/))  
- App criado no Spotify ([Link](https://developer.spotify.com/dashboard/))
- Spark 4.0.0 e Hadoop 3 (
    - [Download](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz)
    - [Winutils](https://github.com/steveloughran/winutils) ou [Winutils](https://github.com/cdarlint/winutils)
)  

## Configuração

### 1. Variáveis de Ambiente  
Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:
```env
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=your_redirect_uri
SPOTIFY_SCOPE=your_scope
AUTH_URL=your_auth_url
TOKEN_URL=your_token_url
```

### 2. Instalação das bibliotecas necessárias

```bash
pip install -r requirements.txt
```

### 3. Configuração do Spark, Hadoop e Java

```PowerShell
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
$env:SPARK_HOME = "C:\spark-4.0.0-bin-hadoop3"
$env:Path += ";$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin"
```

## Estrutura dos dados manipulados

![alt text](src/assets/image.png "Estrutura dos dados manipulados")