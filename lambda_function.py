import psycopg2
from dotenv import load_dotenv
import os
import json
import boto3
import pandas as pd
from io import BytesIO

bucket_name = 'excelfilesevolusom'
prefix = '' 

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DBNAME = os.getenv("DB_NAME")

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    arquivos = []
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.xlsx'):
                arquivos.append(obj['Key'])

    dataframes = []
    for arquivo_key in arquivos:
        print(f'Lendo {arquivo_key} ...')
        arquivo_obj = s3.get_object(Bucket=bucket_name, Key=arquivo_key)
        arquivo_bytes = arquivo_obj['Body'].read()
        df = pd.read_excel(BytesIO(arquivo_bytes)).assign(Arquivo=arquivo_key)
        dataframes.append(df)

    df_vendas = pd.concat(dataframes, ignore_index=True)

    df_vendas = df_vendas.drop(columns=["Unnamed: 0", "Unnamed: 7", "Unnamed: 8"], errors='ignore')
    df_vendas = df_vendas.dropna()

    df_vendas = df_vendas[["Unnamed: 1", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4","Unnamed: 5"]]
    df_vendas = df_vendas.rename(columns={"Unnamed: 1": "id", 
                                          "Unnamed: 3" : "cliente", 
                                          "Unnamed: 2": "data", 
                                          "Unnamed: 4" : "receita", 
                                          "Unnamed: 5" : "status"})

    df_vendas["data"] = df_vendas["data"].astype(str)
    df_vendas["id"] = df_vendas["id"].astype(int)
    print(df_vendas.head())

    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )

        print("Connection successful!")

        cursor = connection.cursor()

        for _, row in df_vendas.iterrows():
            cursor.execute("""
                INSERT INTO tb_vendas_mes (id, data, cliente, receita, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    data = EXCLUDED.data,
                    cliente = EXCLUDED.cliente,
                    receita = EXCLUDED.receita,
                    status = EXCLUDED.status;
            """, (row['id'], row['data'], row['cliente'], row['receita'], row['status']))

        connection.commit()  

        cursor.close()
        connection.close()
        print("Connection closed.")

        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted successfully!')
        }
    
    except Exception as e:
        print(f"Failed to connect or insert data: {e}")

        return {
            'statusCode': 500,
            'body': json.dumps('Failed to connect or insert data!')
        }
