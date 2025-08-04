import streamlit as st
import psycopg2
import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta 
#from dotenv import load_dotenv
from datetime import date, timedelta

# -----------------------
# Função para rodar query no Supabase (PostgreSQL)
# -----------------------

#load_dotenv()

def run_query_pg(query: str) -> pd.DataFrame:
    try:
        conn = psycopg2.connect(
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
            port=int(os.getenv('PORT')),
            dbname=os.getenv('DBNAME'),
            sslmode='require'
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        print('Sucesso')
        return df
    
    except Exception as e:
        #st.error(f"Erro ao executar a consulta no Supabase: {e}")
        print(st.error(f"Erro ao executar a consulta no Supabase: {e}"))

# -----------------------
# Queries adaptadas para PostgreSQL
# -----------------------

DEFAULT_QUERY = """
select
    SUM(receita) AS receita_total,
    COUNT(DISTINCT cliente) AS clientes_unicos,
    COUNT(cliente) AS pedidos,
        GREATEST(date_part('day', (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')) - date_part('day', CURRENT_DATE), 1) AS meta_dia
FROM tb_vendas_mes
WHERE  data BETWEEN DATE '2025-08-01'
                      AND DATE '2025-08-31';
"""

VENDA_DIA_QUERY = """
SELECT 
    TO_CHAR(data, 'DD-MM') AS dia,
    SUM(receita) AS receita_total,
    COUNT(cliente) AS pedidos_realizado
FROM tb_vendas_mes
WHERE  data BETWEEN DATE '2025-08-01'
                      AND DATE '2025-08-31'
GROUP BY dia
ORDER BY dia ASC
"""

df = run_query_pg(DEFAULT_QUERY)

df_vendas_dia = run_query_pg(VENDA_DIA_QUERY)
df_meta_agosto = pd.read_csv('meta_agosto.csv')
df_meta_agosto["dia"] = pd.to_datetime(df_meta_agosto["dia"], format="%Y-%m-%d")
df_meta_agosto["dia"] = df_meta_agosto["dia"].dt.strftime("%d-%m")

df_final = (df_meta_agosto.merge(df_vendas_dia[["dia", "receita_total","pedidos_realizado"]], 
             on="dia",                
             how="left"))      

receita_realizada_acumulada = 0
pedidos_realizada_acumulada = 0

df_final["receita_total_acumulada"] = df_final["receita_total"]
df_final["pedidos_total_acumulada"] = df_final["pedidos_realizado"]

for indice, receita in enumerate(df_final["receita_total"]):
    receita_realizada_acumulada = receita + receita_realizada_acumulada
    df_final["receita_total_acumulada"].iloc[indice] = receita_realizada_acumulada

for indice, pedidos in enumerate(df_final["pedidos_realizado"]):
    pedidos_realizada_acumulada = pedidos + pedidos_realizada_acumulada
    df_final["pedidos_total_acumulada"].iloc[indice] = pedidos_realizada_acumulada

df_final = df_final.set_index("dia")    


# -----------------------
# App Streamlit
# -----------------------

meta_receita_mes = 1442909.46
meta_pedidos_mes = 735


def main():
    st.set_page_config(page_title="Dashboard Vendas - Mês Agosto", layout="wide")
    st.title("📊 Vendas Mês Agosto")
    #st.write(df_final)
    

    # Converte colunas numéricas
    for col in ["receita_total", "clientes_unicos", "pedidos"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    metrics = df.loc[0]

    receita_total = metrics["receita_total"]
    clientes_unicos = int(metrics["clientes_unicos"])
    pedidos = int(metrics["pedidos"])

    pct_pedidos_realizados = pedidos/meta_pedidos_mes 
    pct_receita_realizada = receita_total/meta_receita_mes

    ontem = date.today() - timedelta(days=1) 
    pct_ref_receita = receita_total  - df_final["receita_acumulada"].loc[ontem.strftime("%d-%m")]
    pct_ref_pedidos = pedidos - df_final["pedidos_acumulado"].loc[ontem.strftime("%d-%m")] 

    st.write("##")


    col1, col2 = st.columns(2)

    col1.metric(f"💰 Receita Total | Meta - R${meta_receita_mes:,.2f}", f"R$ {receita_total:,.2f}", delta = f"{pct_ref_receita:,.2f} Reais")
    #col1.st.progress(pct_receita_realizada*100)
    col1.subheader("##")

    col1.metric(f"🚚 Pedidos Realizados | Meta - {meta_pedidos_mes} Pedidos", f"R$ {pedidos:}", delta = f"{pct_ref_pedidos} Pedidos")
    col1.subheader("##")

    st.line_chart(df_final, y=["receita_total_acumulada","receita_acumulada"])
    st.line_chart(df_final, y=["pedidos_total_acumulada","pedidos_acumulado"])


    col2.metric("🔸 Clientes Únicos", f"{clientes_unicos}")
    st.write("##")

    hoje = datetime.now()
    ultimo_dia_mes = (hoje.replace(day=1) + relativedelta(months=1)) - relativedelta(days=1)

    dias_uteis = pd.bdate_range(start=hoje + pd.Timedelta(days=1), end=ultimo_dia_mes)
    dias_uteis_restantes = len(dias_uteis) + 1

    meta_dia = (meta_receita_mes- receita_total) / max(dias_uteis_restantes, 1)
    meta_dia_pedidos = (meta_pedidos_mes - pedidos) / max(dias_uteis_restantes, 1)

    col2.metric("📅 Dias Úteis Restantes", dias_uteis_restantes)
    col2.metric("🚚 Meta Pedidos Diária Atualizada", f"{int(meta_dia_pedidos)}")
    col2.metric("💰 Meta Receita Diária Atualizada", f"R$ {meta_dia:,.2f}")



if __name__ == "__main__":
    main()
