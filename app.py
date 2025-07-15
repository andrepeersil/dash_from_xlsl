import streamlit as st
import sys
import psycopg2
import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta  # para cálculo datas no PostgreSQL
#from dotenv import load_dotenv

# -----------------------
# Função para rodar query no Supabase (PostgreSQL)
# -----------------------

#load_dotenv()

def run_query_pg(query: str) -> pd.DataFrame:
    try:
        conn = psycopg2.connect(
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=int(os.getenv("PORT")),
            dbname=os.getenv("DBNAME"),
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
    CAST(COUNT(cliente) AS FLOAT) / 725 AS pedido_realizado,
    SUM(receita)/1313634.55 AS realizado,
    (1313634.55 - SUM(receita)) / 
        GREATEST(date_part('day', (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')) - date_part('day', CURRENT_DATE), 1) AS meta_dia
FROM tb_vendas_mes
"""

VENDA_DIA_QUERY = """
SELECT 
    TO_CHAR(data, 'DD-MM') AS dia,
    SUM(receita) AS receita_total,
    COUNT(cliente) AS pedidos
FROM tb_vendas_mes
GROUP BY dia
ORDER BY dia ASC
"""


# -----------------------
# App Streamlit
# -----------------------
def main():
    st.set_page_config(page_title="Dashboard Vendas", layout="wide")
    st.title("📊 Dashboard de Vendas")

    df = run_query_pg(DEFAULT_QUERY)
    df_vendas_dia = run_query_pg(VENDA_DIA_QUERY)

    if df.empty or df_vendas_dia.empty:
        st.warning("Nenhum dado disponível para exibir.")
        return

    # Converte colunas numéricas
    for col in ["receita_total", "clientes_unicos", "pedidos", "pedido_realizado", "realizado", "meta_dia"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    metrics = df.iloc[0]

    receita_total = metrics["receita_total"]
    clientes_unicos = int(metrics["clientes_unicos"])
    pedidos = int(metrics["pedidos"])
    pct_pedidos_realizados = metrics["pedido_realizado"]
    pct_realizado = metrics["realizado"]

    col1, col2, col3 = st.columns(3)

    col1.metric("💰 Receita Total", f"R$ {receita_total:,.2f}")
    col1.metric("🔸 Clientes Únicos", f"{clientes_unicos}")
    col1.metric("🚚 Pedidos", f"{pedidos}")

    col2.metric("🚚 Pedidos Realizados", f"{pct_pedidos_realizados:.2%}")
    progresso_pedidos = int(min(max(pct_pedidos_realizados, 0), 1) * 100)
    col2.progress(progresso_pedidos)

    col2.metric("📈 Receitas Realizadas", f"{pct_realizado:.2%}")
    progresso_receita = int(min(max(pct_realizado, 0), 1) * 100)
    col2.progress(progresso_receita)

    hoje = datetime.now()
    ultimo_dia_mes = (hoje.replace(day=1) + relativedelta(months=1)) - relativedelta(days=1)

    dias_uteis = pd.bdate_range(start=hoje + pd.Timedelta(days=1), end=ultimo_dia_mes)
    dias_uteis_restantes = len(dias_uteis)

    meta_dia = (1313634.55 - receita_total) / max(dias_uteis_restantes, 1)
    meta_dia_pedidos = (725 - pedidos) / max(dias_uteis_restantes, 1)

    col3.metric("📅 Dias Úteis Restantes", dias_uteis_restantes)
    col3.metric("🚚 Meta Pedidos Diária Atualizada", f"{int(meta_dia_pedidos)}")
    col3.metric("💰 Meta Receita Diária Atualizada", f"R$ {meta_dia:,.2f}")

    st.bar_chart(df_vendas_dia.set_index("dia")["pedidos"])
    st.line_chart(df_vendas_dia.set_index("dia")["receita_total"])

if __name__ == "__main__":
    main()
