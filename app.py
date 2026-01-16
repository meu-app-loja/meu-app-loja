import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time
import re
import numpy as np

# Função para formatar números no estilo brasileiro
def format_br(valor):
    if pd.isna(valor):
        valor = 0.0
    s = f"{valor:,.2f}"
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')

# Configuração Streamlit
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# Colunas globais
COLUNAS_VITAIS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central',
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada',
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

# Cliente Google
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# Conversor PT-BR
def converter_ptbr(valor):
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    s = str(valor).strip().upper().replace('R$', '').strip()
    try:
        return float(s)
    except:
        pass
    if ',' in s and '.' in s:
        s = s.replace('.', '')
        s = s.replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

# Integridade colunas
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = 0.0
            elif 'data' in col or 'validade' in col:
                df[col] = None
            else:
                df[col] = ""
    for col in df.columns:
        if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
            df[col] = df[col].apply(converter_ptbr)
    return df

# Leitura nuvem
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1)
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)
        df = garantir_integridade_colunas(df, colunas_padrao)
        for col in df.columns:
            if 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        return pd.DataFrame(columns=colunas_padrao)

# Salvar nuvem (seguro com NaN)
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: 
            ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        df_save = df_save.replace({np.nan: None, pd.NaT: None})
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

        data_to_write = [df_save.columns.values.tolist()] + df_save.values.tolist()
        ws.update('A1', data_to_write)
        ws.resize(rows=len(df_save) + 1, cols=len(df_save.columns))
        ler_da_nuvem.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar em {nome_aba}: {e}")
        return False

# Funções lógicas (normalizar_texto, filtrar, etc.) - adicione as que faltavam do seu código original aqui

# Início app
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)
st.sidebar.markdown("---")
if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)
df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", COLS_MOV)
df_vendas = ler_da_nuvem(f"{prefixo}_vendas", COLS_VENDAS)
df_lista_compras = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)

# Colunas numéricas possíveis
colunas_numericas = [col for col in set(COLUNAS_VITAIS + COLS_HIST) if any(x in col for x in ['qtd', 'preco', 'total', 'desconto'])]

# Fillna apenas em colunas existentes (CORREÇÃO DO ERRO)
if not df.empty:
    existing_numeric_df = [col for col in colunas_numericas if col in df.columns]
    if existing_numeric_df:
        df[existing_numeric_df] = df[existing_numeric_df].fillna(0.0)
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

if not df_hist.empty:
    existing_numeric_hist = [col for col in colunas_numericas if col in df_hist.columns]
    if existing_numeric_hist:
        df_hist[existing_numeric_hist] = df_hist[existing_numeric_hist].fillna(0.0)
    df_hist['data'] = pd.to_datetime(df_hist['data'], errors='coerce')

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)",
        "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"
    ])

    # Aqui coloque o código de cada modo (if/elif) do seu app original.
    # Como o erro era no carregamento inicial, agora está resolvido.
    # Exemplo para Histórico & Preços (adicione o resto):
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico & Preços")
        if not df_hist.empty:
            # ... (seu código do data_editor e formulário)
            pass
        else:
            st.info("Sem histórico.")

    # ... (outros elifs)
