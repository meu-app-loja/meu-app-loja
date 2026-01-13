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

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DE NUVEM
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# --- DEFINIÇÃO DE COLUNAS OBRIGATÓRIAS ---
COLUNAS_VITAIS = ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto']
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

# --- CONEXÃO SEGURA ---
@st.cache_resource
def get_google_client():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_creds = json.loads(st.secrets["service_account_json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

# --- FUNÇÃO DE LIMPEZA FINANCEIRA (VACINA 3.19) ---
def sanitizar_float(valor):
    """Converte 3,19 ou 3.19 para float corretamente."""
    if pd.isna(valor) or valor == "" or valor is None:
        return 0.0
    if isinstance(valor, (float, int)):
        return float(valor)
    
    s = str(valor).strip().replace("R$", "").replace("r$", "").strip()
    
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."): 
            s = s.replace(".", "").replace(",", ".")
        else: 
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
            
    s = re.sub(r'[^\d\.-]', '', s)
    try:
        return float(s)
    except:
        return 0.0

# --- FUNÇÃO DE CURA ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']): df[col] = 0.0
            elif 'data' in col or 'validade' in col: df[col] = None
            else: df[col] = ""
    return df

# --- LEITURA DA NUVEM (COM LIMPEZA) ---
@st.cache_data(ttl=5)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.5)
    client = get_google_client()
    if not client: return pd.DataFrame(columns=colunas_padrao)
    
    try:
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        df = garantir_integridade_colunas(df, colunas_padrao)
        
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df[col] = df[col].apply(sanitizar_float)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (COM RESET DE CACHE) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    client = get_google_client()
    if not client: return
    try:
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        
        ws.clear()
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                 df_save[col] = df_save[col].fillna(0.0)

        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear() # Limpa memória para não sobrescrever dados antigos
    except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES AUXILIARES
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto): return normalizar_texto(texto)

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    comum = set_xml.intersection(set_sis)
    if not comum: return 0.0
    total = set_xml.union(set_
