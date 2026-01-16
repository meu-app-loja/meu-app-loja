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
import numpy as np  # Para tratar np.nan

# Função para formatar números no estilo brasileiro (milhar '.', decimal ',')
def format_br(valor):
    if pd.isna(valor):
        valor = 0.0
    s = f"{valor:,.2f}"
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DE NUVEM & SISTEMA
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

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

@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

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

def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: 
            ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)

        # Tratamento de NaN e NaT antes de salvar
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

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

def normalizar_para_busca(texto): return normalizar_texto(texto)

# (outras funções como calcular_pontuacao, encontrar_melhor_match, unificar_produtos_por_codigo, processar_excel_oficial, atualizar_casa_global, ler_xml_nfe permanecem iguais ao código anterior que funcionava)

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================
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

# Tratamento preventivo de NaN
colunas_numericas = [col for col in COLUNAS_VITAIS + COLS_HIST if any(x in col for x in ['qtd', 'preco', 'total', 'desconto'])]
if not df.empty:
    df[colunas_numericas] = df[colunas_numericas].fillna(0.0)
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
if not df_hist.empty:
    df_hist[colunas_numericas] = df_hist[colunas_numericas].fillna(0.0)
    df_hist['data'] = pd.to_datetime(df_hist['data'], errors='coerce')

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)",
        "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"
    ])

    if modo == "📊 Dashboard (Visão Geral)":
        # (código do dashboard igual ao anterior)

    elif modo == "🚚 Transferência em Massa (Picklist)":
        # (código igual)

    elif modo == "📝 Lista de Compras (Planejamento)":
        # (código igual)

    elif modo == "🆕 Cadastrar Produto":
        # (código igual)

    elif modo == "📥 Importar XML (Associação Inteligente)":
        # (código igual)

    elif modo == "⚙️ Configurar Base Oficial":
        # (código igual)

    elif modo == "🔄 Sincronizar (Planograma)":
        # (código igual)

    elif modo == "📉 Baixar Vendas (Do Relatório)":
        # (código igual)

    elif modo == "🏠 Gôndola (Loja)":
        # (código igual)

    elif modo == "🛒 Fornecedor (Compras)":
        # (código igual)

    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico & Preços")
        if not df_hist.empty:
            busca_hist_precos = st.text_input("🔍 Buscar:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
            df_hist_visual = df_hist.copy()
            if busca_hist_precos:
                df_hist_visual = filtrar_dados_inteligente(df_hist_visual, 'produto', busca_hist_precos)
                if df_hist_visual.empty:
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
          
            df_hist_visual['data'] = pd.to_datetime(df_hist_visual['data'], errors='coerce')
            df_editado = st.data_editor(
                df_hist_visual.sort_values(by='data', ascending=False, na_position='last'),
                use_container_width=True,
                key="editor_historico_geral",
                num_rows="dynamic",
                column_config={
                    "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "produto": st.column_config.TextColumn("Produto"),
                    "fornecedor": st.column_config.TextColumn("Fornecedor"),
                    "qtd": st.column_config.NumberColumn("Qtd", format="%.0f"),
                    "preco_pago": st.column_config.TextColumn("Pago (Unit)", help="Digite com vírgula: ex: 3,19"),
                    "total_gasto": st.column_config.TextColumn("Total Gasto", disabled=True),
                    "numero_nota": st.column_config.TextColumn("Nº Nota"),
                    "desconto_total_money": st.column_config.TextColumn("Desconto TOTAL", help="Digite com vírgula: ex: 10,50"),
                    "preco_sem_desconto": st.column_config.TextColumn("Preço Tabela", help="Digite com vírgula: ex: 5,99")
                }
            )
            
            st.divider()
            st.subheader("➕ Adicionar Compra Manual")
            # (formulário igual ao anterior)

            if st.button("💾 Salvar Alterações"):
                # (código de salvamento igual)

        else: st.info("Sem histórico.")

    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
        tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
        # (tab_ver igual)
        with tab_gerenciar:
            # (código completo do formulário, com tratamento de custo/venda 0.0 se NaN)

    elif modo == "📋 Tabela Geral":
        # (código igual)
