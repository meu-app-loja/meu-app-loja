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
# ⚙️ CONFIGURAÇÃO DE NUVEM & SISTEMA
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# --- DEFINIÇÃO DE COLUNAS OBRIGATÓRIAS (GLOBAL) ---
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

# --- FUNÇÃO PARA FORMATAR NÚMEROS NO ESTILO BRASILEIRO ---
def format_br(valor):
    try:
        if pd.isna(valor) or valor == "": return "0,00"
        val_float = float(valor)
        s = f"{val_float:,.2f}"
        return s.replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "0,00"

# --- CONVERSOR INTELIGENTE (TRATA 319 -> 3.19 E 3,19 -> 3.19) ---
def converter_ptbr_inteligente(valor, modo_reparo=False):
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
   
    s = str(valor).strip().upper().replace('R$', '').replace(' ', '').strip()
    
    # Caso 1: Formato com vírgula (3,19 ou 1.200,50)
    if ',' in s:
        if '.' in s: s = s.replace('.', '')
        s = s.replace(',', '.')
        try: return float(s)
        except: pass

    # Caso 2: Valor puramente numérico (pode ser 319 ou 3.19)
    try:
        val = float(s)
        # Lógica de Reparo: Se o valor for um inteiro alto (ex: 319.0), e modo_reparo estiver ativo
        if modo_reparo and val >= 100 and (val % 1 == 0):
            return val / 100.0
        return val
    except:
        s_limpo = re.sub(r'[^\d.]', '', s)
        try: return float(s_limpo)
        except: return 0.0

# --- CONEXÃO SEGURA ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        json_creds = json.loads(st.secrets["service_account_json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        return gspread.authorize(creds)
    except: return None

# --- FUNÇÃO DE CURA ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        col_norm = col.strip().lower()
        if col_norm not in df.columns:
            if any(x in col_norm for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df[col_norm] = 0.0
            elif 'data' in col_norm or 'validade' in col_norm:
                df[col_norm] = None
            else:
                df[col_norm] = ""
    return df

# --- LEITURA DA NUVEM (COM PRESERVAÇÃO E REPARO) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao, reparar_precos=False):
    time.sleep(1) 
    try:
        client = get_google_client()
        if not client: return pd.DataFrame(columns=colunas_padrao)
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
       
        dados = ws.get_all_records()
        if not dados: return pd.DataFrame(columns=colunas_padrao)
        df = pd.DataFrame(dados)
        
        # Preservar colunas existentes (Planograma)
        colunas_existentes = list(df.columns)
        colunas_finais = colunas_existentes.copy()
        for col in colunas_padrao:
            if col not in colunas_finais: colunas_finais.append(col)
        
        df = garantir_integridade_colunas(df, colunas_finais)
        
        # Conversão de tipos
        for col in df.columns:
            if any(x in col for x in ['preco', 'valor', 'custo', 'total', 'desconto']):
                df[col] = df[col].apply(lambda x: converter_ptbr_inteligente(x, modo_reparo=reparar_precos))
            elif 'qtd' in col:
                df[col] = df[col].apply(lambda x: converter_ptbr_inteligente(x, modo_reparo=False))
            elif 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')
               
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (PROTEÇÃO CRÍTICA) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    if df is None or (df.empty and len(colunas_padrao) > 0):
        if "estoque" in nome_aba or "historico" in nome_aba: return
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
       
        df_save = df.copy()
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
            df_save[col] = df_save[col].fillna("")
        
        ws.clear()
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES LÓGICAS
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_texto(texto_busca) in normalizar_texto(x))
    return df[mask]

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    def calcular_score(n1, n2):
        s1 = set(normalizar_texto(n1).split())
        s2 = set(normalizar_texto(n2).split())
        if not s1 or not s2: return 0
        return len(s1.intersection(s2)) / len(s1.union(s2))
    melhor_match = None; maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        score = calcular_score(nome_buscado, opcao)
        if score > maior_score: maior_score = score; melhor_match = opcao
    if maior_score >= cutoff: return melhor_match, "Similaridade"
    return None, "Nenhum"

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        df_outra = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if not df_outra.empty:
            mask = df_outra['nome do produto'].astype(str).str.upper() == str(nome_produto).upper()
            if mask.any():
                idx = df_outra[mask].index[0]
                df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

# --- FUNÇÃO XML ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]
    dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO': dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi': 
            try: dados_nota['data'] = pd.to_datetime(elem.text[:10])
            except: pass
    dets = [e for e in root.iter() if tag_limpa(e) == 'det']
    for det in dets:
        try:
            prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
            if prod:
                item = {'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
                vProd = 0.0; vDesc = 0.0; qCom = 0.0; cEAN = ''; cProd = ''
                for info in prod:
                    t = tag_limpa(info)
                    if t == 'cProd': cProd = info.text
                    elif t == 'cEAN': cEAN = info.text
                    elif t == 'xProd': item['nome'] = normalizar_texto(info.text)
                    elif t == 'qCom': qCom = converter_ptbr_inteligente(info.text)
                    elif t == 'vProd': vProd = converter_ptbr_inteligente(info.text)
                    elif t == 'vDesc': vDesc = converter_ptbr_inteligente(info.text)
                item['ean'] = cEAN if cEAN not in ['SEM GTIN', '', 'None'] else cProd
                if qCom > 0:
                    item['qtd'] = qCom
                    item['preco_un_bruto'] = vProd / qCom
                    item['desconto_total_item'] = vDesc
                    item['preco_un_liquido'] = (vProd - vDesc) / qCom
                    dados_nota['itens'].append(item)
        except: continue
    return dados_nota

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)

if "Principal" in loja_atual: prefixo = "loja1"
elif "Filial" in loja_atual: prefixo = "loja2"
else: prefixo = "loja3"

# --- CARREGAMENTO DE DADOS (COM REPARO ATIVO) ---
df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS, reparar_precos=True)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST, reparar_precos=True)
df_lista_compras = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)

# Menu
modo = st.sidebar.radio("Navegar:", [
    "📊 Dashboard", "📥 Importar XML", "🔄 Sincronizar (Planograma)", 
    "🏠 Gôndola (Loja)", "🏡 Estoque Central (Casa)", "💰 Histórico & Preços", "📋 Tabela Geral"
])

# 1. DASHBOARD
if modo == "📊 Dashboard":
    st.title(f"📊 Painel - {loja_atual}")
    if df.empty: st.info("Sem dados.")
    else:
        valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("📦 Itens Totais", int(df['qtd.estoque'].sum() + df['qtd_central'].sum()))
        c2.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
        c3.info("💡 Dica: Vá em Histórico para fixar valores antigos.")

# 2. IMPORTAR XML
elif modo == "📥 Importar XML":
    st.title("📥 Importar XML")
    arquivo_xml = st.file_uploader("Arraste o XML", type=['xml'])
    if arquivo_xml:
        dados = ler_xml_nfe(arquivo_xml, df_oficial)
        st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
        if st.button("✅ CONFIRMAR E SALVAR"):
            for item in dados['itens']:
                mask = df['código de barras'].astype(str) == str(item['ean'])
                if mask.any():
                    idx = df[mask].index[0]
                    df.at[idx, 'qtd_central'] += item['qtd']
                    df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                    df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                else:
                    novo = {'código de barras': item['ean'], 'nome do produto': item['nome'], 'qtd.estoque': 0, 'qtd_central': item['qtd'], 'qtd_minima': 5, 'preco_custo': item['preco_un_liquido'], 'preco_venda': item['preco_un_liquido'] * 1.5, 'ultimo_fornecedor': dados['fornecedor'], 'categoria': 'GERAL'}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            novos_h = [{'data': dados['data'], 'produto': item['nome'], 'fornecedor': dados['fornecedor'], 'qtd': item['qtd'], 'preco_pago': item['preco_un_liquido'], 'total_gasto': item['qtd'] * item['preco_un_liquido'], 'numero_nota': dados['numero']} for item in dados['itens']]
            df_hist = pd.concat([df_hist, pd.DataFrame(novos_h)], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
            st.success("Processado!"); st.rerun()

# 3. SINCRONIZAR PLANOGRAMA
elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sincronizar Planograma")
    st.info("💡 Este módulo preserva suas colunas extras do Google Sheets.")
    arquivo = st.file_uploader("Arquivo Planograma", type=['xlsx', 'csv'])
    if arquivo:
        if st.button("🚀 SINCRONIZAR"):
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success("Sincronizado!"); st.rerun()

# 4. GÔNDOLA
elif modo == "🏠 Gôndola (Loja)":
    st.title("🏠 Gôndola")
    termo = st.text_input("Buscar Produto:")
    df_f = filtrar_dados_inteligente(df, 'nome do produto', termo)
    if not df_f.empty:
        for idx, row in df_f.iterrows():
            with st.container(border=True):
                st.subheader(row['nome do produto'])
                c1, c2, c3 = st.columns(3)
                c1.metric("Loja", int(row['qtd.estoque']))
                c2.metric("Casa", int(row['qtd_central']))
                c3.metric("Preço", f"R$ {format_br(row['preco_venda'])}")

# 5. ESTOQUE CASA
elif modo == "🏡 Estoque Central (Casa)":
    st.title("🏡 Estoque Central")
    st.dataframe(df[['nome do produto', 'qtd_central', 'preco_custo', 'ultimo_fornecedor']])

# 6. HISTÓRICO & PREÇOS (COM BOTÃO DE REPARO)
elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico de Compras")
    st.warning("⚠️ Se os valores antigos estiverem errados (ex: 319), clique no botão abaixo para corrigir.")
    if st.button("🛠️ CORRIGIR E FIXAR VALORES NO GOOGLE SHEETS"):
        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
        st.success("Valores corrigidos e salvos!"); st.rerun()
    
    if not df_hist.empty:
        df_h_show = df_hist.copy()
        for col in ['preco_pago', 'total_gasto']:
            if col in df_h_show.columns: df_h_show[col] = df_h_show[col].apply(format_br)
        st.dataframe(df_h_show, use_container_width=True)

# 7. TABELA GERAL
elif modo == "📋 Tabela Geral":
    st.title("📋 Tabela Geral (Editável)")
    st.info("💡 Edite e clique em Salvar. O sistema corrigirá a pontuação automaticamente.")
    df_edit = st.data_editor(df, use_container_width=True, num_rows="dynamic")
    if st.button("💾 SALVAR TUDO"):
        for col in df_edit.columns:
            if any(x in col for x in ['preco', 'custo', 'venda']):
                df_edit[col] = df_edit[col].apply(lambda x: converter_ptbr_inteligente(x, modo_reparo=False))
        salvar_na_nuvem(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
        st.success("Salvo!"); st.rerun()
