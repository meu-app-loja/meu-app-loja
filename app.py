import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

# ==============================================================================
# ⚙️ CONFIGURAÇÃO E SEGURANÇA
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

# --- CONVERSOR DE NÚMEROS (RESOLVE O PROBLEMA DO 3,19 -> 319) ---
def forcar_numero(valor):
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    try:
        s = str(valor).strip().replace('R$', '').replace(' ', '')
        # Se tem ponto e vírgula (ex: 1.200,50), remove o ponto e troca a vírgula por ponto
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        # Se só tem vírgula (ex: 3,19), troca por ponto
        elif ',' in s:
            s = s.replace(',', '.')
        return float(s)
    except:
        return 0.0

# --- CONEXÃO SEGURA ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# --- LEITURA DA NUVEM COM PROTEÇÃO ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        
        dados = ws.get_all_records()
        if not dados:
            return pd.DataFrame(columns=colunas_padrao)
            
        df = pd.DataFrame(dados)
        # Limpeza forçada de números ao ler
        for col in df.columns:
            if any(x in col.lower() for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = df[col].apply(forcar_numero)
            if 'data' in col.lower() or 'validade' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except:
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM COM BLOQUEIO DE SEGURANÇA ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    # BLOQUEIO CRÍTICO: Se o DF estiver vazio e a aba for de estoque, não salva para não apagar tudo
    if df.empty and "estoque" in nome_aba:
        st.error("⚠️ Tentativa de apagar dados bloqueada. O sistema evitou que a lista ficasse vazia.")
        return

    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        
        ws.clear()
        df_save = df.copy()
        
        # Garante que todas as colunas existem antes de salvar
        for col in colunas_padrao:
            if col not in df_save.columns:
                df_save[col] = 0.0 if any(x in col for x in ['qtd', 'preco']) else ""

        # Prepara para envio (converte datas para texto)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES AUXILIARES
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper().strip()

def encontrar_melhor_match(nome_buscado, lista_opcoes):
    melhor_match = None; maior_score = 0.0
    nome_buscado = normalizar_texto(nome_buscado)
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        set_xml = set(nome_buscado.split())
        set_sis = set(normalizar_texto(opcao).split())
        comum = set_xml.intersection(set_sis)
        score = len(comum) / len(set_xml.union(set_sis)) if comum else 0
        if score > maior_score: maior_score = score; melhor_match = opcao
    return melhor_match if maior_score >= 0.3 else None

def atualizar_casa_global(nome_produto, qtd_nova, custo, venda, validade, prefixo_origem):
    lojas = ["loja1", "loja2", "loja3"]
    for loja in lojas:
        if loja == prefixo_origem: continue
        df_l = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if not df_l.empty:
            mask = df_l['nome do produto'].apply(normalizar_texto) == normalizar_texto(nome_produto)
            if mask.any():
                idx = df_l[mask].index[0]
                df_l.at[idx, 'qtd_central'] = qtd_nova
                if custo: df_l.at[idx, 'preco_custo'] = custo
                if venda: df_l.at[idx, 'preco_venda'] = venda
                if validade: df_l.at[idx, 'validade'] = validade
                salvar_na_nuvem(f"{loja}_estoque", df_l, COLUNAS_VITAIS)

# --- LEITURA XML (BLINDADA) ---
def ler_xml_nfe(arquivo_xml):
    try:
        tree = ET.parse(arquivo_xml); root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        # Tenta pegar número da nota e fornecedor
        nNF = root.find(".//nfe:nNF", ns)
        xNome = root.find(".//nfe:emit/nfe:xNome", ns)
        dhEmi = root.find(".//nfe:ide/nfe:dhEmi", ns)
        
        dados = {
            'numero': nNF.text if nNF is not None else "S/N",
            'fornecedor': xNome.text if xNome is not None else "DESCONHECIDO",
            'data': pd.to_datetime(dhEmi.text).date() if dhEmi is not None else datetime.now().date(),
            'itens': []
        }
        
        for det in root.findall(".//nfe:det", ns):
            prod = det.find("nfe:prod", ns)
            if prod is not None:
                item = {
                    'nome': prod.find("nfe:xProd", ns).text,
                    'ean': prod.find("nfe:cEAN", ns).text,
                    'qtd': forcar_numero(prod.find("nfe:qCom", ns).text),
                    'vUn': forcar_numero(prod.find("nfe:vUnCom", ns).text),
                    'vDesc': forcar_numero(prod.find("nfe:vDesc", ns).text) if prod.find("nfe:vDesc", ns) is not None else 0.0
                }
                # Preço líquido = (Valor total - desconto) / quantidade
                item['preco_liq'] = item['vUn'] - (item['vDesc'] / item['qtd'] if item['qtd'] > 0 else 0)
                dados['itens'].append(item)
        return dados
    except Exception as e:
        st.error(f"Erro ao ler este arquivo XML: {e}")
        return None

# ==============================================================================
# 🚀 APP INTERFACE
# ==============================================================================
st.sidebar.title("🏢 Gestor Multi-Lojas")
loja_sel = st.sidebar.selectbox("Unidade:", ["Loja 1", "Loja 2", "Loja 3"])
prefixo = "loja1" if "1" in loja_sel else "loja2" if "2" in loja_sel else "loja3"

# CARREGAR DADOS
df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)

modo = st.sidebar.radio("Menu:", ["📊 Dashboard", "📥 Importar XML", "🏠 Gôndola", "🏡 Estoque Casa", "📋 Tabela Geral"])

# --- DASHBOARD ---
if modo == "📊 Dashboard":
    st.title(f"📊 Dashboard - {loja_sel}")
    if not df.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Produtos na Loja", int(df['qtd.estoque'].sum()))
        c2.metric("Produtos na Casa", int(df['qtd_central'].sum()))
        val_inv = (df['qtd.estoque'] * df['preco_custo']).sum()
        c3.metric("Inv. Loja", f"R$ {val_inv:,.2f}")
    else:
        st.info("Nenhum dado para mostrar.")

# --- IMPORTAR XML (CORRIGIDO) ---
elif modo == "📥 Importar XML":
    st.title("📥 Entrada por XML")
    xml_up = st.file_uploader("Selecione o arquivo XML", type=['xml'])
    if xml_up:
        nota = ler_xml_nfe(xml_up)
        if nota:
            st.success(f"Nota {nota['numero']} - {nota['fornecedor']}")
            escolhas = {}
            lista_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].tolist())
            
            for i, item in enumerate(nota['itens']):
                with st.container(border=True):
                    c1, c2 = st.columns(2)
                    c1.write(f"**XML:** {item['nome']}")
                    c1.caption(f"Qtd: {item['qtd']} | Preço Pago: R$ {item['preco_liq']:.2f}")
                    
                    sugestao = encontrar_melhor_match(item['nome'], lista_sistema)
                    idx_sug = lista_sistema.index(sugestao) if sugestao else 0
                    escolhas[i] = c2.selectbox(f"Vincular item {i}:", lista_sistema, index=idx_sug, key=f"xml_{i}")

            if st.button("💾 CONFIRMAR ENTRADA"):
                for i, item in enumerate(nota['itens']):
                    nome_sis = escolhas[i]
                    if nome_sis == "(CRIAR NOVO)":
                        novo = {
                            'código de barras': item['ean'], 'nome do produto': item['nome'].upper(),
                            'qtd.estoque': 0, 'qtd_central': item['qtd'], 'qtd_minima': 5,
                            'preco_custo': item['preco_liq'], 'preco_venda': item['preco_liq']*2,
                            'ultimo_fornecedor': nota['fornecedor']
                        }
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    else:
                        idx = df[df['nome do produto'] == nome_sis].index[0]
                        df.at[idx, 'qtd_central'] += item['qtd']
                        df.at[idx, 'preco_custo'] = item['preco_liq']
                        df.at[idx, 'ultimo_fornecedor'] = nota['fornecedor']
                        # Sincroniza outras lojas
                        atualizar_casa_global(nome_sis, df.at[idx, 'qtd_central'], item['preco_liq'], None, None, prefixo)
                
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success("Estoque atualizado com sucesso!"); st.balloons(); st.rerun()

# --- GÔNDOLA ---
elif modo == "🏠 Gôndola":
    st.title("🏠 Gôndola (Loja)")
    busca = st.text_input("Buscar produto na loja:")
    df_f = df[df['nome do produto'].str.contains(busca, case=False)] if busca else df
    
    for idx, row in df_f.iterrows():
        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 1, 1])
            c1.write(f"**{row['nome do produto']}**")
            c2.write(f"Loja: {int(row['qtd.estoque'])}")
            c3.write(f"Casa: {int(row['qtd_central'])}")
            
            if row['qtd_central'] > 0:
                with st.form(f"f_{idx}"):
                    q = st.number_input("Mover para Loja:", min_value=1, max_value=int(row['qtd_central']), key=f"in_{idx}")
                    if st.form_submit_button("Confirmar"):
                        df.at[idx, 'qtd.estoque'] += q
                        df.at[idx, 'qtd_central'] -= q
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                        st.rerun()

# --- ESTOQUE CASA ---
elif modo == "🏡 Estoque Casa":
    st.title("🏡 Estoque Central (Casa)")
    st.info("Aqui você edita o que tem guardado em casa.")
    df_ed = st.data_editor(df[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True)
    if st.button("Salvar Alterações da Casa"):
        df.update(df_ed)
        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
        st.success("Salvo!"); st.rerun()

# --- TABELA GERAL ---
elif modo == "📋 Tabela Geral":
    st.title("📋 Cadastro Geral")
    df_geral = st.data_editor(df, use_container_width=True, num_rows="dynamic")
    if st.button("Salvar Tudo"):
        salvar_na_nuvem(f"{prefixo}_estoque", df_geral, COLUNAS_VITAIS)
        st.success("Tabela Geral Sincronizada!"); st.rerun()
