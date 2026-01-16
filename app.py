import streamlit as st
import pandas as pd
import datetime
import xml.etree.ElementTree as ET
import unicodedata
import time
import re

# ==============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# ==============================================================================
st.set_page_config(layout="wide", page_title="Gestão Multi-Lojas (Modo Local)")

# Colunas Vitais
COLUNAS_VITAIS = [
    "ean", "descricao", "categoria", 
    "custo_unitario", "preco_venda", 
    "qtd_loja1", "qtd_loja2", "qtd_loja3", 
    "estoque_central", "localizacao"
]

COLS_HIST = ["data", "ean", "descricao", "qtd_compra", "custo_pag", "num_nota"]

# ==============================================================================
# 2. SISTEMA DE BANCO DE DADOS (SIMULADO NA MEMÓRIA)
# ==============================================================================
# Como estamos sem Google Sheets, usamos a memória do app (Session State)

if "db_produtos" not in st.session_state:
    # Cria dados fictícios para você ver o app preenchido
    dados_iniciais = pd.DataFrame([
        {"ean": "789100010010", "descricao": "COCA COLA 2L", "categoria": "BEBIDAS", "custo_unitario": 5.50, "preco_venda": 9.00, "qtd_loja1": 12, "qtd_loja2": 24, "qtd_loja3": 6, "estoque_central": 100, "localizacao": "A1"},
        {"ean": "789100020020", "descricao": "SABAO OMO 1KG", "categoria": "LIMPEZA", "custo_unitario": 12.00, "preco_venda": 18.50, "qtd_loja1": 5, "qtd_loja2": 10, "qtd_loja3": 2, "estoque_central": 50, "localizacao": "B3"},
        {"ean": "789100030030", "descricao": "ARROZ TIO JOAO 5KG", "categoria": "ALIMENTOS", "custo_unitario": 22.00, "preco_venda": 32.90, "qtd_loja1": 20, "qtd_loja2": 15, "qtd_loja3": 10, "estoque_central": 200, "localizacao": "C1"},
    ])
    # Garante que todas as colunas existem
    for col in COLUNAS_VITAIS:
        if col not in dados_iniciais.columns:
            dados_iniciais[col] = 0.0
    st.session_state["db_produtos"] = dados_iniciais

if "db_historico" not in st.session_state:
    st.session_state["db_historico"] = pd.DataFrame(columns=COLS_HIST)

def ler_dados(tipo):
    """Lê da memória."""
    if tipo == "Produtos":
        return st.session_state["db_produtos"]
    else:
        return st.session_state["db_historico"]

def salvar_dados(df, tipo):
    """Salva na memória."""
    if tipo == "Produtos":
        st.session_state["db_produtos"] = df
    else:
        st.session_state["db_historico"] = df
    st.toast(f"Dados salvos em memória!", icon="✅")

# ==============================================================================
# 3. FUNÇÕES AUXILIARES
# ==============================================================================

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

def format_br(valor):
    if valor is None or pd.isna(valor): return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def calcular_match_score(nome_xml, nome_db):
    set_xml = set(normalizar_texto(nome_xml).split())
    set_db = set(normalizar_texto(nome_db).split())
    if not set_xml or not set_db: return 0
    intersection = set_xml.intersection(set_db)
    return len(intersection) / len(set_xml)

def parse_nfe_xml(arquivo_xml):
    # Simples parser de XML
    try:
        tree = ET.parse(arquivo_xml)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        itens = []
        try:
            det_tags = root.findall('.//nfe:det', ns)
            if not det_tags: det_tags = root.findall('.//det')
        except: det_tags = []

        try: nNF = root.find('.//nfe:nNF', ns).text
        except: nNF = "S/N"

        for det in det_tags:
            prod = det.find('nfe:prod', ns) if det.find('nfe:prod', ns) is not None else det.find('prod')
            if prod is not None:
                ean = prod.find('cEAN').text if prod.find('cEAN') is not None else ""
                xProd = prod.find('xProd').text
                qCom = float(prod.find('qCom').text)
                vProd = float(prod.find('vProd').text)
                vDesc = 0.0
                if prod.find('vDesc') is not None:
                    try: vDesc = float(prod.find('vDesc').text)
                    except: pass
                custo_unit = (vProd - vDesc) / qCom
                
                itens.append({
                    "ean": ean, "descricao_xml": xProd, "qtd_xml": qCom, "custo_xml": custo_unit, "nota": nNF
                })
        return itens
    except Exception as e:
        st.error(f"Erro ao ler XML: {e}")
        return []

# ==============================================================================
# 4. INTERFACE
# ==============================================================================

# Sidebar
st.sidebar.title("🏪 Controle Loja")
st.sidebar.info("Modo Demonstração (Sem Banco de Dados)")
loja_selecionada = st.sidebar.selectbox("Selecione a Unidade", ["Loja 1", "Loja 2", "Loja 3"])
col_qtd_loja = f"qtd_{normalizar_texto(loja_selecionada).replace(' ', '')}"
modo_celular = st.sidebar.checkbox("📱 Modo Celular (Simplificado)")

# Menu
menu = st.radio("Menu", ["Dashboard", "Gôndola (Busca)", "Importar XML", "Estoque Central", "Histórico", "Tabela Geral"], horizontal=True)
st.markdown("---")

# Carregar Dados
df_prod = ler_dados("Produtos")
df_hist = ler_dados("Historico")

# --- DASHBOARD ---
if menu == "Dashboard":
    c1, c2, c3 = st.columns(3)
    total_itens = df_prod[col_qtd_loja].sum()
    valor_estoque = (df_prod[col_qtd_loja] * df_prod['custo_unitario']).sum()
    c1.metric("Total Itens (Unidade)", f"{int(total_itens)}")
    c2.metric("Valor Estoque", format_br(valor_estoque))
    c3.metric("Total SKUs", len(df_prod))

# --- GÔNDOLA ---
elif menu == "Gôndola (Busca)":
    termo = st.text_input("🔍 Buscar Produto", "")
    if termo:
        termo_norm = normalizar_texto(termo)
        mask = df_prod['descricao'].apply(normalizar_texto).str.contains(termo_norm) | df_prod['ean'].astype(str).str.contains(termo_norm)
        resultados = df_prod[mask]
        for idx, row in resultados.iterrows():
            with st.container():
                st.markdown(f"""
                <div style="background-color: #f0f2f6; padding: 10px; border-radius: 10px; margin-bottom: 10px;">
                    <h4 style="margin:0; color: #333;">{row['descricao']}</h4>
                    <p style="margin:0; font-size: 0.9em; color: #666;">EAN: {row['ean']} | Loc: {row['localizacao']}</p>
                    <hr style="margin: 5px 0;">
                    <div style="display: flex; justify-content: space-between;">
                        <div><b>Loja:</b> <span style="font-size: 1.2em;">{int(row[col_qtd_loja])}</span></div>
                        <div><b>Preço:</b> <span style="font-size: 1.2em; color: green;">{format_br(row['preco_venda'])}</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

# --- IMPORTAR XML ---
elif menu == "Importar XML":
    arquivo = st.file_uploader("Upload XML NFe", type=["xml"])
    if arquivo:
        itens_xml = parse_nfe_xml(arquivo)
        if itens_xml:
            with st.form("form_import"):
                processar = []
                for i, item in enumerate(itens_xml):
                    st.markdown(f"**{item['descricao_xml']}** (Qtd: {item['qtd_xml']} | Custo: {format_br(item['custo_xml'])})")
                    
                    match_ean = df_prod[df_prod['ean'].astype(str) == str(item['ean'])]
                    if not match_ean.empty:
                        opcoes = [f"{match_ean.index[0]} - {match_ean.iloc[0]['descricao']}"]
                    else:
                        scores = []
                        for idx, row_db in df_prod.iterrows():
                            sc = calcular_match_score(item['descricao_xml'], row_db['descricao'])
                            if sc > 0.1: scores.append((idx, row_db['descricao'], sc))
                        scores.sort(key=lambda x: x[2], reverse=True)
                        opcoes = ["(CRIAR NOVO)"] + [f"{x[0]} - {x[1]}" for x in scores[:3]]
                    
                    escolha = st.selectbox("Vincular a:", opcoes, key=f"sel_{i}")
                    processar.append({"xml": item, "escolha": escolha})
                    st.divider()
                
                if st.form_submit_button("✅ Processar Estoque"):
                    df_temp = df_prod.copy()
                    novos_hist = []
                    for p in processar:
                        item = p['xml']
                        if p['escolha'] == "(CRIAR NOVO)":
                            novo = {c: 0.0 if "qtd" in c or "preco" in c else "" for c in df_prod.columns}
                            novo.update({"ean": item['ean'], "descricao": item['descricao_xml'], "custo_unitario": item['custo_xml'], "preco_venda": item['custo_xml']*1.5, col_qtd_loja: item['qtd_xml']})
                            df_temp = pd.concat([df_temp, pd.DataFrame([novo])], ignore_index=True)
                        else:
                            idx = int(p['escolha'].split(" - ")[0])
                            df_temp.at[idx, 'custo_unitario'] = item['custo_xml']
                            df_temp.at[idx, col_qtd_loja] += item['qtd_xml']
                        novos_hist.append({"data": str(datetime.date.today()), "descricao": item['descricao_xml'], "qtd_compra": item['qtd_xml'], "custo_pag": item['custo_xml']})
                    
                    salvar_dados(df_temp, "Produtos")
                    salvar_dados(pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True), "Historico")
                    st.success("Atualizado!")
                    time.sleep(1)
                    st.rerun()

# --- OUTROS MENUS ---
elif menu == "Estoque Central":
    st.dataframe(df_prod, use_container_width=True)

elif menu == "Histórico":
    st.dataframe(df_hist, use_container_width=True)

elif menu == "Tabela Geral":
    df_edit = st.data_editor(df_prod, num_rows="dynamic", use_container_width=True)
    if st.button("💾 Salvar"):
        salvar_dados(df_edit, "Produtos")
