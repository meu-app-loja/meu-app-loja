import streamlit as st
import pandas as pd
import datetime
import xml.etree.ElementTree as ET
import unicodedata
import time
import re

# ==============================================================================
# 1. CONFIGURAÇÃO (MODO OFFLINE - SEM SENHAS)
# ==============================================================================
st.set_page_config(layout="wide", page_title="Gestão Loja (Modo Rápido)")

# Definição das colunas principais
COLUNAS_VITAIS = [
    "ean", "descricao", "categoria", 
    "custo_unitario", "preco_venda", 
    "qtd_loja1", "qtd_loja2", "qtd_loja3", 
    "estoque_central", "localizacao"
]

# ==============================================================================
# 2. BANCO DE DADOS NA MEMÓRIA (IGNORA O SECRETS)
# ==============================================================================
# Este bloco cria dados falsos para você ver o app funcionando agora

if "db_produtos" not in st.session_state:
    # Dados de exemplo para não começar vazio
    dados_exemplo = [
        {"ean": "7894900011517", "descricao": "REFRIGERANTE COCA COLA 2L", "categoria": "BEBIDAS", "custo_unitario": 5.49, "preco_venda": 8.99, "qtd_loja1": 12, "qtd_loja2": 24, "qtd_loja3": 6, "estoque_central": 100, "localizacao": "A1"},
        {"ean": "7891035800201", "descricao": "SABAO EM PO OMO 800G", "categoria": "LIMPEZA", "custo_unitario": 10.90, "preco_venda": 16.50, "qtd_loja1": 5, "qtd_loja2": 10, "qtd_loja3": 0, "estoque_central": 50, "localizacao": "B3"},
        {"ean": "7896006743120", "descricao": "ARROZ BRANCO TIPO 1 5KG", "categoria": "ALIMENTOS", "custo_unitario": 21.00, "preco_venda": 29.90, "qtd_loja1": 20, "qtd_loja2": 15, "qtd_loja3": 10, "estoque_central": 200, "localizacao": "C1"},
    ]
    df_inicial = pd.DataFrame(dados_exemplo)
    
    # Garante que todas as colunas existem preenchidas com 0
    for col in COLUNAS_VITAIS:
        if col not in df_inicial.columns:
            df_inicial[col] = 0.0
            
    st.session_state["db_produtos"] = df_inicial

if "db_historico" not in st.session_state:
    st.session_state["db_historico"] = pd.DataFrame(columns=["data", "descricao", "qtd", "valor", "nota"])

# Funções simplificadas de leitura e escrita
def ler_dados():
    return st.session_state["db_produtos"]

def salvar_dados(df_novo):
    st.session_state["db_produtos"] = df_novo
    st.toast("Alterações salvas na memória!", icon="💾")

# ==============================================================================
# 3. FUNÇÕES DE AJUDA
# ==============================================================================

def format_br(valor):
    if pd.isna(valor): return "R$ 0,00"
    return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def normalizar(texto):
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

# ==============================================================================
# 4. APLICAÇÃO VISUAL
# ==============================================================================

st.sidebar.title("🏪 Gestão Simples")
st.sidebar.warning("⚠️ Modo Demonstração (Sem Banco de Dados)")

loja = st.sidebar.selectbox("Sua Loja Atual", ["Loja 1", "Loja 2", "Loja 3"])
col_loja = f"qtd_{normalizar(loja).replace(' ', '')}" # ex: qtd_loja1

menu = st.radio("Navegação", ["Dashboard", "Consultar Preço", "Importar Nota (XML)", "Tabela Completa"], horizontal=True)
st.divider()

df = ler_dados()

# --- TELA 1: DASHBOARD ---
if menu == "Dashboard":
    col1, col2 = st.columns(2)
    qtd_total = df[col_loja].sum()
    valor_total = (df[col_loja] * df['custo_unitario']).sum()
    
    col1.metric("📦 Itens nesta Loja", int(qtd_total))
    col2.metric("💰 Valor de Estoque", format_br(valor_total))
    
    st.subheader("Produtos com Estoque Baixo")
    st.dataframe(df[df[col_loja] < 5][['descricao', col_loja]], use_container_width=True)

# --- TELA 2: CONSULTA ---
elif menu == "Consultar Preço":
    busca = st.text_input("🔍 Digite o nome ou código de barras", "")
    if busca:
        termo = normalizar(busca)
        # Filtra o dataframe
        resultado = df[df['descricao'].apply(normalizar).str.contains(termo) | df['ean'].astype(str).str.contains(termo)]
        
        if len(resultado) == 0:
            st.warning("Nenhum produto encontrado.")
        
        for i, row in resultado.iterrows():
            with st.container():
                st.info(f"**{row['descricao']}**")
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"**Preço:** :green[{format_br(row['preco_venda'])}]")
                c2.markdown(f"**Estoque Loja:** {int(row[col_loja])}")
                c3.markdown(f"**Loc:** {row['localizacao']}")

# --- TELA 3: IMPORTAR XML ---
elif menu == "Importar Nota (XML)":
    st.write("Faça upload do XML da Nota Fiscal para dar entrada.")
    arquivo = st.file_uploader("Arquivo XML", type=["xml"])
    
    if arquivo:
        try:
            tree = ET.parse(arquivo)
            root = tree.getroot()
            ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
            
            # Tenta pegar os produtos
            prods = []
            det_tags = root.findall('.//nfe:det', ns)
            if not det_tags: det_tags = root.findall('.//det') # Tenta sem namespace
            
            for det in det_tags:
                xProd = det.find('.//nfe:xProd', ns).text if det.find('.//nfe:xProd', ns) is not None else "Produto"
                qCom = float(det.find('.//nfe:qCom', ns).text) if det.find('.//nfe:qCom', ns) is not None else 0
                prods.append({"nome": xProd, "qtd": qCom})
            
            st.success(f"Leitura com sucesso! Encontrados {len(prods)} itens.")
            st.dataframe(pd.DataFrame(prods))
            
            if st.button("Simular Entrada no Estoque"):
                st.balloons()
                st.success("Estoque atualizado (Simulação)!")
                
        except Exception as e:
            st.error(f"Erro ao ler XML: {e}")

# --- TELA 4: TABELA COMPLETA ---
elif menu == "Tabela Completa":
    st.write("Edite os valores diretamente na tabela abaixo:")
    df_editado = st.data_editor(df, num_rows="dynamic", use_container_width=True)
    
    if st.button("Salvar Alterações"):
        salvar_dados(df_editado)
