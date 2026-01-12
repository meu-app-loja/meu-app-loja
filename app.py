import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import xml.etree.ElementTree as ET

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Sistema de Estoque Nuvem", layout="wide")

# --- CONEXÃO COM GOOGLE SHEETS (O COFRE) ---
def conectar_google_sheets():
    try:
        # Pega a senha que guardamos no Secrets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # O segredo vem como texto, transformamos em dicionário
        json_creds = json.loads(st.secrets["service_account_json"])
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        client = gspread.authorize(creds)
        
        # Abre a planilha 'loja_dados'
        sheet = client.open("loja_dados").sheet1 
        return sheet
    except Exception as e:
        return None

# --- FUNÇÃO PARA LER O ESTOQUE ---
def carregar_dados():
    sheet = conectar_google_sheets()
    if sheet is None:
        return None # Retorna vazio se der erro na conexão
        
    try:
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Se a planilha estiver vazia, cria estrutura padrão
        if df.empty:
            return pd.DataFrame(columns=["Código", "Produto", "Quantidade", "Preço"])
            
        # Garante que colunas numéricas sejam números
        if "Quantidade" in df.columns:
            df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors='coerce').fillna(0)
            
        return df
    except:
        return pd.DataFrame(columns=["Código", "Produto", "Quantidade", "Preço"])

# --- FUNÇÃO PARA SALVAR NO ESTOQUE ---
def salvar_dados(df_novo):
    sheet = conectar_google_sheets()
    if sheet is not None:
        sheet.clear() # Limpa tudo
        # Sobrescreve com os dados novos
        sheet.update([df_novo.columns.values.tolist()] + df_novo.values.tolist())

# --- INÍCIO DO APP ---
st.title("🛒 Controle de Estoque na Nuvem")

# Tenta carregar os dados
df_estoque = carregar_dados()

# SE A CONEXÃO FALHAR (O erro que você estava vendo)
if df_estoque is None:
    st.error("🚨 ERRO DE CONEXÃO COM O GOOGLE!")
    st.write("O problema está na configuração do 'Secrets' no site do Streamlit.")
    st.info("Dica: Verifique se você copiou o arquivo JSON inteiro e colocou entre as aspas triplas.")
    st.stop() # Para o código aqui

# --- MENU LATERAL ---
menu = st.sidebar.selectbox("Menu Principal", 
    ["📊 Ver Estoque", "📥 Importar Planilha (Excel)", "📄 Entrada de Notas (XML)", "💰 Registrar Venda"])

# ---------------------------------------------------------
# MENU 1: VER ESTOQUE
# ---------------------------------------------------------
if menu == "📊 Ver Estoque":
    st.subheader("Estoque Atual")
    if df_estoque.empty:
        st.warning("Estoque vazio. Importe uma planilha para começar.")
    else:
        st.dataframe(df_estoque, use_container_width=True)
        st.metric("Total de Produtos", len(df_estoque))

# ---------------------------------------------------------
# MENU 2: IMPORTAR EXCEL
# ---------------------------------------------------------
elif menu == "📥 Importar Planilha (Excel)":
    st.subheader("Atualizar Estoque via Excel")
    st.write("Use isso para subir o 'Planograma' ou listagem do seu sistema antigo.")
    
    arquivo = st.file_uploader("Arraste o Excel aqui", type=["xlsx", "xls"])
    
    if arquivo:
        try:
            df_novo = pd.read_excel(arquivo)
            st.write("Prévia dos dados:")
            st.dataframe(df_novo.head())
            
            if st.button("✅ Salvar no Sistema"):
                salvar_dados(df_novo)
                st.success("Estoque atualizado com sucesso!")
                st.rerun() # Recarrega a página
        except Exception as e:
            st.error(f"Erro ao ler Excel: {e}")

# ---------------------------------------------------------
# MENU 3: ENTRADA XML
# ---------------------------------------------------------
elif menu == "📄 Entrada de Notas (XML)":
    st.subheader("Ler Nota Fiscal (XML)")
    
    arquivos = st.file_uploader("Solte os XMLs aqui", type=["xml"], accept_multiple_files=True)
    
    if arquivos and not df_estoque.empty:
        if st.button("Processar Entrada"):
            encontrados = 0
            df_estoque["Código"] = df_estoque["Código"].astype(str)
            
            for arq in arquivos:
                try:
                    tree = ET.parse(arq)
                    root = tree.getroot()
                    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
                    
                    for det in root.findall(".//nfe:det", ns):
                        prod = det.find("nfe:prod", ns)
                        cod = prod.find("nfe:cProd", ns).text
                        qtd = float(prod.find("nfe:qCom", ns).text)
                        
                        # Busca e soma
                        mask = df_estoque["Código"] == cod
                        if mask.any():
                            df_estoque.loc[mask, "Quantidade"] += qtd
                            encontrados += 1
                except:
                    st.error(f"Erro ao ler o arquivo {arq.name}")
            
            salvar_dados(df_estoque)
            st.success(f"Entrada concluída! {encontrados} produtos atualizados.")
            st.balloons()

# ---------------------------------------------------------
# MENU 4: VENDA MANUAL
# ---------------------------------------------------------
elif menu == "💰 Registrar Venda":
    st.subheader("Baixa de Estoque")
    
    if not df_estoque.empty:
        prod = st.selectbox("Produto", df_estoque["Produto"].unique())
        qtd = st.number_input("Quantidade", min_value=1, value=1)
        
        if st.button("Confirmar Baixa"):
            idx = df_estoque[df_estoque["Produto"] == prod].index[0]
            atual = float(df_estoque.at[idx, "Quantidade"])
            df_estoque.at[idx, "Quantidade"] = atual - qtd
            
            salvar_dados(df_estoque)
            st.success(f"Venda registrada! Restam: {atual - qtd}")
