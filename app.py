import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import xml.etree.ElementTree as ET

# --- CONFIGURAÇÃO VISUAL DA PÁGINA ---
st.set_page_config(page_title="Sistema de Estoque Pro", layout="wide", page_icon="🛒")

# --- CONEXÃO COM GOOGLE SHEETS (O COFRE) ---
def conectar_google_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_creds = json.loads(st.secrets["service_account_json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        client = gspread.authorize(creds)
        sheet = client.open("loja_dados").sheet1 
        return sheet
    except Exception as e:
        return None

# --- FUNÇÃO INTELIGENTE PARA LER E PADRONIZAR ---
def carregar_dados():
    sheet = conectar_google_sheets()
    if sheet is None:
        return None
    
    try:
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # SE A PLANILHA ESTIVER VAZIA, CRIA AS COLUNAS PADRÃO AUTOMATICAMENTE
        # Isso resolve o seu erro de KeyError!
        if df.empty:
            df = pd.DataFrame(columns=["Código", "Produto", "Quantidade", "Preço", "EAN"])
            
        # Garante que as colunas essenciais existam, mesmo se o Excel vier diferente
        colunas_padrao = ["Código", "Produto", "Quantidade"]
        for col in colunas_padrao:
            if col not in df.columns:
                df[col] = "" # Cria a coluna vazia se não existir
        
        # Garante que Quantidade seja número e Código seja texto
        df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors='coerce').fillna(0)
        df["Código"] = df["Código"].astype(str).str.replace(r'\.0$', '', regex=True) # Tira o .0 do final
        
        return df
    except:
        # Se der pane total, retorna uma tabela virgem para não travar o app
        return pd.DataFrame(columns=["Código", "Produto", "Quantidade", "Preço", "EAN"])

# --- FUNÇÃO PARA SALVAR ---
def salvar_dados(df_novo):
    sheet = conectar_google_sheets()
    if sheet is not None:
        sheet.clear()
        # Atualiza o Google Sheets
        sheet.update([df_novo.columns.values.tolist()] + df_novo.values.tolist())

# ==============================================================================
# INÍCIO DO APLICATIVO (A PARTE VISUAL)
# ==============================================================================

# Carrega os dados da nuvem
df = carregar_dados()

# --- MENU LATERAL BONITO ---
st.sidebar.title("🛒 Menu Gerencial")
menu = st.sidebar.radio("Navegação", 
    ["📊 Painel & Busca", "📥 Atualizar via Excel", "📄 Entrada de Notas (XML)", "💰 Venda Manual"])

st.sidebar.divider()
st.sidebar.info("Conectado ao Google Drive ✅")

# ---------------------------------------------------------
# 1. PAINEL DE BUSCA (AQUELA BUSCA ROBUSTA QUE VOCÊ GOSTA)
# ---------------------------------------------------------
if menu == "📊 Painel & Busca":
    st.title("📊 Visão Geral do Estoque")
    
    if df is None:
        st.error("Erro ao conectar no Google. Verifique o Secrets.")
    elif df.empty:
        st.warning("Seu estoque está vazio. Vá em 'Atualizar via Excel' para começar.")
    else:
        # Métricas no Topo
        col1, col2, col3 = st.columns(3)
        col1.metric("📦 Itens Cadastrados", len(df))
        qtd_total = int(df["Quantidade"].sum())
        col2.metric("🔢 Estoque Físico Total", qtd_total)
        
        # --- A BUSCA PODEROSA ---
        st.divider()
        termo_busca = st.text_input("🔍 Buscar Produto (Nome, Código ou EAN)", placeholder="Digite aqui para filtrar...")
        
        if termo_busca:
            # Filtra onde o termo aparece no Nome OU no Código
            filtro = df[
                df["Produto"].astype(str).str.contains(termo_busca, case=False, na=False) | 
                df["Código"].astype(str).str.contains(termo_busca, case=False, na=False)
            ]
            st.dataframe(filtro, use_container_width=True, height=400)
        else:
            st.dataframe(df, use_container_width=True, height=400)

# ---------------------------------------------------------
# 2. ATUALIZAR VIA EXCEL (PLANOGRAMA)
# ---------------------------------------------------------
elif menu == "📥 Atualizar via Excel":
    st.title("📥 Importar Estoque (Excel)")
    st.write("Use esta opção para fazer o 'upload inicial' ou substituir tudo pelo relatório do seu sistema.")
    
    arquivo = st.file_uploader("Arraste seu arquivo Excel (.xlsx) aqui", type=["xlsx", "xls"])
    
    if arquivo:
        df_upload = pd.read_excel(arquivo)
        st.write("Prévia dos dados encontrados:")
        st.dataframe(df_upload.head(3))
        
        st.warning("⚠️ ATENÇÃO: Isso vai APAGAR o estoque atual do Google e colocar esse novo no lugar.")
        
        if st.button("✅ Confirmar Substituição"):
            # Tenta padronizar nomes de colunas comuns
            rename_map = {
                "Cod": "Código", "CODIGO": "Código", "Codigo": "Código",
                "Descricao": "Produto", "DESCRICAO": "Produto", "Nome": "Produto",
                "Qtd": "Quantidade", "Saldo": "Quantidade", "Estoque": "Quantidade"
            }
            df_upload = df_upload.rename(columns=rename_map)
            
            # Garante que as colunas existem
            if "Código" not in df_upload.columns:
                st.error("Não achei a coluna 'Código' ou 'Cod'. Verifique seu Excel.")
            else:
                salvar_dados(df_upload)
                st.success("Estoque Atualizado na Nuvem! ☁️")
                st.balloons()
                st.rerun()

# ---------------------------------------------------------
# 3. ENTRADA DE NOTAS (XML) - AGORA COM PROTEÇÃO
# ---------------------------------------------------------
elif menu == "📄 Entrada de Notas (XML)":
    st.title("📄 Entrada Automática (XML)")
    
    # Verifica se o estoque tem a estrutura mínima antes de começar
    if df is None or df.empty or "Código" not in df.columns:
        st.error("Para importar XML, primeiro você precisa ter um estoque cadastrado (use a opção 'Atualizar via Excel').")
    else:
        arquivos = st.file_uploader("Selecione os arquivos XML das Notas Fiscais", type=["xml"], accept_multiple_files=True)
        
        if arquivos:
            if st.button("🚀 Processar Notas"):
                # Garante tipos
                df["Código"] = df["Código"].astype(str).str.strip()
                df["Quantidade"] = pd.to_numeric(df["Quantidade"]).fillna(0)
                
                encontrados = 0
                nao_encontrados = []
                
                progresso = st.progress(0)
                
                for i, arq in enumerate(arquivos):
                    try:
                        tree = ET.parse(arq)
                        root = tree.getroot()
                        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
                        
                        for det in root.findall(".//nfe:det", ns):
                            prod = det.find("nfe:prod", ns)
                            cod_xml = prod.find("nfe:cProd", ns).text.strip()
                            qtd_xml = float(prod.find("nfe:qCom", ns).text)
                            nome_xml = prod.find("nfe:xProd", ns).text
                            
                            # Tenta encontrar o produto no DataFrame (usando Código)
                            # Remove zeros a esquerda para facilitar (opcional)
                            
                            mask = df["Código"] == cod_xml
                            
                            if mask.any():
                                df.loc[mask, "Quantidade"] += qtd_xml
                                encontrados += 1
                            else:
                                nao_encontrados.append(f"{nome_xml} (Cód: {cod_xml})")
                                
                    except Exception as e:
                        st.error(f"Erro ao ler {arq.name}: {e}")
                    
                    progresso.progress((i + 1) / len(arquivos))
                
                # Salva o resultado final
                salvar_dados(df)
                
                st.success(f"✅ Processamento concluído! Estoque somado para {encontrados} itens.")
                
                if nao_encontrados:
                    st.warning("⚠️ Alguns produtos do XML não foram achados no seu estoque (não foram somados):")
                    st.write(nao_encontrados)

# ---------------------------------------------------------
# 4. VENDA MANUAL
# ---------------------------------------------------------
elif menu == "💰 Venda Manual":
    st.title("💰 Registrar Venda Rápida")
    
    if df is None or df.empty:
        st.warning("Estoque vazio.")
    else:
        # Caixa de seleção com busca integrada
        lista_produtos = df["Produto"].astype(str) + " | Cód: " + df["Código"].astype(str)
        escolha = st.selectbox("Busque o produto:", lista_produtos)
        
        # Pega o código selecionado
        cod_selecionado = escolha.split(" | Cód: ")[1]
        
        qtd_venda = st.number_input("Quantidade vendida:", min_value=1, value=1)
        
        if st.button("Confirmar Baixa"):
            # Localiza e subtrai
            idx = df[df["Código"].astype(str) == cod_selecionado].index[0]
            atual = float(df.at[idx, "Quantidade"])
            novo = atual - qtd_venda
            
            df.at[idx, "Quantidade"] = novo
            salvar_dados(df)
            
            st.success(f"Venda registrada! Novo saldo: {novo}")
            st.rerun()
