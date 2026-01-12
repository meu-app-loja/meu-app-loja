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
    # Pega a senha que guardamos no Secrets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Transforma o texto do Secrets em um formato que o Google entende
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    client = gspread.authorize(creds)
    
    # Abre a planilha 'loja_dados'
    # Se der erro aqui, é porque o nome da planilha no Google não é 'loja_dados'
    # ou o email do robô não foi colocado como Editor.
    sheet = client.open("loja_dados").sheet1 
    return sheet

# --- FUNÇÃO PARA LER O ESTOQUE ---
def carregar_dados():
    try:
        sheet = conectar_google_sheets()
        dados = sheet.get_all_records()
        df = pd.DataFrame(dados)
        
        # Se a planilha estiver vazia no começo, cria colunas padrão
        if df.empty:
            return pd.DataFrame(columns=["Código", "Produto", "Quantidade", "Preço"])
            
        return df
    except Exception as e:
        st.error(f"Erro ao conectar com a planilha: {e}")
        return pd.DataFrame()

# --- FUNÇÃO PARA SALVAR NO ESTOQUE ---
def salvar_dados(df_novo):
    sheet = conectar_google_sheets()
    sheet.clear() # Limpa a planilha antiga
    # Atualiza com os dados novos (cabeçalho + dados)
    sheet.update([df_novo.columns.values.tolist()] + df_novo.values.tolist())

# --- BARRA LATERAL (MENU) ---
menu = st.sidebar.selectbox("Escolha uma opção:", 
    ["📊 Ver Estoque", "📥 Importar Planilha (Planograma)", "📄 Entrada de Notas (XML)", "💰 Registrar Venda Manual"])

st.title("🛒 Controle de Estoque na Nuvem")

# --- CARREGA O ESTOQUE ATUAL DO GOOGLE ---
df_estoque = carregar_dados()

# ---------------------------------------------------------
# MENU 1: VER ESTOQUE
# ---------------------------------------------------------
if menu == "📊 Ver Estoque":
    st.subheader("Estoque Atual (Salvo no Google Sheets)")
    
    if df_estoque.empty:
        st.warning("Seu estoque está vazio! Vá em 'Importar Planilha' para começar.")
    else:
        # Mostra a tabela colorida e bonita
        st.dataframe(df_estoque, use_container_width=True)
        
        # Filtros rápidos
        st.divider()
        st.metric("Total de Produtos Cadastrados", len(df_estoque))
        if "Quantidade" in df_estoque.columns:
             st.metric("Total de Itens Físicos", df_estoque["Quantidade"].sum())

# ---------------------------------------------------------
# MENU 2: IMPORTAR PLANILHA (DO SEU SISTEMA DE VENDAS)
# ---------------------------------------------------------
elif menu == "📥 Importar Planilha (Planograma)":
    st.subheader("Atualizar Estoque via Excel")
    st.info("Exporte o 'Planograma' do seu sistema antigo e solte aqui.")
    
    arquivo = st.file_uploader("Arraste seu arquivo Excel aqui", type=["xlsx", "xls"])
    
    if arquivo:
        try:
            df_novo = pd.read_excel(arquivo)
            
            # Mostra uma prévia para conferir
            st.write("Prévia dos dados encontrados:")
            st.dataframe(df_novo.head())
            
            if st.button("✅ Confirmar e Substituir Estoque Online"):
                salvar_dados(df_novo)
                st.success("Sucesso! O Google Sheets foi atualizado com essa planilha.")
                st.balloons()
                
        except Exception as e:
            st.error(f"Erro ao ler o arquivo: {e}")

# ---------------------------------------------------------
# MENU 3: ENTRADA DE NOTAS (XML)
# ---------------------------------------------------------
elif menu == "📄 Entrada de Notas (XML)":
    st.subheader("Dar Entrada via XML")
    
    arquivos_xml = st.file_uploader("Solte as Notas Fiscais (XML) aqui", type=["xml"], accept_multiple_files=True)
    
    if arquivos_xml and not df_estoque.empty:
        if st.button("Processar Notas e Atualizar Estoque"):
            
            # Garantir que as colunas existem e são números
            cols_obrigatorias = ["Código", "Quantidade"]
            if not all(col in df_estoque.columns for col in cols_obrigatorias):
                st.error("Sua planilha precisa ter as colunas 'Código' e 'Quantidade' para isso funcionar.")
            else:
                df_estoque["Quantidade"] = pd.to_numeric(df_estoque["Quantidade"], errors='coerce').fillna(0)
                df_estoque["Código"] = df_estoque["Código"].astype(str) # Garante que código é texto para comparar
                
                produtos_encontrados = 0
                
                for arquivo in arquivos_xml:
                    tree = ET.parse(arquivo)
                    root = tree.getroot()
                    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
                    
                    # Procura produtos dentro do XML (estrutura padrão NFe)
                    for det in root.findall(".//nfe:det", ns):
                        prod = det.find("nfe:prod", ns)
                        codigo_xml = prod.find("nfe:cProd", ns).text
                        qtd_xml = float(prod.find("nfe:qCom", ns).text)
                        nome_xml = prod.find("nfe:xProd", ns).text
                        
                        # Tenta achar o produto no estoque pelo código
                        mask = df_estoque["Código"] == codigo_xml
                        
                        if mask.any():
                            df_estoque.loc[mask, "Quantidade"] += qtd_xml
                            produtos_encontrados += 1
                        else:
                            st.warning(f"Produto novo (não cadastrado): {nome_xml} (Cód: {codigo_xml})")
                            # Opcional: Adicionar produto novo automaticamente (pode ser complexo agora)
                
                # Salva tudo no Google Sheets
                salvar_dados(df_estoque)
                st.success(f"Processamento concluído! {produtos_encontrados} produtos tiveram o estoque aumentado.")

# ---------------------------------------------------------
# MENU 4: REGISTRAR VENDA MANUAL
# ---------------------------------------------------------
elif menu == "💰 Registrar Venda Manual":
    st.subheader("Baixa Manual de Estoque")
    
    if df_estoque.empty:
        st.warning("O estoque está vazio.")
    else:
        produto_selecionado = st.selectbox("Selecione o Produto", df_estoque["Produto"].unique())
        qtd_venda = st.number_input("Quantidade Vendida", min_value=1, value=1)
        
        if st.button("Confirmar Venda"):
            # Lógica para encontrar e diminuir
            linha = df_estoque[df_estoque["Produto"] == produto_selecionado].index[0]
            atual = float(df_estoque.at[linha, "Quantidade"])
            
            nova_qtd = atual - qtd_venda
            df_estoque.at[linha, "Quantidade"] = nova_qtd
            
            # Salva na nuvem
            salvar_dados(df_estoque)
            st.success(f"Venda registrada! Novo estoque de {produto_selecionado}: {nova_qtd}")
