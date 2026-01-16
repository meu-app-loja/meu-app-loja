import streamlit as st
import pandas as pd
import datetime
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time
import re

# ==============================================================================
# 1. CONFIGURAÇÃO E CONSTANTES
# ==============================================================================
st.set_page_config(layout="wide", page_title="Gestão Multi-Lojas")

# Colunas Vitais (O sistema garante que estas existam)
COLUNAS_VITAIS = [
    "ean", "descricao", "categoria", 
    "custo_unitario", "preco_venda", 
    "qtd_loja1", "qtd_loja2", "qtd_loja3", 
    "estoque_central", "localizacao"
]

COLS_HIST = ["data", "ean", "descricao", "qtd_compra", "custo_pag", "num_nota"]

# ==============================================================================
# 2. FUNÇÕES AUXILIARES CRÍTICAS
# ==============================================================================

def normalizar_texto(texto):
    """Remove acentos e coloca em minúsculas para comparação."""
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

def format_br(valor):
    """Formata float para moeda BR (R$ 1.000,00)."""
    if valor is None or pd.isna(valor): return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def converter_ptbr(valor, is_price=False):
    """
    Limpeza robusta. Trata vírgulas e R$.
    LÓGICA DE PROTEÇÃO: Se is_price=True e valor >= 100 e for inteiro (ex: 319),
    entende-se que o usuário esqueceu a vírgula -> converte para 3.19.
    """
    if pd.isna(valor) or valor == "": return 0.0
    
    val_str = str(valor).replace("R$", "").strip()
    # Troca vírgula por ponto para conversão
    val_str = val_str.replace(".", "").replace(",", ".")
    
    try:
        val_float = float(val_str)
        
        # Correção de digitação (Ex: digitou 319 querendo dizer 3,19)
        if is_price and val_float >= 100 and val_float.is_integer():
             # Verifica se é um erro provável (preços muito altos sem centavos)
             # Assume-se erro se for maior que 100.0
             val_float = val_float / 100.0
             
        return val_float
    except ValueError:
        return 0.0

def garantir_integridade_colunas(df, colunas_alvo):
    """
    Garante colunas vitais sem apagar colunas extras (planograma, obs).
    Preenche vazios numéricos com 0.0.
    """
    # 1. Garante que as vitais existem
    for col in colunas_alvo:
        if col not in df.columns:
            df[col] = 0.0 if "qtd" in col or "preco" in col or "custo" in col else ""
            
    # 2. Tipagem básica e preenchimento de vazios
    for col in df.columns:
        if "qtd" in col or "preco" in col or "custo" in col or "estoque" in col:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        else:
            df[col] = df[col].fillna("").astype(str)
            
    return df

def calcular_match_score(nome_xml, nome_db):
    """Calcula pontuação de semelhança (Interseção de palavras)."""
    set_xml = set(normalizar_texto(nome_xml).split())
    set_db = set(normalizar_texto(nome_db).split())
    
    if not set_xml or not set_db: return 0
    
    intersection = set_xml.intersection(set_db)
    return len(intersection) / len(set_xml) # Score simples

# ==============================================================================
# 3. INTEGRAÇÃO GOOGLE SHEETS (GSPREAD)
# ==============================================================================

def get_con():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Lê do st.secrets para segurança
    creds_dict = dict(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_base):
    """Lê dados da nuvem com cache de 60s."""
    client = get_con()
    try:
        sheet = client.open("Sistema_Multi_Lojas").worksheet(nome_aba)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
    except gspread.exceptions.WorksheetNotFound:
        # Se não existe, retorna vazio estruturado
        df = pd.DataFrame(columns=colunas_base)
    except gspread.exceptions.SpreadsheetNotFound:
         st.error("Planilha 'Sistema_Multi_Lojas' não encontrada no Google Drive.")
         st.stop()
         
    return garantir_integridade_colunas(df, colunas_base)

def salvar_na_nuvem(df, nome_aba):
    """Salva DF inteiro na nuvem."""
    client = get_con()
    sh = client.open("Sistema_Multi_Lojas")
    
    try:
        worksheet = sh.worksheet(nome_aba)
    except:
        worksheet = sh.add_worksheet(title=nome_aba, rows="100", cols="20")
    
    # Prepara dados (converte datas e floats para strings compatíveis se necessário, 
    # mas gspread lida bem com raw data se json serializable)
    # Convertendo NaN para "" para limpar visual no sheets
    df_save = df.fillna("")
    
    # Limpa e escreve
    worksheet.clear()
    worksheet.update([df_save.columns.values.tolist()] + df_save.values.tolist())
    st.toast(f"Dados salvos em '{nome_aba}' com sucesso!", icon="✅")
    
    # Limpa cache para forçar recarregamento
    ler_da_nuvem.clear()

# ==============================================================================
# 4. LÓGICA DE NEGÓCIOS & XML
# ==============================================================================

def parse_nfe_xml(arquivo_xml):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    # Namespace da NFe
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    
    itens = []
    
    # Tenta encontrar infNFe com ou sem namespace (tratamento de erro comum)
    try:
        det_tags = root.findall('.//nfe:det', ns)
        if not det_tags: det_tags = root.findall('.//det')
    except:
        det_tags = []

    # Número da Nota
    try:
        nNF = root.find('.//nfe:nNF', ns).text
    except:
        nNF = "S/N"

    for det in det_tags:
        prod = det.find('nfe:prod', ns) if det.find('nfe:prod', ns) is not None else det.find('prod')
        
        if prod is not None:
            ean = prod.find('cEAN').text if prod.find('cEAN') is not None else ""
            xProd = prod.find('xProd').text
            qCom = float(prod.find('qCom').text)
            vProd = float(prod.find('vProd').text)
            
            # Desconto se houver
            vDesc = 0.0
            if prod.find('vDesc') is not None:
                try: vDesc = float(prod.find('vDesc').text)
                except: pass
            
            # Custo unitário líquido
            custo_unit = (vProd - vDesc) / qCom
            
            itens.append({
                "ean": ean,
                "descricao_xml": xProd,
                "qtd_xml": qCom,
                "custo_xml": custo_unit,
                "nota": nNF
            })
    return itens

# ==============================================================================
# 5. INTERFACE STREAMLIT
# ==============================================================================

# Sidebar
st.sidebar.title("🏪 Controle Loja")
loja_selecionada = st.sidebar.selectbox("Selecione a Unidade", ["Loja 1", "Loja 2", "Loja 3"])
col_qtd_loja = f"qtd_{normalizar_texto(loja_selecionada).replace(' ', '')}" # ex: qtd_loja1
modo_celular = st.sidebar.checkbox("📱 Modo Celular (Simplificado)")

# Menu Principal
menu = st.radio("Menu", ["Dashboard", "Gôndola (Busca)", "Importar XML", "Estoque Central", "Histórico", "Tabela Geral"], horizontal=True)
st.markdown("---")

# Carregar Dados Iniciais
df_prod = ler_da_nuvem("Produtos", COLUNAS_VITAIS)
df_hist = ler_da_nuvem("Historico", COLS_HIST)

# --- 1. DASHBOARD ---
if menu == "Dashboard":
    c1, c2, c3 = st.columns(3)
    total_itens = df_prod[col_qtd_loja].sum()
    valor_estoque = (df_prod[col_qtd_loja] * df_prod['custo_unitario']).sum()
    
    c1.metric("Total Itens (Unidade Atual)", f"{int(total_itens)}")
    c2.metric("Valor em Estoque (Custo)", format_br(valor_estoque))
    c3.metric("Total de SKUs", len(df_prod))

# --- 2. GÔNDOLA (BUSCA) ---
elif menu == "Gôndola (Busca)":
    termo = st.text_input("🔍 Buscar Produto (Nome ou EAN)", "")
    
    if termo:
        termo_norm = normalizar_texto(termo)
        # Filtro
        mask = df_prod['descricao'].apply(normalizar_texto).str.contains(termo_norm) | \
               df_prod['ean'].astype(str).str.contains(termo_norm)
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
                
                if not modo_celular:
                    st.dataframe(row.to_frame().T, hide_index=True)

# --- 3. IMPORTAR XML ---
elif menu == "Importar XML":
    st.info("Importe o XML da Nota Fiscal para atualizar estoque e custos.")
    arquivo = st.file_uploader("Upload XML NFe", type=["xml"])
    
    if arquivo:
        itens_xml = parse_nfe_xml(arquivo)
        st.write(f"Encontrados {len(itens_xml)} itens na nota.")
        
        # Formulário para processar
        with st.form("form_import"):
            processar_dados = []
            
            for i, item in enumerate(itens_xml):
                st.markdown(f"**Item {i+1}: {item['descricao_xml']}** (Qtd: {item['qtd_xml']} | Custo: {format_br(item['custo_xml'])})")
                
                # Lógica de Match Inteligente
                melhor_match = None
                maior_score = 0
                
                # Tenta primeiro pelo EAN exato
                match_ean = df_prod[df_prod['ean'].astype(str) == str(item['ean'])]
                
                if not match_ean.empty:
                    match_idx = match_ean.index[0]
                    opcoes = [f"{match_idx} - {match_ean.iloc[0]['descricao']}"]
                    index_padrao = 0
                else:
                    # Match por nome (Score)
                    scores = []
                    for idx, row_db in df_prod.iterrows():
                        sc = calcular_match_score(item['descricao_xml'], row_db['descricao'])
                        if sc > 0.1: # Filtro mínimo
                            scores.append((idx, row_db['descricao'], sc))
                    
                    scores.sort(key=lambda x: x[2], reverse=True)
                    top_matches = scores[:3]
                    
                    opcoes = ["(CRIAR NOVO)"] + [f"{x[0]} - {x[1]}" for x in top_matches]
                    index_padrao = 0 if not top_matches else 1 # Se achou match, sugere o primeiro
                
                escolha = st.selectbox(f"Vincular '{item['descricao_xml']}' a:", options=opcoes, index=index_padrao, key=f"sel_{i}")
                
                processar_dados.append({
                    "xml": item,
                    "escolha": escolha
                })
                st.divider()
            
            if st.form_submit_button("✅ Processar Entrada de Estoque"):
                novos_historicos = []
                df_temp = df_prod.copy()
                
                for p in processar_dados:
                    item = p['xml']
                    escolha = p['escolha']
                    
                    # Se for criar novo
                    if escolha == "(CRIAR NOVO)":
                        novo_produto = {col: "" for col in df_prod.columns}
                        novo_produto.update({
                            "ean": item['ean'],
                            "descricao": item['descricao_xml'],
                            "custo_unitario": item['custo_xml'],
                            "preco_venda": item['custo_xml'] * 1.5, # Margem padrão 50%
                            col_qtd_loja: item['qtd_xml'],
                            "estoque_central": 0
                        })
                        # Adiciona linha
                        df_temp = pd.concat([df_temp, pd.DataFrame([novo_produto])], ignore_index=True)
                        ean_ref = item['ean']
                        desc_ref = item['descricao_xml']
                        
                    else:
                        # Atualizar Existente
                        idx_db = int(escolha.split(" - ")[0])
                        # Atualiza custo (Ponderado ou último? Aqui usaremos último para simplificar)
                        df_temp.at[idx_db, 'custo_unitario'] = item['custo_xml']
                        # Soma estoque
                        qtd_atual = float(df_temp.at[idx_db, col_qtd_loja])
                        df_temp.at[idx_db, col_qtd_loja] = qtd_atual + item['qtd_xml']
                        
                        ean_ref = df_temp.at[idx_db, 'ean']
                        desc_ref = df_temp.at[idx_db, 'descricao']

                    # Adiciona ao histórico
                    novos_historicos.append({
                        "data": datetime.date.today().strftime("%Y-%m-%d"),
                        "ean": ean_ref,
                        "descricao": desc_ref,
                        "qtd_compra": item['qtd_xml'],
                        "custo_pag": item['custo_xml'],
                        "num_nota": item['nota']
                    })
                
                # Salvar Atualizações
                salvar_na_nuvem(df_temp, "Produtos")
                
                # Salvar Histórico
                df_hist_novo = pd.concat([df_hist, pd.DataFrame(novos_historicos)], ignore_index=True)
                salvar_na_nuvem(df_hist_novo, "Historico")
                
                st.success("Estoque atualizado com sucesso!")
                time.sleep(2)
                st.rerun()

# --- 4. ESTOQUE CENTRAL ---
elif menu == "Estoque Central":
    st.dataframe(df_prod[['ean', 'descricao', 'estoque_central', 'custo_unitario']], use_container_width=True)

# --- 5. HISTÓRICO ---
elif menu == "Histórico":
    st.dataframe(df_hist.sort_values(by="data", ascending=False), use_container_width=True)

# --- 6. TABELA GERAL (EDIÇÃO) ---
elif menu == "Tabela Geral":
    st.warning("⚠️ Edição Direta. Cuidado ao alterar valores.")
    
    # Data Editor
    df_editado = st.data_editor(
        df_prod, 
        num_rows="dynamic", 
        use_container_width=True,
        column_config={
            "preco_venda": st.column_config.NumberColumn(format="R$ %.2f"),
            "custo_unitario": st.column_config.NumberColumn(format="R$ %.2f"),
        }
    )
    
    if st.button("💾 Salvar Alterações na Nuvem"):
        # Aplica a lógica de converter PT-BR e Correção de Preço
        cols_preco = ['custo_unitario', 'preco_venda']
        
        for col in cols_preco:
            # Aplica a função em cada célula das colunas de preço
            df_editado[col] = df_editado[col].apply(lambda x: converter_ptbr(x, is_price=True))
            
        salvar_na_nuvem(df_editado, "Produtos")
