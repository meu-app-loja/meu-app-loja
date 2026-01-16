import streamlit as st
import pandas as pd
import datetime
import xml.etree.ElementTree as ET
import unicodedata
import time
import re

# ==============================================================================
# 1. CONFIGURAÇÃO GERAL
# ==============================================================================
st.set_page_config(layout="wide", page_title="Gestão Multi-Lojas (Completo Offline)")

# Colunas Vitais (Não altere a ordem)
COLUNAS_VITAIS = [
    "ean", "descricao", "categoria", 
    "custo_unitario", "preco_venda", 
    "qtd_loja1", "qtd_loja2", "qtd_loja3", 
    "estoque_central", "localizacao"
]

COLS_HIST = ["data", "ean", "descricao", "qtd_compra", "custo_pag", "num_nota"]

# ==============================================================================
# 2. BANCO DE DADOS NA MEMÓRIA (COM DADOS INICIAIS)
# ==============================================================================

if "db_produtos" not in st.session_state:
    # Dados iniciais para você não ver tela branca
    dados_iniciais = [
        {"ean": "7894900011517", "descricao": "COCA COLA 2L", "categoria": "BEBIDAS", "custo_unitario": 5.49, "preco_venda": 8.99, "qtd_loja1": 12, "qtd_loja2": 24, "qtd_loja3": 6, "estoque_central": 100, "localizacao": "A1"},
        {"ean": "7891035800201", "descricao": "SABAO OMO 1KG", "categoria": "LIMPEZA", "custo_unitario": 12.50, "preco_venda": 18.90, "qtd_loja1": 5, "qtd_loja2": 10, "qtd_loja3": 2, "estoque_central": 50, "localizacao": "B3"},
        {"ean": "7896006743120", "descricao": "ARROZ TIO JOAO 5KG", "categoria": "ALIMENTOS", "custo_unitario": 22.15, "preco_venda": 32.90, "qtd_loja1": 20, "qtd_loja2": 15, "qtd_loja3": 10, "estoque_central": 200, "localizacao": "C1"},
    ]
    df_inicio = pd.DataFrame(dados_iniciais)
    # Garante estrutura correta
    for col in COLUNAS_VITAIS:
        if col not in df_inicio.columns:
            df_inicio[col] = 0.0
    st.session_state["db_produtos"] = df_inicio

if "db_historico" not in st.session_state:
    st.session_state["db_historico"] = pd.DataFrame(columns=COLS_HIST)

# Funções para ler e salvar na memória
def ler_dados(tipo):
    if tipo == "Produtos": return st.session_state["db_produtos"]
    return st.session_state["db_historico"]

def salvar_dados(df, tipo):
    if tipo == "Produtos":
        # Garante que números são números antes de salvar
        for col in ["custo_unitario", "preco_venda", "qtd_loja1", "qtd_loja2", "qtd_loja3"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        st.session_state["db_produtos"] = df
    else:
        st.session_state["db_historico"] = df
    st.toast("Dados atualizados na memória!", icon="✅")

# ==============================================================================
# 3. FUNÇÕES INTELIGENTES (CORREÇÃO DE FORMATO)
# ==============================================================================

def normalizar_texto(texto):
    """Limpa texto para buscas."""
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower().strip()

def format_br(valor):
    """Mostra R$ bonito na tela."""
    if valor is None or pd.isna(valor): return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

def calcular_match_score(nome_xml, nome_db):
    """Pontuação para sugerir produto parecido."""
    set_xml = set(normalizar_texto(nome_xml).split())
    set_db = set(normalizar_texto(nome_db).split())
    if not set_xml or not set_db: return 0
    intersection = set_xml.intersection(set_db)
    return len(intersection) / len(set_xml)

def parse_nfe_xml(arquivo_xml):
    """Lê a Nota Fiscal XML."""
    try:
        tree = ET.parse(arquivo_xml)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        itens = []
        
        # Tenta achar tags com ou sem namespace
        try: det_tags = root.findall('.//nfe:det', ns)
        except: det_tags = []
        if not det_tags: det_tags = root.findall('.//det')
        
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
                itens.append({"ean": ean, "descricao_xml": xProd, "qtd_xml": qCom, "custo_xml": custo_unit, "nota": nNF})
        return itens
    except Exception as e:
        st.error(f"Erro no XML: {e}")
        return []

# ==============================================================================
# 4. INTERFACE COMPLETA
# ==============================================================================

# --- SIDEBAR ---
st.sidebar.title("🏪 Gestão Pro")
st.sidebar.info("Modo: Memória Local (Rápido)")
loja_selecionada = st.sidebar.selectbox("Selecione a Unidade", ["Loja 1", "Loja 2", "Loja 3"])
col_qtd_loja = f"qtd_{normalizar_texto(loja_selecionada).replace(' ', '')}"
modo_celular = st.sidebar.checkbox("📱 Modo Celular (Simplificado)")

# --- MENU PRINCIPAL ---
menu = st.radio("Menu", ["Dashboard", "Gôndola (Busca)", "Importar XML", "Estoque Central", "Histórico", "Tabela Geral"], horizontal=True)
st.markdown("---")

# Carrega dados atuais
df_prod = ler_dados("Produtos")
df_hist = ler_dados("Historico")

# 1. DASHBOARD
if menu == "Dashboard":
    c1, c2, c3 = st.columns(3)
    total_itens = df_prod[col_qtd_loja].sum()
    valor_estoque = (df_prod[col_qtd_loja] * df_prod['custo_unitario']).sum()
    
    c1.metric("Total Itens (Unidade)", f"{int(total_itens)}")
    c2.metric("Valor Estoque (Custo)", format_br(valor_estoque))
    c3.metric("Total de SKUs", len(df_prod))
    
    st.markdown("### ⚠️ Estoque Baixo")
    st.dataframe(df_prod[df_prod[col_qtd_loja] < 5][['descricao', col_qtd_loja, 'estoque_central']], use_container_width=True)

# 2. GÔNDOLA (BUSCA)
elif menu == "Gôndola (Busca)":
    termo = st.text_input("🔍 Buscar Produto (Nome ou EAN)", "")
    if termo:
        termo_norm = normalizar_texto(termo)
        mask = df_prod['descricao'].apply(normalizar_texto).str.contains(termo_norm) | df_prod['ean'].astype(str).str.contains(termo_norm)
        resultados = df_prod[mask]
        
        for idx, row in resultados.iterrows():
            with st.container():
                st.markdown(f"""
                <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-bottom: 10px; border-left: 5px solid #ff4b4b;">
                    <h4 style="margin:0; color: #333;">{row['descricao']}</h4>
                    <p style="margin:0; color: #666;">EAN: {row['ean']} | Loc: <b>{row['localizacao']}</b></p>
                    <hr style="margin: 8px 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div style="font-size: 1.1em;">Estoque Loja: <b>{int(row[col_qtd_loja])}</b></div>
                        <div style="font-size: 1.3em; color: green; font-weight: bold;">{format_br(row['preco_venda'])}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if not modo_celular:
                    st.dataframe(row.to_frame().T, hide_index=True)

# 3. IMPORTAR XML
elif menu == "Importar XML":
    st.info("Importe o XML da Nota Fiscal para atualizar estoque e custos.")
    arquivo = st.file_uploader("Upload XML NFe", type=["xml"])
    
    if arquivo:
        itens_xml = parse_nfe_xml(arquivo)
        st.write(f"Encontrados {len(itens_xml)} itens.")
        
        with st.form("form_import"):
            processar_dados = []
            for i, item in enumerate(itens_xml):
                st.markdown(f"**Item {i+1}: {item['descricao_xml']}**")
                st.caption(f"Qtd: {item['qtd_xml']} | Custo Nota: {format_br(item['custo_xml'])}")
                
                # Match Inteligente
                match_ean = df_prod[df_prod['ean'].astype(str) == str(item['ean'])]
                if not match_ean.empty:
                    idx_match = match_ean.index[0]
                    opcoes = [f"{idx_match} - {match_ean.iloc[0]['descricao']}"]
                    index_padrao = 0
                else:
                    scores = []
                    for idx, row_db in df_prod.iterrows():
                        sc = calcular_match_score(item['descricao_xml'], row_db['descricao'])
                        if sc > 0.1: scores.append((idx, row_db['descricao'], sc))
                    scores.sort(key=lambda x: x[2], reverse=True)
                    opcoes = ["(CRIAR NOVO)"] + [f"{x[0]} - {x[1]}" for x in scores[:3]]
                    index_padrao = 0 if not scores else 1
                
                escolha = st.selectbox(f"Vincular a:", opcoes, index=index_padrao, key=f"sel_{i}")
                processar_dados.append({"xml": item, "escolha": escolha})
                st.divider()
            
            if st.form_submit_button("✅ Processar Entrada de Estoque"):
                novos_historicos = []
                df_temp = df_prod.copy()
                
                for p in processar_dados:
                    item = p['xml']
                    if p['escolha'] == "(CRIAR NOVO)":
                        novo = {c: 0.0 if "qtd" in c or "preco" in c else "" for c in df_prod.columns}
                        novo.update({
                            "ean": item['ean'], "descricao": item['descricao_xml'],
                            "custo_unitario": float(item['custo_xml']), 
                            "preco_venda": float(item['custo_xml']) * 1.5,
                            col_qtd_loja: float(item['qtd_xml']),
                            "estoque_central": 0
                        })
                        df_temp = pd.concat([df_temp, pd.DataFrame([novo])], ignore_index=True)
                        desc_ref = item['descricao_xml']
                    else:
                        idx_db = int(p['escolha'].split(" - ")[0])
                        df_temp.at[idx_db, 'custo_unitario'] = float(item['custo_xml'])
                        df_temp.at[idx_db, col_qtd_loja] = float(df_temp.at[idx_db, col_qtd_loja]) + float(item['qtd_xml'])
                        desc_ref = df_temp.at[idx_db, 'descricao']
                    
                    novos_historicos.append({
                        "data": datetime.date.today().strftime("%Y-%m-%d"),
                        "ean": item['ean'], "descricao": desc_ref,
                        "qtd_compra": item['qtd_xml'], "custo_pag": item['custo_xml'], "num_nota": item['nota']
                    })
                
                salvar_dados(df_temp, "Produtos")
                salvar_dados(pd.concat([df_hist, pd.DataFrame(novos_historicos)], ignore_index=True), "Historico")
                st.success("Estoque atualizado!")
                time.sleep(1)
                st.rerun()

# 4. ESTOQUE CENTRAL
elif menu == "Estoque Central":
    st.dataframe(df_prod[['ean', 'descricao', 'estoque_central', 'custo_unitario']], use_container_width=True)

# 5. HISTÓRICO
elif menu == "Histórico":
    st.dataframe(df_hist, use_container_width=True)

# 6. TABELA GERAL (EDIÇÃO)
elif menu == "Tabela Geral":
    st.warning("⚠️ Edite os valores abaixo. O sistema salvará automaticamente ao clicar no botão.")
    
    # Configuração para garantir que números sejam tratados como dinheiro/float
    column_config = {
        "preco_venda": st.column_config.NumberColumn("Preço Venda", format="R$ %.2f", step=0.01),
        "custo_unitario": st.column_config.NumberColumn("Custo", format="R$ %.2f", step=0.01),
        "qtd_loja1": st.column_config.NumberColumn("Qtd Loja 1", step=1),
        "qtd_loja2": st.column_config.NumberColumn("Qtd Loja 2", step=1),
        "qtd_loja3": st.column_config.NumberColumn("Qtd Loja 3", step=1),
    }

    df_editado = st.data_editor(
        df_prod, 
        num_rows="dynamic", 
        use_container_width=True,
        column_config=column_config
    )
    
    if st.button("💾 Salvar Alterações na Memória"):
        salvar_dados(df_editado, "Produtos")
