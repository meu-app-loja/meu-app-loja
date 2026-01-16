import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import unicodedata
import time
import re

# ==============================================================================
# ⚙️ CONFIGURAÇÃO GERAL
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas (Pro Offline)", layout="wide", page_icon="🏪")

# Fuso Horário Amazonas
FUSO_HORARIO = -4

def agora_am():
    return datetime.utcnow() + timedelta(hours=FUSO_HORARIO)

# Colunas Obrigatórias
COLUNAS_VITAIS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central',
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada',
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']

# ==============================================================================
# 🧠 BANCO DE DADOS NA MEMÓRIA (SEM SENHA)
# ==============================================================================

# Inicializa as tabelas na memória se não existirem
if "dados_offline" not in st.session_state:
    st.session_state["dados_offline"] = {
        "loja1_estoque": pd.DataFrame(columns=COLUNAS_VITAIS),
        "loja2_estoque": pd.DataFrame(columns=COLUNAS_VITAIS),
        "loja3_estoque": pd.DataFrame(columns=COLUNAS_VITAIS),
        "loja1_historico_compras": pd.DataFrame(columns=COLS_HIST),
        "loja1_movimentacoes": pd.DataFrame(columns=COLS_MOV),
        "loja1_vendas": pd.DataFrame(columns=COLS_VENDAS),
        "loja1_lista_compras": pd.DataFrame(columns=COLS_LISTA),
        # Adicionei dados fictícios para você ver funcionando
        "loja1_estoque": pd.DataFrame([
            {'código de barras': '7894900011517', 'nome do produto': 'COCA COLA 2L', 'qtd.estoque': 12, 'qtd_central': 100, 'qtd_minima': 10, 'preco_custo': 5.49, 'preco_venda': 8.99, 'validade': None, 'categoria': 'BEBIDAS'},
            {'código de barras': '7891035800201', 'nome do produto': 'SABAO OMO 1KG', 'qtd.estoque': 5, 'qtd_central': 50, 'qtd_minima': 5, 'preco_custo': 12.50, 'preco_venda': 18.90, 'validade': None, 'categoria': 'LIMPEZA'},
        ])
    }
    # Garante colunas vitais nos dados de exemplo
    for col in COLUNAS_VITAIS:
        if col not in st.session_state["dados_offline"]["loja1_estoque"].columns:
            st.session_state["dados_offline"]["loja1_estoque"][col] = 0.0

def ler_da_memoria(nome_chave, colunas_padrao):
    """Lê da memória do celular/computador."""
    if nome_chave not in st.session_state["dados_offline"]:
        st.session_state["dados_offline"][nome_chave] = pd.DataFrame(columns=colunas_padrao)
    
    df = st.session_state["dados_offline"][nome_chave]
    
    # Garante integridade
    if df.empty: return pd.DataFrame(columns=colunas_padrao)
    
    for col in colunas_padrao:
        if col not in df.columns:
            df[col] = 0.0 if "qtd" in col or "preco" in col else ""
            
    return df

def salvar_na_memoria(nome_chave, df, colunas_padrao):
    """Salva na memória."""
    st.session_state["dados_offline"][nome_chave] = df.copy()
    # Limpa cache visual (opcional)
    st.toast("Dados salvos na memória!", icon="💾")

# ==============================================================================
# 🔧 FUNÇÕES DE MATEMÁTICA CORRIGIDAS (O ERRO DO 349)
# ==============================================================================

def converter_ptbr(valor):
    """
    CORREÇÃO ABSOLUTA:
    1. Se vier do XML (ex: 5.49 float), mantém float.
    2. Se vier texto com vírgula (5,49), vira 5.49.
    3. Se vier texto com ponto (5.49), mantém 5.49.
    """
    if valor is None or str(valor).strip() == "": return 0.0
    
    # Se já for número, retorna direto (Evita converter 5.49 para 549)
    if isinstance(valor, (float, int)):
        return float(valor)
        
    s = str(valor).strip().upper().replace('R$', '').strip()
    
    # Se tiver vírgula, assumimos que é decimal brasileiro (ex: 5,49)
    if "," in s:
        s = s.replace(".", "") # Remove separador de milhar se houver (1.000,00 -> 1000,00)
        s = s.replace(",", ".") # Troca vírgula por ponto (1000,00 -> 1000.00)
    
    # Tenta converter
    try:
        return float(s)
    except:
        return 0.0

def format_br(valor):
    if not isinstance(valor, (float, int)): return "0,00"
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper().strip()

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_texto(nome_xml).split())
    set_sis = set(normalizar_texto(nome_sistema).split())
    if not set_xml or not set_sis: return 0.0
    comum = set_xml.intersection(set_sis)
    return len(comum) / len(set_xml.union(set_sis))

# ==============================================================================
# 📄 LEITURA DE XML (CORRIGIDA)
# ==============================================================================
def ler_xml_nfe(arquivo_xml):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    
    # Tenta pegar fornecedor e nota
    try: 
        nNF = root.find('.//nfe:nNF', ns).text 
    except: 
        try: nNF = root.find('.//nNF').text
        except: nNF = "S/N"
        
    try: 
        xNome = root.find('.//nfe:emit/nfe:xNome', ns).text
    except: 
        try: xNome = root.find('.//emit/xNome').text
        except: xNome = "Fornecedor XML"

    itens = []
    # Busca tags 'det' com ou sem namespace
    det_tags = root.findall('.//nfe:det', ns)
    if not det_tags: det_tags = root.findall('.//det')

    for det in det_tags:
        prod = det.find('nfe:prod', ns) if det.find('nfe:prod', ns) is not None else det.find('prod')
        if prod is not None:
            # Captura segura dos valores
            def get_val(tag):
                el = prod.find(f'nfe:{tag}', ns)
                if el is None: el = prod.find(tag)
                return el.text if el is not None else None

            ean = get_val('cEAN') or ""
            if ean == "SEM GTIN": ean = ""
            
            nome = get_val('xProd') or "Produto Sem Nome"
            
            # Conversão direta do XML (XML usa ponto, então float() direto funciona melhor)
            try: qCom = float(get_val('qCom'))
            except: qCom = 0.0
            
            try: vProd = float(get_val('vProd'))
            except: vProd = 0.0
            
            try: vDesc = float(get_val('vDesc')) if get_val('vDesc') else 0.0
            except: vDesc = 0.0

            p_liq = (vProd - vDesc) / qCom if qCom > 0 else 0.0
            p_bruto = vProd / qCom if qCom > 0 else 0.0

            itens.append({
                'nome': normalizar_texto(nome),
                'qtd': qCom,
                'ean': ean,
                'preco_un_liquido': p_liq,
                'preco_un_bruto': p_bruto,
                'desconto_total_item': vDesc
            })
    return {'numero': nNF, 'fornecedor': xNome, 'data': agora_am(), 'itens': itens}

# ==============================================================================
# 🚀 APLICAÇÃO
# ==============================================================================
st.sidebar.title("🏢 Gestão Pro (Offline)")
loja_atual = st.sidebar.selectbox("Unidade", ["Loja 1 (Principal)", "Loja 2", "Loja 3"])
prefixo = "loja1" if "1" in loja_atual else ("loja2" if "2" in loja_atual else "loja3")

# Carrega DataFrames da Memória
df = ler_da_memoria(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_memoria(f"{prefixo}_historico_compras", COLS_HIST)
df_lista = ler_da_memoria(f"{prefixo}_lista_compras", COLS_LISTA)

# Garante tipagem numérica correta
for col in ['preco_custo', 'preco_venda', 'qtd.estoque', 'qtd_central']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

# Menu
st.sidebar.markdown("---")
modo = st.sidebar.radio("Menu:", [
    "📊 Dashboard", 
    "📥 Importar XML (NFe)", 
    "🏠 Gôndola (Busca)", 
    "🆕 Cadastrar Produto", 
    "📝 Lista de Compras", 
    "💰 Histórico", 
    "📋 Tabela Geral"
])

# 1. DASHBOARD
if modo == "📊 Dashboard":
    st.title("📊 Visão Geral")
    c1, c2, c3 = st.columns(3)
    total_itens = df['qtd.estoque'].sum() + df['qtd_central'].sum()
    valor_total = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
    
    c1.metric("📦 Estoque Total (Loja+Casa)", int(total_itens))
    c2.metric("💰 Valor de Custo", format_br(valor_total))
    c3.metric("🚨 Baixo Estoque", len(df[(df['qtd.estoque'] + df['qtd_central']) < df['qtd_minima']]))
    
    st.divider()
    st.markdown("### ⚠️ Produtos Críticos")
    st.dataframe(df[(df['qtd.estoque'] + df['qtd_central']) < df['qtd_minima']][['nome do produto', 'qtd.estoque', 'qtd_central']], use_container_width=True)

# 2. IMPORTAR XML
elif modo == "📥 Importar XML (NFe)":
    st.title("📥 Entrada via XML")
    arquivo = st.file_uploader("Upload XML", type=['xml'])
    
    if arquivo:
        dados = ler_xml_nfe(arquivo)
        st.info(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
        
        with st.form("form_xml"):
            processar = []
            for i, item in enumerate(dados['itens']):
                st.markdown(f"**{item['nome']}**")
                c_info, c_sel = st.columns([2, 2])
                c_info.caption(f"Qtd: {item['qtd']} | Custo Liq: {format_br(item['preco_un_liquido'])}")
                
                # Lógica de Match
                opcoes = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                match_idx = 0
                
                # Tenta match por EAN
                if str(item['ean']) != "":
                    mask = df['código de barras'].astype(str) == str(item['ean'])
                    if mask.any():
                        nome_db = df.loc[mask, 'nome do produto'].values[0]
                        if nome_db in opcoes: match_idx = opcoes.index(nome_db)
                
                escolha = c_sel.selectbox("Vincular a:", opcoes, index=match_idx, key=f"xml_{i}")
                processar.append({'xml': item, 'escolha': escolha})
                st.divider()
            
            if st.form_submit_button("✅ Processar Entrada"):
                novos_hist = []
                count_novos = 0
                
                for p in processar:
                    item = p['xml']
                    escolha = p['escolha']
                    nome_final = ""
                    
                    if escolha == "(CRIAR NOVO)":
                        novo_prod = {c: 0.0 if "qtd" in c or "preco" in c else "" for c in COLUNAS_VITAIS}
                        novo_prod.update({
                            'código de barras': item['ean'],
                            'nome do produto': item['nome'],
                            'qtd.estoque': 0,
                            'qtd_central': item['qtd'], # Entra na casa
                            'preco_custo': item['preco_un_liquido'],
                            'preco_venda': item['preco_un_liquido'] * 1.5,
                            'ultimo_fornecedor': dados['fornecedor']
                        })
                        df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                        nome_final = item['nome']
                        count_novos += 1
                    else:
                        idx = df[df['nome do produto'] == escolha].index[0]
                        df.at[idx, 'qtd_central'] += item['qtd']
                        df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                        df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                        nome_final = escolha
                    
                    novos_hist.append({
                        'data': dados['data'], 'produto': nome_final, 'fornecedor': dados['fornecedor'],
                        'qtd': item['qtd'], 'preco_pago': item['preco_un_liquido'], 
                        'total_gasto': item['qtd'] * item['preco_un_liquido'], 'numero_nota': dados['numero']
                    })
                
                salvar_na_memoria(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                if novos_hist:
                    df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                    salvar_na_memoria(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                
                st.success(f"Entrada concluída! {count_novos} novos produtos cadastrados.")
                time.sleep(1)
                st.rerun()

# 3. GÔNDOLA
elif modo == "🏠 Gôndola (Busca)":
    st.title("🏠 Gôndola")
    termo = st.text_input("🔍 Buscar Produto", "")
    
    if termo:
        termo = normalizar_texto(termo)
        mask = df['nome do produto'].apply(normalizar_texto).str.contains(termo) | df['código de barras'].astype(str).str.contains(termo)
        res = df[mask]
        
        for idx, row in res.iterrows():
            with st.container(border=True):
                st.subheader(row['nome do produto'])
                c1, c2, c3 = st.columns(3)
                c1.metric("Loja", int(row['qtd.estoque']))
                c2.metric("Casa", int(row['qtd_central']))
                c3.metric("Preço", format_br(row['preco_venda']))
                
                if row['qtd_central'] > 0:
                    with st.form(f"baixa_{idx}"):
                        c_in, c_btn = st.columns([2,1])
                        qtd_baixa = c_in.number_input("Baixar p/ Loja:", min_value=1, max_value=int(row['qtd_central']), key=f"num_{idx}")
                        if c_btn.form_submit_button("⬇️ Baixar"):
                            df.at[idx, 'qtd.estoque'] += qtd_baixa
                            df.at[idx, 'qtd_central'] -= qtd_baixa
                            salvar_na_memoria(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            st.success("Transferido!")
                            st.rerun()

# 4. CADASTRO MANUAL
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Novo Produto")
    with st.form("novo_prod"):
        c1, c2 = st.columns(2)
        cod = c1.text_input("Código de Barras")
        nome = c2.text_input("Nome")
        c3, c4 = st.columns(2)
        custo = c3.number_input("Custo", format="%.2f", step=0.01)
        venda = c4.number_input("Venda", format="%.2f", step=0.01)
        if st.form_submit_button("Salvar"):
            novo = {c: 0 for c in COLUNAS_VITAIS}
            novo.update({'código de barras': cod, 'nome do produto': nome.upper(), 'preco_custo': custo, 'preco_venda': venda})
            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
            salvar_na_memoria(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success("Cadastrado!")

# 5. LISTA COMPRAS
elif modo == "📝 Lista de Compras":
    st.title("📝 Lista de Sugestão")
    abaixo = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
    if not abaixo.empty:
        st.table(abaixo[['nome do produto', 'qtd.estoque', 'ultimo_fornecedor']])
    else:
        st.info("Estoque saudável. Nada abaixo do mínimo.")

# 6. HISTÓRICO
elif modo == "💰 Histórico":
    st.title("💰 Histórico de Compras")
    st.dataframe(df_hist.sort_values('data', ascending=False), use_container_width=True)

# 7. TABELA GERAL
elif modo == "📋 Tabela Geral":
    st.title("📋 Edição Geral")
    st.warning("⚠️ Edite os valores diretamente aqui.")
    
    # Data Editor com formatação de dinheiro forçada
    df_edit = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "preco_venda": st.column_config.NumberColumn("Venda", format="R$ %.2f", step=0.01),
            "preco_custo": st.column_config.NumberColumn("Custo", format="R$ %.2f", step=0.01),
            "qtd.estoque": st.column_config.NumberColumn("Loja", format="%.0f"),
            "qtd_central": st.column_config.NumberColumn("Casa", format="%.0f"),
        }
    )
    
    if st.button("💾 SALVAR TABELA"):
        # Aplica conversão PT-BR se o usuário digitou com vírgula na tabela
        for col in ['preco_venda', 'preco_custo']:
            df_edit[col] = df_edit[col].apply(converter_ptbr)
            
        salvar_na_memoria(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
