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
# ⚙️ CONFIGURAÇÃO
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

FUSO_HORARIO = -4
def agora_am(): return datetime.utcnow() + timedelta(hours=FUSO_HORARIO)

COLUNAS_VITAIS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central',
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada',
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

# ==============================================================================
# 🔌 CONEXÃO
# ==============================================================================
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if "service_account_json" in st.secrets:
            if isinstance(st.secrets["service_account_json"], str):
                json_creds = json.loads(st.secrets["service_account_json"])
            else:
                json_creds = dict(st.secrets["service_account_json"])
            return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope))
        else:
            st.error("Secret 'service_account_json' não encontrado.")
            return None
    except Exception as e:
        st.error(f"Erro Conexão: {e}")
        return None

# ==============================================================================
# 🔧 MATEMÁTICA E DADOS (AGRESSIVA)
# ==============================================================================
def converter_ptbr(valor):
    """Converte qualquer coisa para float na força bruta."""
    if valor is None or str(valor).strip() == "": return 0.0
    if isinstance(valor, (float, int)): return float(valor)
    
    # Remove R$ e espaços
    s = str(valor).strip().upper().replace('R$', '').strip()
    
    # Se tem vírgula, assume padrão BR (remove ponto milhar, troca virgula por ponto)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    
    try: return float(s)
    except: return 0.0

def format_br(valor):
    if not isinstance(valor, (float, int)): return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    # Normaliza nomes de colunas
    df.columns = df.columns.str.strip().str.lower()
    
    # Adiciona colunas faltantes
    for col in colunas_alvo:
        if col not in df.columns:
            df[col] = 0.0 if any(x in col for x in ['qtd', 'preco', 'valor', 'custo']) else ""
    
    # Força conversão numérica em colunas de preço/qtd
    cols_num = [c for c in df.columns if any(x in c for x in ['qtd', 'preco', 'custo', 'valor'])]
    for col in cols_num:
        df[col] = df[col].apply(converter_ptbr)
    
    return df

@st.cache_data(ttl=1) # Cache curto para garantir atualização
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.5)
    try:
        client = get_google_client()
        if not client: return pd.DataFrame(columns=colunas_padrao)
        
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
            
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        return garantir_integridade_colunas(df, colunas_padrao)
    except Exception as e:
        # Se der erro (ex: planilha vazia), retorna DF vazio estruturado
        return pd.DataFrame(columns=colunas_padrao)

def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        if not client: return
        
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        # 1. Garante que as colunas existem
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        
        # 2. Converte DATAS para texto (obrigatório para JSON do Gsheets)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        
        # 3. TRATAMENTO DE NULOS
        for col in df_save.columns:
            if pd.api.types.is_numeric_dtype(df_save[col]):
                df_save[col] = df_save[col].fillna(0.0)
            else:
                df_save[col] = df_save[col].fillna("")

        # 4. Limpa e Salva
        ws.clear()
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear() # Limpa cache do Streamlit
        
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 LÓGICA XML
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto)
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper().strip()

def ler_xml_nfe(arquivo_xml, df_ref):
    try:
        tree = ET.parse(arquivo_xml); root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        # Tenta pegar Numero da Nota
        try: nNF = root.find('.//nfe:nNF', ns).text 
        except: 
            try: nNF = root.find('.//nNF').text
            except: nNF = "S/N"
            
        # Tenta pegar Fornecedor
        try: xNome = root.find('.//nfe:emit/nfe:xNome', ns).text
        except: 
            try: xNome = root.find('.//emit/xNome').text
            except: xNome = "Fornecedor XML"
        
        itens = []
        # Tenta achar itens com ou sem namespace
        det_tags = root.findall('.//nfe:det', ns) or root.findall('.//det')
        
        for det in det_tags:
            prod = det.find('nfe:prod', ns) if det.find('nfe:prod', ns) is not None else det.find('prod')
            if prod is not None:
                def g(t): 
                    x = prod.find(f'nfe:{t}', ns)
                    if x is None: x = prod.find(t)
                    return x.text if x is not None else None
                
                ean = g('cEAN') or ""
                nome = g('xProd') or "Item"
                if ean == "SEM GTIN": ean = ""
                
                # Conversão robusta na leitura do XML
                q = converter_ptbr(g('qCom'))
                v = converter_ptbr(g('vProd'))
                d = converter_ptbr(g('vDesc') or 0)
                
                # Proteção matemática
                p_un = (v - d) / q if q > 0 else 0
                
                itens.append({
                    'nome': normalizar_texto(nome), 
                    'qtd': q, 
                    'ean': ean, 
                    'preco_un_liquido': p_un, 
                    'preco_un_bruto': v/q if q else 0, 
                    'desconto_total_item': d
                })
        return {'numero': nNF, 'fornecedor': xNome, 'data': agora_am(), 'itens': itens}
    except Exception as e: 
        st.error(f"Erro XML: {e}")
        return None

# ==============================================================================
# APP VISUAL
# ==============================================================================
st.sidebar.title("🏢 Gestão Loja")
loja_atual = st.sidebar.selectbox("Unidade", ["Loja 1 (Principal)", "Loja 2", "Loja 3"])
prefixo = "loja1" if "1" in loja_atual else ("loja2" if "2" in loja_atual else "loja3")

# Carrega Dados
df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)
df_lista = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)

modo = st.sidebar.radio("Menu", ["📊 Dashboard", "📥 Importar XML", "🏠 Gôndola (Busca)", "🆕 Cadastrar Produto", "📝 Lista de Compras", "💰 Histórico", "📋 Tabela Geral"])

# 1. DASHBOARD
if modo == "📊 Dashboard":
    st.title(f"📊 Painel - {loja_atual}")
    c1, c2, c3 = st.columns(3)
    
    qtd_total = df['qtd.estoque'].sum() + df['qtd_central'].sum()
    valor_total = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
    
    c1.metric("📦 Estoque Total", int(qtd_total))
    c2.metric("💰 Valor Estoque", format_br(valor_total))
    
    criticos = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
    c3.metric("🚨 Repor", len(criticos))
    
    if not criticos.empty:
        st.write("### 🚨 Itens Críticos")
        st.dataframe(criticos[['nome do produto', 'qtd.estoque', 'qtd_central', 'ultimo_fornecedor']])

# 2. IMPORTAR XML
elif modo == "📥 Importar XML":
    st.title("📥 Nota Fiscal")
    up = st.file_uploader("Arquivo XML", type=['xml'])
    
    if up:
        dados = ler_xml_nfe(up, df_oficial)
        if dados:
            st.info(f"Nota: {dados['numero']} | {dados['fornecedor']}")
            
            with st.form("xml"):
                processar = []
                for i, it in enumerate(dados['itens']):
                    st.write(f"**{it['nome']}** | Qtd: {it['qtd']} | Custo: {format_br(it['preco_un_liquido'])}")
                    
                    opcoes = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].unique().tolist())
                    match_idx = 0
                    
                    # Tenta achar pelo EAN
                    if it['ean']:
                        match = df[df['código de barras'].astype(str) == str(it['ean'])]
                        if not match.empty:
                            nm_match = match.iloc[0]['nome do produto']
                            if nm_match in opcoes:
                                match_idx = opcoes.index(nm_match)
                    
                    escolha = st.selectbox("Vincular:", opcoes, index=match_idx, key=f"s_{i}")
                    processar.append((it, escolha))
                
                if st.form_submit_button("✅ Processar"):
                    novos_h = []
                    for it, esc in processar:
                        if esc == "(CRIAR NOVO)":
                            novo = {c: 0 for c in COLUNAS_VITAIS}
                            novo.update({
                                'código de barras': it['ean'], 
                                'nome do produto': it['nome'], 
                                'qtd_central': it['qtd'], 
                                'preco_custo': it['preco_un_liquido'], 
                                'preco_venda': it['preco_un_liquido'] * 1.6, # Margem padrão
                                'ultimo_fornecedor': dados['fornecedor']
                            })
                            # --- CORREÇÃO DO ERRO DE SINTAXE AQUI ---
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            nm_final = it['nome']
                        else:
                            idx = df[df['nome do produto'] == esc].index[0]
                            df.at[idx, 'qtd_central'] += it['qtd']
                            df.at[idx, 'preco_custo'] = it['preco_un_liquido']
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            nm_final = esc
                        
                        novos_h.append({
                            'data': str(datetime.now()), 
                            'produto': nm_final, 
                            'qtd': it['qtd'], 
                            'preco_pago': it['preco_un_liquido'], 
                            'total_gasto': it['qtd']*it['preco_un_liquido'], 
                            'numero_nota': dados['numero'], 
                            'fornecedor': dados['fornecedor']
                        })
                    
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    if novos_h:
                        salvar_na_nuvem(f"{prefixo}_historico_compras", pd.concat([df_hist, pd.DataFrame(novos_h)], ignore_index=True), COLS_HIST)
                    
                    st.success("Sucesso! Estoque atualizado.")
                    time.sleep(1)
                    st.rerun()

# 3. GÔNDOLA
elif modo == "🏠 Gôndola (Busca)":
    st.title("🏠 Gôndola")
    b = st.text_input("Buscar Produto (Nome ou EAN):")
    
    if b:
        b_norm = normalizar_texto(b)
        res = df[df['nome do produto'].str.contains(b_norm, na=False) | df['código de barras'].astype(str).str.contains(b, na=False)]
        
        if res.empty:
            st.warning("Nenhum produto encontrado.")
        
        for i, r in res.iterrows():
            with st.container(border=True):
                st.subheader(r['nome do produto'])
                c1, c2, c3 = st.columns(3)
                c1.metric("Loja (Frente)", int(r['qtd.estoque']))
                c2.metric("Estoque (Casa)", int(r['qtd_central']))
                c3.metric("Preço Venda", format_br(r['preco_venda']))
                
                if r['qtd_central'] > 0:
                    if st.button(f"⬇️ Baixar para Loja (ID {i})", key=f"btn_{i}"):
                        df.at[i, 'qtd.estoque'] += 1
                        df.at[i, 'qtd_central'] -= 1
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        st.rerun()

# 4. CADASTRO
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Cadastro Manual")
    with st.form("cad"):
        c1, c2 = st.columns(2)
        cod = c1.text_input("Código de Barras")
        nom = c2.text_input("Nome do Produto")
        
        c3, c4 = st.columns(2)
        cus = c3.number_input("Preço de Custo (R$)", min_value=0.0, format="%.2f")
        ven = c4.number_input("Preço de Venda (R$)", min_value=0.0, format="%.2f")
        
        if st.form_submit_button("Salvar Produto"):
            if not nom:
                st.error("O nome é obrigatório.")
            else:
                n = {c: 0 for c in COLUNAS_VITAIS}
                n.update({
                    'código de barras': cod, 
                    'nome do produto': nom.upper(), 
                    'preco_custo': cus, 
                    'preco_venda': ven
                })
                df = pd.concat([df, pd.DataFrame([n])], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success("Produto cadastrado com sucesso!")

# 5. LISTA
elif modo == "📝 Lista de Compras":
    st.title("📝 Lista de Compras (Sugestão)")
    st.dataframe(df_lista, use_container_width=True)

# 6. HISTÓRICO
elif modo == "💰 Histórico":
    st.title("💰 Histórico de Compras")
    st.dataframe(df_hist, use_container_width=True)

# 7. TABELA GERAL & REPARO
elif modo == "📋 Tabela Geral":
    st.title("📋 Tabela Geral & Reparo")
    
    # --- ÁREA DE CORREÇÃO ---
    with st.expander("🛠️ PAINEL DE REPARO DE PREÇOS (CLIQUE PARA ABRIR)", expanded=True):
        st.info("Use estes botões se os preços estiverem aparecendo sem vírgula (ex: 599,00 ao invés de 5,99).")
        c_rep1, c_rep2 = st.columns(2)
        
        # Correção 599 -> 5.99
        if c_rep1.button("🚨 CORRIGIR PREÇOS / 100 (Ex: 599 virar 5,99)"):
            afetados = 0
            df_temp = df.copy()
            for col in ['preco_custo', 'preco_venda']:
                df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce').fillna(0.0)
                mask = df_temp[col] > 100
                if mask.any():
                    df_temp.loc[mask, col] = df_temp.loc[mask, col] / 100
                    afetados += mask.sum()
            
            if afetados > 0:
                salvar_na_nuvem(f"{prefixo}_estoque", df_temp, COLUNAS_VITAIS)
                st.success(f"✅ Feito! {afetados} preços foram corrigidos.")
                time.sleep(2); st.rerun()
            else:
                st.warning("Nenhum preço maior que 100 foi encontrado para corrigir.")

        # Correção 35 -> 3.50
        if c_rep2.button("⚠️ CORRIGIR PREÇOS / 10 (Ex: 35 virar 3,50)"):
            afetados = 0
            df_temp = df.copy()
            for col in ['preco_custo', 'preco_venda']:
                df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce').fillna(0.0)
                mask = (df_temp[col] >= 10) & (df_temp[col] < 100)
                if mask.any():
                    df_temp.loc[mask, col] = df_temp.loc[mask, col] / 10
                    afetados += mask.sum()
            
            if afetados > 0:
                salvar_na_nuvem(f"{prefixo}_estoque", df_temp, COLUNAS_VITAIS)
                st.success(f"✅ Feito! {afetados} preços foram corrigidos.")
                time.sleep(2); st.rerun()
            else:
                st.warning("Nenhum preço entre 10 e 99 foi encontrado.")

    st.write("---")
    st.write("### Edição Manual")
    
    # Editor com configuração de colunas para exibição correta
    df_edit = st.data_editor(
        df, 
        num_rows="dynamic", 
        use_container_width=True, 
        column_config={
            "preco_venda": st.column_config.NumberColumn("Preço Venda", format="R$ %.2f", min_value=0, step=0.01),
            "preco_custo": st.column_config.NumberColumn("Preço Custo", format="R$ %.2f", min_value=0, step=0.01),
            "código de barras": st.column_config.TextColumn("Código Barras"),
        }
    )
    
    if st.button("💾 SALVAR ALTERAÇÕES NA TABELA"):
        salvar_na_nuvem(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
        st.success("Tabela salva com sucesso!")
        time.sleep(1)
        st.rerun()
