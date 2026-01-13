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
# ⚙️ CONFIGURAÇÃO DE NUVEM (ADAPTADOR TRANSPARENTE)
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# --- DEFINIÇÃO DAS COLUNAS (GLOBAL - PARA EVITAR ERROS) ---
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

# --- CONEXÃO SEGURA ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# --- FUNÇÃO DE CURA (CORRIGE O KEYERROR) ---
def garantir_integridade_colunas(df, colunas_referencia):
    if df.empty: return pd.DataFrame(columns=colunas_referencia)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_referencia:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total']): df[col] = 0.0
            else: df[col] = ""
    return df

# --- LEITURA DA NUVEM (SUBSTITUI O READ_EXCEL) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1) # Pausa técnica
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        df = garantir_integridade_colunas(df, colunas_padrao)
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (SUBSTITUI O TO_EXCEL) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        ws.clear()
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 SUAS FUNÇÕES ORIGINAIS (MANTIDAS 100%)
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto): return normalizar_texto(texto)

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    comum = set_xml.intersection(set_sis)
    if not comum: return 0.0
    total = set_xml.union(set_sis)
    score = len(comum) / len(total)
    for palavra in comum:
        if any(u in palavra for u in ['L', 'ML', 'KG', 'G', 'M']): 
            if any(c.isdigit() for c in palavra): score += 0.5
    return score

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor_match = None; maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score: maior_score = score; melhor_match = opcao
    if maior_score >= cutoff: return melhor_match, "Nome Similar (Palavras)"
    return None, "Nenhum"

def unificar_produtos_por_codigo(df):
    if df.empty: return df
    df = garantir_integridade_colunas(df, COLUNAS_VITAIS)
    cols_num = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']
    for col in cols_num:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    lista_final = []
    df['código de barras'] = df['código de barras'].astype(str).str.strip()
    sem_codigo = df[df['código de barras'] == ""]
    com_codigo = df[df['código de barras'] != ""]
    for cod, grupo in com_codigo.groupby('código de barras'):
        if len(grupo) > 1:
            melhor_nome = max(grupo['nome do produto'].tolist(), key=len)
            base_ref = grupo[grupo['nome do produto'] == melhor_nome].iloc[0].to_dict()
            base_ref['qtd.estoque'] = grupo['qtd.estoque'].sum()
            base_ref['qtd_central'] = grupo['qtd_central'].sum()
            base_ref['preco_custo'] = grupo['preco_custo'].max()
            base_ref['preco_venda'] = grupo['preco_venda'].max()
            lista_final.append(base_ref)
        else: lista_final.append(grupo.iloc[0].to_dict())
    df_novo = pd.DataFrame(lista_final)
    if not sem_codigo.empty: df_novo = pd.concat([df_novo, sem_codigo], ignore_index=True)
    return df_novo

def processar_excel_oficial(arquivo_subido):
    try:
        df_temp = pd.read_csv(arquivo_subido) if arquivo_subido.name.endswith('.csv') else pd.read_excel(arquivo_subido)
        if 'obrigatório' in str(df_temp.iloc[0].values): df_temp = df_temp.iloc[1:].reset_index(drop=True)
        df_temp.columns = df_temp.columns.str.strip()
        col_nome = next((c for c in df_temp.columns if 'nome' in c.lower()), 'Nome')
        col_cod = next((c for c in df_temp.columns if 'código' in c.lower() or 'barras' in c.lower()), 'Código de Barras Primário')
        df_limpo = df_temp[[col_nome, col_cod]].copy()
        df_limpo.columns = ['nome do produto', 'código de barras']
        df_limpo['nome do produto'] = df_limpo['nome do produto'].apply(normalizar_texto)
        df_limpo['código de barras'] = df_limpo['código de barras'].astype(str).str.replace('.0', '', regex=False).str.strip()
        salvar_na_nuvem("base_oficial", df_limpo, COLS_OFICIAL)
        return True
    except: return False

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        df_outra = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if not df_outra.empty:
            mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
            if mask.any():
                idx = df_outra[mask].index[0]
                df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

# --- FUNÇÃO XML HÍBRIDA (ADAPTADA PARA SEU FORMATO) ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]
    
    # Tenta pegar cabeçalho do XML novo
    info = root.find("Info")
    if info is not None:
        try:
            forn = info.find("Fornecedor").text; num = info.find("NumeroNota").text
            dt = datetime.strptime(f"{info.find('DataCompra').text} {info.find('HoraCompra').text}", "%d/%m/%Y %H:%M:%S")
            dados_nota = {'numero': num, 'fornecedor': forn, 'data': dt, 'itens': []}
        except: dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
    else:
        dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
        for elem in root.iter():
            tag = tag_limpa(elem)
            if tag == 'nNF': dados_nota['numero'] = elem.text
            elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO': dados_nota['fornecedor'] = elem.text

    # Tenta ler itens do XML novo
    itens_custom = root.findall(".//Item")
    if itens_custom:
        for it in itens_custom:
            try:
                nome = it.find("Nome").text
                qtd = float(it.find("Quantidade").text)
                valor = float(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
                desc = float(it.find("ValorDesconto").text) if it.find("ValorDesconto") is not None else 0.0
                p_liq = valor/qtd if qtd>0 else 0
                p_bruto = (valor+desc)/qtd if qtd>0 else 0
                dados_nota['itens'].append({'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(), 'preco_un_liquido': p_liq, 'preco_un_bruto': p_bruto, 'desconto_total_item': desc})
            except: continue
    else:
        # Fallback para NFe padrão
        dets = [e for e in root.iter() if tag_limpa(e) == 'det']
        for det in dets:
            prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
            if prod:
                item = {'nome': normalizar_texto(prod.find(".//{*}xProd").text), 'qtd': float(prod.find(".//{*}qCom").text), 'ean': prod.find(".//{*}cEAN").text, 'preco_un_bruto': float(prod.find(".//{*}vProd").text)/float(prod.find(".//{*}qCom").text), 'preco_un_liquido': float(prod.find(".//{*}vProd").text)/float(prod.find(".//{*}qCom").text), 'desconto_total_item': 0}
                dados_nota['itens'].append(item)

    # Match com base oficial
    lista_nomes_ref = []; dict_ref_ean = {}
    if not df_referencia.empty:
        for idx, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            dict_ref_ean[nm] = str(row['código de barras']).strip()
            lista_nomes_ref.append(nm)
    for item in dados_nota['itens']:
        if item['ean'] in ['SEM GTIN', '', 'None', 'NAN'] and lista_nomes_ref:
            melhor, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
            if melhor: item['ean'] = dict_ref_ean.get(melhor, item['ean'])
            
    return dados_nota

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=False)
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# --- CARREGAMENTO INICIAL ---
df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)
df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", COLS_MOV)
df_vendas = ler_da_nuvem(f"{prefixo}_vendas", COLS_VENDAS)
df_lista_compras = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)

if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial", "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"])

    # 1. DASHBOARD
    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle - {loja_atual}")
        if df.empty: st.info("Comece cadastrando produtos.")
        else:
            hoje = datetime.now(); df_valido = df[pd.notnull(df['validade'])].copy()
            df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
            df_atencao = df_valido[(df_valido['validade'] > hoje + timedelta(days=5)) & (df_valido['validade'] <= hoje + timedelta(days=10))]
            valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
            c2.metric("💰 Valor Investido", f"R$ {valor_estoque:,.2f}")
            c3.metric("🚨 Vencendo", len(df_critico))
            c4.metric("⚠️ Atenção", len(df_atencao))
            st.divider()
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty: st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo!")
            if not df_critico.empty: st.error("🚨 Produtos Vencendo!"); st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])

    # 1.5 PICKLIST
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        arquivo_pick = st.file_uploader("📂 Subir Picklist", type=['xlsx', 'xls'])
        if arquivo_pick:
            try:
                df_pick = pd.read_excel(arquivo_pick)
                df_pick.columns = df_pick.columns.str.strip().str.lower()
                col_barras = next((c for c in df_pick.columns if 'barras' in c), None)
                col_qtd = next((c for c in df_pick.columns if 'transferir' in c), None)
                if not col_barras or not col_qtd: st.error("❌ Colunas não encontradas.")
                else:
                    if st.button("🚀 PROCESSAR"):
                        movidos = 0; log_movs = []; total = len(df_pick); bar = st.progress(0)
                        for i, row in df_pick.iterrows():
                            cod = str(row[col_barras]).replace('.0', '').strip()
                            qtd = pd.to_numeric(row[col_qtd], errors='coerce')
                            if qtd > 0:
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    idx = df[mask].index[0]
                                    df.at[idx, 'qtd_central'] -= qtd
                                    df.at[idx, 'qtd.estoque'] += qtd
                                    log_movs.append({'data_hora': datetime.now(), 'produto': df.at[idx, 'nome do produto'], 'qtd_movida': qtd})
                                    atualizar_casa_global(df.at[idx, 'nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    movidos += 1
                            bar.progress((i+1)/total)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        if log_movs:
                            df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV)
                        st.success(f"✅ {movidos} transferidos!")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 LISTA DE COMPRAS
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        t1, t2 = st.tabs(["📋 Ver Lista", "➕ Adicionar"])
        with t1:
            st.dataframe(df_lista_compras, use_container_width=True)
            if st.button("🗑️ Limpar Lista"):
                salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=COLS_LISTA), COLS_LISTA); st.rerun()
        with t2:
            if st.button("🚀 Gerar pelo Mínimo"):
                baixo = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
                novos = []
                for _, r in baixo.iterrows():
                    novos.append({'produto': r['nome do produto'], 'qtd_sugerida': r['qtd_minima']*3, 'fornecedor': r['ultimo_fornecedor'], 'custo_previsto': r['preco_custo'], 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'A Comprar'})
                if novos:
                    df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA)
                    st.success(f"{len(novos)} itens!"); st.rerun()
            st.divider(); st.subheader("✋ Manual")
            with st.form("add_m"):
                l_p = [""] + sorted(df['nome do produto'].unique().tolist()); p = st.selectbox("Produto", l_p); q = st.number_input("Qtd", 10); o = st.text_input("Obs")
                if st.form_submit_button("Add"):
                    if p:
                        c = df.loc[df['nome do produto']==p, 'preco_custo'].values[0] if not df[df['nome do produto']==p].empty else 0
                        n = {'produto': p, 'qtd_sugerida': q, 'fornecedor': o, 'custo_previsto': c, 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'Manual'}
                        df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame([n])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA); st.success("Ok!"); st.rerun()

    # 2. CADASTRAR PRODUTO
    elif modo == "🆕 Cadastrar Produto":
        st.title("🆕 Cadastro")
        with st.form("cad"):
            c1, c2 = st.columns(2); nc = c1.text_input("Cod"); nn = c1.text_input("Nome"); ncu = c2.number_input("Custo"); nv = c2.number_input("Venda")
            if st.form_submit_button("Salvar"):
                n = {'código de barras': nc, 'nome do produto': nn.upper(), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': ncu, 'preco_venda': nv, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
                df = pd.concat([df, pd.DataFrame([n])], ignore_index=True); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Ok!"); st.rerun()

    # 2.5 IMPORTAR XML
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title("📥 XML NFe"); arq = st.file_uploader("XML", type=['xml'])
        if arq:
            dados = ler_xml_nfe(arq, df_oficial); l_s = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].unique().tolist()); escolhas = {}
            st.success(f"Nota: {dados['numero']} - {dados['fornecedor']} ({dados['data']})")
            for i, item in enumerate(dados['itens']):
                st.divider(); c1, c2 = st.columns([1,1]); c1.write(f"XML: {item['nome']} (x{item['qtd']})"); c1.caption(f"Desc: {item['desconto_total_item']:.2f}"); m = "(CRIAR NOVO)"
                if not df.empty:
                    msk = df['código de barras'] == item['ean']
                    if msk.any(): m = df.loc[msk, 'nome do produto'].values[0]
                    else: 
                        best, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist())
                        if best: m = best
                escolhas[i] = c2.selectbox(f"Vincular {i+1}", l_s, index=l_s.index(m) if m in l_s else 0, key=f"x_{i}")
            if st.button("✅ CONFIRMAR"):
                hist_n = []
                for i, it in enumerate(dados['itens']):
                    esc = escolhas[i]; q = it['qtd']; c = it['preco_un_liquido']; br = it['preco_un_bruto']; desc = it.get('desconto_total_item', 0)
                    if esc == "(CRIAR NOVO)":
                        n = {'código de barras': it['ean'], 'nome do produto': it['nome'], 'qtd.estoque': 0, 'qtd_central': q, 'qtd_minima': 5, 'preco_custo': c, 'preco_venda': br*2, 'status_compra': 'OK', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': br}
                        df = pd.concat([df, pd.DataFrame([n])], ignore_index=True); atualizar_casa_global(it['nome'], q, c, None, None, prefixo); esc = it['nome']
                    else:
                        msk = df['nome do produto'] == esc
                        if msk.any():
                            ix = df[msk].index[0]; df.at[ix, 'qtd_central'] += q; df.at[ix, 'preco_custo'] = c; df.at[ix, 'preco_sem_desconto'] = br; df.at[ix, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizar_casa_global(esc, df.at[ix, 'qtd_central'], c, None, None, prefixo)
                    hist_n.append({'data': dados['data'], 'produto': esc, 'fornecedor': dados['fornecedor'], 'qtd': q, 'preco_pago': c, 'total_gasto': q*c, 'numero_nota': dados['numero'], 'desconto_total_money': desc, 'preco_sem_desconto': br})
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                if hist_n: df_hist = pd.concat([df_hist, pd.DataFrame(hist_n)], ignore_index=True); salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                st.success("Salvo!"); st.rerun()

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title("🔄 Sincronizar")
        f = st.file_uploader("Arquivo", type=['xlsx','csv'])
        if f:
            r = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f); c1, c2, c3 = st.columns(3); ic = c1.selectbox("Cód", r.columns, 0); inm = c2.selectbox("Nome", r.columns, 1); iq = c3.selectbox("Qtd", r.columns, len(r.columns)-1)
            if st.button("🚀 SINCRONIZAR"):
                for i, row in r.iterrows():
                    c = str(row[ic]).replace('.0','').strip(); n = normalizar_texto(str(row[inm])); q = pd.to_numeric(row[iq], errors='coerce')
                    msk = df['código de barras'] == c
                    if msk.any(): df.loc[msk, 'qtd.estoque'] = q
                    else: df = pd.concat([df, pd.DataFrame([{'código de barras': c, 'nome do produto': n, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'status_compra': 'OK'}])], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Sincronizado!"); st.rerun()

    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title("📉 Baixar Vendas")
        t1, t2 = st.tabs(["📂 Importar", "📜 Histórico"])
        with t1:
            f = st.file_uploader("Relatório", type=['xlsx'])
            if f:
                r = pd.read_excel(f); c1, c2 = st.columns(2); inu = c1.selectbox("Nome", r.columns); iq = c2.selectbox("Qtd", r.columns)
                if st.button("Processar"):
                    novos = []
                    for i, row in r.iterrows():
                        n = str(row[inu]); q = pd.to_numeric(row[iq], errors='coerce')
                        msk = df['nome do produto'].str.contains(n, case=False, na=False)
                        if msk.any() and q > 0:
                            ix = df[msk].index[0]; df.at[ix, 'qtd.estoque'] -= q
                            novos.append({'data_hora': datetime.now(), 'produto': df.at[ix, 'nome do produto'], 'qtd_vendida': q, 'estoque_restante': df.at[ix, 'qtd.estoque']})
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    if novos: df_vendas = pd.concat([df_vendas, pd.DataFrame(novos)], ignore_index=True); salvar_na_nuvem(f"{prefixo}_vendas", df_vendas, COLS_VENDAS); st.success("Baixado!"); st.rerun()
        with t2:
            if not df_vendas.empty and 'data_hora' in df_vendas.columns: st.dataframe(df_vendas.sort_values(by='data_hora', ascending=False), use_container_width=True)

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        if df.empty: st.warning("Cadastre produtos.")
        else:
            if usar_modo_mobile:
                termo = st.text_input("🔍 Buscar Produto:")
                df_show = filtrar_dados_inteligente(df, 'nome do produto', termo)
                for idx, row in df_show.iterrows():
                    with st.container(border=True):
                        st.subheader(row['nome do produto'])
                        c1, c2 = st.columns(2); c1.metric("Loja", int(row['qtd.estoque'])); c2.metric("Casa", int(row['qtd_central']))
                        if row['qtd_central'] > 0:
                            with st.form(key=f"fm_{idx}"):
                                q = st.number_input("Qtd:", min_value=1, max_value=int(row['qtd_central']), key=f"n_{idx}")
                                if st.form_submit_button("⬇️ Baixar"):
                                    df.at[idx, 'qtd.estoque'] += q; df.at[idx, 'qtd_central'] -= q
                                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo); st.rerun()
            else:
                tab_acao, tab_hist = st.tabs(["🚚 Repor / Consultar", "📜 Histórico"])
                with tab_acao:
                    df['display_busca'] = df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)
                    opcao = st.selectbox("🔍 Buscar Produto:", [""] + df['display_busca'].tolist())
                    if opcao:
                        idx = df[df['display_busca'] == opcao].index[0]; nome_prod = df.at[idx, 'nome do produto']
                        st.markdown(f"## 📦 {nome_prod}")
                        c1, c2, c3 = st.columns(3); c1.info(f"Loja: {int(df.at[idx, 'qtd.estoque'])}"); c2.success(f"Casa: {int(df.at[idx, 'qtd_central'])}")
                        val = df.at[idx, 'validade']; c3.write(f"Validade: {val.strftime('%d/%m/%Y') if pd.notnull(val) else 'Sem data'}")
                        if df.at[idx, 'qtd_central'] > 0:
                            with st.form("f_transf"):
                                c_dt, c_hr, c_qtd = st.columns(3)
                                dt_t = c_dt.date_input("Data:", datetime.today()); hr_t = c_hr.time_input("Hora:", datetime.now().time()); q_t = c_qtd.number_input(f"Qtd (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']))
                                if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                    if q_t > 0:
                                        df.at[idx, 'qtd.estoque'] += q_t; df.at[idx, 'qtd_central'] -= q_t
                                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                        df_mov = pd.concat([df_mov, pd.DataFrame([{'data_hora': datetime.combine(dt_t, hr_t), 'produto': nome_prod, 'qtd_movida': q_t}])], ignore_index=True)
                                        salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV); st.success("Sucesso!"); st.rerun()
                        with st.expander("🛠️ Ajustes Manuais"):
                            cn = st.text_input("Corrigir Nome:", value=nome_prod); cf = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
                            cc, cv = st.columns(2); n_c = cc.number_input("Custo:", value=float(df.at[idx, 'preco_custo'])); n_v = cv.number_input("Venda:", value=float(df.at[idx, 'preco_venda']))
                            c1, c2 = st.columns(2); n_ql = c1.number_input("Qtd Real Loja:", value=int(df.at[idx, 'qtd.estoque'])); n_vl = c2.date_input("Nova Validade:", value=val if pd.notnull(val) else None)
                            if st.button("💾 SALVAR CORREÇÕES"):
                                df.at[idx, 'nome do produto'] = cn.upper().strip(); df.at[idx, 'ultimo_fornecedor'] = cf.strip(); df.at[idx, 'preco_custo'] = n_c; df.at[idx, 'preco_venda'] = n_v; df.at[idx, 'qtd.estoque'] = n_ql; df.at[idx, 'validade'] = pd.to_datetime(n_vl)
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Salvo!"); st.rerun()
                with tab_hist:
                    busca_h = st.text_input("🔍 Buscar no Histórico:")
                    df_m_s = filtrar_dados_inteligente(df_mov, 'produto', busca_h)
                    if not df_m_s.empty and 'data_hora' in df_m_s.columns: st.dataframe(df_m_s.sort_values(by='data_hora', ascending=False), use_container_width=True)

    # 6. FORNECEDOR
    elif modo == "🛒 Fornecedor (Compras)":
        st.title(f"🛒 Compras - {loja_atual}")
        pen = df[df['status_compra'] == 'PENDENTE']
        if not pen.empty:
            st.table(pen[['nome do produto', 'qtd_comprada']])
            item = st.selectbox("Dar entrada:", pen['nome do produto'])
            if item:
                idx = df[df['nome do produto'] == item].index[0]
                with st.form("compra"):
                    st.write(f"📝 Detalhes da Compra de: **{item}**")
                    c_dt, c_hr = st.columns(2); dt_c = c_dt.date_input("Data:", datetime.today()); hr_c = c_hr.time_input("Hora:", datetime.now().time())
                    forn = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
                    c1, c2, c3 = st.columns(3); q = c1.number_input("Qtd:", value=int(df.at[idx, 'qtd_comprada'])); c = c2.number_input("Custo:", value=float(df.at[idx, 'preco_custo'])); v = c3.number_input("Venda:", value=float(df.at[idx, 'preco_venda']))
                    if st.form_submit_button("✅ ENTRAR NO ESTOQUE"):
                        df.at[idx, 'qtd_central'] += q; df.at[idx, 'preco_custo'] = c; df.at[idx, 'preco_venda'] = v; df.at[idx, 'status_compra'] = 'OK'; df.at[idx, 'qtd_comprada'] = 0; df.at[idx, 'ultimo_fornecedor'] = forn
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); atualizar_casa_global(item, df.at[idx, 'qtd_central'], c, v, None, prefixo)
                        df_hist = pd.concat([df_hist, pd.DataFrame([{'data': datetime.combine(dt_c, hr_c), 'produto': item, 'fornecedor': forn, 'qtd': q, 'preco_pago': c, 'total_gasto': q*c}])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST); st.success("Sucesso!"); st.rerun()

    # 7. HISTÓRICO
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico"); 
        if not df_hist.empty: st.dataframe(df_hist.sort_values(by='data', ascending=False), use_container_width=True)

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central - {loja_atual}")
        tab_v, tab_g = st.tabs(["📋 Visualizar", "✍️ Gerenciar"])
        with tab_v:
            b = st.text_input("Buscar:"); df_s = filtrar_dados_inteligente(df, 'nome do produto', b)
            df_ed = st.data_editor(df_s[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True, key="ed_casa")
            if st.button("Salvar Tabela"):
                df.update(df_ed); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                for i, r in df_ed.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], None, r['validade'], prefixo)
                st.success("Salvo!"); st.rerun()
        with tab_g:
            l_p = sorted(df['nome do produto'].unique().tolist()); p = st.selectbox("Selecione:", l_p)
            if p:
                ix = df[df['nome do produto'] == p].index[0]
                with st.form("f_c"):
                    c_n = st.text_input("Nome:", value=df.at[ix, 'nome do produto'])
                    c_v, c_c = st.columns(2); n_v = c_v.date_input("Val:", value=df.at[ix, 'validade'] if pd.notnull(df.at[ix, 'validade']) else None); n_c = c_c.number_input("Custo:", value=float(df.at[ix, 'preco_custo']))
                    q_i = st.number_input("Qtd:", min_value=0); ac = st.radio("Ação:", ["Somar (+)", "Substituir (=)"])
                    if st.form_submit_button("Salvar"):
                        if ac.startswith("Somar"): df.at[ix, 'qtd_central'] += q_i
                        else: df.at[ix, 'qtd_central'] = q_i
                        df.at[ix, 'nome do produto'] = c_n.upper(); df.at[ix, 'validade'] = pd.to_datetime(n_v); df.at[ix, 'preco_custo'] = n_c
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); atualizar_casa_global(c_n.upper(), df.at[ix, 'qtd_central'], n_c, None, pd.to_datetime(n_v), prefixo)
                        st.success("Salvo!"); st.rerun()

    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Geral")
        b = st.text_input("Buscar:"); df_s = filtrar_dados_inteligente(df, 'nome do produto', b)
        df_ed = st.data_editor(df_s, use_container_width=True, num_rows="dynamic")
        c1, c2 = st.columns(2)
        if c1.button("Salvar Tudo"):
            df.update(df_ed); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            for i, r in df_ed.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], r['preco_venda'], r['validade'], prefixo)
            st.success("Salvo!"); st.rerun()
        if c2.button("Unificar"):
            df = unificar_produtos_por_codigo(df); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Feito!"); st.rerun()
