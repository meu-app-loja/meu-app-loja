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
# ⚙️ CONFIGURAÇÃO DE NUVEM E INTEGRIDADE
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas Pro", layout="wide", page_icon="🏪")

COLUNAS_VITAIS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]

@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# --- FUNÇÕES DE LÓGICA E UTILITÁRIOS (MOVIDAS PARA O TOPO) ---

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto): 
    return normalizar_texto(texto)

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
    if maior_score >= cutoff: return melhor_match, "Nome Similar"
    return None, "Nenhum"

def garantir_schema(df, colunas_obrigatorias):
    if df.empty: return pd.DataFrame(columns=colunas_obrigatorias)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_obrigatorias:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo']): df[col] = 0.0
            else: df[col] = ""
    return df[colunas_obrigatorias]

@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1.2)
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
        df = garantir_schema(df, colunas_padrao)
        
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

def salvar_na_nuvem(nome_aba, df, colunas_padrao=None):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        ws.clear()
        df_save = df.copy()
        if colunas_padrao: df_save = garantir_schema(df_save, colunas_padrao)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e: st.error(f"Erro ao salvar: {e}")

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

def unificar_produtos_por_codigo(df):
    if df.empty: return df
    df = garantir_schema(df, COLUNAS_VITAIS)
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

def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]
    dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
    itens_custom = root.findall(".//Item")
    if itens_custom:
        for it in itens_custom:
            try:
                nome = it.find("Nome").text
                qtd = float(it.find("Quantidade").text)
                valor = float(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
                dados_nota['itens'].append({'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(), 'preco_un_liquido': valor/qtd if qtd>0 else 0, 'preco_un_bruto': valor/qtd if qtd>0 else 0, 'desconto_total_item': 0})
            except: continue
    else:
        for elem in root.iter():
            tag = tag_limpa(elem)
            if tag == 'nNF': dados_nota['numero'] = elem.text
            elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO': dados_nota['fornecedor'] = elem.text
        dets = [e for e in root.iter() if tag_limpa(e) == 'det']
        for det in dets:
            try:
                prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
                if prod:
                    item = {'nome': normalizar_texto(prod.find(".//{*}xProd").text), 'qtd': float(prod.find(".//{*}qCom").text), 'ean': prod.find(".//{*}cEAN").text, 'preco_un_bruto': float(prod.find(".//{*}vProd").text)/float(prod.find(".//{*}qCom").text), 'preco_un_liquido': float(prod.find(".//{*}vProd").text)/float(prod.find(".//{*}qCom").text), 'desconto_total_item': 0}
                    dados_nota['itens'].append(item)
            except: continue
    return dados_nota

# ==============================================================================
# 🏢 NAVEGAÇÃO E CARREGAMENTO
# ==============================================================================

st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=False)
st.sidebar.markdown("---")

prefixo = "loja1" if "1" in loja_atual else "loja2" if "2" in loja_atual else "loja3"

df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto'])
df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", ['data_hora', 'produto', 'qtd_movida'])
df_vendas = ler_da_nuvem(f"{prefixo}_vendas", ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'])
df_lista_compras = ler_da_nuvem(f"{prefixo}_lista_compras", ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'])
df_oficial = ler_da_nuvem("base_oficial", ['nome do produto', 'código de barras'])

if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

st.sidebar.title("🏪 Menu")
modo = st.sidebar.radio("Navegar:", ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial", "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"])

# ------------------------------------------------------------------
# 1. DASHBOARD
# ------------------------------------------------------------------
if modo == "📊 Dashboard (Visão Geral)":
    st.title(f"📊 Painel de Controle - {loja_atual}")
    if df.empty: st.info("Comece cadastrando produtos.")
    else:
        hoje = datetime.now(); df_valido = df[pd.notnull(df['validade'])].copy()
        df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
        valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Loja", int(df['qtd.estoque'].sum()))
        c2.metric("💰 Valor", f"R$ {valor_estoque:,.2f}")
        c3.metric("🚨 Vencendo", len(df_critico))
        c4.metric("🏡 Casa", int(df['qtd_central'].sum()))
        if not df_critico.empty:
            st.error("🚨 Produtos Vencendo!"); st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])

# ------------------------------------------------------------------
# 2. GÔNDOLA
# ------------------------------------------------------------------
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
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                st.rerun()
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
                            dt_t = c_dt.date_input("Data:", datetime.today()); hr_t = c_hr.time_input("Hora:", datetime.now().time())
                            q_t = c_qtd.number_input(f"Qtd (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']))
                            if st.form_submit_button("⬇️ CONFIRMAR"):
                                if q_t > 0:
                                    df.at[idx, 'qtd.estoque'] += q_t; df.at[idx, 'qtd_central'] -= q_t
                                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    novo_m = {'data_hora': datetime.combine(dt_t, hr_t), 'produto': nome_prod, 'qtd_movida': q_t}
                                    df_mov = pd.concat([df_mov, pd.DataFrame([novo_m])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov); st.success("Sucesso!"); st.rerun()
                    with st.expander("🛠️ Ajustes Manuais"):
                        cn = st.text_input("Nome:", value=nome_prod); cf = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
                        cc, cv = st.columns(2); n_c = cc.number_input("Custo:", value=float(df.at[idx, 'preco_custo']), format="%.2f"); n_v = cv.number_input("Venda:", value=float(df.at[idx, 'preco_venda']), format="%.2f")
                        if st.button("💾 SALVAR CORREÇÕES"):
                            df.at[idx, 'nome do produto'] = cn.upper(); df.at[idx, 'ultimo_fornecedor'] = cf; df.at[idx, 'preco_custo'] = n_c; df.at[idx, 'preco_venda'] = n_v
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Salvo!"); st.rerun()
            with tab_hist:
                filtro_h = st.text_input("🔍 Filtrar Histórico:")
                df_m_s = filtrar_dados_inteligente(df_mov, 'produto', filtro_h)
                st.dataframe(df_m_s.sort_values(by='data_hora', ascending=False), use_container_width=True)

# ------------------------------------------------------------------
# 3. ESTOQUE CENTRAL
# ------------------------------------------------------------------
elif modo == "🏡 Estoque Central (Casa)":
    st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
    tab_v, tab_g = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
    with tab_v:
        b_c = st.text_input("🔍 Buscar na Casa:"); df_v_c = filtrar_dados_inteligente(df, 'nome do produto', b_c)
        df_e_c = st.data_editor(df_v_c[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True, key="ed_casa")
        if st.button("💾 SALVAR CORREÇÕES DA TABELA"):
            df.update(df_e_c); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            for i, r in df_e_c.iterrows(): atualizar_casa_global(df.at[i, 'nome do produto'], r['qtd_central'], r['preco_custo'], None, r['validade'], prefixo)
            st.success("Sincronizado!"); st.rerun()
    with tab_g:
        l_p = sorted(df['nome do produto'].unique().tolist()); p_o = st.selectbox("Produto:", l_p)
        if p_o:
            idx_p = df[df['nome do produto'] == p_o].index[0]
            with st.form("f_c_f"):
                c_n = st.text_input("Nome:", value=df.at[idx_p, 'nome do produto'])
                c_v, c_c = st.columns(2); n_val = c_v.date_input("Validade:", value=df.at[idx_p, 'validade'] if pd.notnull(df.at[idx_p, 'validade']) else None); n_cus = c_c.number_input("Custo:", value=float(df.at[idx_p, 'preco_custo']))
                q_i = st.number_input("Qtd:", min_value=0); ac = st.radio("Ação:", ["Somar (+)", "Substituir (=)", "Apenas Salvar Dados"])
                if st.form_submit_button("💾 SALVAR"):
                    if ac.startswith("Somar"): df.at[idx_p, 'qtd_central'] += q_i
                    elif ac.startswith("Substituir"): df.at[idx_p, 'qtd_central'] = q_i
                    df.at[idx_p, 'nome do produto'] = c_n.upper(); df.at[idx_p, 'validade'] = pd.to_datetime(n_val); df.at[idx_p, 'preco_custo'] = n_cus
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    atualizar_casa_global(c_n.upper(), df.at[idx_p, 'qtd_central'], n_cus, None, pd.to_datetime(n_val), prefixo)
                    st.success("Salvo!"); st.rerun()

# ------------------------------------------------------------------
# 4. TABELA GERAL
# ------------------------------------------------------------------
elif modo == "📋 Tabela Geral":
    st.title("📋 Visão Geral (Editável)")
    b_g = st.text_input("🔍 Buscar:"); df_v_g = filtrar_dados_inteligente(df, 'nome do produto', b_g)
    df_ed_g = st.data_editor(df_v_g, use_container_width=True, num_rows="dynamic", key="ed_geral")
    c1, c2 = st.columns(2)
    if c1.button("💾 SALVAR ALTERAÇÕES"):
        df.update(df_ed_g); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
        for i, r in df_ed_g.iterrows(): atualizar_casa_global(df.at[i, 'nome do produto'], r['qtd_central'], r['preco_custo'], r['preco_venda'], r['validade'], prefixo)
        st.success("Tudo Salvo!"); st.rerun()
    if c2.button("🔮 UNIFICAR PELO CÓDIGO"):
        df = unificar_produtos_por_codigo(df); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Mágica feita!"); st.rerun()

# ------------------------------------------------------------------
# DEMAIS MÓDULOS (XML, SYNC, CADASTRO)
# ------------------------------------------------------------------
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Cadastro")
    with st.form("f_cad"):
        c1, c2 = st.columns(2); nc = c1.text_input("Código"); nn = c1.text_input("Nome")
        ncu = c2.number_input("Custo"); nv = c2.number_input("Venda")
        if st.form_submit_button("💾 CADASTRAR"):
            new = {'código de barras': nc, 'nome do produto': nn.upper(), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': ncu, 'preco_venda': nv, 'status_compra': 'OK'}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Ok!"); st.rerun()

elif modo == "📥 Importar XML (Associação Inteligente)":
    st.title("📥 XML NFe")
    arq = st.file_uploader("XML", type=['xml'])
    if arq:
        dados = ler_xml_nfe(arq, df_oficial); l_s = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].unique().tolist()); escolhas = {}
        for i, item in enumerate(dados['itens']):
            st.divider(); c1, c2 = st.columns([1,1]); c1.write(f"XML: {item['nome']} (x{item['qtd']})")
            m = "(CRIAR NOVO)"
            if not df.empty:
                msk = df['código de barras'] == item['ean']
                if msk.any(): m = df.loc[msk, 'nome do produto'].values[0]
                else: 
                    best, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist())
                    if best: m = best
            escolhas[i] = c2.selectbox(f"Vincular {i+1}", l_s, index=l_s.index(m) if m in l_s else 0, key=f"x_{i}")
        if st.button("✅ CONFIRMAR"):
            for i, it in enumerate(dados['itens']):
                esc = escolhas[i]; q = it['qtd']; c = it['preco_un_liquido']
                if esc == "(CRIAR NOVO)":
                    n = {'código de barras': it['ean'], 'nome do produto': it['nome'], 'qtd.estoque': 0, 'qtd_central': q, 'qtd_minima': 5, 'preco_custo': c, 'preco_venda': it['preco_un_bruto']*2, 'status_compra': 'OK', 'ultimo_fornecedor': dados['fornecedor']}
                    df = pd.concat([df, pd.DataFrame([n])], ignore_index=True)
                    atualizar_casa_global(it['nome'], q, c, None, None, prefixo)
                else:
                    msk = df['nome do produto'] == esc
                    if msk.any():
                        ix = df[msk].index[0]; df.at[ix, 'qtd_central'] += q; df.at[ix, 'preco_custo'] = c
                        atualizar_casa_global(esc, df.at[ix, 'qtd_central'], c, None, None, prefixo)
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Salvo!"); st.rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sync Excel")
    f = st.file_uploader("Arquivo", type=['xlsx','csv'])
    if f:
        r = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f)
        c1, c2, c3 = st.columns(3); ic = c1.selectbox("Cód", r.columns, 0); inm = c2.selectbox("Nome", r.columns, 1); iq = c3.selectbox("Qtd", r.columns, len(r.columns)-1)
        if st.button("🚀 SINCRONIZAR"):
            for i, row in r.iterrows():
                c = str(row[ic]).replace('.0','').strip(); n = normalizar_texto(str(row[inm])); q = pd.to_numeric(row[iq], errors='coerce')
                msk = df['código de barras'] == c
                if msk.any(): df.loc[msk, 'qtd.estoque'] = q
                else:
                    new = {'código de barras': c, 'nome do produto': n, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'status_compra': 'OK'}
                    df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Sincronizado!"); st.rerun()

elif modo == "⚙️ Configurar Base Oficial":
    st.title("⚙️ Base Oficial")
    ab = st.file_uploader("Suba o arquivo", type=['xlsx', 'csv'])
    if ab:
        if st.button("🚀 Processar"):
            if processar_excel_oficial(ab): st.success("OK!"); st.rerun()

elif modo == "📝 Lista de Compras (Planejamento)":
    st.title("📝 Lista de Compras")
    st.dataframe(df_lista_compras, use_container_width=True)
    if st.button("🗑️ Limpar Lista"):
        salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']))
        st.rerun()

elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico")
    st.dataframe(df_hist.sort_values(by='data', ascending=False), use_container_width=True)
