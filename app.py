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

def garantir_integridade_colunas(df):
    if df.empty: return pd.DataFrame(columns=COLUNAS_VITAIS)
    df.columns = df.columns.str.strip().str.lower()
    for col in COLUNAS_VITAIS:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo']): df[col] = 0.0
            else: df[col] = ""
    return df

@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba):
    time.sleep(1) 
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            if "estoque" in nome_aba: ws.append_row(COLUNAS_VITAIS)
            return pd.DataFrame(columns=COLUNAS_VITAIS if "estoque" in nome_aba else [])
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        if "estoque" in nome_aba: df = garantir_integridade_colunas(df)
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame()

def salvar_na_nuvem(nome_aba, df):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        ws.clear()
        df_save = df.copy()
        if "estoque" in nome_aba: df_save = garantir_integridade_colunas(df_save)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e: st.error(f"Erro: {e}")

# ==============================================================================
# 🧠 FUNÇÕES DE LÓGICA (IDÊNTICAS À REFERÊNCIA)
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

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        df_outra = ler_da_nuvem(f"{loja}_estoque")
        if not df_outra.empty:
            mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
            if mask.any():
                idx = df_outra[mask].index[0]
                df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                salvar_na_nuvem(f"{loja}_estoque", df_outra)

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
# 🚀 INÍCIO DO APP
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=False)
st.sidebar.markdown("---")

prefixo = "loja1" if "1" in loja_atual else "loja2" if "2" in loja_atual else "loja3"

df = ler_da_nuvem(f"{prefixo}_estoque")
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras")
df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes")
df_vendas = ler_da_nuvem(f"{prefixo}_vendas")
df_lista_compras = ler_da_nuvem(f"{prefixo}_lista_compras")
df_oficial = ler_da_nuvem("base_oficial")

if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

modo = st.sidebar.radio("Navegar:", ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial", "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"])

# 1. DASHBOARD
if modo == "📊 Dashboard (Visão Geral)":
    st.title(f"📊 Painel - {loja_atual}")
    if not df.empty:
        df_valido = df[pd.notnull(df['validade'])].copy()
        df_critico = df_valido[(df_valido['validade'] <= datetime.now() + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
        val_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Loja", int(df['qtd.estoque'].sum()))
        c2.metric("💰 Valor", f"R$ {val_estoque:,.2f}")
        c3.metric("🚨 Vencendo", len(df_critico))
        c4.metric("🏡 Casa", int(df['qtd_central'].sum()))

# 5. GÔNDOLA (TOTALMENTE RESTAURADA)
elif modo == "🏠 Gôndola (Loja)":
    st.title(f"🏠 Gôndola - {loja_atual}")
    if not df.empty:
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
                                salvar_na_nuvem(f"{prefixo}_estoque", df); atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo); st.rerun()
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
                        with st.form("form_transf_gondola"):
                            c_dt, c_hr, c_qtd = st.columns(3)
                            dt_t = c_dt.date_input("Data:", datetime.today()); hr_t = c_hr.time_input("Hora:", datetime.now().time()); q_t = c_qtd.number_input(f"Qtd (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']))
                            if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                if q_t > 0:
                                    df.at[idx, 'qtd.estoque'] += q_t; df.at[idx, 'qtd_central'] -= q_t
                                    salvar_na_nuvem(f"{prefixo}_estoque", df); atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    df_mov = pd.concat([df_mov, pd.DataFrame([{'data_hora': datetime.combine(dt_t, hr_t), 'produto': nome_prod, 'qtd_movida': q_t}])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov); st.success("Sucesso!"); st.rerun()
                    with st.expander("🛠️ Ajustes Manuais (Completo)"):
                        cn = st.text_input("Corrigir Nome:", value=nome_prod); cf = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
                        cc, cv = st.columns(2); n_c = cc.number_input("Custo:", value=float(df.at[idx, 'preco_custo'])); n_v = cv.number_input("Venda:", value=float(df.at[idx, 'preco_venda']))
                        c1, c2 = st.columns(2); n_ql = c1.number_input("Qtd Real Loja:", value=int(df.at[idx, 'qtd.estoque'])); n_vl = c2.date_input("Nova Validade:", value=val if pd.notnull(val) else None)
                        if st.button("💾 SALVAR CORREÇÕES"):
                            df.at[idx, 'nome do produto'] = cn.upper(); df.at[idx, 'ultimo_fornecedor'] = cf; df.at[idx, 'preco_custo'] = n_c; df.at[idx, 'preco_venda'] = n_v; df.at[idx, 'qtd.estoque'] = n_ql; df.at[idx, 'validade'] = pd.to_datetime(n_vl)
                            salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Salvo!"); st.rerun()
            with tab_hist:
                df_m_s = filtrar_dados_inteligente(df_mov, 'produto', st.text_input("🔍 Buscar no Histórico:")); st.dataframe(df_m_s.sort_values(by='data_hora', ascending=False), use_container_width=True)

# 8. ESTOQUE CENTRAL (TOTALMENTE RESTAURADO)
elif modo == "🏡 Estoque Central (Casa)":
    st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
    tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
    with tab_ver:
        b_c = st.text_input("🔍 Buscar na Casa:"); df_v_c = filtrar_dados_inteligente(df, 'nome do produto', b_c)
        df_e_c = st.data_editor(df_v_c[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True, key="ed_casa")
        if st.button("💾 SALVAR CORREÇÕES DA TABELA"):
            df.update(df_e_c); salvar_na_nuvem(f"{prefixo}_estoque", df)
            for i, r in df_e_c.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], None, r['validade'], prefixo)
            st.success("Salvo!"); st.rerun()
    with tab_gerenciar:
        l_p = sorted(df['nome do produto'].unique().tolist()); p_o = st.selectbox("Selecione:", l_p)
        if p_o:
            ix = df[df['nome do produto'] == p_o].index[0]
            with st.form("edit_estoque_casa_full"):
                c_dt, c_hr = st.columns(2); dt_r = c_dt.date_input("Data:", datetime.today()); hr_r = c_hr.time_input("Hora:", datetime.now().time()); c_f = st.text_input("Fornecedor:", value=df.at[ix, 'ultimo_fornecedor'])
                c_n = st.text_input("Nome (Editável):", value=df.at[ix, 'nome do produto'])
                c_v, c_c, c_v_v = st.columns(3); n_v = c_v.date_input("Validade:", value=df.at[ix, 'validade'] if pd.notnull(df.at[ix, 'validade']) else None); n_cus = c_c.number_input("Custo:", value=float(df.at[ix, 'preco_custo'])); n_venda = c_v_v.number_input("Venda:", value=float(df.at[ix, 'preco_venda']))
                c_q, c_a = st.columns([1, 2]); q_i = c_q.number_input("Qtd:", min_value=0); acao = c_a.radio("Ação:", ["Somar (+)", "Substituir (=)", "Apenas Salvar Dados"])
                if st.form_submit_button("💾 SALVAR REGISTRO COMPLETO"):
                    if acao.startswith("Somar"): df.at[ix, 'qtd_central'] += q_i
                    elif acao.startswith("Substituir"): df.at[ix, 'qtd_central'] = q_i
                    df.at[ix, 'nome do produto'] = c_n.upper(); df.at[ix, 'validade'] = pd.to_datetime(n_v); df.at[ix, 'preco_custo'] = n_cus; df.at[ix, 'preco_venda'] = n_venda
                    salvar_na_nuvem(f"{prefixo}_estoque", df); atualizar_casa_global(c_n.upper(), df.at[ix, 'qtd_central'], n_cus, n_venda, pd.to_datetime(n_v), prefixo); st.success("Salvo!"); st.rerun()

# 9. GERAL (TOTALMENTE RESTAURADA)
elif modo == "📋 Tabela Geral":
    st.title("📋 Visão Geral")
    b_g = st.text_input("🔍 Buscar:"); df_v_g = filtrar_dados_inteligente(df, 'nome do produto', b_g)
    df_ed = st.data_editor(df_v_g, use_container_width=True, num_rows="dynamic", key="ed_geral")
    c1, c2 = st.columns(2)
    if c1.button("💾 SALVAR ALTERAÇÕES GERAIS"):
        df.update(df_ed); salvar_na_nuvem(f"{prefixo}_estoque", df)
        for i, r in df_ed.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], r['preco_venda'], r['validade'], prefixo)
        st.success("Salvo!"); st.rerun()
    if c2.button("🔮 CORRIGIR NOMES E UNIFICAR (Pelo Código)"):
        df = unificar_produtos_por_codigo(df); salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Unificado!"); st.rerun()

# Outros Módulos (Sincronizados e Intactos)
elif modo == "🆕 Cadastrar Produto":
    with st.form("cad"):
        c1, c2 = st.columns(2); nc = c1.text_input("Cod"); nn = c1.text_input("Nome"); ncu = c2.number_input("Custo"); nv = c2.number_input("Venda")
        if st.form_submit_button("💾 CADASTRAR"):
            new = {'código de barras': nc, 'nome do produto': nn.upper(), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': ncu, 'preco_venda': nv, 'status_compra': 'OK'}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True); salvar_na_nuvem(f"{prefixo}_estoque", df); st.rerun()

elif modo == "📝 Lista de Compras (Planejamento)":
    t1, t2 = st.tabs(["📋 Ver Lista", "➕ Adicionar"])
    with t1:
        st.dataframe(df_lista_compras, use_container_width=True)
        if st.button("🗑️ Limpar Lista"): salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=df_lista_compras.columns)); st.rerun()
    with t2:
        if st.button("🚀 Gerar pelo Mínimo"):
            baixo = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            novos = [{'produto': r['nome do produto'], 'qtd_sugerida': r['qtd_minima']*3, 'fornecedor': r['ultimo_fornecedor'], 'custo_previsto': r['preco_custo'], 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'A Comprar'} for _, r in baixo.iterrows()]
            df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos)], ignore_index=True); salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras); st.rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    f = st.file_uploader("Excel", type=['xlsx'])
    if f:
        r = pd.read_excel(f); c1, c2, c3 = st.columns(3); ic = c1.selectbox("Cod", r.columns, 0); inm = c2.selectbox("Nome", r.columns, 1); iq = c3.selectbox("Qtd", r.columns, len(r.columns)-1)
        if st.button("🚀 SINCRONIZAR"):
            for i, row in r.iterrows():
                c = str(row[ic]).replace('.0','').strip(); n = normalizar_texto(str(row[inm])); q = pd.to_numeric(row[iq], errors='coerce')
                msk = df['código de barras'] == c
                if msk.any(): df.loc[msk, 'qtd.estoque'] = q
                else: df = pd.concat([df, pd.DataFrame([{'código de barras': c, 'nome do produto': n, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'status_compra': 'OK'}])], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Ok!"); st.rerun()
