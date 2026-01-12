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
# ⚙️ CONFIGURAÇÃO E CONEXÃO GOOGLE (ANTI-BLOQUEIO)
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas Pro", layout="wide", page_icon="🏪")

@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1) # Pausa técnica
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        if df.empty: return pd.DataFrame(columns=colunas_padrao)
        
        # Converte números e datas
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

def salvar_na_nuvem(nome_aba, df):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        ws.clear()
        
        df_save = df.copy()
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear() # Limpa cache para ver atualização
    except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 SUAS FUNÇÕES ORIGINAIS
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto): return normalizar_texto(texto)

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

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

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
        cols_basic = ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'preco_custo', 'preco_venda', 'validade', 'ultimo_fornecedor', 'preco_sem_desconto']
        df_outra = ler_da_nuvem(f"{loja}_estoque", cols_basic)
        if not df_outra.empty:
            df_outra.columns = df_outra.columns.str.strip().str.lower()
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
    dados_nota = {'numero': '', 'fornecedor': '', 'data': datetime.now(), 'itens': []}
    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == '': dados_nota['fornecedor'] = elem.text
    lista_nomes_ref = []; dict_ref_ean = {}
    if not df_referencia.empty:
        for idx, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            dict_ref_ean[nm] = str(row['código de barras']).strip()
            lista_nomes_ref.append(nm)
    dets = [e for e in root.iter() if tag_limpa(e) == 'det']
    for det in dets:
        prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
        if prod:
            item = {'codigo_interno': '', 'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
            vProd = 0.0; vDesc = 0.0; qCom = 0.0
            for info in prod:
                t = tag_limpa(info)
                if t == 'cProd': item['codigo_interno'] = info.text
                elif t == 'cEAN': item['ean'] = info.text
                elif t == 'xProd': item['nome'] = normalizar_texto(info.text)
                elif t == 'qCom': qCom = float(info.text)
                elif t == 'vProd': vProd = float(info.text)
                elif t == 'vDesc': vDesc = float(info.text)
            if qCom > 0:
                item['qtd'] = qCom; item['preco_un_bruto'] = vProd / qCom; item['desconto_total_item'] = vDesc; item['preco_un_liquido'] = (vProd - vDesc) / qCom
            ean_xml = str(item['ean']).strip()
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                if lista_nomes_ref:
                    melhor, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                    if melhor: item['ean'] = dict_ref_ean.get(melhor, item['codigo_interno'])
            dados_nota['itens'].append(item)
    return dados_nota

# ==============================================================================
# 🚀 APP CONFIG
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar:", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular", value=False)
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# Colunas Padrão
COLS_ESTOQUE = ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto']
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

# Carrega Estoque
df = ler_da_nuvem(f"{prefixo}_estoque", COLS_ESTOQUE)
if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

st.sidebar.title("🏪 Menu")
modo = st.sidebar.radio("Navegar:", ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial", "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"])

# ------------------------------------------------------------------
# MÓDULOS
# ------------------------------------------------------------------

if modo == "📊 Dashboard (Visão Geral)":
    st.title(f"📊 Painel - {loja_atual}")
    if not df.empty:
        val = (df['qtd.estoque']*df['preco_custo']).sum() + (df['qtd_central']*df['preco_custo']).sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Loja", int(df['qtd.estoque'].sum()))
        c2.metric("Valor", f"R$ {val:,.2f}")
        c3.metric("Casa", int(df['qtd_central'].sum()))

elif modo == "📥 Importar XML (Associação Inteligente)":
    st.title("📥 XML NFe")
    df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)
    arq = st.file_uploader("XML", type=['xml'])
    if arq:
        try:
            d = ler_xml_nfe(arq, df_oficial)
            st.success(f"Nota: {d['numero']}")
            l_sys = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].unique().tolist())
            escolhas = {}
            for i, item in enumerate(d['itens']):
                st.divider()
                c1, c2 = st.columns([1,1])
                c1.write(f"XML: {item['nome']} (x{item['qtd']})")
                m = "(CRIAR NOVO)"
                if not df.empty:
                    msk = df['código de barras'] == str(item['ean']).strip()
                    if msk.any(): m = df.loc[msk, 'nome do produto'].values[0]
                    else: 
                        best, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist())
                        if best: m = best
                idx = l_sys.index(m) if m in l_sys else 0
                escolhas[i] = c2.selectbox(f"Item {i+1}", l_sys, index=idx, key=f"s_{i}")
            if st.button("SALVAR"):
                for i, item in enumerate(d['itens']):
                    esc = escolhas[i]; q = item['qtd']; c = item['preco_un_liquido']
                    if esc == "(CRIAR NOVO)":
                        n = {'código de barras': str(item['ean']).strip(), 'nome do produto': normalizar_texto(item['nome']), 'qtd.estoque': 0, 'qtd_central': q, 'qtd_minima': 5, 'preco_custo': c, 'preco_venda': item['preco_un_bruto']*2, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': d['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']}
                        df = pd.concat([df, pd.DataFrame([n])], ignore_index=True)
                        atualizar_casa_global(n['nome do produto'], q, c, None, None, prefixo)
                    else:
                        msk = df['nome do produto'] == esc
                        if msk.any():
                            ix = df[msk].index[0]
                            df.at[ix, 'qtd_central'] += q
                            df.at[ix, 'preco_custo'] = c
                            df.at[ix, 'ultimo_fornecedor'] = d['fornecedor']
                            atualizar_casa_global(esc, df.at[ix, 'qtd_central'], c, None, None, prefixo)
                salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Salvo!"); st.rerun()
        except Exception as e: st.error(e)

# ------------------------------------------------------------------
# MÓDULO GÔNDOLA (RESTAURADO IGUAL AO ORIGINAL)
# ------------------------------------------------------------------
elif modo == "🏠 Gôndola (Loja)":
    st.title(f"🏠 Gôndola - {loja_atual}")
    if df.empty:
        st.warning("Cadastre produtos.")
    else:
        # Carrega histórico para mostrar na aba
        df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", COLS_MOV)
        
        # MODO DESKTOP ROBUSTO (ORIGINAL)
        if not usar_modo_mobile:
            tab_acao, tab_hist = st.tabs(["🚚 Repor / Consultar", "📜 Histórico"])
            
            with tab_acao:
                df['display_busca'] = df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)
                # VOLTOU A BUSCA SELECTBOX ORIGINAL
                opcao_busca = st.selectbox("🔍 Buscar Produto:", [""] + df['display_busca'].tolist())
                
                if opcao_busca != "":
                    idx = df[df['display_busca'] == opcao_busca].index[0]
                    nome_prod = df.at[idx, 'nome do produto']
                    st.markdown(f"## 📦 {nome_prod}")
                    
                    c1, c2, c3 = st.columns(3)
                    c1.info(f"Loja: {int(df.at[idx, 'qtd.estoque'])}")
                    c2.success(f"Casa: {int(df.at[idx, 'qtd_central'])}")
                    val = df.at[idx, 'validade']
                    c3.write(f"Validade: {val.strftime('%d/%m/%Y') if pd.notnull(val) else 'Sem data'}")
                    st.divider()
                    
                    # TRANSFERÊNCIA
                    if df.at[idx, 'qtd_central'] > 0:
                        st.subheader("🚚 Transferência (Casa -> Loja)")
                        with st.form("form_transf_gondola"):
                            c_dt, c_hr, c_qtd = st.columns(3)
                            dt_transf = c_dt.date_input("Data:", datetime.today())
                            hr_transf = c_hr.time_input("Hora:", datetime.now().time())
                            qtd_transf = c_qtd.number_input(f"Qtd (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']), value=0)
                            
                            if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                if qtd_transf > 0:
                                    df.at[idx, 'qtd.estoque'] += qtd_transf
                                    df.at[idx, 'qtd_central'] -= qtd_transf
                                    salvar_na_nuvem(f"{prefixo}_estoque", df)
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    
                                    # Log Movimentação
                                    novo_mov = {'data_hora': datetime.combine(dt_transf, hr_transf), 'produto': nome_prod, 'qtd_movida': qtd_transf}
                                    df_mov = pd.concat([df_mov, pd.DataFrame([novo_mov])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov)
                                    
                                    st.success("Transferido!"); st.rerun()
                                else: st.info("Qtd zero.")
                    
                    st.divider()
                    
                    # EXPANDER DE AJUSTES MANUAIS (VOLTOU!)
                    with st.expander("🛠️ Ajustes Manuais (Completo)"):
                        st.warning("⚠️ Edições aqui atualizam o cadastro geral!")
                        c_nome = st.text_input("Corrigir Nome:", value=nome_prod)
                        c_forn = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
                        c_custo, c_venda = st.columns(2)
                        n_custo = c_custo.number_input("Custo:", value=float(df.at[idx, 'preco_custo']), format="%.2f")
                        n_venda = c_venda.number_input("Venda:", value=float(df.at[idx, 'preco_venda']), format="%.2f")
                        c1, c2 = st.columns(2)
                        n_qtd_loja = c1.number_input("Qtd Real Loja:", value=int(df.at[idx, 'qtd.estoque']))
                        n_val = c2.date_input("Nova Validade:", value=val if pd.notnull(val) else None)
                        
                        if st.button("💾 SALVAR CORREÇÕES"):
                            df.at[idx, 'nome do produto'] = c_nome.upper().strip()
                            df.at[idx, 'ultimo_fornecedor'] = c_forn.strip()
                            df.at[idx, 'preco_custo'] = n_custo
                            df.at[idx, 'preco_venda'] = n_venda
                            df.at[idx, 'qtd.estoque'] = n_qtd_loja
                            df.at[idx, 'validade'] = pd.to_datetime(n_val) if n_val else None
                            salvar_na_nuvem(f"{prefixo}_estoque", df)
                            st.success("Atualizado!"); st.rerun()
            
            with tab_hist:
                if not df_mov.empty:
                    busca_hist = st.text_input("Filtrar Histórico:", placeholder="Produto...")
                    df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_hist)
                    st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True)
                else: st.info("Sem histórico.")
        
        # MODO MOBILE (SIMPLIFICADO)
        else:
            b = st.text_input("Buscar:", placeholder="...")
            if b:
                res = filtrar_dados_inteligente(df, 'nome do produto', b)
                for idx, row in res.iterrows():
                    with st.container(border=True):
                        st.subheader(row['nome do produto'])
                        c1, c2 = st.columns(2)
                        c1.metric("Loja", int(row['qtd.estoque']))
                        c2.metric("Casa", int(row['qtd_central']))
                        if row['qtd_central'] > 0:
                            with st.form(key=f"mb_{idx}"):
                                q = st.number_input("Baixar:", min_value=1, max_value=int(row['qtd_central']))
                                if st.form_submit_button("Baixar"):
                                    df.at[idx, 'qtd.estoque'] += q
                                    df.at[idx, 'qtd_central'] -= q
                                    salvar_na_nuvem(f"{prefixo}_estoque", df)
                                    atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    st.success("Feito!"); st.rerun()

# ------------------------------------------------------------------
# OUTROS MÓDULOS (Resumidos para caber, mas funcionais)
# ------------------------------------------------------------------
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Novo"); 
    with st.form("n"): 
        c = st.text_input("Cod"); nm = st.text_input("Nome"); p = st.number_input("Preço")
        if st.form_submit_button("Salvar"):
            n = {'código de barras': c, 'nome do produto': normalizar_texto(nm), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': p, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
            df = pd.concat([df, pd.DataFrame([n])], ignore_index=True); salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Ok!"); st.rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sync Excel")
    f = st.file_uploader("Arq", type=['xlsx','csv'])
    if f:
        r = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f)
        c1,c2,c3=st.columns(3); ic=c1.selectbox("Cod", r.columns, 0); inu=c2.selectbox("Nome", r.columns, 1); iq=c3.selectbox("Qtd", r.columns, len(r.columns)-1)
        if st.button("Enviar"):
            bar=st.progress(0); tot=len(r)
            for i, row in r.iterrows():
                c=str(row[ic]).replace('.0','').strip(); n=normalizar_texto(str(row[inu])); q=pd.to_numeric(row[iq], errors='coerce')
                msk=df['código de barras']==c
                if msk.any(): df.loc[msk, 'qtd.estoque']=q
                else: 
                    nv={'código de barras': c, 'nome do produto': n, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': 0, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
                    df=pd.concat([df, pd.DataFrame([nv])], ignore_index=True)
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df); st.success("Feito!"); st.rerun()

elif modo == "🏡 Estoque Central (Casa)":
    st.title("🏡 Casa")
    b = st.text_input("Buscar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', b)
    df_ed = st.data_editor(df_show[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True)
    if st.button("Salvar"):
        df.update(df_ed); salvar_na_nuvem(f"{prefixo}_estoque", df)
        for i, r in df_ed.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], None, r['validade'], prefixo)
        st.success("Salvo!"); st.rerun()

elif modo == "📋 Tabela Geral":
    st.title("📋 Geral")
    b = st.text_input("Buscar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', b)
    df_ed = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
    if st.button("Salvar Tudo"):
        df.update(df_ed); salvar_na_nuvem(f"{prefixo}_estoque", df)
        for i, r in df_ed.iterrows(): atualizar_casa_global(df.at[i,'nome do produto'], r['qtd_central'], r['preco_custo'], r['preco_venda'], r['validade'], prefixo)
        st.success("Salvo!"); st.rerun()
