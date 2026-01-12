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
# ⚙️ CONFIGURAÇÃO E CONEXÃO GOOGLE (COM PROTEÇÃO DE DADOS)
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# --- LISTA DE COLUNAS QUE O SISTEMA EXIGE PARA NÃO QUEBRAR ---
COLUNAS_OBRIGATORIAS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]

# --- FUNÇÃO DE AUTO-CURA (CORRIGE O ERRO KEYERROR) ---
def garantir_colunas(df):
    """
    Se o Planograma apagou colunas importantes (como qtd_central),
    esta função as recria automaticamente para o app não travar.
    """
    if df.empty:
        return pd.DataFrame(columns=COLUNAS_OBRIGATORIAS)
    
    # Normaliza nomes das colunas
    df.columns = df.columns.str.strip().str.lower()
    
    for col in COLUNAS_OBRIGATORIAS:
        if col not in df.columns:
            # Se faltar coluna de número, preenche com 0.0
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo']):
                df[col] = 0.0
            # Se faltar data ou texto, preenche com vazio
            else:
                df[col] = ""
                
    return df

# --- CONEXÃO COM O COFRE ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# --- LEITURA DA NUVEM (COM CACHE E PROTEÇÃO) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba):
    time.sleep(1) # Pausa para evitar erro 429 do Google
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            # Cria aba se não existir
            ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
            if "estoque" in nome_aba:
                ws.append_row(COLUNAS_OBRIGATORIAS)
                return pd.DataFrame(columns=COLUNAS_OBRIGATORIAS)
            return pd.DataFrame()
            
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        
        # APLICA A CURA SE FOR TABELA DE ESTOQUE
        if "estoque" in nome_aba:
            df = garantir_colunas(df)
            
        # Converte tipos (Números e Datas)
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        return df
    except Exception as e:
        # st.error(f"Erro leitura {nome_aba}: {e}")
        if "estoque" in nome_aba: return pd.DataFrame(columns=COLUNAS_OBRIGATORIAS)
        return pd.DataFrame()

# --- SALVAR NA NUVEM ---
def salvar_na_nuvem(nome_aba, df):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        ws.clear()
        
        # Garante estrutura antes de salvar
        df_save = df.copy()
        if "estoque" in nome_aba:
            df_save = garantir_colunas(df_save)
            
        # Converte datas para texto (JSON não aceita datetime)
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
                
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear() # Limpa memória para ver dados novos
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 SUAS FUNÇÕES ORIGINAIS (MANTIDAS INTACTAS)
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
    # Garante estrutura antes de processar
    df = garantir_colunas(df)
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
# 🚀 INÍCIO DO APP
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
# Padrão desktop (False) como você pediu
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=False, help="Melhora a visualização para iPhone/Android")
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# Carrega os dados principais (Estoque)
df = ler_da_nuvem(f"{prefixo}_estoque")

# Formatação inicial se carregou
if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

st.sidebar.title("🏪 Menu")
modo = st.sidebar.radio("Navegar:", ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial", "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)", "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"])

# -----------------------------------------------------------------------------
# 1. DASHBOARD
# -----------------------------------------------------------------------------
if modo == "📊 Dashboard (Visão Geral)":
    st.title(f"📊 Painel de Controle - {loja_atual}")
    if df.empty:
        st.info("Comece cadastrando produtos.")
    else:
        hoje = datetime.now()
        df_valido = df[pd.notnull(df['validade'])].copy()
        df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5))]
        
        valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
        c2.metric("💰 Valor Investido", f"R$ {valor_estoque:,.2f}")
        c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
        c4.metric("🏡 Casa", int(df['qtd_central'].sum()))
        st.divider()
        
        baixo = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
        if not baixo.empty:
            st.warning(f"🚨 Existem {len(baixo)} produtos com estoque baixo!")

# -----------------------------------------------------------------------------
# 2. IMPORTAR XML
# -----------------------------------------------------------------------------
elif modo == "📥 Importar XML (Associação Inteligente)":
    st.title("📥 Importar XML da Nota Fiscal")
    df_oficial = ler_da_nuvem("base_oficial")
    
    arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
    
    if arquivo_xml:
        try:
            dados = ler_xml_nfe(arquivo_xml, df_oficial)
            st.success(f"Nota Fiscal: **{dados['numero']}** | Fornecedor: **{dados['fornecedor']}**")
            
            lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
            escolhas = {}
            
            for i, item in enumerate(dados['itens']):
                st.divider()
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"📄 XML: **{item['nome']}**")
                c1.caption(f"EAN: {item['ean']} | Qtd: {int(item['qtd'])} | R$ {item['preco_un_liquido']:.2f}")
                
                match_inicial = "(CRIAR NOVO)"
                tipo_match = "Nenhum"
                
                if not df.empty:
                    mask_ean = df['código de barras'].astype(str) == str(item['ean']).strip()
                    if mask_ean.any():
                        match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                        tipo_match = "Código EAN"
                    else:
                        melhor, tipo = encontrar_melhor_match(item['nome'], df['nome do produto'].astype(str).tolist())
                        if melhor:
                            match_inicial = melhor
                            tipo_match = tipo
                
                idx_inicial = lista_produtos_sistema.index(match_inicial) if match_inicial in lista_produtos_sistema else 0
                escolhas[i] = c2.selectbox(f"Vincular ({tipo_match}):", lista_produtos_sistema, index=idx_inicial, key=f"s_{i}")
            
            if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
                for i, item in enumerate(dados['itens']):
                    prod_escolhido = escolhas[i]
                    qtd = item['qtd']; custo = item['preco_un_liquido']
                    
                    if prod_escolhido == "(CRIAR NOVO)":
                        novo = {
                            'código de barras': str(item['ean']).strip(), 'nome do produto': normalizar_texto(item['nome']),
                            'qtd.estoque': 0, 'qtd_central': qtd, 'qtd_minima': 5,
                            'preco_custo': custo, 'preco_venda': item['preco_un_bruto']*2,
                            'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL',
                            'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']
                        }
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        atualizar_casa_global(novo['nome do produto'], qtd, custo, None, None, prefixo)
                    else:
                        mask = df['nome do produto'] == prod_escolhido
                        if mask.any():
                            idx = df[mask].index[0]
                            df.at[idx, 'qtd_central'] += qtd
                            df.at[idx, 'preco_custo'] = custo
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizar_casa_global(prod_escolhido, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                st.success("Estoque salvo e sincronizado!")
                st.balloons()
                st.rerun()
        except Exception as e: st.error(f"Erro XML: {e}")

# -----------------------------------------------------------------------------
# 3. GÔNDOLA (RESTAURADA E ROBUSTA)
# -----------------------------------------------------------------------------
elif modo == "🏠 Gôndola (Loja)":
    st.title(f"🏠 Gôndola - {loja_atual}")
    if df.empty:
        st.warning("Cadastre produtos.")
    else:
        df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes")
        
        # MODO DESKTOP ROBUSTO
        if not usar_modo_mobile:
            tab_acao, tab_hist = st.tabs(["🚚 Repor / Consultar", "📜 Histórico"])
            
            with tab_acao:
                df['display_busca'] = df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)
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
                            qtd_transf = c_qtd.number_input(f"Quantidade (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']), value=0)
                            
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
                                    st.success(f"Sucesso! {qtd_transf} unid. transferidas.")
                                    st.rerun()
                    
                    st.divider()
                    
                    # AJUSTES MANUAIS (EXPANDER)
                    with st.expander("🛠️ Ajustes Manuais (Completo)"):
                        st.warning("⚠️ Edições aqui atualizam o cadastro geral!")
                        c_nome = st.text_input("Corrigir Nome:", value=nome_prod)
                        c_forn = st.text_input("Fornecedor Principal:", value=df.at[idx, 'ultimo_fornecedor'])
                        c_custo, c_venda = st.columns(2)
                        n_custo = c_custo.number_input("Preço Custo:", value=float(df.at[idx, 'preco_custo']), format="%.2f")
                        n_venda = c_venda.number_input("Preço Venda:", value=float(df.at[idx, 'preco_venda']), format="%.2f")
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
                            st.success("Atualizado!")
                            st.rerun()
            
            with tab_hist:
                if not df_mov.empty:
                    busca_gondola_hist = st.text_input("🔍 Buscar no Histórico:", placeholder="Ex: oleo...", key="busca_gondola_hist")
                    df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_gondola_hist)
                    st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True)
                else: st.info("Sem histórico.")
        
        # MODO MOBILE SIMPLIFICADO
        else:
            termo_busca = st.text_input("🔍 Buscar Produto:", placeholder="Digite aqui...")
            df_show = filtrar_dados_inteligente(df, 'nome do produto', termo_busca)
            for idx, row in df_show.iterrows():
                with st.container(border=True):
                    st.subheader(row['nome do produto'])
                    c1, c2 = st.columns(2)
                    c1.metric("Loja", int(row['qtd.estoque']))
                    c2.metric("Casa", int(row['qtd_central']))
                    if row['qtd_central'] > 0:
                        with st.form(key=f"mob_{idx}"):
                            q = st.number_input("Baixar:", min_value=1, max_value=int(row['qtd_central']))
                            if st.form_submit_button("Baixar"):
                                df.at[idx, 'qtd.estoque'] += q
                                df.at[idx, 'qtd_central'] -= q
                                salvar_na_nuvem(f"{prefixo}_estoque", df)
                                atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                st.success("Feito!")
                                st.rerun()

# -----------------------------------------------------------------------------
# 4. ESTOQUE CENTRAL (COM PROTEÇÃO DE ERRO)
# -----------------------------------------------------------------------------
elif modo == "🏡 Estoque Central (Casa)":
    st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
    tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
    
    with tab_ver:
        if not df.empty:
            busca_central = st.text_input("🔍 Buscar na Casa:", placeholder="Ex: arroz...")
            df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_central)
            
            # Garante que as colunas existem antes de mostrar no editor
            cols_to_show = ['nome do produto', 'qtd_central', 'preco_custo', 'validade']
            # O 'garantir_colunas' lá no carregamento já cuidou disso, mas reforçamos:
            df_ed = st.data_editor(df_show[cols_to_show], use_container_width=True)
            
            if st.button("💾 SALVAR CORREÇÕES DA TABELA"):
                df.update(df_ed)
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                # Sync
                for i, row in df_ed.iterrows():
                    atualizar_casa_global(df.at[i, 'nome do produto'], row['qtd_central'], row['preco_custo'], None, row['validade'], prefixo)
                st.success("Salvo!")
                st.rerun()
    
    with tab_gerenciar:
        if not df.empty:
            lista_prods = sorted(df['nome do produto'].astype(str).unique().tolist())
            prod_opcao = st.selectbox("Selecione o Produto:", lista_prods)
            if prod_opcao:
                mask = df['nome do produto'] == prod_opcao
                if mask.any():
                    idx = df[mask].index[0]
                    with st.form("edit_casa_form"):
                        st.write(f"Editando: {prod_opcao}")
                        c1, c2 = st.columns(2)
                        n_qtd = c1.number_input("Nova Qtd:", value=0)
                        acao = c2.radio("Tipo:", ["Somar (+)", "Substituir (=)"])
                        if st.form_submit_button("Salvar"):
                            if acao == "Somar (+)": df.at[idx, 'qtd_central'] += n_qtd
                            else: df.at[idx, 'qtd_central'] = n_qtd
                            salvar_na_nuvem(f"{prefixo}_estoque", df)
                            atualizar_casa_global(prod_opcao, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                            st.success("Atualizado!")
                            st.rerun()

# -----------------------------------------------------------------------------
# 5. HISTÓRICO (AGORA CARREGA CERTO)
# -----------------------------------------------------------------------------
elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico & Preços")
    df_hist = ler_da_nuvem(f"{prefixo}_historico_compras")
    
    if not df_hist.empty:
        busca = st.text_input("🔍 Buscar:", placeholder="Produto ou fornecedor...")
        df_show = filtrar_dados_inteligente(df_hist, 'produto', busca)
        if df_show.empty: df_show = filtrar_dados_inteligente(df_hist, 'fornecedor', busca)
        
        st.info("Edite abaixo se precisar corrigir valores.")
        df_ed = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
        
        if st.button("💾 SALVAR HISTÓRICO"):
            salvar_na_nuvem(f"{prefixo}_historico_compras", df_ed)
            st.success("Salvo!")
            st.rerun()
    else: st.info("Histórico vazio.")

# -----------------------------------------------------------------------------
# 6. TABELA GERAL
# -----------------------------------------------------------------------------
elif modo == "📋 Tabela Geral":
    st.title("📋 Visão Geral (Editável)")
    busca_g = st.text_input("🔍 Buscar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_g)
    
    df_ed = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
    
    c1, c2 = st.columns(2)
    if c1.button("💾 SALVAR TUDO"):
        df.update(df_ed)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        for i, row in df_ed.iterrows():
            atualizar_casa_global(df.at[i,'nome do produto'], row['qtd_central'], row['preco_custo'], row['preco_venda'], row['validade'], prefixo)
        st.success("Salvo!")
        st.rerun()
        
    if c2.button("🔮 UNIFICAR DUPLICADOS"):
        df = unificar_produtos_por_codigo(df)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        st.success("Unificado!")
        st.rerun()

# -----------------------------------------------------------------------------
# OUTROS MÓDULOS ESSENCIAIS
# -----------------------------------------------------------------------------
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Novo Produto")
    with st.form("cad"):
        c1, c2 = st.columns(2)
        cod = c1.text_input("Código")
        nom = c2.text_input("Nome")
        pr = st.number_input("Preço Venda")
        if st.form_submit_button("Salvar"):
            n = {'código de barras': cod, 'nome do produto': normalizar_texto(nom), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': pr, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
            df = pd.concat([df, pd.DataFrame([n])], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            st.success("Cadastrado!")
            st.rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sincronizar Excel")
    f = st.file_uploader("Arquivo", type=['xlsx', 'csv'])
    if f:
        r = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f)
        c1, c2, c3 = st.columns(3)
        ic = c1.selectbox("Código", r.columns, 0)
        inm = c2.selectbox("Nome", r.columns, 1)
        iq = c3.selectbox("Qtd", r.columns, len(r.columns)-1)
        if st.button("Sincronizar"):
            bar = st.progress(0); tot=len(r)
            for i, row in r.iterrows():
                try:
                    c = str(row[ic]).replace('.0','').strip()
                    n = normalizar_texto(str(row[inm]))
                    q = pd.to_numeric(row[iq], errors='coerce')
                    mask = df['código de barras'] == c
                    if mask.any():
                        df.loc[mask, 'qtd.estoque'] = q
                    else:
                        nv = {'código de barras': c, 'nome do produto': n, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': 0, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
                        df = pd.concat([df, pd.DataFrame([nv])], ignore_index=True)
                except: pass
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            st.success("Sincronizado!")
            st.rerun()

elif modo == "📉 Baixar Vendas (Do Relatório)":
    st.title("📉 Baixar Vendas")
    df_vendas = ler_da_nuvem(f"{prefixo}_vendas")
    f = st.file_uploader("Relatório", type=['xlsx'])
    if f:
        raw = pd.read_excel(f)
        c1, c2 = st.columns(2)
        inm = c1.selectbox("Nome", raw.columns)
        iq = c2.selectbox("Qtd", raw.columns)
        if st.button("Processar"):
            bar = st.progress(0); tot=len(raw); novos=[]
            for i, r in raw.iterrows():
                nm = str(r[inm]); q = pd.to_numeric(r[iq], errors='coerce')
                mask = df['nome do produto'].str.contains(nm, case=False, na=False)
                if mask.any() and q > 0:
                    idx = df[mask].index[0]
                    df.at[idx, 'qtd.estoque'] -= q
                    novos.append({'data_hora': datetime.now(), 'produto': df.at[idx, 'nome do produto'], 'qtd_vendida': q, 'estoque_restante': df.at[idx, 'qtd.estoque']})
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            if novos: 
                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos)], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_vendas", df_vendas)
            st.success("Baixado!"); st.rerun()

elif modo == "🛒 Fornecedor (Compras)":
    st.title("🛒 Compras Pendentes")
    pen = df[df['status_compra'] == 'PENDENTE']
    if not pen.empty:
        item = st.selectbox("Dar entrada:", pen['nome do produto'])
        with st.form("ent"):
            q = st.number_input("Qtd", value=1)
            if st.form_submit_button("Confirmar"):
                idx = df[df['nome do produto']==item].index[0]
                df.at[idx, 'qtd_central'] += q
                df.at[idx, 'status_compra'] = 'OK'
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                st.success("Ok!"); st.rerun()
    else: st.success("Nada pendente.")
