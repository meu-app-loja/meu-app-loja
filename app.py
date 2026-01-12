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
# ⚙️ CONFIGURAÇÃO INICIAL E CONEXÃO GOOGLE
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas Pro", layout="wide", page_icon="🏪")

# --- CONEXÃO COM O COFRE (SECRETS) ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# --- LEITURA INTELIGENTE (COM CACHE E PAUSA) ---
# TTL=60 significa que ele lembra dos dados por 60 segundos para não ir no Google toda hora
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1) # Respira para não travar o Google
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
            
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        
        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)
            
        # Garante números
        for col in df.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Garante datas
        for col in df.columns:
            if 'data' in col.lower() or 'validade' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        return df
    except Exception as e:
        # st.error(f"Erro ao ler {nome_aba}: {e}")
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAMENTO NA NUVEM ---
def salvar_na_nuvem(nome_aba, df):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        ws.clear()
        
        # Prepara para JSON (Datas viram texto)
        df_save = df.copy()
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
                
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        
        # Limpa memória para ver atualização imediata
        ler_da_nuvem.clear()
        
    except Exception as e:
        st.error(f"Erro ao salvar {nome_aba}: {e}")

# ==============================================================================
# 🧠 SUAS FUNÇÕES DE NEGÓCIO (MANTIDAS INTACTAS)
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto):
    if not isinstance(texto, str): return ""
    return normalizar_texto(texto)

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
        if score > maior_score:
            maior_score = score; melhor_match = opcao
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
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if not sem_codigo.empty: df_novo = pd.concat([df_novo, sem_codigo], ignore_index=True)
    return df_novo

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

# Função Especial: Atualiza as outras lojas (Sincronização)
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        # Lê a outra loja (com cache para ser rápido)
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

# --- FUNÇÃO XML ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
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
                item['qtd'] = qCom
                item['preco_un_bruto'] = vProd / qCom
                item['desconto_total_item'] = vDesc
                item['preco_un_liquido'] = (vProd - vDesc) / qCom
            
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
loja_atual = st.sidebar.selectbox("Gerenciar unidade:", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])

st.sidebar.markdown("---")
# AQUI ESTÁ A CORREÇÃO: VALUE=FALSE PARA COMEÇAR EM MODO DESKTOP
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=False, help="Marque se estiver no celular")
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# --- DEFINIÇÃO DAS COLUNAS (PARA CRIAR AS ABAS SE NÃO EXISTIREM) ---
COLS_ESTOQUE = ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto']
COLS_HIST = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

# --- CARREGAMENTO INICIAL OTIMIZADO (SÓ CARREGA O ESTOQUE) ---
df = ler_da_nuvem(f"{prefixo}_estoque", COLS_ESTOQUE)

if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))

st.sidebar.title("🏪 Menu")
modo = st.sidebar.radio("Navegar:", [
    "📊 Dashboard (Visão Geral)",
    "🚚 Transferência em Massa (Picklist)",
    "📝 Lista de Compras (Planejamento)",
    "🆕 Cadastrar Produto", 
    "📥 Importar XML (Associação Inteligente)", 
    "⚙️ Configurar Base Oficial",
    "🔄 Sincronizar (Planograma)",
    "📉 Baixar Vendas (Do Relatório)",
    "🏠 Gôndola (Loja)", 
    "🛒 Fornecedor (Compras)", 
    "💰 Histórico & Preços",
    "🏡 Estoque Central (Casa)",
    "📋 Tabela Geral"
])

# -----------------------------------------------------------------------------
# 1. DASHBOARD
# -----------------------------------------------------------------------------
if modo == "📊 Dashboard (Visão Geral)":
    st.title(f"📊 Painel de Controle - {loja_atual}")
    if df.empty:
        st.info("Comece cadastrando produtos ou importando uma planilha.")
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
        if not baixo.empty: st.warning(f"🚨 {len(baixo)} produtos com estoque baixo!")

# -----------------------------------------------------------------------------
# 2. IMPORTAR XML
# -----------------------------------------------------------------------------
elif modo == "📥 Importar XML (Associação Inteligente)":
    st.title(f"📥 Importar XML")
    # Só carrega a base oficial aqui
    df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)
    
    arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
    
    if arquivo_xml:
        try:
            dados = ler_xml_nfe(arquivo_xml, df_oficial)
            st.success(f"Nota: {dados['numero']} - {dados['fornecedor']}")
            
            lista_prods = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
            escolhas = {}
            
            for i, item in enumerate(dados['itens']):
                st.divider()
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"**XML:** {item['nome']} (x{int(item['qtd'])})")
                c1.caption(f"EAN: {item['ean']} | R$ {item['preco_un_liquido']:.2f}")
                
                match_ini = "(CRIAR NOVO)"
                if not df.empty:
                    mask = df['código de barras'].astype(str) == str(item['ean']).strip()
                    if mask.any(): match_ini = df.loc[mask, 'nome do produto'].values[0]
                    else:
                        melhor, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist())
                        if melhor: match_ini = melhor
                
                idx_ini = lista_prods.index(match_ini) if match_ini in lista_prods else 0
                escolhas[i] = c2.selectbox(f"Vincular item {i+1}:", lista_prods, index=idx_ini, key=f"s_{i}")
            
            if st.button("✅ CONFIRMAR E SALVAR"):
                for i, item in enumerate(dados['itens']):
                    esc = escolhas[i]
                    qtd = item['qtd']; custo = item['preco_un_liquido']
                    if esc == "(CRIAR NOVO)":
                        novo = {'código de barras': str(item['ean']).strip(), 'nome do produto': normalizar_texto(item['nome']), 'qtd.estoque': 0, 'qtd_central': qtd, 'qtd_minima': 5, 'preco_custo': custo, 'preco_venda': item['preco_un_bruto']*2, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']}
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        atualizar_casa_global(novo['nome do produto'], qtd, custo, None, None, prefixo)
                    else:
                        mask = df['nome do produto'] == esc
                        if mask.any():
                            idx = df[mask].index[0]
                            df.at[idx, 'qtd_central'] += qtd
                            df.at[idx, 'preco_custo'] = custo
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizar_casa_global(esc, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                st.success("Estoque Atualizado!")
                st.balloons()
                st.rerun()
        except Exception as e: st.error(f"Erro: {e}")

# -----------------------------------------------------------------------------
# 3. TRANSFERENCIA PICKLIST
# -----------------------------------------------------------------------------
elif modo == "🚚 Transferência em Massa (Picklist)":
    st.title("🚚 Transferência (Picklist)")
    arq_pick = st.file_uploader("Arquivo Excel", type=['xlsx'])
    if arq_pick:
        df_pick = pd.read_excel(arq_pick)
        # Lógica simplificada de transferência
        if st.button("PROCESSAR"):
            # Carrega mov apenas aqui
            df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", COLS_MOV)
            cols = df_pick.columns.str.lower()
            c_cod = next((c for c in cols if 'barras' in c), None)
            c_qtd = next((c for c in cols if 'transferir' in c), None)
            
            if c_cod and c_qtd:
                for i, row in df_pick.iterrows():
                    cod = str(row[c_cod]).replace('.0','').strip()
                    qtd = pd.to_numeric(row[c_qtd], errors='coerce')
                    if qtd > 0:
                        mask = df['código de barras'] == cod
                        if mask.any():
                            idx = df[mask].index[0]
                            df.at[idx, 'qtd_central'] -= qtd
                            df.at[idx, 'qtd.estoque'] += qtd
                            atualizar_casa_global(df.at[idx, 'nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                            # Log
                            log = {'data_hora': datetime.now(), 'produto': df.at[idx, 'nome do produto'], 'qtd_movida': qtd}
                            df_mov = pd.concat([df_mov, pd.DataFrame([log])], ignore_index=True)
                
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov)
                st.success("Transferência concluída!")
                st.rerun()

# -----------------------------------------------------------------------------
# 4. LISTA DE COMPRAS
# -----------------------------------------------------------------------------
elif modo == "📝 Lista de Compras (Planejamento)":
    st.title("📝 Lista de Compras")
    df_lista = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
    
    t1, t2 = st.tabs(["Ver Lista", "Adicionar"])
    with t1:
        if not df_lista.empty:
            st.dataframe(df_lista, use_container_width=True)
            if st.button("🗑️ Limpar Lista"):
                salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=COLS_LISTA))
                st.rerun()
        else: st.info("Lista vazia.")
        
    with t2:
        if st.button("Gerar pelo Mínimo"):
            baixo = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            novos = []
            for _, r in baixo.iterrows():
                novos.append({'produto': r['nome do produto'], 'qtd_sugerida': r['qtd_minima']*3, 'fornecedor': r['ultimo_fornecedor'], 'custo_previsto': r['preco_custo'], 'data_inclusao': str(datetime.now().date()), 'status': 'A Comprar'})
            if novos:
                df_lista = pd.concat([df_lista, pd.DataFrame(novos)], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista)
                st.success("Lista Gerada!")
                st.rerun()

# -----------------------------------------------------------------------------
# 5. CADASTRAR E SINCRONIZAR
# -----------------------------------------------------------------------------
elif modo == "🆕 Cadastrar Produto":
    st.title("🆕 Novo Produto")
    with st.form("new"):
        c = st.text_input("Código"); n = st.text_input("Nome"); p = st.number_input("Preço Venda")
        if st.form_submit_button("Salvar"):
            novo = {'código de barras': c, 'nome do produto': normalizar_texto(n), 'qtd.estoque': 0, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': p, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            st.success("Cadastrado!"); st.rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sincronizar Excel")
    f = st.file_uploader("Excel", type=['xlsx', 'csv'])
    if f:
        df_raw = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f)
        c1, c2, c3 = st.columns(3)
        ic = c1.selectbox("Código", df_raw.columns, 0)
        inm = c2.selectbox("Nome", df_raw.columns, 1)
        iq = c3.selectbox("Qtd", df_raw.columns, len(df_raw.columns)-1)
        if st.button("Sincronizar"):
            bar = st.progress(0); tot=len(df_raw)
            for i, r in df_raw.iterrows():
                c = str(r[ic]).replace('.0','').strip(); nm = normalizar_texto(str(r[inm])); q = pd.to_numeric(r[iq], errors='coerce')
                mask = df['código de barras'] == c
                if mask.any(): df.loc[mask, 'qtd.estoque'] = q
                else: 
                    novo = {'código de barras': c, 'nome do produto': nm, 'qtd.estoque': q, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': 0, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            st.success("Feito!"); st.rerun()

# -----------------------------------------------------------------------------
# 6. GONDOLA E CASA
# -----------------------------------------------------------------------------
elif modo == "🏠 Gôndola (Loja)":
    st.title("🏠 Gôndola")
    b = st.text_input("Buscar:", placeholder="Nome ou EAN...")
    if b:
        r = filtrar_dados_inteligente(df, 'nome do produto', b)
        if r.empty: r = df[df['código de barras'].str.contains(b, na=False)]
        
        for idx, row in r.iterrows():
            with st.container(border=True):
                st.write(f"**{row['nome do produto']}**")
                c1, c2 = st.columns(2)
                c1.info(f"Loja: {int(row['qtd.estoque'])}")
                c2.success(f"Casa: {int(row['qtd_central'])}")
                
                col_a, col_b = st.columns(2)
                bx = col_a.number_input(f"Baixar da Casa:", min_value=1, max_value=int(row['qtd_central']) if row['qtd_central']>0 else 1, key=f"bx_{idx}")
                if col_a.button("⬇️ Baixar", key=f"btn_{idx}"):
                    if row['qtd_central'] >= bx:
                        df.at[idx, 'qtd.estoque'] += bx; df.at[idx, 'qtd_central'] -= bx
                        salvar_na_nuvem(f"{prefixo}_estoque", df)
                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                        st.success("Transferido!"); st.rerun()
                    else: st.error("Sem saldo.")

elif modo == "🏡 Estoque Central (Casa)":
    st.title("🏡 Casa")
    b = st.text_input("Buscar na Casa:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', b)
    df_ed = st.data_editor(df_show[['nome do produto', 'qtd_central', 'preco_custo', 'validade']], use_container_width=True, key="ed_casa")
    if st.button("Salvar Casa"):
        df.update(df_ed)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        # Sync
        bar = st.progress(0); tot=len(df_ed)
        for i, (idx, row) in enumerate(df_ed.iterrows()):
            atualizar_casa_global(df.at[idx, 'nome do produto'], row['qtd_central'], row['preco_custo'], None, row['validade'], prefixo)
            bar.progress((i+1)/tot)
        st.success("Salvo!"); st.rerun()

# -----------------------------------------------------------------------------
# 7. HISTÓRICO, VENDAS E TABELA GERAL
# -----------------------------------------------------------------------------
elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico")
    df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)
    b = st.text_input("Filtrar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df_hist, 'produto', b)
    if not df_show.empty:
        df_ed = st.data_editor(df_show.sort_values('data', ascending=False), use_container_width=True, num_rows="dynamic")
        if st.button("Salvar Histórico"):
            salvar_na_nuvem(f"{prefixo}_historico_compras", df_ed); st.success("Ok!"); st.rerun()

elif modo == "📉 Baixar Vendas (Do Relatório)":
    st.title("📉 Baixar Vendas")
    df_vendas = ler_da_nuvem(f"{prefixo}_vendas", COLS_VENDAS)
    f = st.file_uploader("Relatório", type=['xlsx'])
    if f:
        raw = pd.read_excel(f)
        c1, c2 = st.columns(2)
        inm = c1.selectbox("Nome", raw.columns); iq = c2.selectbox("Qtd", raw.columns)
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

elif modo == "📋 Tabela Geral":
    st.title("📋 Tabela Geral")
    b = st.text_input("Filtrar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', b)
    df_ed = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
    c1, c2 = st.columns(2)
    if c1.button("Salvar Tudo"):
        df.update(df_ed)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        st.success("Salvo!"); st.rerun()
    if c2.button("Unificar Duplicados"):
        df = unificar_produtos_por_codigo(df)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        st.success("Unificado!"); st.rerun()
