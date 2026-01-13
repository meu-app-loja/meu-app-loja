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

# --- DEFINIÇÃO DE COLUNAS OBRIGATÓRIAS (GLOBAL) ---
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

# --- FUNÇÃO DE LIMPEZA MATEMÁTICA ABSOLUTA V3 ---
def converter_numero_seguro(valor):
    if pd.isna(valor) or valor == "": return 0.0
    s_valor = str(valor).strip().replace('R$', '').replace('r$', '').strip()
    
    # Tenta conversão direta se já tiver ponto
    try:
        if '.' in s_valor and ',' not in s_valor: return float(s_valor)
    except: pass

    # Lógica Brasil (Vírgula para decimal)
    if ',' in s_valor:
        s_valor = s_valor.replace('.', '') # Remove milhar (1.000 -> 1000)
        s_valor = s_valor.replace(',', '.') # Virgula vira ponto
    
    try: return float(s_valor)
    except: return 0.0

# --- FUNÇÃO DE CURA ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total']): df[col] = 0.0
            elif 'data' in col or 'validade' in col: df[col] = None
            else: df[col] = ""
    return df

# --- LEITURA DA NUVEM ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.5) 
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
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df[col] = df[col].apply(converter_numero_seguro)
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except: return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        
        ws.clear()
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        
        for col in df_save.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                 df_save[col] = df_save[col].apply(converter_numero_seguro)
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
                
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES AUXILIARES
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
        df[col] = df[col].apply(converter_numero_seguro)
        
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
        if arquivo_subido.name.endswith('.csv'): df_temp = pd.read_csv(arquivo_subido, dtype=str)
        else: df_temp = pd.read_excel(arquivo_subido, dtype=str)
        
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
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

# --- FUNÇÃO ATUALIZAR CASA GLOBAL ---
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    time.sleep(1)
    
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        try:
            client = get_google_client()
            sh = client.open("loja_dados")
            ws_nome = f"{loja}_estoque"
            try: ws = sh.worksheet(ws_nome)
            except: continue 
            
            dados = ws.get_all_records()
            df_outra = pd.DataFrame(dados)
            
            if not df_outra.empty:
                df_outra = garantir_integridade_colunas(df_outra, COLUNAS_VITAIS)
                df_outra.columns = df_outra.columns.str.strip().str.lower()
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                
                if mask.any():
                    idx = df_outra[mask].index[0]
                    if qtd_nova_casa is not None: df_outra.at[idx, 'qtd_central'] = converter_numero_seguro(qtd_nova_casa)
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = converter_numero_seguro(novo_custo)
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = converter_numero_seguro(novo_venda)
                    if nova_validade is not None: 
                         df_outra.at[idx, 'validade'] = str(nova_validade).replace('NaT', '')
                    
                    ws.update([df_outra.columns.values.tolist()] + df_outra.values.tolist())
        except: pass 

# --- FUNÇÃO XML HÍBRIDA ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]
    
    info_custom = root.find("Info")
    if info_custom is not None:
        try:
            forn = info_custom.find("Fornecedor").text
            num = info_custom.find("NumeroNota").text
            dt_s = info_custom.find("DataCompra").text
            hr_s = info_custom.find("HoraCompra").text
            data_final = datetime.strptime(f"{dt_s} {hr_s}", "%d/%m/%Y %H:%M:%S")
            dados_nota = {'numero': num, 'fornecedor': forn, 'data': data_final, 'itens': []}
        except:
            dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
    else:
        dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
        for elem in root.iter():
            tag = tag_limpa(elem)
            if tag == 'nNF': dados_nota['numero'] = elem.text
            elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO': dados_nota['fornecedor'] = elem.text
    
    itens_custom = root.findall(".//Item")
    if itens_custom:
        for it in itens_custom:
            try:
                nome = it.find("Nome").text
                qtd = converter_numero_seguro(it.find("Quantidade").text)
                valor = converter_numero_seguro(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
                desc = 0.0
                if it.find("ValorDesconto") is not None:
                    desc = converter_numero_seguro(it.find("ValorDesconto").text)
                p_liq = valor / qtd if qtd > 0 else 0
                p_bruto = (valor + desc) / qtd if qtd > 0 else 0
                
                dados_nota['itens'].append({
                    'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(), 
                    'preco_un_liquido': p_liq, 'preco_un_bruto': p_bruto, 'desconto_total_item': desc
                })
            except: continue
    else:
        dets = [e for e in root.iter() if tag_limpa(e) == 'det']
        for det in dets:
            try:
                prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
                if prod:
                    item = {'codigo_interno': '', 'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
                    vProd = 0.0; vDesc = 0.0; qCom = 0.0
                    for info in prod:
                        t = tag_limpa(info)
                        if t == 'cProd': item['codigo_interno'] = info.text
                        elif t == 'cEAN': item['ean'] = info.text
                        elif t == 'xProd': item['nome'] = normalizar_texto(info.text)
                        elif t == 'qCom': qCom = converter_numero_seguro(info.text)
                        elif t == 'vProd': vProd = converter_numero_seguro(info.text)
                        elif t == 'vDesc': vDesc = converter_numero_seguro(info.text)
                    if qCom > 0:
                        item['qtd'] = qCom
                        item['preco_un_bruto'] = vProd / qCom
                        item['desconto_total_item'] = vDesc
                        item['preco_un_liquido'] = (vProd - vDesc) / qCom
                    ean_xml = str(item['ean']).strip()
                    if ean_xml in ['SEM GTIN', '', 'None', 'NAN']: item['ean'] = item['codigo_interno']
                    dados_nota['itens'].append(item)
            except: continue
            
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
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True, help="Melhora a visualização para iPhone/Android")
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
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty: st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo!")
            if not df_critico.empty: st.error("🚨 Produtos Vencendo!"); st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])
            
    # 1.5 PICKLIST
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        arquivo_pick = st.file_uploader("📂 Subir Picklist (.xlsx)", type=['xlsx', 'xls'])
        if arquivo_pick:
            try:
                df_pick = pd.read_excel(arquivo_pick, dtype=str)
                df_pick.columns = df_pick.columns.str.strip().str.lower()
                col_barras = next((c for c in df_pick.columns if 'barras' in c), None)
                col_qtd = next((c for c in df_pick.columns if 'transferir' in c), None)
                if not col_barras or not col_qtd: st.error("❌ Colunas obrigatórias não encontradas.")
                else:
                    if st.button("🚀 PROCESSAR TRANSFERÊNCIA"):
                        movidos = 0; bar = st.progress(0); log_movs = []; total = len(df_pick)
                        for i, row in df_pick.iterrows():
                            cod_pick = str(row[col_barras]).replace('.0', '').strip()
                            qtd_pick = converter_numero_seguro(row[col_qtd])
                            if qtd_pick > 0:
                                mask = df['código de barras'] == cod_pick
                                if mask.any():
                                    idx = df[mask].index[0]
                                    nome_prod = df.at[idx, 'nome do produto']
                                    df.at[idx, 'qtd_central'] -= qtd_pick
                                    df.at[idx, 'qtd.estoque'] += qtd_pick
                                    log_movs.append({'data_hora': datetime.now(), 'produto': nome_prod, 'qtd_movida': qtd_pick})
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    movidos += 1
                            bar.progress((i+1)/total)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        if log_movs:
                            df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV)
                        st.success(f"✅ {movidos} produtos transferidos!")
            except Exception as e: st.error(f"Erro: {e}")
            
    # 2.8 BASE OFICIAL
    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base de Produtos")
        arquivo_base = st.file_uploader("Suba o arquivo Excel/CSV aqui", type=['xlsx', 'csv'])
        if arquivo_base:
            if st.button("🚀 Processar e Salvar Base"):
                sucesso = processar_excel_oficial(arquivo_base)
                if sucesso: st.success("Base Oficial atualizada!"); st.rerun()
        st.divider()
        with st.expander("🚨 ÁREA DE PERIGO (LIMPEZA TOTAL)"):
            if st.button("🗑️ ZERAR BANCO DE DADOS COMPLETO"):
                client = get_google_client()
                sh = client.open("loja_dados")
                for aba in [f"{prefixo}_estoque", f"{prefixo}_historico_compras", f"{prefixo}_movimentacoes", f"{prefixo}_vendas", f"{prefixo}_lista_compras", "base_oficial"]:
                    try: sh.worksheet(aba).clear()
                    except: pass
                st.success("Tudo limpo!"); st.rerun()

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("📂 Arquivo Planograma (XLSX ou CSV)", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, header=None, dtype=str)
                else: df_raw = pd.read_excel(arquivo, header=None, dtype=str)
                
                st.write("### 1️⃣ Identifique as colunas:")
                cols = df_raw.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("CÓDIGO BARRAS", cols, index=0)
                idx_nome = c2.selectbox("NOME PRODUTO", cols, index=1 if len(cols)>1 else 0)
                idx_qtd = c3.selectbox("QUANTIDADE", cols, index=len(cols)-1)
                opcoes_preco = ["(Não Atualizar Preço)"] + cols
                idx_preco = c4.selectbox("PREÇO VENDA", opcoes_preco)
                
                st.divider()
                st.write("### 2️⃣ Pré-visualização (Verifique se os números estão certos)")
                
                dividir_100 = st.checkbox("⚠️ Os números entraram sem vírgula? (Ex: 11599 virou 11599.00)", value=False)
                
                df_preview = df_raw.head(5).copy()
                def tratar_preview(x):
                    val = converter_numero_seguro(x)
                    if dividir_100 and val > 0: return val / 100
                    return val
                
                df_preview['QTD_PREVIEW'] = df_preview[idx_qtd].apply(tratar_preview)
                if idx_preco != "(Não Atualizar Preço)":
                    df_preview['PRECO_PREVIEW'] = df_preview[idx_preco].apply(tratar_preview)
                
                st.dataframe(df_preview)
                
                if st.button("🚀 TUDO CERTO! SINCRONIZAR AGORA"):
                    df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
                    alt = 0; novos = 0; bar = st.progress(0); total_linhas = len(df_raw); novos_produtos = []
                    
                    for i, row in df_raw.iterrows():
                        try:
                            cod = str(row[idx_barras]).replace('.0', '').strip()
                            nome_planilha = str(row[idx_nome]).strip()
                            qtd = converter_numero_seguro(row[idx_qtd])
                            if dividir_100: qtd = qtd / 100 # Se for quantidade fracionada que veio errada
                            
                            nome_norm = normalizar_texto(nome_planilha)
                            
                            if cod and nome_norm:
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if idx_preco != "(Não Atualizar Preço)":
                                        val_preco = converter_numero_seguro(row[idx_preco])
                                        if dividir_100: val_preco = val_preco / 100
                                        if val_preco > 0: df.loc[mask, 'preco_venda'] = val_preco
                                    alt += 1
                                else:
                                    novo_preco_venda = 0.0
                                    if idx_preco != "(Não Atualizar Preço)":
                                        val_p = converter_numero_seguro(row[idx_preco])
                                        if dividir_100: val_p = val_p / 100
                                        if val_p > 0: novo_preco_venda = val_p
                                    novo_prod = {
                                        'código de barras': cod, 'nome do produto': nome_norm, 
                                        'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 
                                        'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 
                                        'preco_custo': 0.0, 'preco_venda': novo_preco_venda, 
                                        'categoria': 'GERAL', 'ultimo_fornecedor': '', 
                                        'preco_sem_desconto': 0.0
                                    }
                                    novos_produtos.append(novo_prod)
                                    novos += 1
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    
                    if novos_produtos:
                        df = pd.concat([df, pd.DataFrame(novos_produtos)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success(f"✅ Sucesso! {alt} atualizados e {novos} cadastrados."); st.balloons()
            except Exception as e: st.error(f"Erro: {e}")

    # 9. GERAL (COM BOTÃO SOS)
    elif modo == "📋 Tabela Geral":
        st.title("📋 Visão Geral (Editável)")
        if not df.empty:
            busca_geral = st.text_input("🔍 Buscar:", placeholder="Ex: oleo...", key="busca_geral")
            df_visual_geral = filtrar_dados_inteligente(df, 'nome do produto', busca_geral)
            df_edit = st.data_editor(df_visual_geral, use_container_width=True, num_rows="dynamic", key="geral_editor")
            
            c1, c2 = st.columns(2)
            with c1:
                if st.button("💾 SALVAR ALTERAÇÕES GERAIS"):
                    indices_originais = df_visual_geral.index.tolist()
                    indices_editados = df_edit.index.tolist()
                    indices_removidos = list(set(indices_originais) - set(indices_editados))
                    if indices_removidos: df = df.drop(indices_removidos)
                    df.update(df_edit)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    
                    bar = st.progress(0); total = len(df_edit)
                    for i, (idx, row) in enumerate(df_edit.iterrows()):
                        atualizar_casa_global(df.at[idx, 'nome do produto'], row['qtd_central'], row['preco_custo'], row['preco_venda'], row['validade'], prefixo)
                        bar.progress((i+1)/total)
                    st.success("Salvo!"); st.rerun()
            with c2:
                # BOTÃO SOS - CORREÇÃO DE DECIMAL
                if st.button("🆘 CORRIGIR PREÇOS (Dividir por 100)"):
                    count = 0
                    for idx, row in df.iterrows():
                        if row['preco_venda'] > 100: # Critério de segurança
                            df.at[idx, 'preco_venda'] = row['preco_venda'] / 100
                            count += 1
                        if row['preco_custo'] > 100:
                            df.at[idx, 'preco_custo'] = row['preco_custo'] / 100
                    
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.warning(f"Correção aplicada em {count} preços! Verifique a tabela."); st.rerun()

    # (MANTENHA OS OUTROS MENUS IGUAIS: 2, 4, 5, 6, 7, 8)
    # ELES JÁ ESTÃO FUNCIONAIS. O CÓDIGO ACIMA FOCA NA CORREÇÃO DO PLANOGRAMA.
    # REPLIQUE O CONTEÚDO DOS MENUS 2, 4, 5, 6, 7, 8 DA RESPOSTA ANTERIOR AQUI PARA COMPLETAR.
    # PARA O CÓDIGO FICAR COMPLETO NA RESPOSTA, VOU REPETIR OS MENUS ABAIXO:

    elif modo == "🆕 Cadastrar Produto":
        st.title(f"🆕 Cadastro - {loja_atual}")
        with st.form("form_cadastro"):
            c1, c2 = st.columns(2)
            with c1:
                novo_cod = st.text_input("Código de Barras:")
                novo_nome = st.text_input("Nome do Produto:")
                nova_cat = st.text_input("Categoria:")
            with c2:
                novo_custo = st.number_input("Preço Custo:", min_value=0.0, format="%.2f")
                novo_venda = st.number_input("Preço Venda:", min_value=0.0, format="%.2f")
                novo_min = st.number_input("Estoque Mínimo:", min_value=0, value=5)
            st.divider()
            c3, c4, c5 = st.columns(3)
            with c3: ini_loja = st.number_input("Qtd Loja:", min_value=0)
            with c4: ini_casa = st.number_input("Qtd Casa:", min_value=0)
            with c5: ini_val = st.date_input("Validade:", value=None)
            if st.form_submit_button("💾 CADASTRAR"):
                if not novo_cod or not novo_nome: st.error("Código e Nome obrigatórios!")
                elif not df.empty and df['código de barras'].astype(str).str.contains(str(novo_cod).strip()).any(): st.error("Código já existe!")
                else:
                    novo = {'código de barras': str(novo_cod).strip(), 'nome do produto': novo_nome.upper().strip(), 'qtd.estoque': ini_loja, 'qtd_central': ini_casa, 'qtd_minima': novo_min, 'validade': pd.to_datetime(ini_val) if ini_val else None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': novo_custo, 'preco_venda': novo_venda, 'categoria': nova_cat, 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success("Cadastrado!"); st.rerun()

    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Importar Vendas - {loja_atual}")
        tab_imp, tab_hist_vendas = st.tabs(["📂 Importar Arquivo", "📜 Histórico"])
        with tab_imp:
            arquivo_vendas = st.file_uploader("📂 Relatório de Vendas", type=['xlsx', 'xls'], key="up_vendas")
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None, dtype=str)
                    st.dataframe(df_bruto.head(5), use_container_width=True)
                    linha_titulo = st.number_input("Número da linha dos TÍTULOS:", min_value=0, max_value=10, value=0)
                    arquivo_vendas.seek(0)
                    df_vendas_temp = pd.read_excel(arquivo_vendas, header=linha_titulo, dtype=str)
                    cols = df_vendas_temp.columns.tolist()
                    c1, c2, c3 = st.columns(3)
                    col_nome = c1.selectbox("Coluna NOME?", cols)
                    col_qtd = c2.selectbox("Coluna QUANTIDADE?", cols)
                    col_data = c3.selectbox("Coluna DATA?", cols)
                    if st.button("🚀 PROCESSAR VENDAS"):
                        if not df.empty:
                            atualizados = 0; novos_registros = []; bar = st.progress(0); total = len(df_vendas_temp)
                            for i, row in df_vendas_temp.iterrows():
                                nome = str(row[col_nome]).strip()
                                qtd = converter_numero_seguro(row[col_qtd])
                                try:
                                    dt_v = pd.to_datetime(row[col_data], dayfirst=True)
                                    if pd.isna(dt_v): dt_v = datetime.now()
                                except: dt_v = datetime.now()
                                if pd.isna(qtd) or qtd <= 0: continue
                                mask = (df['código de barras'].astype(str).str.contains(nome, na=False) | df['nome do produto'].astype(str).str.contains(nome, case=False, na=False))
                                if mask.any():
                                    idx = df[mask].index[0]
                                    antigo = df.at[idx, 'qtd.estoque']
                                    df.at[idx, 'qtd.estoque'] = antigo - qtd
                                    atualizados += 1
                                    novos_registros.append({"data_hora": dt_v, "produto": df.at[idx, 'nome do produto'], "qtd_vendida": qtd, "estoque_restante": df.at[idx, 'qtd.estoque']})
                                bar.progress((i+1)/total)
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            if novos_registros:
                                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos_registros)], ignore_index=True)
                                salvar_na_nuvem(f"{prefixo}_vendas", df_vendas, COLS_VENDAS)
                            st.success(f"✅ {atualizados} vendas baixadas!")
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist_vendas:
            if not df_vendas.empty:
                busca_vendas_hist = st.text_input("🔍 Buscar no Histórico de Vendas:", placeholder="Ex: oleo...", key="busca_vendas_hist")
                df_v_show = filtrar_dados_inteligente(df_vendas, 'produto', busca_vendas_hist)
                if 'data_hora' in df_v_show.columns:
                    st.dataframe(df_v_show.sort_values(by="data_hora", ascending=False), use_container_width=True, hide_index=True)

    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        if df.empty: st.warning("Cadastre produtos.")
        else:
            if usar_modo_mobile:
                st.info("📱 Modo Celular Ativado")
                termo_busca = st.text_input("🔍 Buscar Produto (Nome ou Código):", placeholder="Digite aqui...")
                df_show = filtrar_dados_inteligente(df, 'nome do produto', termo_busca)
                if df_show.empty: st.warning("Nenhum produto encontrado.")
                else:
                    for idx, row in df_show.iterrows():
                        cor_borda = "grey"
                        if row['qtd.estoque'] <= 0: cor_borda = "red"
                        elif row['qtd.estoque'] < row['qtd_minima']: cor_borda = "orange"
                        with st.container(border=True):
                            st.subheader(row['nome do produto'])
                            c1, c2 = st.columns(2)
                            c1.metric("🏪 Loja", int(row['qtd.estoque']))
                            c2.metric("🏡 Casa", int(row['qtd_central']))
                            if row['qtd_central'] > 0:
                                with st.form(key=f"form_mob_{idx}"):
                                    col_in, col_btn = st.columns([2, 1])
                                    q_tr = col_in.number_input("Qtd para Baixar:", min_value=1, max_value=int(row['qtd_central']), key=f"n_{idx}", label_visibility="collapsed")
                                    if col_btn.form_submit_button("⬇️ Baixar"):
                                        df.at[idx, 'qtd.estoque'] += q_tr
                                        df.at[idx, 'qtd_central'] -= q_tr
                                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                        st.success(f"Baixado {q_tr} un!"); st.rerun()
                            else: st.warning("🚫 Casa Zerada (Sem estoque para baixar)")
            else:
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
                        if df.at[idx, 'qtd_central'] > 0:
                            st.subheader("🚚 Transferência (Casa -> Loja)")
                            with st.form("form_transf_gondola"):
                                c_dt, c_hr, c_qtd = st.columns(3)
                                dt_transf = c_dt.date_input("Data da Transferência:", datetime.today())
                                hr_transf = c_hr.time_input("Hora:", datetime.now().time())
                                qtd_transf = c_qtd.number_input(f"Quantidade (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']), value=0)
                                if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                    if qtd_transf > 0:
                                        df.at[idx, 'qtd.estoque'] += qtd_transf
                                        df.at[idx, 'qtd_central'] -= qtd_transf
                                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                        atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                        data_final = datetime.combine(dt_transf, hr_transf)
                                        novo_mov = {'data_hora': data_final, 'produto': nome_prod, 'qtd_movida': qtd_transf}
                                        df_mov = pd.concat([df_mov, pd.DataFrame([novo_mov])], ignore_index=True)
                                        salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV)
                                        st.success(f"Sucesso! {qtd_transf} unid. transferidas em {data_final}. Casa sincronizada."); st.rerun()
                                    else: st.info("Sem estoque na Casa para transferir.")
                        st.divider()
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
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                st.success("Atualizado em todo o sistema!"); st.rerun()
                with tab_hist:
                    if not df_mov.empty and 'data_hora' in df_mov.columns:
                        busca_gondola_hist = st.text_input("🔍 Buscar no Histórico de Gôndola:", placeholder="Ex: oleo...", key="busca_gondola_hist")
                        df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_gondola_hist)
                        if not df_mov_show.empty:
                            st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True, hide_index=True)
                    else: st.info("Sem histórico registrado.")

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
                    c_dt, c_hr = st.columns(2)
                    dt_compra = c_dt.date_input("Data da Compra:", datetime.today())
                    hr_compra = c_hr.time_input("Hora da Compra:", datetime.now().time())
                    forn_compra = st.text_input("Fornecedor desta compra:", value=df.at[idx, 'ultimo_fornecedor'])
                    c1, c2, c3 = st.columns(3)
                    qtd = c1.number_input("Qtd Chegada:", value=int(df.at[idx, 'qtd_comprada']))
                    custo = c2.number_input("Preço Pago (UN):", value=float(df.at[idx, 'preco_custo']), format="%.2f")
                    venda = c3.number_input("Novo Preço Venda:", value=float(df.at[idx, 'preco_venda']), format="%.2f")
                    if st.form_submit_button("✅ ENTRAR NO ESTOQUE"):
                        df.at[idx, 'qtd_central'] += qtd
                        df.at[idx, 'preco_custo'] = custo
                        df.at[idx, 'preco_venda'] = venda
                        df.at[idx, 'status_compra'] = 'OK'
                        df.at[idx, 'qtd_comprada'] = 0
                        df.at[idx, 'ultimo_fornecedor'] = forn_compra
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        atualizar_casa_global(item, df.at[idx, 'qtd_central'], custo, venda, None, prefixo)
                        dt_full = datetime.combine(dt_compra, hr_compra)
                        hist = {'data': dt_full, 'produto': item, 'fornecedor': forn_compra, 'qtd': qtd, 'preco_pago': custo, 'total_gasto': qtd*custo}
                        df_hist = pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                        st.success("Estoque atualizado e Casa sincronizada!"); st.rerun()
        else: st.success("Sem compras pendentes.")

    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico & Preços")
        if not df_hist.empty:
            busca_hist_precos = st.text_input("🔍 Buscar no Histórico de Compras:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
            df_hist_visual = df_hist
            if busca_hist_precos:
                df_hist_visual = filtrar_dados_inteligente(df_hist, 'produto', busca_hist_precos)
                if df_hist_visual.empty:
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
            st.info("✅ Você pode editar ou **excluir** linhas (selecione a linha e aperte Delete).")
            df_editado = st.data_editor(
                df_hist_visual.sort_values(by='data', ascending=False),
                use_container_width=True,
                key="editor_historico_geral",
                num_rows="dynamic",
                column_config={
                    "preco_sem_desconto": st.column_config.NumberColumn("Preço Tabela", format="R$ %.2f"),
                    "desconto_total_money": st.column_config.NumberColumn("Desconto TOTAL", format="R$ %.2f"),
                    "preco_pago": st.column_config.NumberColumn("Pago (Unit)", format="R$ %.2f", disabled=True),
                    "total_gasto": st.column_config.NumberColumn("Total Gasto", format="R$ %.2f", disabled=True)
                }
            )
            
            if st.button("💾 Salvar Alterações e Exclusões", use_container_width=True):
                indices_originais = df_hist_visual.index.tolist()
                indices_editados = df_editado.index.tolist()
                indices_removidos = list(set(indices_originais) - set(indices_editados))
                if indices_removidos:
                    df_hist = df_hist.drop(indices_removidos)
                    st.warning(f"🗑️ {len(indices_removidos)} registros excluídos permanentemente.")
                df_hist.update(df_editado)
                for idx, row in df_hist.iterrows():
                    try:
                        q = converter_numero_seguro(row.get('qtd', 0))
                        p_tab = converter_numero_seguro(row.get('preco_sem_desconto', 0))
                        d_tot = converter_numero_seguro(row.get('desconto_total_money', 0))
                        if q > 0 and p_tab > 0:
                            total_liq = (p_tab * q) - d_tot
                            df_hist.at[idx, 'preco_pago'] = total_liq / q
                            df_hist.at[idx, 'total_gasto'] = total_liq
                    except: pass
                salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                st.success("Histórico salvo com sucesso!"); st.rerun()
            
            st.markdown("---")
            with st.container(border=True):
                st.subheader("🛠️ ÁREA DE SINCRONIZAÇÃO GERAL")
                st.info("⚠️ **Atenção:** Isso sincronizará o sistema inteiro. O PREÇO e FORNECEDOR serão puxados do Histórico. A QUANTIDADE da Casa (Estoque Central) será unificada em todas as lojas.")
                
                if st.button("🔄 CLIQUE AQUI PARA ATUALIZAR TODO O SISTEMA COM ESTES DADOS", use_container_width=True, type="primary"):
                    if df.empty: st.warning("Tabela de estoque vazia.")
                    else:
                        with st.spinner("⏳ Conectando às lojas e baixando dados..."):
                            lojas_data = {}
                            todas_lojas = ["loja1", "loja2", "loja3"]
                            client = get_google_client()
                            sh = client.open("loja_dados")
                            for loja_x in todas_lojas:
                                try:
                                    ws_x = sh.worksheet(f"{loja_x}_estoque")
                                    d_x = pd.DataFrame(ws_x.get_all_records())
                                    if not d_x.empty:
                                        d_x = garantir_integridade_colunas(d_x, COLUNAS_VITAIS)
                                        d_x.columns = d_x.columns.str.strip().str.lower()
                                        lojas_data[loja_x] = d_x
                                except: pass
                        
                        mapa_historico = {} 
                        df_hist_sorted = df_hist.sort_values(by='data', ascending=True)
                        for _, row in df_hist_sorted.iterrows():
                            nm = str(row['produto']).strip()
                            pr = converter_numero_seguro(row['preco_pago'])
                            forn = str(row['fornecedor'])
                            if pr > 0: mapa_historico[nm] = {'custo': pr, 'forn': forn}
                        
                        mapa_estoque_mestre = {}
                        for _, row in df.iterrows():
                            nm = str(row['nome do produto']).strip()
                            qtd_c = converter_numero_seguro(row['qtd_central'])
                            mapa_estoque_mestre[nm] = qtd_c

                        with st.status("🛠️ Aplicando correções em memória...", expanded=True) as status:
                            for loja_nome, df_loja in lojas_data.items():
                                status.write(f"Processando {loja_nome}...")
                                alterou = False
                                for idx, row in df_loja.iterrows():
                                    nome_prod = str(row['nome do produto']).strip()
                                    if nome_prod in mapa_historico:
                                        dados_hist = mapa_historico[nome_prod]
                                        novo_custo = dados_hist['custo']
                                        novo_forn = dados_hist['forn']
                                        atual_custo = converter_numero_seguro(row['preco_custo'])
                                        atual_forn = str(row['ultimo_fornecedor'])
                                        if abs(atual_custo - novo_custo) > 0.001 or atual_forn != novo_forn:
                                            df_loja.at[idx, 'preco_custo'] = novo_custo
                                            df_loja.at[idx, 'ultimo_fornecedor'] = novo_forn
                                            alterou = True
                                    if nome_prod in mapa_estoque_mestre:
                                        nova_qtd_casa = mapa_estoque_mestre[nome_prod]
                                        atual_qtd_casa = converter_numero_seguro(row['qtd_central'])
                                        if abs(atual_qtd_casa - nova_qtd_casa) > 0.001:
                                            df_loja.at[idx, 'qtd_central'] = nova_qtd_casa
                                            alterou = True
                                if alterou:
                                    status.write(f"💾 Salvando alterações em {loja_nome}...")
                                    try:
                                        ws_save = sh.worksheet(f"{loja_nome}_estoque")
                                        for col in df_loja.columns:
                                            c_low = col.lower()
                                            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                                                 df_loja[col] = df_loja[col].apply(converter_numero_seguro)
                                            if pd.api.types.is_datetime64_any_dtype(df_loja[col]):
                                                df_loja[col] = df_loja[col].astype(str).replace('NaT', '')
                                        ws_save.update([df_loja.columns.values.tolist()] + df_loja.values.tolist())
                                    except Exception as e: st.error(f"Erro ao salvar {loja_nome}: {e}")
                            status.update(label="✅ Finalizado!", state="complete", expanded=False)
                        st.success(f"✅ Sincronizado com Sucesso!"); st.rerun()
        else: st.info("Sem histórico de compras.")

    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
        tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
        with tab_ver:
            if not df.empty:
                if usar_modo_mobile:
                    st.info("📱 Modo Celular (Edição Rápida)")
                    busca_central = st.text_input("🔍 Buscar na Casa:", placeholder="Ex: arroz...")
                    df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_central)
                    for idx, row in df_show.iterrows():
                        with st.container(border=True):
                            st.write(f"**{row['nome do produto']}**")
                            col1, col2 = st.columns(2)
                            nova_qtd = col1.number_input(f"Qtd Casa:", value=int(row['qtd_central']), key=f"q_{idx}")
                            novo_custo = col2.number_input(f"Custo:", value=float(row['preco_custo']), key=f"c_{idx}")
                            if st.button(f"💾 Salvar {row['nome do produto']}", key=f"btn_{idx}"):
                                df.at[idx, 'qtd_central'] = nova_qtd
                                df.at[idx, 'preco_custo'] = novo_custo
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(row['nome do produto'], nova_qtd, novo_custo, None, None, prefixo)
                                st.success("Salvo!"); st.rerun()
                else:
                    st.info("✏️ Se precisar corrigir o estoque, edite abaixo e clique em SALVAR.")
                    busca_central = st.text_input("🔍 Buscar Produto na Casa:", placeholder="Ex: oleo concordia...", key="busca_central")
                    colunas_visiveis = ['nome do produto', 'qtd_central', 'validade', 'preco_custo', 'ultimo_fornecedor']
                    df_visual = filtrar_dados_inteligente(df, 'nome do produto', busca_central)[colunas_visiveis]
                    df_editado = st.data_editor(df_visual, use_container_width=True, num_rows="dynamic", key="edit_casa")
                    if st.button("💾 SALVAR CORREÇÕES DA TABELA"):
                        indices_originais = df_visual.index.tolist()
                        indices_editados = df_editado.index.tolist()
                        indices_removidos = list(set(indices_originais) - set(indices_editados))
                        if indices_removidos:
                            df = df.drop(indices_removidos)
                            st.warning(f"{len(indices_removidos)} itens removidos permanentemente.")
                        df.update(df_editado)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        bar = st.progress(0); total = len(df_editado)
                        for i, (idx, row) in enumerate(df_editado.iterrows()):
                            atualizar_casa_global(df.at[idx, 'nome do produto'], row['qtd_central'], row['preco_custo'], None, row['validade'], prefixo)
                            bar.progress((i+1)/total)
                        st.success("Estoque atualizado e sincronizado em todas as lojas!"); st.rerun()
        with tab_gerenciar:
            st.info("Adicione mercadoria manualmente (ex: compra sem pedido) ou edite dados.")
            if not df.empty:
                lista_prods = sorted(df['nome do produto'].astype(str).unique().tolist())
                prod_opcao = st.selectbox("Selecione o Produto:", lista_prods)
                if prod_opcao:
                    mask = df['nome do produto'].astype(str) == str(prod_opcao)
                    if mask.any():
                        idx_prod = df[mask].index[0]
                        nome_atual = df.at[idx_prod, 'nome do produto']
                        val_atual = df.at[idx_prod, 'validade']
                        custo_atual = float(df.at[idx_prod, 'preco_custo'])
                        venda_atual = float(df.at[idx_prod, 'preco_venda'])
                        forn_atual = str(df.at[idx_prod, 'ultimo_fornecedor'])
                        with st.form("edit_estoque_casa_full"):
                            st.markdown(f"### Detalhes do Registro")
                            c_dt, c_hr = st.columns(2)
                            dt_reg = c_dt.date_input("Data da Entrada/Edição:", datetime.today())
                            hr_reg = c_hr.time_input("Hora:", datetime.now().time())
                            c_forn = st.text_input("Fornecedor desta entrada:", value=forn_atual)
                            st.markdown("---")
                            c_nome = st.text_input("Nome do Produto (Editável):", value=nome_atual)
                            c_val, c_custo, c_venda = st.columns(3)
                            nova_val = c_val.date_input("Validade:", value=val_atual if pd.notnull(val_atual) else None)
                            novo_custo = c_custo.number_input("Preço Custo (UN):", value=custo_atual, format="%.2f")
                            novo_venda = c_venda.number_input("Preço Venda (UN):", value=venda_atual, format="%.2f")
                            st.markdown("---")
                            c_qtd, c_acao = st.columns([1, 2])
                            qtd_input = c_qtd.number_input("Quantidade:", min_value=0, value=0)
                            acao = c_acao.radio("Ação sobre o estoque:", ["Somar (+) Entrada de Mercadoria", "Substituir (=) Correção de Estoque", "Apenas Salvar Dados (Sem mudar qtd)"], index=2)
                            if st.form_submit_button("💾 SALVAR REGISTRO COMPLETO"):
                                df.at[idx_prod, 'nome do produto'] = c_nome.upper().strip()
                                df.at[idx_prod, 'validade'] = pd.to_datetime(nova_val) if nova_val else None
                                df.at[idx_prod, 'preco_custo'] = novo_custo
                                df.at[idx_prod, 'preco_venda'] = novo_venda
                                if c_forn: df.at[idx_prod, 'ultimo_fornecedor'] = c_forn
                                msg_acao = "Dados atualizados"
                                if acao.startswith("Somar") and qtd_input > 0:
                                    df.at[idx_prod, 'qtd_central'] += qtd_input
                                    msg_acao = f"Adicionado +{qtd_input}"
                                    dt_full = datetime.combine(dt_reg, hr_reg)
                                    hist = {'data': dt_full, 'produto': c_nome.upper().strip(), 'fornecedor': c_forn, 'qtd': qtd_input, 'preco_pago': novo_custo, 'total_gasto': qtd_input * novo_custo}
                                    df_hist = pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input
                                    msg_acao = f"Estoque corrigido para {qtd_input}"
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(c_nome.upper().strip(), df.at[idx_prod, 'qtd_central'], novo_custo, novo_venda, pd.to_datetime(nova_val) if nova_val else None, prefixo)
                                st.success(f"✅ {msg_acao} e sincronizado com outras lojas!"); st.rerun()
