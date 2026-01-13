import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time
import re

# ==============================================================================
# ⚙️ CONFIGURAÇÃO E BLINDAGEM
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# Colunas que NÃO podem faltar de jeito nenhum
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
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_creds = json.loads(st.secrets["service_account_json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"☠️ ERRO CRÍTICO DE CONEXÃO: {e}")
        return None

# --- VACINA CONTRA ERRO DE PREÇO (3,19 vira 3.19) ---
def sanitizar_float(valor):
    """
    Remove R$, espaços e converte formato BR (vírgula) para formato Sistema (ponto).
    """
    if pd.isna(valor) or valor == "" or valor is None:
        return 0.0
    
    # Se já for número, retorna ele mesmo
    if isinstance(valor, (float, int)):
        return float(valor)
    
    s = str(valor).strip()
    s = s.replace("R$", "").replace("r$", "").strip()
    
    # Detecção inteligente: se tem vírgula, assume que é decimal
    if "," in s:
        if "." in s: # Caso 1.000,50
            if s.rfind(",") > s.rfind("."): 
                s = s.replace(".", "").replace(",", ".") # Formato BR
            else: 
                s = s.replace(",", "") # Formato US misto
        else:
            s = s.replace(",", ".") # Caso simples 3,19 -> 3.19
            
    # Remove qualquer lixo que sobrou
    s = re.sub(r'[^\d\.-]', '', s)
    
    try:
        return float(s)
    except:
        return 0.0

# --- GARANTIA DE COLUNAS ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']): df[col] = 0.0
            elif 'data' in col or 'validade' in col: df[col] = None
            else: df[col] = ""
    return df

# --- LEITURA BLINDADA (NÃO RETORNA VAZIO SE FALHAR) ---
@st.cache_data(ttl=5) # Cache curtíssimo para garantir dados frescos
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.2)
    client = get_google_client()
    if not client:
        st.stop() # PARA TUDO SE NÃO TIVER CONEXÃO
        
    try:
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: 
            # Se não existe a aba, cria. Isso é seguro.
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        df = garantir_integridade_colunas(df, colunas_padrao)
        
        # APLICA VACINA EM TUDO QUE É NÚMERO
        for col in df.columns:
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df[col] = df[col].apply(sanitizar_float)
            
            if 'data' in c_low or 'validade' in c_low:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"❌ Erro ao ler '{nome_aba}': {e}")
        st.stop() # TRAVA DE SEGURANÇA: Não deixa o app rodar com dados corrompidos

# --- SALVAMENTO SEGURO (LIMPA CACHE E FORMATA P/ BRASIL) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    client = get_google_client()
    if not client:
        st.error("Não foi possível salvar: Sem conexão.")
        return
        
    try:
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        
        ws.clear()
        
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        
        # Formata para salvar no Google Sheets de jeito que não confunda
        for col in df_save.columns:
            # Datas viram texto
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
            
            # Números: Tenta manter float puro para o GSheets formatar, ou string com vírgula se preferir visual
            c_low = col.lower()
            if any(x in c_low for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                # Estratégia Híbrida: Converte para float puro. O GSheets deve ser configurado para número.
                # Se der problema visual lá, troque por: df_save[col].apply(lambda x: str(x).replace('.', ','))
                df_save[col] = df_save[col].fillna(0.0)

        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        
        # LIMPEZA DE CACHE OBRIGATÓRIA
        ler_da_nuvem.clear()
        
    except Exception as e: 
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES AUXILIARES E LÓGICA
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
    for col in df.columns:
        if 'qtd' in col or 'preco' in col: df[col] = df[col].apply(sanitizar_float)

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
        if arquivo_subido.name.endswith('.csv'): df_temp = pd.read_csv(arquivo_subido)
        else: df_temp = pd.read_excel(arquivo_subido)
        
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
        ler_da_nuvem.clear() # Garante leitura fresca
        df_outra = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if not df_outra.empty:
            df_outra.columns = df_outra.columns.str.strip().str.lower()
            mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
            if mask.any():
                idx = df_outra[mask].index[0]
                df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    
    info_custom = root.find("Info")
    if info_custom is not None:
        try:
            forn = info_custom.find("Fornecedor").text
            num = info_custom.find("NumeroNota").text
            dt_s = info_custom.find("DataCompra").text
            hr_s = info_custom.find("HoraCompra").text
            data_final = datetime.strptime(f"{dt_s} {hr_s}", "%d/%m/%Y %H:%M:%S")
            dados_nota = {'numero': num, 'fornecedor': forn, 'data': data_final, 'itens': []}
        except: dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
    else:
        dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
        def tag_limpa(element): return element.tag.split('}')[-1]
        for elem in root.iter():
            tag = tag_limpa(elem)
            if tag == 'nNF': dados_nota['numero'] = elem.text
            elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO': dados_nota['fornecedor'] = elem.text

    itens_custom = root.findall(".//Item")
    if itens_custom:
        for it in itens_custom:
            try:
                nome = it.find("Nome").text
                qtd = sanitizar_float(it.find("Quantidade").text)
                valor = sanitizar_float(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
                desc = 0.0
                if it.find("ValorDesconto") is not None: desc = sanitizar_float(it.find("ValorDesconto").text)
                
                p_liq = valor / qtd if qtd > 0 else 0
                p_bruto = (valor + desc) / qtd if qtd > 0 else 0
                
                dados_nota['itens'].append({
                    'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(),
                    'preco_un_liquido': p_liq, 'preco_un_bruto': p_bruto, 'desconto_total_item': desc
                })
            except: continue
    else:
        # Fallback NFe Padrão
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
                        elif t == 'qCom': qCom = sanitizar_float(info.text)
                        elif t == 'vProd': vProd = sanitizar_float(info.text)
                        elif t == 'vDesc': vDesc = sanitizar_float(info.text)
                    if qCom > 0:
                        item['qtd'] = qCom; item['preco_un_bruto'] = vProd / qCom; item['desconto_total_item'] = vDesc; item['preco_un_liquido'] = (vProd - vDesc) / qCom
                    ean_xml = str(item['ean']).strip()
                    if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                        item['ean'] = item['codigo_interno']
                    dados_nota['itens'].append(item)
            except: continue
            
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
# 🚀 INTERFACE DO APP
# ==============================================================================

st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)
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
                df_pick = pd.read_excel(arquivo_pick)
                df_pick.columns = df_pick.columns.str.strip().str.lower()
                col_barras = next((c for c in df_pick.columns if 'barras' in c), None)
                col_qtd = next((c for c in df_pick.columns if 'transferir' in c), None)
                if not col_barras or not col_qtd: st.error("❌ Colunas 'Código de Barras' ou 'Transferir' não encontradas.")
                else:
                    if st.button("🚀 PROCESSAR TRANSFERÊNCIA"):
                        movidos = 0; bar = st.progress(0); log_movs = []; total_linhas = len(df_pick)
                        for i, row in df_pick.iterrows():
                            cod_pick = str(row[col_barras]).replace('.0', '').strip()
                            qtd_pick = sanitizar_float(row[col_qtd])
                            if qtd_pick > 0:
                                mask = df['código de barras'] == cod_pick
                                if mask.any():
                                    idx = df[mask].index[0]
                                    df.at[idx, 'qtd_central'] -= qtd_pick
                                    df.at[idx, 'qtd.estoque'] += qtd_pick
                                    log_movs.append({'data_hora': datetime.now(), 'produto': df.at[idx, 'nome do produto'], 'qtd_movida': qtd_pick})
                                    atualizar_casa_global(df.at[idx, 'nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    movidos += 1
                            bar.progress((i+1)/total_linhas)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        if log_movs:
                            df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV)
                        st.success(f"✅ {movidos} produtos transferidos!")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 LISTA DE COMPRAS
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        tab_lista, tab_add = st.tabs(["📋 Ver Lista Atual", "➕ Adicionar Itens"])
        with tab_lista:
            if not df_lista_compras.empty:
                if usar_modo_mobile:
                    for idx, row in df_lista_compras.iterrows():
                        with st.container(border=True):
                            st.write(f"**{row['produto']}**")
                            st.caption(f"Qtd: {int(row['qtd_sugerida'])} | Status: {row['status']}")
                else: st.dataframe(df_lista_compras, use_container_width=True)
                if st.button("🗑️ Limpar Lista"):
                    salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=COLS_LISTA), COLS_LISTA); st.success("Limpo!"); st.rerun()
            else: st.info("Lista vazia.")
        with tab_add:
            if st.button("🚀 Gerar pelo Estoque Baixo"):
                mask_baixo = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
                produtos_baixo = df[mask_baixo]
                if produtos_baixo.empty: st.success("Nada em falta.")
                else:
                    novos = []
                    for _, row in produtos_baixo.iterrows():
                        novos.append({'produto': row['nome do produto'], 'qtd_sugerida': row['qtd_minima'] * 3, 'fornecedor': row['ultimo_fornecedor'], 'custo_previsto': row['preco_custo'], 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'A Comprar'})
                    df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA); st.success("Gerado!"); st.rerun()

    # 2. CADASTRAR PRODUTO
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
            c3, c4, c5 = st.columns(3)
            with c3: ini_loja = st.number_input("Qtd Loja:", min_value=0)
            with c4: ini_casa = st.number_input("Qtd Casa:", min_value=0)
            with c5: ini_val = st.date_input("Validade:", value=None)
            if st.form_submit_button("💾 CADASTRAR"):
                if not novo_cod or not novo_nome: st.error("Código e Nome obrigatórios!")
                else:
                    novo = {'código de barras': str(novo_cod).strip(), 'nome do produto': novo_nome.upper().strip(), 'qtd.estoque': ini_loja, 'qtd_central': ini_casa, 'qtd_minima': novo_min, 'validade': pd.to_datetime(ini_val) if ini_val else None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': novo_custo, 'preco_venda': novo_venda, 'categoria': nova_cat, 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Cadastrado!"); st.rerun()

    # 2.5 IMPORTAR XML
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML da Nota Fiscal")
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota: {dados['numero']} | Forn: {dados['fornecedor']}")
                st.markdown("---")
                escolhas = {}
                lista_prods = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                for i, item in enumerate(dados['itens']):
                    c1, c2 = st.columns([1, 1])
                    with c1: st.write(f"📄 **{item['nome']}** (Qtd: {int(item['qtd'])}) | R$ {item['preco_un_liquido']:.2f}")
                    with c2:
                        match = "(CRIAR NOVO)"
                        if not df.empty:
                            m_ean = df[df['código de barras'] == item['ean']]
                            if not m_ean.empty: match = m_ean.iloc[0]['nome do produto']
                        escolhas[i] = st.selectbox(f"Vincular item {i+1}:", lista_prods, index=lista_prods.index(match) if match in lista_prods else 0, key=f"xml_{i}")
                    st.divider()
                if st.button("✅ CONFIRMAR IMPORTAÇÃO"):
                    for i, item in enumerate(dados['itens']):
                        prod_sys = escolhas[i]
                        qtd = int(item['qtd'])
                        custo = item['preco_un_liquido']
                        if prod_sys == "(CRIAR NOVO)":
                            novo = {'código de barras': item['ean'], 'nome do produto': item['nome'], 'qtd.estoque': 0, 'qtd_central': qtd, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': custo, 'preco_venda': custo*2, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']}
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            atualizar_casa_global(item['nome'], qtd, custo, None, None, prefixo)
                        else:
                            idx = df[df['nome do produto'] == prod_sys].index[0]
                            df.at[idx, 'qtd_central'] += qtd
                            df.at[idx, 'preco_custo'] = custo
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizar_casa_global(prod_sys, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Importado!"); st.balloons(); st.rerun()
            except Exception as e: st.error(f"Erro no XML: {e}")

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title("🔄 Sincronizar Planograma")
        st.info("⚠️ Este processo atualiza seu estoque com base na planilha enviada.")
        arquivo = st.file_uploader("📂 Subir Excel/CSV", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, header=None)
            else: df_raw = pd.read_excel(arquivo, header=None)
            st.dataframe(df_raw.head())
            cols = df_raw.columns.tolist()
            c1, c2, c3, c4 = st.columns(4)
            idx_barras = c1.selectbox("Cód. Barras", cols, 0)
            idx_nome = c2.selectbox("Nome", cols, 1)
            idx_qtd = c3.selectbox("Qtd", cols, 2)
            idx_preco = c4.selectbox("Preço (Opcional)", ["Ignorar"] + cols)
            
            if st.button("🚀 SINCRONIZAR AGORA"):
                # RECARREGA TUDO DA NUVEM PARA NÃO PERDER DADOS DE OUTRAS SESSÕES
                ler_da_nuvem.clear()
                df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
                
                novos = []
                bar = st.progress(0); total = len(df_raw)
                for i in range(1, total):
                    try:
                        cod = str(df_raw.iloc[i, idx_barras]).replace('.0', '').strip()
                        qtd = sanitizar_float(df_raw.iloc[i, idx_qtd])
                        nome = normalizar_texto(df_raw.iloc[i, idx_nome])
                        if cod:
                            mask = df['código de barras'] == cod
                            if mask.any():
                                df.loc[mask, 'qtd.estoque'] = qtd
                                if idx_preco != "Ignorar":
                                    p = sanitizar_float(df_raw.iloc[i, idx_preco])
                                    if p > 0: df.loc[mask, 'preco_venda'] = p
                            else:
                                pv = 0.0
                                if idx_preco != "Ignorar": pv = sanitizar_float(df_raw.iloc[i, idx_preco])
                                novos.append({'código de barras': cod, 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': pv, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0})
                    except: pass
                    bar.progress((i+1)/total)
                
                if novos: df = pd.concat([df, pd.DataFrame(novos)], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success("Sincronizado e Salvo!"); st.balloons()

    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title("📉 Baixar Vendas")
        arquivo_vendas = st.file_uploader("📂 Relatório Vendas", type=['xlsx', 'xls'])
        if arquivo_vendas:
            df_bruto = pd.read_excel(arquivo_vendas); st.dataframe(df_bruto.head())
            col_nome = st.selectbox("Coluna Nome", df_bruto.columns)
            col_qtd = st.selectbox("Coluna Qtd", df_bruto.columns)
            if st.button("🚀 PROCESSAR"):
                for i, row in df_bruto.iterrows():
                    nome = str(row[col_nome]).strip()
                    qtd = sanitizar_float(row[col_qtd])
                    mask = df['nome do produto'].str.contains(nome, case=False, na=False)
                    if mask.any():
                        idx = df[mask].index[0]
                        df.at[idx, 'qtd.estoque'] -= qtd
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success("Baixado!")

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title("🏠 Gôndola")
        termo = st.text_input("🔍 Buscar Produto:")
        df_show = filtrar_dados_inteligente(df, 'nome do produto', termo)
        for idx, row in df_show.iterrows():
            with st.container(border=True):
                st.subheader(row['nome do produto'])
                c1, c2 = st.columns(2)
                c1.metric("Loja", int(row['qtd.estoque']))
                c2.metric("Casa", int(row['qtd_central']))
                if row['qtd_central'] > 0:
                    with st.form(key=f"baixa_{idx}"):
                        q = st.number_input("Baixar Qtd:", 1, int(row['qtd_central']), key=f"q_{idx}")
                        if st.form_submit_button("⬇️ Baixar"):
                            df.at[idx, 'qtd.estoque'] += q
                            df.at[idx, 'qtd_central'] -= q
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                            st.rerun()

    # 6. FORNECEDOR
    elif modo == "🛒 Fornecedor (Compras)":
        st.title("🛒 Entrada Manual")
        item = st.selectbox("Produto", df['nome do produto'].unique())
        if item:
            idx = df[df['nome do produto'] == item].index[0]
            c1, c2 = st.columns(2)
            qtd = c1.number_input("Qtd Chegada", 0)
            custo = c2.number_input("Custo Unit (R$)", 0.0)
            if st.button("✅ DAR ENTRADA"):
                df.at[idx, 'qtd_central'] += qtd
                df.at[idx, 'preco_custo'] = custo
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                atualizar_casa_global(item, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                st.success("Estoque atualizado!")

    # 7. HISTÓRICO
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico")
        st.dataframe(df_hist, use_container_width=True)

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title("🏡 Estoque Casa")
        termo = st.text_input("🔍 Buscar:")
        df_show = filtrar_dados_inteligente(df, 'nome do produto', termo)
        for idx, row in df_show.iterrows():
            with st.container(border=True):
                st.write(f"**{row['nome do produto']}**")
                c1, c2 = st.columns(2)
                nq = c1.number_input("Qtd", value=int(row['qtd_central']), key=f"cq_{idx}")
                nc = c2.number_input("Custo", value=float(row['preco_custo']), key=f"cc_{idx}")
                if st.button("💾 Salvar", key=f"cbtn_{idx}"):
                    df.at[idx, 'qtd_central'] = nq
                    df.at[idx, 'preco_custo'] = nc
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    atualizar_casa_global(row['nome do produto'], nq, nc, None, None, prefixo)
                    st.success("Salvo!")

    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Tabela Geral (Correção Final)")
        st.info("Aqui você pode editar qualquer valor manualmente e salvar para corrigir a nuvem.")
        df_edit = st.data_editor(df, use_container_width=True, num_rows="dynamic")
        if st.button("💾 SALVAR ALTERAÇÕES GERAIS"):
            salvar_na_nuvem(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
            for idx, row in df_edit.iterrows():
                atualizar_casa_global(row['nome do produto'], row['qtd_central'], row['preco_custo'], row['preco_venda'], row['validade'], prefixo)
            st.success("Banco de dados atualizado e limpo!"); st.balloons()
