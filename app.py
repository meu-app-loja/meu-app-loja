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
# ⚙️ CONFIGURAÇÃO DE NUVEM & SISTEMA
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

# --- FUNÇÃO DE LIMPEZA E CONVERSÃO DE NÚMEROS (3,19) ---
def converter_ptbr(valor):
    """Converte valores brasileiros para float de forma robusta."""
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    # Remove R$, espaços e converte para string limpa
    s = str(valor).strip().upper().replace('R$', '').replace(' ', '')
    try:
        # Se tem ponto e vírgula (ex: 1.000,00) -> remove ponto, troca virgula por ponto
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        # Se só tem vírgula (ex: 3,19) -> troca virgula por ponto
        elif ',' in s:
            s = s.replace(',', '.')
        return float(s)
    except:
        return 0.0

# --- FUNÇÃO VISUAL ---
def format_br(valor):
    try:
        return f"{float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "0,00"

# --- FUNÇÃO DE CURA ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total']): 
                df[col] = 0.0
            elif 'data' in col or 'validade' in col: 
                df[col] = None
            else: 
                df[col] = ""
    # Aplica conversor em colunas numéricas
    for col in df.columns:
        if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
            df[col] = df[col].apply(converter_ptbr)
    return df

# --- LEITURA DA NUVEM ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1)
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
        
        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)

        df = garantir_integridade_colunas(df, colunas_padrao)
        for col in df.columns:
            if 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e: 
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (COM TRAVA DE SEGURANÇA) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    # TRAVA: Não deixa salvar se a lista estiver vazia e for de estoque
    if df.empty and ("estoque" in nome_aba or "historico" in nome_aba):
        st.error("⚠️ SEGURANÇA ATIVADA: O sistema impediu que os dados fossem apagados por um erro de leitura. Tente recarregar a página.")
        return

    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: ws = sh.worksheet(nome_aba)
        except: ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        
        ws.clear()
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)
        
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime('%Y-%m-%d').replace('NaT', '')
                
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 FUNÇÕES LÓGICAS
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
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
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

# --- FUNÇÃO XML HÍBRIDA ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]
    
    info_custom = root.find("Info")
    if info_custom is not None:
        try:
            forn = info_custom.find("Fornecedor").text; num = info_custom.find("NumeroNota").text
            dt_s = info_custom.find("DataCompra").text; hr_s = info_custom.find("HoraCompra").text
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
                qtd = converter_ptbr(it.find("Quantidade").text)
                valor = converter_ptbr(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
                desc = 0.0
                if it.find("ValorDesconto") is not None: desc = converter_ptbr(it.find("ValorDesconto").text)
                p_liq = valor / qtd if qtd > 0 else 0
                p_bruto = (valor + desc) / qtd if qtd > 0 else 0
                dados_nota['itens'].append({'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(), 'preco_un_liquido': p_liq, 'preco_un_bruto': p_bruto, 'desconto_total_item': desc})
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
                        elif t == 'qCom': qCom = converter_ptbr(info.text)
                        elif t == 'vProd': vProd = converter_ptbr(info.text)
                        elif t == 'vDesc': vDesc = converter_ptbr(info.text)
                    if qCom > 0:
                        item['qtd'] = qCom; item['preco_un_bruto'] = vProd / qCom; item['desconto_total_item'] = vDesc; item['preco_un_liquido'] = (vProd - vDesc) / qCom
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
            c2.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
            
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty: st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras' para ver.")
            if not df_critico.empty: st.error("🚨 Produtos Vencendo!"); st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])
            
    # 1.5 PICKLIST (MANTIDO IGUAL)
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        st.markdown("**Sistema Shoppbud/Transferência:** Suba o Excel para mover estoque da Casa para a Loja.")
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
                        movidos = 0; erros = 0; bar = st.progress(0); log_movs = []; total_linhas = len(df_pick)
                        for i, row in df_pick.iterrows():
                            cod_pick = str(row[col_barras]).replace('.0', '').strip()
                            qtd_pick = converter_ptbr(row[col_qtd])
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
                                else: erros += 1
                            bar.progress((i+1)/total_linhas)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        if log_movs:
                            df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_movimentacoes", df_mov, COLS_MOV)
                        st.success(f"✅ {movidos} produtos transferidos!"); 
                        if erros > 0: st.warning(f"⚠️ {erros} não encontrados.")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 LISTA DE COMPRAS (MANTIDO IGUAL)
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        tab_lista, tab_add = st.tabs(["📋 Ver Lista Atual", "➕ Adicionar Itens"])
        with tab_lista:
            if not df_lista_compras.empty:
                st.info("💡 Esta é sua lista de compras.")
                if usar_modo_mobile:
                    for idx, row in df_lista_compras.iterrows():
                        with st.container(border=True):
                            st.write(f"**{row['produto']}**")
                            c1, c2 = st.columns(2)
                            c1.caption(f"Qtd: {int(row['qtd_sugerida'])}")
                            c2.caption(f"Status: {row['status']}")
                else: st.dataframe(df_lista_compras, use_container_width=True)
                if st.button("🗑️ Limpar Lista Inteira (Após Comprar)"):
                    salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=COLS_LISTA), COLS_LISTA); st.success("Lista limpa!"); st.rerun()
            else: st.info("Sua lista de compras está vazia.")
        with tab_add:
            st.subheader("🤖 Gerador Automático")
            if st.button("🚀 Gerar Lista Baseada no Estoque Baixo"):
                if df.empty: st.warning("Sem produtos cadastrados.")
                else:
                    mask_baixo = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
                    produtos_baixo = df[mask_baixo]
                    if produtos_baixo.empty: st.success("Tudo certo! Nada abaixo do mínimo.")
                    else:
                        novos_itens = []
                        for _, row in produtos_baixo.iterrows():
                            ja_na_lista = False
                            if not df_lista_compras.empty:
                                ja_na_lista = df_lista_compras['produto'].astype(str).str.contains(row['nome do produto'], regex=False).any()
                            if not ja_na_lista:
                                novos_itens.append({'produto': row['nome do produto'], 'qtd_sugerida': row['qtd_minima'] * 3, 'fornecedor': row['ultimo_fornecedor'], 'custo_previsto': row['preco_custo'], 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'A Comprar'})
                        if novos_itens:
                            df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos_itens)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA)
                            st.success(f"{len(novos_itens)} itens adicionados!"); st.rerun()
                        else: st.warning("Itens já estão na lista.")
            st.divider()
            st.subheader("✋ Adicionar Manualmente")
            with st.form("add_manual_lista"):
                lista_prods = [""] + sorted(df['nome do produto'].astype(str).unique().tolist())
                prod_man = st.selectbox("Produto:", lista_prods)
                qtd_man = st.number_input("Qtd a Comprar:", min_value=1, value=10)
                obs_man = st.text_input("Fornecedor/Obs:", placeholder="Ex: Atacadão")
                if st.form_submit_button("Adicionar à Lista"):
                    if prod_man:
                        preco_ref = 0.0
                        mask = df['nome do produto'] == prod_man
                        if mask.any(): preco_ref = df.loc[mask, 'preco_custo'].values[0]
                        novo_item = {'produto': prod_man, 'qtd_sugerida': qtd_man, 'fornecedor': obs_man, 'custo_previsto': preco_ref, 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'Manual'}
                        df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame([novo_item])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA); st.success("Adicionado!"); st.rerun()
                    else: st.error("Selecione um produto.")

    # 2. CADASTRAR PRODUTO (MANTIDO IGUAL)
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

    # 2.5 IMPORTAR XML
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML da Nota Fiscal")
        st.markdown("O sistema tentará encontrar os produtos. **Confirme se o vínculo está correto antes de salvar.**")
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota Fiscal: **{dados['numero']}** | Fornecedor: **{dados['fornecedor']}**")
                st.markdown("---"); st.subheader("🛠️ Conferência e Cálculo de Descontos")
                lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                escolhas = {}
                for i, item in enumerate(dados['itens']):
                    ean_xml = str(item.get('ean', '')).strip()
                    nome_xml = str(item['nome']).strip()
                    qtd_xml = item['qtd']
                    p_bruto = item['preco_un_bruto']; p_liq = item['preco_un_liquido']; desc_total = item.get('desconto_total_item', 0)
                    match_inicial = "(CRIAR NOVO)"; tipo_match = "Nenhum"; ean_sistema = ""
                    if not df.empty:
                        mask_ean = df['código de barras'].astype(str) == ean_xml
                        if mask_ean.any():
                            match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                            ean_sistema = df.loc[mask_ean, 'código de barras'].values[0]
                            tipo_match = "Código de Barras (Exato)"
                        else:
                            melhor_nome, tipo_encontrado = encontrar_melhor_match(nome_xml, df['nome do produto'].astype(str).tolist())
                            if melhor_nome:
                                match_inicial = melhor_nome; tipo_match = tipo_encontrado
                                mask_nome = df['nome do produto'].astype(str) == match_inicial
                                if mask_nome.any(): ean_sistema = df.loc[mask_nome, 'código de barras'].values[0]
                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.markdown(f"📄 XML: **{nome_xml}**")
                        st.caption(f"EAN XML: `{ean_xml}` | Qtd: {int(qtd_xml)}")
                        st.markdown(f"💰 Tabela: R$ {format_br(p_bruto)} | **Pago (Desc): R$ {format_br(p_liq)}**")
                        if desc_total > 0: st.caption(f"📉 Desconto Total na nota: {format_br(desc_total)}")
                    with c2:
                        idx_inicial = lista_produtos_sistema.index(str(match_inicial)) if str(match_inicial) in lista_produtos_sistema else 0
                        escolha_usuario = st.selectbox(f"Vincular ao Sistema ({tipo_match}):", lista_produtos_sistema, index=idx_inicial, key=f"sel_{i}")
                        if escolha_usuario != "(CRIAR NOVO)": st.info(f"🆔 Sistema: {escolha_usuario}")
                        escolhas[i] = escolha_usuario
                    st.divider()
                
                if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
                    novos_hist = []; criados_cont = 0; atualizados_cont = 0
                    for i, item in enumerate(dados['itens']):
                        produto_escolhido = escolhas[i]
                        qtd_xml = int(item['qtd']); preco_pago = item['preco_un_liquido']; preco_sem_desc = item['preco_un_bruto']; desc_total_val = item.get('desconto_total_item', 0)
                        ean_xml = str(item.get('ean', '')).strip(); nome_xml = str(item['nome']).strip()
                        nome_final = ""; qtd_central_final = 0
                        if produto_escolhido == "(CRIAR NOVO)":
                            novo_prod = {'código de barras': ean_xml, 'nome do produto': nome_xml.upper(), 'qtd.estoque': 0, 'qtd_central': qtd_xml, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': preco_pago, 'preco_venda': preco_pago * 2, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': preco_sem_desc}
                            df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                            criados_cont += 1; nome_final = nome_xml.upper(); qtd_central_final = qtd_xml
                        else:
                            mask = df['nome do produto'].astype(str) == str(produto_escolhido)
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] += qtd_xml
                                df.at[idx, 'preco_custo'] = preco_pago
                                df.at[idx, 'preco_sem_desconto'] = preco_sem_desc
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1
                                nome_final = produto_escolhido; qtd_central_final = df.at[idx, 'qtd_central']
                        
                        # Atualiza global
                        atualizar_casa_global(nome_final, qtd_central_final, preco_pago, None, None, prefixo)
                        novos_hist.append({'data': dados['data'], 'produto': nome_final, 'fornecedor': dados['fornecedor'], 'qtd': qtd_xml, 'preco_pago': preco_pago, 'total_gasto': qtd_xml * preco_pago, 'numero_nota': dados['numero'], 'desconto_total_money': desc_total_val, 'preco_sem_desconto': preco_sem_desc})
                    
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                    st.success(f"✅ Processado! {criados_cont} novos, {atualizados_cont} atualizados e CASA sincronizada.")
                    st.balloons(); st.rerun()
            except Exception as e: st.error(f"Erro ao ler XML: {e}")

    # 2.8 BASE OFICIAL
    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base de Produtos Oficial")
        arquivo_base = st.file_uploader("Suba o arquivo Excel/CSV aqui", type=['xlsx', 'csv'])
        if arquivo_base:
            if st.button("🚀 Processar e Salvar Base"):
                sucesso = processar_excel_oficial(arquivo_base)
                if sucesso: st.success("Base Oficial atualizada!"); st.rerun()

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("📂 Arquivo Planograma", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, header=None)
                else: df_raw = pd.read_excel(arquivo, header=None)
                cols = df_raw.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("Coluna CÓDIGO", cols, index=0)
                idx_nome = c2.selectbox("Coluna NOME", cols, index=1 if len(cols)>1 else 0)
                idx_qtd = c3.selectbox("Coluna QTD", cols, index=len(cols)-1)
                idx_preco = c4.selectbox("Coluna PREÇO", ["(Não Atualizar)"] + cols)
                if st.button("🚀 SINCRONIZAR"):
                    df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
                    alt = 0; novos = 0; total_linhas = len(df_raw); novos_produtos = []; bar = st.progress(0)
                    for i in range(1, total_linhas):
                        try:
                            cod = str(df_raw.iloc[i, idx_barras]).replace('.0', '').strip()
                            nome = str(df_raw.iloc[i, idx_nome]).strip(); qtd = converter_ptbr(df_raw.iloc[i, idx_qtd])
                            if cod and nome:
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if idx_preco != "(Não Atualizar)": 
                                        vp = converter_ptbr(df_raw.iloc[i, idx_preco])
                                        if vp > 0: df.loc[mask, 'preco_venda'] = vp
                                    alt += 1
                                else:
                                    vp = 0.0
                                    if idx_preco != "(Não Atualizar)": vp = converter_ptbr(df_raw.iloc[i, idx_preco])
                                    novos_produtos.append({'código de barras': cod, 'nome do produto': normalizar_texto(nome), 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0.0, 'preco_venda': vp, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0})
                                    novos += 1
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    if novos_produtos: df = pd.concat([df, pd.DataFrame(novos_produtos)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success(f"✅ {alt} atualizados, {novos} novos."); st.balloons()
            except Exception as e: st.error(f"Erro: {e}")

    # 4. BAIXAR VENDAS (MANTIDO IGUAL)
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title("📉 Baixar Vendas")
        tab_imp, tab_hist_vendas = st.tabs(["📂 Importar", "📜 Histórico"])
        with tab_imp:
            arquivo_vendas = st.file_uploader("📂 Relatório", type=['xlsx', 'xls'], key="up_vendas")
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None)
                    st.dataframe(df_bruto.head(5))
                    linha_titulo = st.number_input("Linha TÍTULOS:", min_value=0, value=0)
                    arquivo_vendas.seek(0)
                    df_vendas_temp = pd.read_excel(arquivo_vendas, header=linha_titulo)
                    cols = df_vendas_temp.columns.tolist()
                    c1, c2, c3 = st.columns(3)
                    col_nome = c1.selectbox("NOME?", cols)
                    col_qtd = c2.selectbox("QTD?", cols)
                    col_data = c3.selectbox("DATA?", cols)
                    if st.button("🚀 PROCESSAR"):
                        if not df.empty:
                            atualizados = 0; novos_registros = []
                            for i, row in df_vendas_temp.iterrows():
                                nm = str(row[col_nome]).strip(); qtd = converter_ptbr(row[col_qtd])
                                try: dt_v = pd.to_datetime(row[col_data], dayfirst=True)
                                except: dt_v = datetime.now()
                                if pd.isna(qtd) or qtd <= 0: continue
                                mask = (df['código de barras'].astype(str).str.contains(nm, na=False) | df['nome do produto'].astype(str).str.contains(nm, case=False, na=False))
                                if mask.any():
                                    idx = df[mask].index[0]; df.at[idx, 'qtd.estoque'] -= qtd; atualizados += 1
                                    novos_registros.append({"data_hora": dt_v, "produto": df.at[idx, 'nome do produto'], "qtd_vendida": qtd, "estoque_restante": df.at[idx, 'qtd.estoque']})
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            if novos_registros:
                                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos_registros)], ignore_index=True)
                                salvar_na_nuvem(f"{prefixo}_vendas", df_vendas, COLS_VENDAS)
                            st.success(f"✅ {atualizados} vendas baixadas!")
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist_vendas:
            if not df_vendas.empty: st.dataframe(df_vendas.sort_values(by="data_hora", ascending=False))

    # 5. GÔNDOLA (MANTIDO IGUAL)
    elif modo == "🏠 Gôndola (Loja)":
        st.title("🏠 Gôndola")
        termo = st.text_input("🔍 Buscar:")
        df_show = filtrar_dados_inteligente(df, 'nome do produto', termo)
        for idx, row in df_show.iterrows():
            with st.container(border=True):
                st.subheader(row['nome do produto'])
                c1, c2 = st.columns(2)
                c1.metric("Loja", int(row['qtd.estoque'])); c2.metric("Casa", int(row['qtd_central']))
                if row['qtd_central'] > 0:
                    with st.form(f"g_{idx}"):
                        q = st.number_input("Baixar:", min_value=1, max_value=int(row['qtd_central']), key=f"q_{idx}")
                        if st.form_submit_button("⬇️ Mover"):
                            df.at[idx, 'qtd.estoque'] += q; df.at[idx, 'qtd_central'] -= q
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                            st.rerun()

    # 6. FORNECEDOR (MANTIDO IGUAL)
    elif modo == "🛒 Fornecedor (Compras)":
        st.title("🛒 Compras Pendentes")
        pen = df[df['status_compra'] == 'PENDENTE']
        if not pen.empty:
            item = st.selectbox("Item:", pen['nome do produto'])
            if item:
                idx = df[df['nome do produto'] == item].index[0]
                with st.form("c"):
                    qtd = st.number_input("Qtd:", value=int(df.at[idx, 'qtd_comprada']))
                    custo = st.number_input("Custo:", value=float(df.at[idx, 'preco_custo']))
                    if st.form_submit_button("✅ Aceitar"):
                        df.at[idx, 'qtd_central'] += qtd; df.at[idx, 'preco_custo'] = custo; df.at[idx, 'status_compra'] = 'OK'; df.at[idx, 'qtd_comprada'] = 0
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        atualizar_casa_global(item, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                        st.success("Feito!"); st.rerun()
        else: st.success("Nada pendente.")

    # 7. HISTÓRICO & PREÇOS (NOVO BOTÃO DE SINCRONIZAÇÃO)
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico")
        
        # --- NOVO BOTÃO DE SINCRONIZAÇÃO ---
        if st.button("🔄 FORÇAR ATUALIZAÇÃO DO ESTOQUE COM DADOS DAQUI"):
            if not df_hist.empty:
                for idx, row in df_hist.iterrows():
                    # Procura o produto no estoque principal
                    mask = df['nome do produto'] == row['produto']
                    if mask.any():
                        idx_est = df[mask].index[0]
                        # Atualiza preço e fornecedor
                        df.at[idx_est, 'preco_custo'] = row['preco_pago']
                        df.at[idx_est, 'ultimo_fornecedor'] = row['fornecedor']
                        # Sincroniza globalmente
                        atualizar_casa_global(row['produto'], df.at[idx_est, 'qtd_central'], row['preco_pago'], None, None, prefixo)
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success("Sincronização forçada concluída!"); time.sleep(1); st.rerun()
            else: st.warning("Histórico vazio.")
        
        st.divider()
        busca = st.text_input("Buscar:")
        df_show = filtrar_dados_inteligente(df_hist, 'produto', busca) if busca else df_hist
        
        df_edit = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
        if st.button("💾 Salvar Histórico"):
            # Recalcula totais matematicamente ao salvar
            for idx, row in df_edit.iterrows():
                try:
                    q = converter_ptbr(row.get('qtd', 0)); p = converter_ptbr(row.get('preco_sem_desconto', 0))
                    if q > 0 and p > 0: df_edit.at[idx, 'preco_pago'] = p; df_edit.at[idx, 'total_gasto'] = p * q
                except: pass
            salvar_na_nuvem(f"{prefixo}_historico_compras", df_edit, COLS_HIST)
            st.success("Salvo!"); st.rerun()

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title("🏡 Estoque Central")
        df_edit = st.data_editor(df[['nome do produto', 'qtd_central', 'preco_custo']], use_container_width=True)
        if st.button("💾 Salvar"):
            df.update(df_edit)
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            for i, r in df_edit.iterrows(): atualizar_casa_global(r['nome do produto'], r['qtd_central'], r['preco_custo'], None, None, prefixo)
            st.success("Atualizado!"); st.rerun()

    # 9. GERAL (NOVO BOTÃO DE CORREÇÃO)
    elif modo == "📋 Tabela Geral":
        st.title("📋 Tabela Geral")
        
        # --- NOVO BOTÃO DE CORREÇÃO ---
        if st.button("⚠️ CORRIGIR VALORES ALTOS (Dividir por 100)"):
            c = 0
            for idx, row in df.iterrows():
                if row['preco_custo'] > 50: # Exemplo: Se custa mais que 50, assume erro
                    df.at[idx, 'preco_custo'] = row['preco_custo'] / 100; c += 1
                if row['preco_venda'] > 100:
                    df.at[idx, 'preco_venda'] = row['preco_venda'] / 100
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success(f"{c} produtos corrigidos!"); st.rerun()
            
        df_edit = st.data_editor(df, use_container_width=True, num_rows="dynamic")
        if st.button("💾 Salvar Tabela"):
            salvar_na_nuvem(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
            st.success("Salvo!"); st.rerun()
