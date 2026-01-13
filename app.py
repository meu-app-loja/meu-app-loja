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
    if pd.isna(valor) or str(valor).strip() == "": return 0.0
    s_valor = str(valor).strip().replace('R$', '').replace('r$', '').strip()
    try:
        if '.' in s_valor and ',' not in s_valor: return float(s_valor)
        if ',' in s_valor:
            s_valor = s_valor.replace('.', '').replace(',', '.')
        return float(s_valor)
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

# --- LEITURA DA NUVEM (COM RETRY AUTOMÁTICO - ANTI ERRO 429) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    # Tentativa com recuo exponencial (espera se der erro)
    max_tentativas = 3
    for tentativa in range(max_tentativas):
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
        
        except Exception as e:
            # Se for erro de cota (429), espera e tenta de novo
            if "429" in str(e) or "Quota" in str(e):
                tempo_espera = (tentativa + 1) * 3  # Espera 3s, 6s, 9s...
                time.sleep(tempo_espera)
                if tentativa == max_tentativas - 1: st.error(f"Erro de conexão (Muitos acessos). Tente novamente em 1 minuto.")
            else:
                return pd.DataFrame(columns=colunas_padrao)
    return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (COM RETRY AUTOMÁTICO - ANTI ERRO 429) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    max_tentativas = 3
    for tentativa in range(max_tentativas):
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
            return # Sucesso, sai da função
        except Exception as e:
            if "429" in str(e) or "Quota" in str(e):
                time.sleep((tentativa + 1) * 3) # Backoff
            else:
                st.error(f"Erro ao salvar: {e}")
                break

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

# --- NOVA FUNÇÃO DE PROPAGAÇÃO SEGURA (COM PAUSA) ---
def propagar_dados_massa(df_referencia, prefixo_ignorar):
    status_box = st.status("🔄 Sincronizando (Aguarde...)", expanded=True)
    todas_lojas = ["loja1", "loja2", "loja3"]
    client = get_google_client()
    sh = client.open("loja_dados")
    
    ref_map = {}
    for _, row in df_referencia.iterrows():
        nome = str(row['nome do produto']).strip()
        ref_map[nome] = {
            'qtd_central': converter_numero_seguro(row['qtd_central']),
            'custo': converter_numero_seguro(row['preco_custo']),
            'venda': converter_numero_seguro(row['preco_venda']),
            'forn': str(row['ultimo_fornecedor'])
        }

    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        status_box.write(f"Verificando {loja}...")
        time.sleep(2) # PAUSA OBRIGATÓRIA PARA NÃO TRAVAR O GOOGLE (Anti 429)
        
        try:
            ws = sh.worksheet(f"{loja}_estoque")
            dados_loja = ws.get_all_records()
            if not dados_loja: continue
            
            df_dest = pd.DataFrame(dados_loja)
            df_dest = garantir_integridade_colunas(df_dest, COLUNAS_VITAIS)
            df_dest.columns = df_dest.columns.str.strip().str.lower()
            
            alterou = False
            for i, row_d in df_dest.iterrows():
                nome_d = str(row_d['nome do produto']).strip()
                if nome_d in ref_map:
                    novos = ref_map[nome_d]
                    if abs(converter_numero_seguro(row_d['qtd_central']) - novos['qtd_central']) > 0.01:
                        df_dest.at[i, 'qtd_central'] = novos['qtd_central']; alterou = True
                    if abs(converter_numero_seguro(row_d['preco_custo']) - novos['custo']) > 0.01:
                        df_dest.at[i, 'preco_custo'] = novos['custo']; alterou = True
                    if novos['venda'] > 0 and abs(converter_numero_seguro(row_d['preco_venda']) - novos['venda']) > 0.01:
                        df_dest.at[i, 'preco_venda'] = novos['venda']; alterou = True
                    if novos['forn'] and str(row_d['ultimo_fornecedor']) != novos['forn']:
                        df_dest.at[i, 'ultimo_fornecedor'] = novos['forn']; alterou = True
            
            if alterou:
                status_box.write(f"Salvando {loja}...")
                for col in df_dest.columns:
                    if any(x in col for x in ['qtd', 'preco', 'valor']):
                        df_dest[col] = df_dest[col].apply(converter_numero_seguro)
                    if 'validade' in col:
                        df_dest[col] = df_dest[col].astype(str).replace('NaT', '')
                ws.update([df_dest.columns.values.tolist()] + df_dest.values.tolist())
                
        except Exception as e: status_box.write(f"Pulado {loja}: {e}")
            
    status_box.update(label="✅ Sincronizado!", state="complete", expanded=False)

# --- FUNÇÃO ATUALIZAR CASA GLOBAL (INDIVIDUAL) ---
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    # Função leve para uso pontual
    time.sleep(0.5) 
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        try:
            client = get_google_client(); sh = client.open("loja_dados")
            try: ws = sh.worksheet(f"{loja}_estoque")
            except: continue
            
            dados = ws.get_all_records(); df_outra = pd.DataFrame(dados)
            if not df_outra.empty:
                df_outra = garantir_integridade_colunas(df_outra, COLUNAS_VITAIS)
                df_outra.columns = df_outra.columns.str.strip().str.lower()
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    if qtd_nova_casa is not None: df_outra.at[idx, 'qtd_central'] = converter_numero_seguro(qtd_nova_casa)
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = converter_numero_seguro(novo_custo)
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = converter_numero_seguro(novo_venda)
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = str(nova_validade).replace('NaT', '')
                    ws.update([df_outra.columns.values.tolist()] + df_outra.values.tolist())
        except: pass

# --- FUNÇÃO XML ---
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
        except: dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}
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
                nome = it.find("Nome").text; qtd = converter_numero_seguro(it.find("Quantidade").text)
                valor = converter_numero_seguro(it.find("ValorPagoFinal").text); ean = it.find("CodigoBarras").text
                desc = 0.0
                if it.find("ValorDesconto") is not None: desc = converter_numero_seguro(it.find("ValorDesconto").text)
                p_liq = valor / qtd if qtd > 0 else 0; p_bruto = (valor + desc) / qtd if qtd > 0 else 0
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
                        elif t == 'qCom': qCom = converter_numero_seguro(info.text)
                        elif t == 'vProd': vProd = converter_numero_seguro(info.text)
                        elif t == 'vDesc': vDesc = converter_numero_seguro(info.text)
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
                            c1, c2 = st.columns(2)
                            c1.caption(f"Qtd: {int(row['qtd_sugerida'])}")
                            c2.caption(f"Status: {row['status']}")
                else: st.dataframe(df_lista_compras, use_container_width=True)
                if st.button("🗑️ Limpar Lista Inteira"):
                    salvar_na_nuvem(f"{prefixo}_lista_compras", pd.DataFrame(columns=COLS_LISTA), COLS_LISTA); st.success("Limpo!"); st.rerun()
            else: st.info("Sua lista de compras está vazia.")
        with tab_add:
            st.subheader("🤖 Gerador Automático")
            if st.button("🚀 Gerar Lista Baseada no Estoque Baixo"):
                if df.empty: st.warning("Sem produtos.")
                else:
                    mask_baixo = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
                    produtos_baixo = df[mask_baixo]
                    if produtos_baixo.empty: st.success("Tudo certo!")
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
            st.divider()
            with st.form("add_manual_lista"):
                lista_prods = [""] + sorted(df['nome do produto'].astype(str).unique().tolist())
                prod_man = st.selectbox("Produto:", lista_prods)
                qtd_man = st.number_input("Qtd:", min_value=1, value=10)
                obs_man = st.text_input("Obs:", placeholder="Ex: Atacadão")
                if st.form_submit_button("Adicionar"):
                    if prod_man:
                        novo_item = {'produto': prod_man, 'qtd_sugerida': qtd_man, 'fornecedor': obs_man, 'custo_previsto': 0.0, 'data_inclusao': datetime.now().strftime("%d/%m/%Y"), 'status': 'Manual'}
                        df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame([novo_item])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA); st.success("Adicionado!"); st.rerun()

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
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
                st.markdown("---")
                lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                escolhas = {}
                for i, item in enumerate(dados['itens']):
                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.markdown(f"**{item['nome']}**")
                        st.caption(f"Qtd: {int(item['qtd'])} | R$ {item['preco_un_liquido']:.2f}")
                    with c2:
                        match_inicial = "(CRIAR NOVO)"
                        if not df.empty:
                            mask_ean = df['código de barras'].astype(str) == item['ean']
                            if mask_ean.any(): match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                            else:
                                mel, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].astype(str).tolist())
                                if mel: match_inicial = mel
                        idx_inicial = lista_produtos_sistema.index(str(match_inicial)) if str(match_inicial) in lista_produtos_sistema else 0
                        escolhas[i] = st.selectbox(f"Vincular:", lista_produtos_sistema, index=idx_inicial, key=f"sel_{i}")
                    st.divider()
                if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
                    novos_hist = []; criados_cont = 0; atualizados_cont = 0
                    for i, item in enumerate(dados['itens']):
                        prod_esc = escolhas[i]
                        qtd = item['qtd']; preco = item['preco_un_liquido']
                        if prod_esc == "(CRIAR NOVO)":
                            novo = {'código de barras': item['ean'], 'nome do produto': item['nome'].upper(), 'qtd.estoque': 0, 'qtd_central': qtd, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': preco, 'preco_venda': preco*2, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']}
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            criados_cont += 1; nm_final = item['nome'].upper(); qtd_final = qtd
                        else:
                            mask = df['nome do produto'].astype(str) == str(prod_esc)
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] += qtd
                                df.at[idx, 'preco_custo'] = preco
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1
                                nm_final = prod_esc; qtd_final = df.at[idx, 'qtd_central']
                        
                        atualizar_casa_global(nm_final, qtd_final, preco, None, None, prefixo)
                        novos_hist.append({'data': dados['data'], 'produto': nm_final, 'fornecedor': dados['fornecedor'], 'qtd': qtd, 'preco_pago': preco, 'total_gasto': qtd*preco, 'numero_nota': dados['numero'], 'desconto_total_money': item['desconto_total_item'], 'preco_sem_desconto': item['preco_un_bruto']})
                    
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                    st.success(f"Sucesso! {criados_cont} novos, {atualizados_cont} atualizados."); time.sleep(2); st.rerun()
            except Exception as e: st.error(f"Erro: {e}")
    
    # 2.8 BASE OFICIAL
    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base")
        arquivo_base = st.file_uploader("Suba o arquivo Excel/CSV aqui", type=['xlsx', 'csv'])
        if arquivo_base:
            if st.button("🚀 Processar"):
                if processar_excel_oficial(arquivo_base): st.success("Base atualizada!"); st.rerun()
        st.divider()
        if st.button("🗑️ ZERAR TUDO"):
            client = get_google_client(); sh = client.open("loja_dados")
            for aba in [f"{prefixo}_estoque", f"{prefixo}_historico_compras", f"{prefixo}_movimentacoes", f"{prefixo}_vendas", f"{prefixo}_lista_compras", "base_oficial"]:
                try: sh.worksheet(aba).clear()
                except: pass
            st.success("Limpo!"); st.rerun()

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("Planograma (XLSX/CSV)", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                # CORREÇÃO CRUCIAL: Ler como texto
                if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, header=None, dtype=str)
                else: df_raw = pd.read_excel(arquivo, header=None, dtype=str)
                st.write("Identifique as colunas:")
                cols = df_raw.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("BARRAS", cols, index=0)
                idx_nome = c2.selectbox("NOME", cols, index=1)
                idx_qtd = c3.selectbox("QTD", cols, index=len(cols)-1)
                opcoes_preco = ["(Não Atualizar)"] + cols
                idx_preco = c4.selectbox("PREÇO VENDA", opcoes_preco)
                
                dividir_100 = st.checkbox("⚠️ Números vieram sem ponto? (Ex: 11599 em vez de 115.99)", value=False)
                
                if st.button("🚀 SINCRONIZAR"):
                    df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
                    alt = 0; novos = 0; bar = st.progress(0); total = len(df_raw); lst_novos = []
                    for i, row in df_raw.iterrows():
                        try:
                            cod = str(row[idx_barras]).replace('.0', '').strip()
                            nm = normalizar_texto(str(row[idx_nome]))
                            qtd = converter_numero_seguro(row[idx_qtd])
                            if dividir_100 and qtd > 0: qtd /= 100
                            if cod and nm:
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if idx_preco != "(Não Atualizar)":
                                        p = converter_numero_seguro(row[idx_preco])
                                        if dividir_100 and p > 0: p /= 100
                                        if p > 0: df.loc[mask, 'preco_venda'] = p
                                    alt += 1
                                else:
                                    p_nov = 0.0
                                    if idx_preco != "(Não Atualizar)":
                                        p_nov = converter_numero_seguro(row[idx_preco])
                                        if dividir_100 and p_nov > 0: p_nov /= 100
                                    lst_novos.append({'código de barras': cod, 'nome do produto': nm, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': p_nov, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0})
                                    novos += 1
                        except: pass
                        bar.progress((i+1)/total)
                    if lst_novos: df = pd.concat([df, pd.DataFrame(lst_novos)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    propagar_dados_massa(df, prefixo)
                    st.success(f"Feito! {alt} alt, {novos} novos."); st.balloons()
            except Exception as e: st.error(f"Erro: {e}")

    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Vendas - {loja_atual}")
        tab_imp, tab_hist = st.tabs(["Importar", "Histórico"])
        with tab_imp:
            arq = st.file_uploader("Relatório", type=['xlsx', 'xls'])
            if arq:
                try:
                    df_b = pd.read_excel(arq, header=None, dtype=str)
                    st.dataframe(df_b.head())
                    ln = st.number_input("Linha Títulos:", 0, 10, 0)
                    arq.seek(0); df_v = pd.read_excel(arq, header=ln, dtype=str)
                    cols = df_v.columns.tolist()
                    c1, c2, c3 = st.columns(3)
                    c_nm = c1.selectbox("NOME", cols); c_qtd = c2.selectbox("QTD", cols); c_dt = c3.selectbox("DATA", cols)
                    if st.button("🚀 PROCESSAR"):
                        at = 0; regs = []; bar = st.progress(0); tot = len(df_v)
                        for i, row in df_v.iterrows():
                            nm = str(row[c_nm]).strip(); qtd = converter_numero_seguro(row[c_qtd])
                            try: dt = pd.to_datetime(row[c_dt], dayfirst=True)
                            except: dt = datetime.now()
                            if qtd <= 0: continue
                            mask = (df['código de barras'].astype(str).str.contains(nm, na=False) | df['nome do produto'].astype(str).str.contains(nm, case=False, na=False))
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd.estoque'] -= qtd
                                at += 1
                                regs.append({"data_hora": dt, "produto": df.at[idx, 'nome do produto'], "qtd_vendida": qtd, "estoque_restante": df.at[idx, 'qtd.estoque']})
                            bar.progress((i+1)/tot)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        if regs:
                            df_vendas = pd.concat([df_vendas, pd.DataFrame(regs)], ignore_index=True)
                            salvar_na_nuvem(f"{prefixo}_vendas", df_vendas, COLS_VENDAS)
                        st.success(f"{at} baixas!")
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist:
            if not df_vendas.empty:
                b = st.text_input("Buscar venda:"); d_sh = filtrar_dados_inteligente(df_vendas, 'produto', b)
                st.dataframe(d_sh, use_container_width=True)

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        if usar_modo_mobile:
            b = st.text_input("Buscar:"); sh = filtrar_dados_inteligente(df, 'nome do produto', b)
            for idx, row in sh.iterrows():
                with st.container(border=True):
                    st.subheader(row['nome do produto'])
                    c1, c2 = st.columns(2); c1.metric("Loja", int(row['qtd.estoque'])); c2.metric("Casa", int(row['qtd_central']))
                    if row['qtd_central'] > 0:
                        with st.form(f"f_{idx}"):
                            c_i, c_b = st.columns([2,1]); q = c_i.number_input("Baixar:", 1, int(row['qtd_central']), key=f"k_{idx}")
                            if c_b.form_submit_button("⬇️"):
                                df.at[idx, 'qtd.estoque'] += q; df.at[idx, 'qtd_central'] -= q
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                st.rerun()
        else:
            opt = st.selectbox("Produto:", [""] + df['nome do produto'].tolist())
            if opt:
                idx = df[df['nome do produto'] == opt].index[0]
                st.write(f"Loja: {df.at[idx, 'qtd.estoque']} | Casa: {df.at[idx, 'qtd_central']}")
                if df.at[idx, 'qtd_central'] > 0:
                    with st.form("tr"):
                        q = st.number_input("Transferir:", 1, int(df.at[idx, 'qtd_central']))
                        if st.form_submit_button("Confirmar"):
                            df.at[idx, 'qtd.estoque'] += q; df.at[idx, 'qtd_central'] -= q
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            atualizar_casa_global(opt, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                            st.success("Ok!"); st.rerun()

    # 6. FORNECEDOR
    elif modo == "🛒 Fornecedor (Compras)":
        st.title("🛒 Compras")
        pen = df[df['status_compra'] == 'PENDENTE']
        if not pen.empty:
            item = st.selectbox("Item:", pen['nome do produto'])
            if item:
                idx = df[df['nome do produto'] == item].index[0]
                with st.form("c"):
                    q = st.number_input("Qtd:", value=int(df.at[idx, 'qtd_comprada']))
                    custo = st.number_input("Custo:", value=float(df.at[idx, 'preco_custo']))
                    if st.form_submit_button("Entrar"):
                        df.at[idx, 'qtd_central'] += q; df.at[idx, 'preco_custo'] = custo; df.at[idx, 'status_compra'] = 'OK'; df.at[idx, 'qtd_comprada'] = 0
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        atualizar_casa_global(item, df.at[idx, 'qtd_central'], custo, None, None, prefixo)
                        st.success("Entrada ok!"); st.rerun()
        else: st.info("Nada pendente.")

    # 7. HISTÓRICO E PREÇOS
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico")
        if not df_hist.empty:
            b = st.text_input("Buscar:"); v = filtrar_dados_inteligente(df_hist, 'produto', b)
            ed = st.data_editor(v, use_container_width=True, num_rows="dynamic")
            if st.button("💾 Salvar"):
                df_hist = ed; salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST); st.success("Salvo!")
        else: st.info("Sem histórico de compras.")

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Casa - {loja_atual}")
        tab1, tab2 = st.tabs(["Visualizar", "Entrada Manual"])
        with tab1:
            b = st.text_input("Buscar:"); v = filtrar_dados_inteligente(df, 'nome do produto', b)
            ed = st.data_editor(v, use_container_width=True, num_rows="dynamic")
            if st.button("💾 Salvar"):
                df.update(ed); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                propagar_dados_massa(df, prefixo)
                st.success("Salvo!"); st.rerun()
        with tab2:
            op = st.selectbox("Produto:", [""] + df['nome do produto'].tolist())
            if op:
                idx = df[df['nome do produto'] == op].index[0]
                with st.form("man"):
                    q = st.number_input("Qtd:", value=0); c = st.number_input("Custo:", value=float(df.at[idx, 'preco_custo']))
                    if st.form_submit_button("Salvar"):
                        df.at[idx, 'qtd_central'] += q; df.at[idx, 'preco_custo'] = c
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        atualizar_casa_global(op, df.at[idx, 'qtd_central'], c, None, None, prefixo)
                        st.success("Atualizado!"); st.rerun()

    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Geral")
        b = st.text_input("Buscar:"); v = filtrar_dados_inteligente(df, 'nome do produto', b)
        ed = st.data_editor(v, use_container_width=True, num_rows="dynamic", key="geral_editor")
        c1, c2 = st.columns(2)
        if c1.button("💾 Salvar"):
            df.update(ed); salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            propagar_dados_massa(df, prefixo)
            st.success("Salvo!")
        with c2:
            fator = st.selectbox("Dividir por:", [10, 100])
            corte = st.number_input("Apenas maiores que:", 1.0)
            if st.button("🚨 Corrigir"):
                c=0
                for i, r in df.iterrows():
                    if r['preco_venda'] > corte: df.at[i, 'preco_venda'] /= fator; c+=1
                    if r['preco_custo'] > corte: df.at[i, 'preco_custo'] /= fator
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS); st.success(f"{c} corrigidos!"); st.rerun()
