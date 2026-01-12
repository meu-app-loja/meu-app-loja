import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time # Importante para dar pausas

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DA CONEXÃO COM A NUVEM
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas Cloud", layout="wide", page_icon="🏪")

# Função para conectar ao "Cofre" (Cacheada para não conectar toda hora)
@st.cache_resource
def get_google_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    client = gspread.authorize(creds)
    return client

# Função INTELIGENTE que lê da nuvem (Com memória cache de 60 segundos)
@st.cache_data(ttl=60) 
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1.5) # PAUSA DE 1.5 SEGUNDOS PARA NÃO BLOQUEAR O GOOGLE (ERRO 429)
    try:
        client = get_google_connection()
        # Tenta abrir a planilha principal
        try:
            sh = client.open("loja_dados")
        except:
            st.error("Planilha 'loja_dados' não encontrada no Google Drive.")
            return pd.DataFrame(columns=colunas_padrao)

        try:
            worksheet = sh.worksheet(nome_aba)
        except:
            # Se a aba não existe, cria ela
            worksheet = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
            worksheet.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
        
        dados = worksheet.get_all_records()
        df = pd.DataFrame(dados)
        
        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)
            
        # Garante números
        for col in df.columns:
            if any(x in col.lower() for x in ["qtd", "preco", "valor", "custo"]):
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                 
        return df
    except Exception as e:
        st.warning(f"Aguardando conexão com {nome_aba}... ({e})")
        return pd.DataFrame(columns=colunas_padrao)

# Função para Salvar (Limpa o cache para mostrar dados novos na hora)
def salvar_na_nuvem(nome_aba, df):
    try:
        client = get_google_connection()
        sh = client.open("loja_dados")
        try:
            worksheet = sh.worksheet(nome_aba)
        except:
            worksheet = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        worksheet.clear()
        
        # Prepara dados (converte datas para texto)
        df_save = df.copy()
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
        
        worksheet.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        
        # LIMPA A MEMÓRIA DO SISTEMA PARA ELE LER OS DADOS NOVOS
        ler_da_nuvem.clear()
        
    except Exception as e:
        st.error(f"Erro ao salvar em {nome_aba}: {e}")

# ==============================================================================
# 🧠 SUAS FUNÇÕES DE NEGÓCIO (MANTIDAS IGUAIS)
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto):
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
    if maior_score >= cutoff: return melhor_match, "Nome Similar"
    return None, "Nenhum"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

# Atualiza em TODAS as abas de todas as lojas
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        # Lê da nuvem da outra loja (usa cache)
        df_outra = ler_da_nuvem(f"{loja}_estoque", ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'preco_custo', 'preco_venda', 'validade', 'ultimo_fornecedor', 'preco_sem_desconto'])
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
                    melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                    if melhor_nome: item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            dados_nota['itens'].append(item)
    return dados_nota

# ==============================================================================
# 🚀 INÍCIO DO APP (LÓGICA SOB DEMANDA)
# ==============================================================================

st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Unidade:", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# --- AQUI ESTÁ O TRUQUE: CARREGA APENAS O ESTOQUE PRINCIPAL AGORA ---
cols_estoque = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]
df = ler_da_nuvem(f"{prefixo}_estoque", cols_estoque)
if not df.empty:
    df.columns = df.columns.str.strip().str.lower()
    df['código de barras'] = df['código de barras'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
    df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')

st.sidebar.title("🏪 Menu Completo")
modo = st.sidebar.radio("Navegar:", [
    "📊 Dashboard",
    "🚚 Transferência (Picklist)",
    "📝 Lista de Compras",
    "📥 Importar XML (NFe)", 
    "🔄 Sincronizar (Excel)",
    "📉 Baixar Vendas",
    "🏠 Gôndola (Loja)", 
    "🛒 Fornecedor (Compras)", 
    "💰 Histórico & Preços",
    "🏡 Estoque Central (Casa)",
    "📋 Tabela Geral"
])

# ------------------------------------------------------------------
# SÓ CARREGA OS DADOS NECESSÁRIOS PARA CADA MENU!
# ------------------------------------------------------------------

if modo == "📊 Dashboard":
    st.title(f"📊 Painel Cloud - {loja_atual}")
    if df.empty: st.info("Estoque vazio.")
    else:
        valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("📦 Loja", int(df['qtd.estoque'].sum()))
        c2.metric("💰 Valor Total", f"R$ {valor_estoque:,.2f}")
        c3.metric("🏡 Casa", int(df['qtd_central'].sum()))

elif modo == "📥 Importar XML (NFe)":
    st.title("📥 Entrada XML")
    cols_oficial = ['nome do produto', 'código de barras']
    df_oficial = ler_da_nuvem("base_oficial", cols_oficial) # Carrega só se precisar
    
    arquivo_xml = st.file_uploader("Arraste o XML", type=['xml'])
    if arquivo_xml:
        try:
            dados = ler_xml_nfe(arquivo_xml, df_oficial)
            st.success(f"Nota: {dados['numero']} - {dados['fornecedor']}")
            lista_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
            escolhas = {}
            for i, item in enumerate(dados['itens']):
                st.divider()
                c1, c2 = st.columns([1, 1])
                c1.write(f"**XML:** {item['nome']} (Qtd: {item['qtd']})")
                
                # Match
                match_inicial = "(CRIAR NOVO)"
                if not df.empty:
                    mask_ean = df['código de barras'].astype(str) == str(item['ean']).strip()
                    if mask_ean.any(): match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                    else:
                        melhor, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].astype(str).tolist())
                        if melhor: match_inicial = melhor
                
                idx_ini = 0
                if match_inicial in lista_sistema: idx_ini = lista_sistema.index(match_inicial)
                escolhas[i] = c2.selectbox(f"Vincular item {i+1}:", lista_sistema, index=idx_ini, key=f"s_{i}")

            if st.button("💾 SALVAR"):
                for i, item in enumerate(dados['itens']):
                    escolhido = escolhas[i]
                    if escolhido == "(CRIAR NOVO)":
                        novo = {
                            'código de barras': str(item['ean']).strip(), 'nome do produto': normalizar_texto(item['nome']),
                            'qtd.estoque': 0, 'qtd_central': item['qtd'], 'qtd_minima': 5,
                            'preco_custo': item['preco_un_liquido'], 'preco_venda': item['preco_un_bruto']*2, 'validade': None,
                            'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL',
                            'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto']
                        }
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        atualizar_casa_global(novo['nome do produto'], item['qtd'], item['preco_un_liquido'], None, None, prefixo)
                    else:
                        mask = df['nome do produto'] == escolhido
                        if mask.any():
                            idx = df[mask].index[0]
                            df.at[idx, 'qtd_central'] += item['qtd']
                            df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizar_casa_global(escolhido, df.at[idx, 'qtd_central'], item['preco_un_liquido'], None, None, prefixo)
                salvar_na_nuvem(f"{prefixo}_estoque", df)
                st.success("Salvo!")
                st.rerun()
        except Exception as e: st.error(f"Erro: {e}")

elif modo == "🔄 Sincronizar (Excel)":
    st.title("🔄 Sincronizar Excel")
    arq = st.file_uploader("Arquivo", type=['xlsx', 'csv', 'xls'])
    if arq:
        if arq.name.endswith('.csv'): df_raw = pd.read_csv(arq, header=None)
        else: df_raw = pd.read_excel(arq, header=None)
        cols = df_raw.columns.tolist()
        c1, c2, c3 = st.columns(3)
        i_cod = c1.selectbox("CÓDIGO", cols, 0)
        i_nom = c2.selectbox("NOME", cols, 1)
        i_qtd = c3.selectbox("QTD", cols, len(cols)-1)
        if st.button("🚀 ENVIAR"):
            bar = st.progress(0); tot = len(df_raw)
            for i in range(1, tot):
                try:
                    cod = str(df_raw.iloc[i, i_cod]).replace('.0', '').strip()
                    nome = normalizar_texto(str(df_raw.iloc[i, i_nom]))
                    qtd = pd.to_numeric(df_raw.iloc[i, i_qtd], errors='coerce') or 0
                    if cod and nome:
                        mask = df['código de barras'] == cod
                        if mask.any(): df.loc[mask, 'qtd.estoque'] = qtd
                        else:
                            novo = {'código de barras': cod, 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'preco_custo': 0, 'preco_venda': 0, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0}
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                except: pass
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            st.success("Sincronizado!")
            st.rerun()

elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico")
    # CARREGA APENAS AQUI
    cols_hist = ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto']
    df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", cols_hist)
    
    if not df_hist.empty:
        df_hist['data'] = pd.to_datetime(df_hist['data'], errors='coerce')
        busca = st.text_input("Filtrar:", placeholder="Produto ou fornecedor...")
        df_show = filtrar_dados_inteligente(df_hist, 'produto', busca)
        if df_show.empty: df_show = filtrar_dados_inteligente(df_hist, 'fornecedor', busca)
        
        df_edit = st.data_editor(df_show.sort_values('data', ascending=False), num_rows="dynamic", use_container_width=True, key="editor_h")
        if st.button("💾 SALVAR HISTÓRICO"):
            salvar_na_nuvem(f"{prefixo}_historico_compras", df_edit)
            st.success("Salvo!")
            st.rerun()

elif modo == "🏠 Gôndola (Loja)":
    st.title("🏠 Gôndola")
    busca = st.text_input("Buscar Produto:", placeholder="Digite...")
    if busca:
        res = filtrar_dados_inteligente(df, 'nome do produto', busca)
        for idx, row in res.iterrows():
            with st.container(border=True):
                st.write(f"**{row['nome do produto']}**")
                c1, c2 = st.columns(2)
                bx = c1.number_input(f"Baixar ({int(row['qtd_central'])} disp):", min_value=1, key=f"bx_{idx}")
                if c2.button("⬇️ Baixar", key=f"btn_{idx}"):
                    if row['qtd_central'] >= bx:
                        df.at[idx, 'qtd.estoque'] += bx
                        df.at[idx, 'qtd_central'] -= bx
                        salvar_na_nuvem(f"{prefixo}_estoque", df)
                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                        st.success("Feito!")
                        st.rerun()
                    else: st.error("Sem saldo.")

elif modo == "📋 Tabela Geral":
    st.title("📋 Tabela Geral (Editável)")
    busca_g = st.text_input("Buscar:", placeholder="...")
    df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_g)
    df_ed = st.data_editor(df_show, num_rows="dynamic", use_container_width=True)
    if st.button("💾 SALVAR TUDO"):
        df.update(df_ed)
        salvar_na_nuvem(f"{prefixo}_estoque", df)
        # Sincroniza casa
        bar = st.progress(0); tot = len(df_ed)
        for i, (idx, row) in enumerate(df_ed.iterrows()):
            atualizar_casa_global(df.at[idx, 'nome do produto'], df.at[idx, 'qtd_central'], df.at[idx, 'preco_custo'], df.at[idx, 'preco_venda'], df.at[idx, 'validade'], prefixo)
            bar.progress((i+1)/tot)
        st.success("Salvo e Sincronizado!")
        st.rerun()

elif modo == "📉 Baixar Vendas":
    st.title("📉 Baixar Vendas")
    # Carrega vendas só aqui
    cols_vendas = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
    df_vendas = ler_da_nuvem(f"{prefixo}_vendas", cols_vendas)
    
    arq_v = st.file_uploader("Relatório Vendas", type=['xlsx', 'xls'])
    if arq_v:
        df_v_raw = pd.read_excel(arq_v)
        c1, c2 = st.columns(2)
        col_n = c1.selectbox("NOME", df_v_raw.columns)
        col_q = c2.selectbox("QTD", df_v_raw.columns)
        if st.button("PROCESSAR"):
            bar = st.progress(0); tot = len(df_v_raw); novos = []
            for i, row in df_v_raw.iterrows():
                nm = str(row[col_n]).strip(); q = pd.to_numeric(row[col_q], errors='coerce')
                if q > 0:
                    mask = df['nome do produto'].astype(str).str.contains(nm, case=False, na=False)
                    if mask.any():
                        idx = df[mask].index[0]
                        df.at[idx, 'qtd.estoque'] -= q
                        novos.append({'data_hora': datetime.now(), 'produto': df.at[idx, 'nome do produto'], 'qtd_vendida': q, 'estoque_restante': df.at[idx, 'qtd.estoque']})
                bar.progress((i+1)/tot)
            salvar_na_nuvem(f"{prefixo}_estoque", df)
            if novos:
                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos)], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_vendas", df_vendas)
            st.success("Vendas baixadas!")
            st.rerun()
