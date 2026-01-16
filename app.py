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

# Função para formatar números no estilo brasileiro (milhar '.', decimal ',')
def format_br(valor):
    s = f"{valor:,.2f}" # Formata com , para milhar e . para decimal
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')

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

# --- FUNÇÃO DE LIMPEZA E CONVERSÃO DE NÚMEROS (CORREÇÃO 3,19) ---
def converter_ptbr(valor):
    """Converte valores brasileiros (com vírgula) para padrão computador (ponto) sem erros."""
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
  
    s = str(valor).strip().upper().replace('R$', '').strip()
  
    # Se já for um número puro, retorna
    try:
        return float(s)
    except:
        pass
    # Lógica para 1.000,00 -> 1000.00
    if ',' in s and '.' in s:
        s = s.replace('.', '') # Tira o ponto de milhar
        s = s.replace(',', '.') # Troca virgula por ponto
    elif ',' in s:
        s = s.replace(',', '.') # Troca virgula por ponto
      
    try:
        return float(s)
    except:
        return 0.0

# --- FUNÇÃO DE CURA ---
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty: return pd.DataFrame(columns=colunas_alvo)
  
    # Normaliza nomes das colunas
    df.columns = df.columns.str.strip().str.lower()
  
    # Garante que todas as colunas vitais existem
    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col] = 0.0
            elif 'data' in col or 'validade' in col:
                df[col] = None
            else:
                df[col] = ""
  
    # Garante que colunas numéricas sejam números de verdade
    for col in df.columns:
        if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
            df[col] = df[col].apply(converter_ptbr)
          
    return df

# --- LEITURA DA NUVEM (CORRIGIDA PARA NÃO APAGAR DADOS) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1) # Pausa técnica para evitar bloqueio do Google
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)
      
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
      
        # Se a tabela vier vazia, retorna estrutura vazia
        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)
        df = garantir_integridade_colunas(df, colunas_padrao)
      
        # Tratamento especial para Datas
        for col in df.columns:
            if 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')
              
        return df
    except Exception as e:
        # Em caso de erro grave, não retorna vazio para não apagar o cache local visual
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM (VERSÃO SEGURA - NÃO APAGA TUDO SE DER ERRO) ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try: 
            ws = sh.worksheet(nome_aba)
        except: 
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)

        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')

        data_to_write = [df_save.columns.values.tolist()] + df_save.values.tolist()

        # Atualiza a partir da célula A1 (sobrescreve sem limpar tudo antes)
        ws.update('A1', data_to_write)
        
        # Remove linhas/colunas extras que sobraram
        ws.resize(rows=len(df_save) + 1, cols=len(df_save.columns))

        ler_da_nuvem.clear()  # Limpa cache para forçar recarregamento
        return True
    except Exception as e:
        st.error(f"Erro ao salvar em {nome_aba}: {e}")
        return False

# ==============================================================================
# 🧠 FUNÇÕES LÓGICAS (MANTIDAS E AJUSTADAS)
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
  
    # 1. TENTA FORMATO NOVO
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
  
    # 2. ITENS DO XML NOVO
    itens_custom = root.findall(".//Item")
    if itens_custom:
        for it in itens_custom:
            try:
                nome = it.find("Nome").text
                qtd = converter_ptbr(it.find("Quantidade").text)
                valor = converter_ptbr(it.find("ValorPagoFinal").text)
                ean = it.find("CodigoBarras").text
              
                desc = 0.0
                if it.find("ValorDesconto") is not None:
                    desc = converter_ptbr(it.find("ValorDesconto").text)
              
                p_liq = valor / qtd if qtd > 0 else 0
                p_bruto = (valor + desc) / qtd if qtd > 0 else 0
              
                dados_nota['itens'].append({'nome': normalizar_texto(nome), 'qtd': qtd, 'ean': str(ean).strip(), 'preco_un_liquido': p_liq, 'preco_un_bruto': p_bruto, 'desconto_total_item': desc})
            except:
                continue
    else:
        # 3. ITENS NFE
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
                        item['qtd'] = qCom
                        item['preco_un_bruto'] = vProd / qCom
                        item['desconto_total_item'] = vDesc
                        item['preco_un_liquido'] = (vProd - vDesc) / qCom
                  
                    ean_xml = str(item['ean']).strip()
                    if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                        item['ean'] = item['codigo_interno']
                    dados_nota['itens'].append(item)
            except: continue
          
    # MATCH
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
            c2.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
          
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty: st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras' para ver.")
            if not df_critico.empty: st.error("🚨 Produtos Vencendo!"); st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])
          
    # 1.5 PICKLIST
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
                      
                        st.success(f"✅ {movidos} produtos transferidos!")
                        if erros > 0: st.warning(f"⚠️ {erros} não encontrados.")
            except Exception as e: st.error(f"Erro: {e}")
    # 1.6 LISTA DE COMPRAS
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
              
                c_del, c_pdf = st.columns(2)
                if c_del.button("🗑️ Limpar Lista Inteira (Após Comprar)"):
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
                    with c2:
                        idx_inicial = lista_produtos_sistema.index(str(match_inicial)) if str(match_inicial) in lista_produtos_sistema else 0
                        escolha_usuario = st.selectbox(f"Vincular ao Sistema ({tipo_match}):", lista_produtos_sistema, index=idx_inicial, key=f"sel_{i}")
                        if escolha_usuario != "(CRIAR NOVO)":
                            st.info(f"🆔 Sistema: {escolha_usuario}")
                        escolhas[i] = escolha_usuario
                    st.divider()
              
                if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
                    novos_hist = []; criados_cont = 0; atualizados_cont = 0
                  
                    for i, item in enumerate(dados['itens']):
                        produto_escolhido = escolhas[i]
                        qtd_xml = int(item['qtd'])
                        preco_pago = item['preco_un_liquido']
                        preco_sem_desc = item['preco_un_bruto']
                        desc_total_val = item.get('desconto_total_item', 0)
                        ean_xml = str(item.get('ean', '')).strip()
                        nome_xml = str(item['nome']).strip()
                      
                        nome_final = ""
                      
                        if produto_escolhido == "(CRIAR NOVO)":
                            novo_prod = {'código de barras': ean_xml, 'nome do produto': nome_xml.upper(), 'qtd.estoque': 0, 'qtd_central': qtd_xml, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': preco_pago, 'preco_venda': preco_pago * 2, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': preco_sem_desc}
                            df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                            criados_cont += 1
                            nome_final = nome_xml.upper()
                        else:
                            mask = df['nome do produto'].astype(str) == str(produto_escolhido)
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] += qtd_xml
                                df.at[idx, 'preco_custo'] = preco_pago
                                df.at[idx, 'preco_sem_desconto'] = preco_sem_desc
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1
                                nome_final = produto_escolhido
                      
                        # Garante que atualiza nas outras lojas (preço e qtd central)
                        if nome_final:
                            atualizar_casa_global(nome_final, df.loc[df['nome do produto'] == nome_final, 'qtd_central'].values[0], preco_pago, None, None, prefixo)
                      
                        novos_hist.append({'data': dados['data'], 'produto': nome_final, 'fornecedor': dados['fornecedor'], 'qtd': qtd_xml, 'preco_pago': preco_pago, 'total_gasto': qtd_xml * preco_pago, 'numero_nota': dados['numero'], 'desconto_total_money': desc_total_val, 'preco_sem_desconto': preco_sem_desc})
                  
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                  
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                  
                    st.success(f"✅ Processado! {criados_cont} novos, {atualizados_cont} atualizados e sincronizado.")
                    st.balloons(); st.rerun()
                  
            except Exception as e: st.error(f"Erro ao ler XML: {e}")
    # 2.8 BASE OFICIAL
    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base de Produtos Oficial")
        st.markdown("Suba aqui o arquivo **product-2025...** exportado do sistema.")
        arquivo_base = st.file_uploader("Suba o arquivo Excel/CSV aqui", type=['xlsx', 'csv'])
        if arquivo_base:
            if st.button("🚀 Processar e Salvar Base"):
                sucesso = processar_excel_oficial(arquivo_base)
                if sucesso:
                    st.success("Base Oficial atualizada com sucesso!")
                    st.rerun()
    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        st.info("💡 Este módulo agora IMPORTA produtos novos da planilha e atualiza os existentes.")
        arquivo = st.file_uploader("📂 Arquivo Planograma (XLSX ou CSV)", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, header=None)
                else: df_raw = pd.read_excel(arquivo, header=None)
              
                st.write("Identifique as colunas:")
                st.dataframe(df_raw.head())
                cols = df_raw.columns.tolist()
              
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("Coluna CÓDIGO BARRAS", cols, index=0)
                idx_nome = c2.selectbox("Coluna NOME DO PRODUTO", cols, index=1 if len(cols)>1 else 0)
                idx_qtd = c3.selectbox("Coluna QUANTIDADE", cols, index=len(cols)-1)
              
                opcoes_preco = ["(Não Atualizar Preço)"] + cols
                idx_preco = c4.selectbox("Coluna PREÇO VENDA", opcoes_preco)
              
                if st.button("🚀 SINCRONIZAR TUDO (Importar + Atualizar)"):
                    df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
                    alt = 0; novos = 0; bar = st.progress(0); total_linhas = len(df_raw); novos_produtos = []
                    start_row = 1
                  
                    for i in range(start_row, total_linhas):
                        try:
                            cod = str(df_raw.iloc[i, idx_barras]).replace('.0', '').strip()
                            nome_planilha = str(df_raw.iloc[i, idx_nome]).strip()
                          
                            # Usa o conversor seguro
                            qtd = converter_ptbr(df_raw.iloc[i, idx_qtd])
                          
                            nome_norm = normalizar_texto(nome_planilha)
                          
                            if cod and nome_norm:
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if idx_preco != "(Não Atualizar Preço)":
                                        val_preco = converter_ptbr(df_raw.iloc[i, idx_preco])
                                        if val_preco > 0: df.loc[mask, 'preco_venda'] = val_preco
                                    alt += 1
                                else:
                                    novo_preco_venda = 0.0
                                    if idx_preco != "(Não Atualizar Preço)":
                                        novo_preco_venda = converter_ptbr(df_raw.iloc[i, idx_preco])
                                  
                                    novo_prod = {'código de barras': cod, 'nome do produto': nome_norm, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': novo_preco_venda, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0}
                                    novos_produtos.append(novo_prod)
                                    novos += 1
                        except: pass
                        bar.progress((i+1)/total_linhas)
                  
                    if novos_produtos:
                        df = pd.concat([df, pd.DataFrame(novos_produtos)], ignore_index=True)
                  
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success(f"✅ Sucesso! {alt} produtos atualizados e {novos} NOVOS produtos cadastrados.")
                    if novos > 0: st.balloons()
            except Exception as e: st.error(f"Erro: {e}")
    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Importar Vendas - {loja_atual}")
        tab_imp, tab_hist_vendas = st.tabs(["📂 Importar Arquivo", "📜 Histórico"])
        with tab_imp:
            arquivo_vendas = st.file_uploader("📂 Relatório de Vendas", type=['xlsx', 'xls'], key="up_vendas")
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None)
                    st.dataframe(df_bruto.head(5), use_container_width=True)
                    linha_titulo = st.number_input("Número da linha dos TÍTULOS:", min_value=0, max_value=10, value=0)
                    arquivo_vendas.seek(0)
                    df_vendas_temp = pd.read_excel(arquivo_vendas, header=linha_titulo)
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
                                qtd = converter_ptbr(row[col_qtd])
                              
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
    # 5. GÔNDOLA
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
                            else: st.warning("🚫 Casa Zerada")
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
                                        st.success(f"Sucesso! {qtd_transf} unid. transferidas."); st.rerun()
                                    else: st.info("Quantidade deve ser maior que 0.")
                      
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
                        busca_gondola_hist = st.text_input("🔍 Buscar no Histórico:", placeholder="Ex: oleo...", key="busca_gondola_hist")
                        df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_gondola_hist)
                        if not df_mov_show.empty:
                            st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True, hide_index=True)
                    else: st.info("Sem histórico.")
    # 6. FORNECEDOR
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
                    forn_compra = st.text_input("Fornecedor:", value=df.at[idx, 'ultimo_fornecedor'])
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
                        st.success("Estoque atualizado!"); st.rerun()
        else: st.success("Sem compras pendentes.")
    # 7. HISTÓRICO & PREÇOS
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico & Preços")
        if not df_hist.empty:
            busca_hist_precos = st.text_input("🔍 Buscar:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
            df_hist_visual = df_hist
            if busca_hist_precos:
                df_hist_visual = filtrar_dados_inteligente(df_hist, 'produto', busca_hist_precos)
                if df_hist_visual.empty:
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
          
            st.info("✅ Edite ou **exclua** linhas (selecione a linha e aperte Delete).")
            df_editado = st.data_editor(
                df_hist_visual.sort_values(by='data', ascending=False),
                use_container_width=True,
                key="editor_historico_geral",
                num_rows="dynamic",
                column_config={
                    "data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "produto": st.column_config.TextColumn("Produto"),
                    "fornecedor": st.column_config.TextColumn("Fornecedor"),
                    "qtd": st.column_config.NumberColumn("Qtd", format="%.0f"),
                    "preco_pago": st.column_config.TextColumn("Pago (Unit)", help="Digite com vírgula: ex: 3,19"),
                    "total_gasto": st.column_config.TextColumn("Total Gasto", disabled=True),
                    "numero_nota": st.column_config.TextColumn("Nº Nota"),
                    "desconto_total_money": st.column_config.TextColumn("Desconto TOTAL", help="Digite com vírgula: ex: 10,50"),
                    "preco_sem_desconto": st.column_config.TextColumn("Preço Tabela", help="Digite com vírgula: ex: 5,99")
                }
            )
            
            st.divider()
            st.subheader("➕ Adicionar Compra Manual")

            with st.form("form_nova_compra_manual"):
                col1, col2 = st.columns(2)
                produto_nome = col1.text_input("Nome do Produto (novo ou existente)")
                fornecedor = col2.text_input("Fornecedor", value="" if df.empty else df.iloc[0]['ultimo_fornecedor'])
                
                col3, col4, col5 = st.columns(3)
                qtd_nova = col3.number_input("Quantidade", min_value=1, value=1)
                preco_tabela = col4.text_input("Preço Tabela (com vírgula)", placeholder="ex: 5,99")
                desconto_total = col5.text_input("Desconto Total (R$ com vírgula)", value="0,00")
                
                col6, col7 = st.columns(2)
                preco_pago_un = col6.text_input("Preço Pago Unitário (com vírgula)", placeholder="ex: 3,19")
                numero_nota = col7.text_input("Número da Nota")
                
                data_compra = st.date_input("Data da Compra", datetime.today())

                if st.form_submit_button("✅ Registrar Compra e Atualizar Estoque"):
                    if not produto_nome:
                        st.error("Nome do produto obrigatório!")
                    else:
                        # Converte valores
                        qtd = int(qtd_nova)
                        p_tab = converter_ptbr(preco_tabela)
                        desc = converter_ptbr(desconto_total)
                        p_pago = converter_ptbr(preco_pago_un) if preco_pago_un else (p_tab * qtd - desc) / qtd if qtd > 0 else 0
                        
                        total_gasto = qtd * p_pago
                        
                        # Atualiza ou cria produto no estoque
                        nome_norm = normalizar_texto(produto_nome)
                        mask = df['nome do produto'].apply(normalizar_texto) == nome_norm
                        if mask.any():
                            idx = mask.idxmax()
                            df.at[idx, 'qtd_central'] += qtd
                            df.at[idx, 'preco_custo'] = p_pago
                            df.at[idx, 'ultimo_fornecedor'] = fornecedor
                            df.at[idx, 'preco_sem_desconto'] = p_tab
                            nome_final = df.at[idx, 'nome do produto']
                        else:
                            # Cria novo produto
                            novo = {
                                'código de barras': '', 'nome do produto': produto_nome.upper().strip(),
                                'qtd.estoque': 0, 'qtd_central': qtd, 'qtd_minima': 5, 'validade': None,
                                'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': p_pago,
                                'preco_venda': p_pago * 2, 'categoria': 'GERAL',
                                'ultimo_fornecedor': fornecedor, 'preco_sem_desconto': p_tab
                            }
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            nome_final = produto_nome.upper().strip()
                        
                        # Sincroniza com outras lojas
                        qtd_central_atual = df[df['nome do produto'] == nome_final]['qtd_central'].values[0]
                        atualizar_casa_global(nome_final, qtd_central_atual, p_pago, None, None, prefixo)
                        
                        # Salva estoque
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                        
                        # Adiciona ao histórico
                        nova_linha_hist = {
                            'data': data_compra, 'produto': nome_final, 'fornecedor': fornecedor,
                            'qtd': qtd, 'preco_pago': p_pago, 'total_gasto': total_gasto,
                            'numero_nota': numero_nota, 'desconto_total_money': desc,
                            'preco_sem_desconto': p_tab
                        }
                        df_hist = pd.concat([df_hist, pd.DataFrame([nova_linha_hist])], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                        
                        st.success(f"Compra registrada! {qtd} un de {nome_final} adicionadas à Casa.")
                        st.rerun()

            if st.button("💾 Salvar Alterações"):
                indices_originais = df_hist_visual.index.tolist()
                indices_editados = df_editado.index.tolist()
                indices_removidos = list(set(indices_originais) - set(indices_editados))
                if indices_removidos:
                    df_hist = df_hist.drop(indices_removidos)
                    st.warning(f"🗑️ {len(indices_removidos)} registros excluídos.")
                df_hist.update(df_editado)
                # Recalcula totais
                for idx, row in df_hist.iterrows():
                    try:
                        q = converter_ptbr(row.get('qtd', 0)); p_tab = converter_ptbr(row.get('preco_sem_desconto', 0)); d_tot = converter_ptbr(row.get('desconto_total_money', 0))
                        if q > 0 and p_tab > 0:
                            total_liq = (p_tab * q) - d_tot
                            df_hist.at[idx, 'preco_pago'] = total_liq / q
                            df_hist.at[idx, 'total_gasto'] = total_liq
                    except: pass
              
                salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                st.success("Salvo!"); st.rerun()
        else: st.info("Sem histórico.")
    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
        tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
        with tab_ver:
            if not df.empty:
                if usar_modo_mobile:
                    st.info("📱 Modo Celular")
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
                    st.info("✏️ Edição em Tabela")
                    busca_central = st.text_input("🔍 Buscar Produto:", placeholder="Ex: oleo...", key="busca_central")
                    colunas_visiveis = ['nome do produto', 'qtd_central', 'validade', 'preco_custo', 'ultimo_fornecedor']
                    df_visual = filtrar_dados_inteligente(df, 'nome do produto', busca_central)[colunas_visiveis]
                    df_editado = st.data_editor(df_visual, use_container_width=True, num_rows="dynamic", key="edit_casa")
                  
                    if st.button("💾 SALVAR TABELA"):
                        indices_originais = df_visual.index.tolist()
                        indices_editados = df_editado.index.tolist()
                        indices_removidos = list(set(indices_originais) - set(indices_editados))
                        if indices_removidos:
                            df = df.drop(indices_removidos)
                        df.update(df_editado)
                        salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                      
                        bar = st.progress(0); total = len(df_editado)
                        for i, (idx, row) in enumerate(df_editado.iterrows()):
                            atualizar_casa_global(df.at[idx, 'nome do produto'], row['qtd_central'], row['preco_custo'], None, row['validade'], prefixo)
                            bar.progress((i+1)/total)
                        st.success("Sincronizado!"); st.rerun()
        with tab_gerenciar:
            st.info("Entrada Manual ou Ajuste.")
            if not df.empty:
                lista_prods = sorted(df['nome do produto'].astype(str).unique().tolist())
                prod_opcao = st.selectbox("Selecione o Produto:", lista_prods)
                if prod_opcao:
                    mask = df['nome do produto'].astype(str) == str(prod_opcao)
                    if mask.any():
                        idx_prod = df[mask].index[0]
                        with st.form("edit_estoque_casa_full"):
                            st.markdown(f"### Detalhes")
                            c_dt, c_hr = st.columns(2)
                            dt_reg = c_dt.date_input("Data:", datetime.today())
                            hr_reg = c_hr.time_input("Hora:", datetime.now().time())
                            c_forn = st.text_input("Fornecedor:", value=str(df.at[idx_prod, 'ultimo_fornecedor']))
                          
                            c_nome = st.text_input("Nome:", value=df.at[idx_prod, 'nome do produto'])
                            c_val, c_custo, c_venda = st.columns(3)
                            nova_val = c_val.date_input("Validade:", value=df.at[idx_prod, 'validade'] if pd.notnull(df.at[idx_prod, 'validade']) else None)
                            novo_custo = c_custo.number_input("Custo:", value=float(df.at[idx_prod, 'preco_custo']), format="%.2f")
                            novo_venda = c_venda.number_input("Venda:", value=float(df.at[idx_prod, 'preco_venda']), format="%.2f")
                          
                            c_qtd, c_acao = st.columns([1, 2])
                            qtd_input = c_qtd.number_input("Quantidade:", min_value=0, value=0)
                            acao = c_acao.radio("Ação:", ["Somar (+) Entrada", "Substituir (=) Correção", "Apenas Salvar Dados"], index=2)
                          
                            if st.form_submit_button("💾 SALVAR"):
                                df.at[idx_prod, 'nome do produto'] = c_nome.upper().strip()
                                df.at[idx_prod, 'validade'] = pd.to_datetime(nova_val) if nova_val else None
                                df.at[idx_prod, 'preco_custo'] = novo_custo
                                df.at[idx_prod, 'preco_venda'] = novo_venda
                                if c_forn: df.at[idx_prod, 'ultimo_fornecedor'] = c_forn
                              
                                if acao.startswith("Somar") and qtd_input > 0:
                                    df.at[idx_prod, 'qtd_central'] += qtd_input
                                    dt_full = datetime.combine(dt_reg, hr_reg)
                                    hist = {'data': dt_full, 'produto': c_nome.upper().strip(), 'fornecedor': c_forn, 'qtd': qtd_input, 'preco_pago': novo_custo, 'total_gasto': qtd_input * novo_custo}
                                    df_hist = pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input
                              
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(c_nome.upper().strip(), df.at[idx_prod, 'qtd_central'], novo_custo, novo_venda, pd.to_datetime(nova_val) if nova_val else None, prefixo)
                                st.success("Salvo!"); st.rerun()
    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Visão Geral (Editável)")
        if not df.empty:
            st.info("💡 DICA: Se um produto veio errado, corrija aqui.")
            busca_geral = st.text_input("🔍 Buscar:", placeholder="Ex: oleo...", key="busca_geral")
            df_visual_geral = filtrar_dados_inteligente(df, 'nome do produto', busca_geral)
            df_edit = st.data_editor(df_visual_geral, use_container_width=True, num_rows="dynamic", key="geral_editor")
          
            c1, c2 = st.columns(2)
            with c1:
                if st.button("💾 SALVAR ALTERAÇÕES GERAIS"):
                    indices_originais = df_visual_geral.index.tolist()
                    indices_editados = df_edit.index.tolist()
                    indices_removidos = list(set(indices_originais) - set(indices_editados))
                    if indices_removidos:
                        df = df.drop(indices_removidos)
                    df.update(df_edit)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                  
                    bar = st.progress(0); total = len(df_edit)
                    for i, (idx, row) in enumerate(df_edit.iterrows()):
                        atualizar_casa_global(df.at[idx, 'nome do produto'], row['qtd_central'], row['preco_custo'], row['preco_venda'], row['validade'], prefixo)
                        bar.progress((i+1)/total)
                    st.success("Tabela Geral atualizada!")
                    st.rerun()
            with c2:
                if st.button("🔮 CORRIGIR NOMES E UNIFICAR"):
                    df.update(df_edit)
                    qtd_antes = len(df)
                    df = unificar_produtos_por_codigo(df)
                    qtd_depois = len(df)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success(f"✅ {qtd_antes - qtd_depois} produtos duplicados foram unidos.")
                    st.balloons(); st.rerun()
