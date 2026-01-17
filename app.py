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

# --- FUSO HORÁRIO AMAZONAS (UTC -4) ---
FUSO_HORARIO = -4

def agora_am():
    """Retorna data/hora ajustada para o Amazonas"""
    return datetime.utcnow() + timedelta(hours=FUSO_HORARIO)

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

# --- CONEXÃO SEGURA (MODO HÍBRIDO) ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Tenta ler como dicionário (novo padrão) ou json string (velho padrão)
    try:
        if isinstance(st.secrets["service_account_json"], str):
            json_creds = json.loads(st.secrets["service_account_json"])
        else:
            json_creds = dict(st.secrets["service_account_json"])
            
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro na conexão com Google: {e}")
        st.stop()

# --- FUNÇÃO DE MATEMÁTICA CORRIGIDA (SOLUÇÃO DO ERRO 599) ---
def converter_ptbr(valor):
    """
    Função BLINDADA para converter preços.
    Evita que 5.99 vire 599.
    """
    if valor is None or str(valor).strip() == "": return 0.0
    
    # Se já for número (float ou int), devolve como float puro
    if isinstance(valor, (float, int)):
        return float(valor)

    s = str(valor).strip().upper().replace('R$', '').strip()
    
    # Se tem vírgula, assume que é decimal brasileiro (ex: 5,99)
    if "," in s:
        s = s.replace(".", "")  # Remove ponto de milhar (1.000 -> 1000)
        s = s.replace(",", ".") # Troca vírgula por ponto (1000,00 -> 1000.00)
    
    # Se não tem vírgula, mas tem ponto (ex: 5.99), o Python já entende.
    # O problema antigo era remover esse ponto. Agora não removemos.
    
    try:
        return float(s)
    except:
        return 0.0

def format_br(valor):
    """Exibe R$ 1.000,00 corretamente"""
    if not isinstance(valor, (float, int)): return "0,00"
    try:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(valor)

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

# --- LEITURA DA NUVEM ---
@st.cache_data(ttl=10)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.5) 
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

        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)
        df = garantir_integridade_colunas(df, colunas_padrao)

        # Tratamento especial para Datas
        for col in df.columns:
            if 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df
    except Exception as e:
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

        # Prepara cópia para salvar
        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)

        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                if 'validade' in col.lower():
                    df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
                else:
                    df_save[col] = df_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Converte NaN para string vazia ou 0.0
        for col in df_save.columns:
            if pd.api.types.is_numeric_dtype(df_save[col]):
                df_save[col] = df_save[col].fillna(0.0)
            else:
                df_save[col] = df_save[col].fillna("")

        ws.clear()
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🧠 LÓGICA DE NEGÓCIO
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_texto(nome_xml).split())
    set_sis = set(normalizar_texto(nome_sistema).split())
    comum = set_xml.intersection(set_sis)
    if not comum: return 0.0
    total = set_xml.union(set_sis)
    return len(comum) / len(total)

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor_match = None; maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score: maior_score = score; melhor_match = opcao
    if maior_score >= cutoff: return melhor_match, "Nome Similar"
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
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if not sem_codigo.empty:
        df_novo = pd.concat([df_novo, sem_codigo], ignore_index=True)
    return df_novo

def processar_excel_oficial(arquivo_subido):
    try:
        if arquivo_subido.name.endswith('.csv'): df_temp = pd.read_csv(arquivo_subido)
        else: df_temp = pd.read_excel(arquivo_subido)
        df_temp.columns = df_temp.columns.str.strip()
        col_nome = next((c for c in df_temp.columns if 'nome' in c.lower()), 'Nome')
        col_cod = next((c for c in df_temp.columns if 'código' in c.lower() or 'barras' in c.lower()), 'Código de Barras')
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
                if qtd_nova_casa is not None: df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml); root = tree.getroot()
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    # Tenta pegar fornecedor e nota
    try: nNF = root.find('.//nfe:nNF', ns).text 
    except: 
        try: nNF = root.find('.//nNF').text
        except: nNF = "S/N"
    try: xNome = root.find('.//nfe:emit/nfe:xNome', ns).text
    except: 
        try: xNome = root.find('.//emit/xNome').text
        except: xNome = "Fornecedor XML"
    
    itens = []
    det_tags = root.findall('.//nfe:det', ns)
    if not det_tags: det_tags = root.findall('.//det')

    for det in det_tags:
        prod = det.find('nfe:prod', ns) if det.find('nfe:prod', ns) is not None else det.find('prod')
        if prod is not None:
            def get_val(tag):
                el = prod.find(f'nfe:{tag}', ns)
                if el is None: el = prod.find(tag)
                return el.text if el is not None else None
            
            ean = get_val('cEAN') or ""
            if ean == "SEM GTIN": ean = ""
            nome = get_val('xProd') or "Produto Sem Nome"
            
            try: qCom = float(get_val('qCom'))
            except: qCom = 0.0
            try: vProd = float(get_val('vProd'))
            except: vProd = 0.0
            try: vDesc = float(get_val('vDesc')) if get_val('vDesc') else 0.0
            except: vDesc = 0.0
            
            p_liq = (vProd - vDesc) / qCom if qCom > 0 else 0.0
            p_bruto = vProd / qCom if qCom > 0 else 0.0
            
            itens.append({
                'nome': normalizar_texto(nome),
                'qtd': qCom,
                'ean': ean,
                'preco_un_liquido': p_liq,
                'preco_un_bruto': p_bruto,
                'desconto_total_item': vDesc
            })
    return {'numero': nNF, 'fornecedor': xNome, 'data': agora_am(), 'itens': itens}

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
            hoje = agora_am(); df_valido = df[pd.notnull(df['validade'])].copy()
            df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
            valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
            c2.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            
            bajo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not bajo_estoque.empty: st.warning(f"🚨 Existem {len(bajo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras'.")
            
    # 1.5 PICKLIST
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência - {loja_atual}")
        arquivo_pick = st.file_uploader("📂 Subir Picklist (.xlsx)", type=['xlsx', 'xls'])
        if arquivo_pick:
            try:
                df_pick = pd.read_excel(arquivo_pick)
                df_pick.columns = df_pick.columns.str.strip().str.lower()
                col_barras = next((c for c in df_pick.columns if 'barras' in c), None)
                col_qtd = next((c for c in df_pick.columns if 'transferir' in c), None)
                
                if col_barras and col_qtd and st.button("🚀 PROCESSAR"):
                    movidos = 0
                    for i, row in df_pick.iterrows():
                        cod_pick = str(row[col_barras]).replace('.0', '').strip()
                        qtd_pick = converter_ptbr(row[col_qtd])
                        if qtd_pick > 0:
                            mask = df['código de barras'] == cod_pick
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] -= qtd_pick
                                df.at[idx, 'qtd.estoque'] += qtd_pick
                                movidos += 1
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success(f"✅ {movidos} produtos movidos!")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 LISTA COMPRAS
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento")
        if st.button("🚀 Gerar Lista Automática (Baixo Estoque)"):
            mask = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
            prods = df[mask]
            if not prods.empty:
                novos = []
                for _, r in prods.iterrows():
                    novos.append({'produto': r['nome do produto'], 'qtd_sugerida': r['qtd_minima']*3, 'fornecedor': r['ultimo_fornecedor'], 'custo_previsto': r['preco_custo'], 'data_inclusao': str(agora_am()), 'status': 'A Comprar'})
                df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos)], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_lista_compras", df_lista_compras, COLS_LISTA)
                st.success("Lista gerada!"); st.rerun()
        st.dataframe(df_lista_compras, use_container_width=True)

    # 2. CADASTRAR PRODUTO
    elif modo == "🆕 Cadastrar Produto":
        st.title("🆕 Novo Produto")
        with st.form("cad"):
            c1, c2 = st.columns(2)
            cod = c1.text_input("Código")
            nome = c2.text_input("Nome")
            c3, c4 = st.columns(2)
            custo = c3.number_input("Custo", format="%.2f")
            venda = c4.number_input("Venda", format="%.2f")
            if st.form_submit_button("Salvar"):
                novo = {c: 0 for c in COLUNAS_VITAIS}
                novo.update({'código de barras': cod, 'nome do produto': nome.upper(), 'preco_custo': custo, 'preco_venda': venda})
                df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success("Salvo!")

    # 2.5 IMPORTAR XML
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title("📥 Importar XML")
        arquivo = st.file_uploader("Upload XML", type=['xml'])
        if arquivo:
            dados = ler_xml_nfe(arquivo, df_oficial)
            st.info(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
            
            with st.form("xml_form"):
                processar = []
                for i, item in enumerate(dados['itens']):
                    st.write(f"**{item['nome']}** | Qtd: {item['qtd']} | Custo: {format_br(item['preco_un_liquido'])}")
                    opcoes = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique())
                    escolha = st.selectbox("Vincular a:", opcoes, key=f"sel_{i}")
                    processar.append({'xml': item, 'escolha': escolha})
                
                if st.form_submit_button("✅ Salvar Entrada"):
                    for p in processar:
                        it = p['xml']; esc = p['escolha']
                        if esc == "(CRIAR NOVO)":
                            novo = {c: 0 for c in COLUNAS_VITAIS}
                            novo.update({'código de barras': it['ean'], 'nome do produto': it['nome'], 'qtd_central': it['qtd'], 'preco_custo': it['preco_un_liquido'], 'preco_venda': it['preco_un_liquido']*1.5, 'ultimo_fornecedor': dados['fornecedor']})
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        else:
                            idx = df[df['nome do produto'] == esc].index[0]
                            df.at[idx, 'qtd_central'] += it['qtd']
                            df.at[idx, 'preco_custo'] = it['preco_un_liquido']
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success("Estoque Atualizado!"); st.rerun()

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        termo = st.text_input("🔍 Buscar:", "")
        if termo:
            res = df[df['nome do produto'].str.contains(termo.upper())]
            for idx, row in res.iterrows():
                with st.container(border=True):
                    st.subheader(row['nome do produto'])
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Loja", int(row['qtd.estoque']))
                    c2.metric("Casa", int(row['qtd_central']))
                    c3.metric("Preço", format_br(row['preco_venda']))
                    if row['qtd_central'] > 0:
                        if st.button(f"⬇️ Baixar 1 un (ID: {idx})"):
                            df.at[idx, 'qtd.estoque'] += 1
                            df.at[idx, 'qtd_central'] -= 1
                            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                            st.rerun()

    # 6. FORNECEDOR
    elif modo == "🛒 Fornecedor (Compras)":
        st.title("🛒 Compras Pendentes")
        st.info("Aqui aparecerão itens marcados como 'PENDENTE'.")

    # 7. HISTÓRICO
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico")
        st.dataframe(df_hist, use_container_width=True)

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title("🏡 Estoque Central")
        df_edit = st.data_editor(df[['nome do produto', 'qtd_central', 'preco_custo']], use_container_width=True)
        if st.button("Salvar Central"):
            # Atualiza DF original com os editados
            for idx, row in df_edit.iterrows():
                df.at[idx, 'qtd_central'] = row['qtd_central']
                df.at[idx, 'preco_custo'] = row['preco_custo']
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success("Salvo!")

    # 9. GERAL (COM BOTÃO DE EMERGÊNCIA)
    elif modo == "📋 Tabela Geral":
        st.title("📋 Visão Geral (Editável)")
        st.warning("⚠️ Edite os valores abaixo e clique em Salvar.")
        
        # Botão de Emergência para consertar os 599
        with st.expander("🆘 CORREÇÃO DE PREÇOS (CLIQUE AQUI SE TIVER VALORES ERRADOS)"):
            st.write("Se o Milho Verde aparece como 599 ao invés de 5.99, clique no botão abaixo. Ele vai dividir por 100 todos os preços absurdos.")
            if st.button("🔧 Dividir Preços Grandes por 100"):
                afetados = 0
                for col in ['preco_custo', 'preco_venda']:
                    # Só altera se for maior que 50 (assumindo que nenhum item custa mais de 50 reais que seja grocery)
                    # Você pode ajustar esse limite
                    mask = df[col] > 50 
                    if mask.any():
                        df.loc[mask, col] = df.loc[mask, col] / 100
                        afetados += mask.sum()
                
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                st.success(f"Correção aplicada em {afetados} preços! Verifique a tabela abaixo.")
                time.sleep(2)
                st.rerun()

        # Editor normal
        df_edit = st.data_editor(
            df, 
            num_rows="dynamic", 
            use_container_width=True,
            column_config={
                "preco_venda": st.column_config.NumberColumn("Venda", format="R$ %.2f"),
                "preco_custo": st.column_config.NumberColumn("Custo", format="R$ %.2f"),
            }
        )
        
        c1, c2 = st.columns(2)
        if c1.button("💾 SALVAR TUDO"):
            salvar_na_nuvem(f"{prefixo}_estoque", df_edit, COLUNAS_VITAIS)
            st.success("Dados salvos e corrigidos!")
            st.rerun()
            
        if c2.button("🔮 Unificar Duplicados"):
            df = unificar_produtos_por_codigo(df_edit)
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success("Unificado!")
            st.rerun()
