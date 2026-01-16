import streamlit as st
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time
import re

# Removido locale.setlocale para evitar erro em ambientes sem suporte a 'pt_BR.UTF-8'

# Função para formatar números no estilo brasileiro (milhar '.', decimal ',')
def format_br(valor):
    try:
        if pd.isna(valor) or valor == "":
            return "0,00"
        val_float = float(valor)
        s = f"{val_float:,.2f}"  # Formata com , para milhar e . para decimal
        return s.replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "0,00"

# CONFIGURAÇÃO DE NUVEM & SISTEMA
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# DEFINIÇÃO DE COLUNAS OBRIGATÓRIAS (GLOBAL)
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

# CONEXÃO SEGURA
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        json_creds = json.loads(st.secrets["service_account_json"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
        return gspread.authorize(creds)
    except:
        return None

# Função de limpeza e conversão de números
def converter_ptbr(valor):
    """
    Converte valores brasileiros para padrão decimal correto.
    Aceita números com vírgula e ponto como separadores e trata casos de valores inteiros altos.
    """
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    s = str(valor).strip().upper().replace('R$', '').replace(' ', '')
    s = re.sub(r"[^\d\.,\-]", "", s)
    if s in {"", "-", ".", ","}:
        return 0.0
    if ',' in s:
        if '.' in s and s.find('.') < s.find(','):
            s = s.replace('.', '')
        s = s.replace(',', '.')
        try:
            return float(s)
        except:
            return 0.0
    try:
        num = float(s)
        return num
    except:
        s_limpo = re.sub(r'[^\d\.\-]', '', s)
        try:
            return float(s_limpo)
        except:
            return 0.0

# Função de cura
def garantir_integridade_colunas(df, colunas_alvo):
    if df.empty:
        return pd.DataFrame(columns=colunas_alvo)
    df.columns = df.columns.str.strip().str.lower()
    for col in colunas_alvo:
        col_l = str(col).strip().lower()
        if col_l not in df.columns:
            if any(x in col_l for x in ['qtd', 'preco', 'valor', 'custo', 'total']):
                df[col_l] = 0.0
            elif 'data' in col_l or 'validade' in col_l:
                df[col_l] = None
            else:
                df[col_l] = ""
    for col in df.columns:
        col_l = str(col).lower()
        if any(x in col_l for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
            def fix(v):
                num = converter_ptbr(v)
                if any(p in col_l for p in ['preco', 'custo', 'venda', 'valor', 'total']):
                    if (num % 1 == 0) and num >= 1000:
                        return num / 100.0
                return num
            df[col] = df[col].apply(fix)
        elif 'data' in col_l or 'validade' in col_l:
            pass
        else:
            df[col] = df[col].fillna("")
    return df

# Funções de normalização e busca
def normalizar_texto(texto):
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    texto_norm = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto_norm.upper().strip()

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca:
        return df
    texto_buscado = normalizar_texto(texto_busca)
    mask = df[coluna_busca].astype(str).apply(lambda x: texto_buscado in normalizar_texto(x))
    return df[mask]

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_texto(nome_xml).split())
    set_sis = set(normalizar_texto(nome_sistema).split())
    comum = set_xml.intersection(set_sis)
    if not comum:
        return 0.0
    total = set_xml.union(set_sis)
    return len(comum) / len(total)

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor = None
    maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)":
            continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score:
            maior_score = score
            melhor = opcao
    if maior_score >= cutoff:
        return melhor, "Similaridade"
    return None, "Nenhum"

# Integração Google Sheets
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(0.5)
    client = get_google_client()
    if not client:
        return pd.DataFrame(columns=colunas_padrao)
    try:
        sh = client.open("loja_dados")
    except:
        return pd.DataFrame(columns=colunas_padrao)
    try:
        ws = sh.worksheet(nome_aba)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
        ws.append_row(colunas_padrao)
        return pd.DataFrame(columns=colunas_padrao)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=colunas_padrao)
    colunas_totais = list(df.columns)
    for col in colunas_padrao:
        if col not in colunas_totais:
            colunas_totais.append(col)
    df = garantir_integridade_colunas(df, colunas_totais)
    for col in df.columns:
        if 'data' in col or 'validade' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    client = get_google_client()
    if not client:
        st.error("Falha na conexão com Google Sheets.")
        return
    sh = client.open("loja_dados")
    try:
        ws = sh.worksheet(nome_aba)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
    df_save = df.copy()
    for col in df_save.columns:
        if pd.api.types.is_datetime64_any_dtype(df_save[col]):
            df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')
        elif pd.api.types.is_numeric_dtype(df_save[col]):
            df_save[col] = df_save[col].fillna(0.0)
        else:
            df_save[col] = df_save[col].fillna("")
    ws.clear()
    ws.update([df_save.columns.values.tolist()] + df_save.values.tolist(), value_input_option="RAW")
    ler_da_nuvem.clear()

# XML Parser
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element):
        return element.tag.split('}')[-1]
    dados_nota = {'numero':'S/N','fornecedor':'IMPORTADO','data':datetime.now(),'itens':[]}
    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF':
            dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == 'IMPORTADO':
            dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi':
            try:
                dados_nota['data'] = pd.to_datetime(elem.text[:10])
            except:
                pass
    dets = [e for e in root.iter() if tag_limpa(e) == 'det']
    for det in dets:
        try:
            prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
            if prod:
                item = {'ean':'','nome':'','qtd':0.0,'preco_un_liquido':0.0,'preco_un_bruto':0.0,'desconto_total_item':0.0}
                vProd = 0.0; vDesc = 0.0; qCom = 0.0; cEAN = ''; cProd = ''
                for info in prod:
                    t = tag_limpa(info)
                    if t == 'cProd':
                        cProd = info.text
                    elif t == 'cEAN':
                        cEAN = info.text
                    elif t == 'xProd':
                        item['nome'] = normalizar_texto(info.text)
                    elif t == 'qCom':
                        qCom = converter_ptbr(info.text)
                    elif t == 'vProd':
                        vProd = converter_ptbr(info.text)
                    elif t == 'vDesc':
                        vDesc = converter_ptbr(info.text)
                item['ean'] = cEAN if cEAN not in ['SEM GTIN','','None'] else cProd
                if qCom > 0:
                    item['qtd'] = qCom
                    item['preco_un_bruto'] = vProd / qCom
                    item['desconto_total_item'] = vDesc
                    item['preco_un_liquido'] = (vProd - vDesc) / qCom
                    dados_nota['itens'].append(item)
        except:
            continue
    return dados_nota

# INÍCIO DO APP (INTERFACE STREAMLIT)
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True, help="Melhora a visualização para iPhone/Android")

prefixo = "loja1" if "Principal" in loja_atual else "loja2" if "Filial" in loja_atual else "loja3"

# Carregamento de dados
df = ler_da_nuvem(f"{prefixo}_estoque", COLUNAS_VITAIS)
df_hist = ler_da_nuvem(f"{prefixo}_historico_compras", COLS_HIST)
df_mov = ler_da_nuvem(f"{prefixo}_movimentacoes", COLS_MOV)
df_vendas = ler_da_nuvem(f"{prefixo}_vendas", COLS_VENDAS)
df_lista = ler_da_nuvem(f"{prefixo}_lista_compras", COLS_LISTA)
df_oficial = ler_da_nuvem("base_oficial", COLS_OFICIAL)

# Menu
modo = st.sidebar.radio("Navegar:", [
    "📊 Dashboard",
    "📥 Importar XML",
    "🔄 Sincronizar (Planograma)",
    "🏠 Gôndola (Loja)",
    "🏡 Estoque Central",
    "💰 Histórico & Preços",
    "📋 Tabela Geral"
])

if modo == "📊 Dashboard":
    st.title(f"📊 Painel de Controle - {loja_atual}")
    if df.empty:
        st.info("Comece cadastrando produtos.")
    else:
        valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
        col2.metric("🏡 Itens na Casa", int(df['qtd_central'].sum()))
        col3.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
        st.markdown("### Produtos abaixo do mínimo")
        abaixo_min = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
        if abaixo_min.empty:
            st.success("Nenhum produto abaixo do mínimo")
        else:
            st.warning(f"{len(abaixo_min)} produtos com estoque baixo")
            st.dataframe(
                abaixo_min[['nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima']],
                use_container_width=True
            )

elif modo == "📥 Importar XML":
    st.title("📥 Importar XML (Nota Fiscal)")
    st.info("O sistema tentará encontrar produtos. Confirme se o vínculo está correto antes de salvar.")
    xml_file = st.file_uploader("Arraste o arquivo XML da NFe:", type=['xml'])
    if xml_file:
        dados = ler_xml_nfe(xml_file, df_oficial)
        st.success(f"Nota Fiscal: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
        st.markdown("---")
        st.subheader("Itens encontrados")
        if not dados['itens']:
            st.warning("Nenhum item no XML.")
        else:
            lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
            escolhas = {}
            for i, item in enumerate(dados['itens']):
                with st.container(border=True):
                    c1, c2 = st.columns([2, 1])
                    c1.markdown(f"**{item['nome']}**")
                    c1.caption(f"EAN: {item['ean']} | Qtd: {int(item['qtd'])}")
                    c1.caption(f"Preço Unit. Pago: R$ {format_br(item['preco_un_liquido'])} | Sem Desc: R$ {format_br(item['preco_un_bruto'])}")
                    match_inicial = "(CRIAR NOVO)"
                    tipo_match = "Nenhum"
                    ean_xml = str(item['ean']).strip()
                    if not df.empty:
                        m_ean = df['código de barras'].astype(str) == ean_xml
                        if m_ean.any():
                            match_inicial = df.loc[m_ean, 'nome do produto'].values[0]
                            tipo_match = "EAN"
                        else:
                            melhor_nome, tipo_m = encontrar_melhor_match(item['nome'], lista_produtos_sistema)
                            if melhor_nome:
                                match_inicial = melhor_nome
                                tipo_match = tipo_m
                    idx_inicial = lista_produtos_sistema.index(match_inicial) if match_inicial in lista_produtos_sistema else 0
                    escolhas[i] = c2.selectbox(f"Match ({tipo_match})", lista_produtos_sistema, index=idx_inicial, key=f"sel_{i}")
            if st.button("✅ Confirmar e salvar"):
                novos_hist = []
                criados = 0
                atualizados = 0
                for i, item in enumerate(dados['itens']):
                    prod_escolhido = escolhas[i]
                    q = item['qtd']
                    p_pago = item['preco_un_liquido']
                    p_bruto = item['preco_un_bruto']
                    nm = item['nome']
                    ean = item['ean']
                    nome_final = ""
                    if prod_escolhido == "(CRIAR NOVO)":
                        novo = {
                            'código de barras': ean,
                            'nome do produto': nm.upper(),
                            'qtd.estoque': 0,
                            'qtd_central': q,
                            'qtd_minima': 5,
                            'validade': None,
                            'status_compra': 'OK',
                            'qtd_comprada': 0,
                            'preco_custo': p_pago,
                            'preco_venda': p_pago * 2,
                            'categoria': 'GERAL',
                            'ultimo_fornecedor': dados['fornecedor'],
                            'preco_sem_desconto': p_bruto
                        }
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        criados += 1
                        nome_final = nm.upper()
                    else:
                        m = df['nome do produto'] == prod_escolhido
                        if m.any():
                            idx = df[m].index[0]
                            df.at[idx, 'qtd_central'] += q
                            df.at[idx, 'preco_custo'] = p_pago
                            df.at[idx, 'preco_sem_desconto'] = p_bruto
                            df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                            atualizados += 1
                            nome_final = prod_escolhido
                    if nome_final:
                        novos_hist.append({
                            'data': dados['data'],
                            'produto': nome_final,
                            'fornecedor': dados['fornecedor'],
                            'qtd': q,
                            'preco_pago': p_pago,
                            'total_gasto': q * p_pago,
                            'numero_nota': dados['numero'],
                            'desconto_total_money': item.get('desconto_total_item', 0.0),
                            'preco_sem_desconto': p_bruto
                        })
                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                if novos_hist:
                    df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                st.success(f"Processado! Novos: {criados}, Atualizados: {atualizados}")
                st.balloons()
                st.experimental_rerun()

elif modo == "🔄 Sincronizar (Planograma)":
    st.title("🔄 Sincronizar Planograma")
    st.info("Importa arquivos de planograma, preservando colunas extras.")
    arquivo = st.file_uploader("Arquivo Planograma (XLSX ou CSV)", type=['xlsx','csv'])
    if arquivo:
        if st.button("🚀 Sincronizar"):
            salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
            st.success("Sincronização completa!")
            st.experimental_rerun()

elif modo == "🏠 Gôndola (Loja)":
    st.title("🏠 Gôndola")
    termo = st.text_input("Buscar produto (nome ou código):")
    df_filtrado = filtrar_dados_inteligente(df, 'nome do produto', termo)
    if df_filtrado.empty:
        st.info("Nenhum produto encontrado.")
    else:
        for idx, row in df_filtrado.iterrows():
            with st.container(border=True):
                st.subheader(row['nome do produto'])
                col1, col2, col3 = st.columns(3)
                col1.metric("Loja", int(row['qtd.estoque']))
                col2.metric("Casa", int(row['qtd_central']))
                col3.metric("Preço", f"R$ {format_br(row['preco_venda'])}")

elif modo == "🏡 Estoque Central":
    st.title("🏡 Estoque Central")
    st.dataframe(
        df[['nome do produto', 'qtd_central', 'preco_custo', 'ultimo_fornecedor']],
        use_container_width=True
    )

elif modo == "💰 Histórico & Preços":
    st.title("💰 Histórico de Compras e Preços")
    if df_hist.empty:
        st.info("Nenhum histórico encontrado.")
    else:
        df_show = df_hist.copy()
        for col in ['preco_pago','total_gasto','preco_sem_desconto','desconto_total_money']:
            if col in df_show.columns:
                df_show[col] = df_show[col].apply(format_br)
        st.dataframe(df_show.sort_values(by='data', ascending=False), use_container_width=True)

elif modo == "📋 Tabela Geral":
    st.title("📋 Tabela Geral (Editável)")
    if df.empty:
        st.info("Sem produtos cadastrados.")
    else:
        st.info("Edite a tabela e clique em Salvar.")
        df_editavel = st.data_editor(df, use_container_width=True, num_rows="dynamic")
        if st.button("💾 Salvar Tudo"):
            df_temp = df_editavel.copy()
            for col in df_temp.columns:
                col_l = str(col).lower()
                if any(x in col_l for x in ['preco','custo','venda','valor','total']):
                    df_temp[col] = df_temp[col].apply(converter_ptbr)
            salvar_na_nuvem(f"{prefixo}_estoque", df_temp, COLUNAS_VITAIS)
            st.success("Tabela salva!")
            st.experimental_rerun()
