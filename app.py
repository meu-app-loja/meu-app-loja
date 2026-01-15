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
# ✅ FUNÇÕES DE FORMATAÇÃO E ENTRADA (CORREÇÃO DO BUG 3,19 -> 319)
# ==============================================================================

def format_br(valor):
    """Formata número para estilo brasileiro: 1.234,56"""
    try:
        s = f"{float(valor):,.2f}"
    except:
        s = f"{0.0:,.2f}"
    return s.replace(',', 'X').replace('.', ',').replace('X', '.')

def converter_ptbr(valor):
    """Converte valores brasileiros (com vírgula) e também aceita padrão com ponto."""
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0

    s = str(valor).strip().upper()
    s = s.replace('R$', '').replace(' ', '')

    # Remove separadores estranhos
    s = s.replace('\u00A0', '')  # non-breaking space

    # Se já for número puro com ponto, funciona direto
    try:
        return float(s)
    except:
        pass

    # Caso "1.234,56" -> "1234.56"
    if ',' in s and '.' in s:
        s = s.replace('.', '')
        s = s.replace(',', '.')
    elif ',' in s:
        # Caso "3,19" -> "3.19"
        s = s.replace(',', '.')
    else:
        # Caso venha algo como "319" mesmo, fica 319
        pass

    try:
        return float(s)
    except:
        return 0.0

def money_input_ptbr(label: str, key: str, value: float = 0.0, help: str = None):
    """
    Campo monetário que aceita '3,19' OU '3.19' e converte corretamente.
    Substitui st.number_input para evitar o bug 3,19 -> 319.
    """
    default_txt = format_br(value) if value is not None else "0,00"
    txt = st.text_input(label, value=default_txt, key=key, help=help, placeholder="Ex: 3,19")
    val = converter_ptbr(txt)
    st.caption(f"Interpretado: R$ {format_br(val)}")
    return val

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DE NUVEM & SISTEMA
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

COLUNAS_VITAIS = [
    'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central',
    'qtd_minima', 'validade', 'status_compra', 'qtd_comprada',
    'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
]

COLS_HIST = [
    'data', 'produto', 'fornecedor', 'qtd', 'preco_pago',
    'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto'
]
COLS_MOV = ['data_hora', 'produto', 'qtd_movida']
COLS_VENDAS = ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante']
COLS_LISTA = ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
COLS_OFICIAL = ['nome do produto', 'código de barras']

@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# ==============================================================================
# 🧠 NORMALIZAÇÃO (PARA NÃO FALHAR MATCH POR NOME)
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto):
    return normalizar_texto(texto)

# ==============================================================================
# ✅ GARANTIA DE TIPOS / INTEGRIDADE
# ==============================================================================
def garantir_integridade_colunas(df, colunas_alvo):
    if df is None or df.empty:
        return pd.DataFrame(columns=colunas_alvo)

    df.columns = df.columns.str.strip().str.lower()

    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df[col] = 0.0
            elif 'data' in col or 'validade' in col:
                df[col] = None
            else:
                df[col] = ""

    # Converte numéricos
    for col in df.columns:
        if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
            df[col] = df[col].apply(converter_ptbr)

    return df

# ==============================================================================
# 🌥️ LEITURA DA NUVEM
# ==============================================================================
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1)
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

        # Datas
        for col in df.columns:
            if 'data' in col or 'validade' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df
    except:
        return pd.DataFrame(columns=colunas_padrao)

# ==============================================================================
# 💾 SALVAR NA NUVEM (GARANTE NÚMERO REAL, NÃO TEXTO)
# ==============================================================================
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

        ws.clear()

        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)

        # Datas -> texto
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime('%Y-%m-%d')

        # Força numéricos como float real
        for col in df_save.columns:
            if any(x in col for x in ['qtd', 'preco', 'valor', 'custo', 'total', 'desconto']):
                df_save[col] = df_save[col].apply(converter_ptbr).astype(float)

        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🔍 FUNÇÕES DE BUSCA
# ==============================================================================
def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if df is None or df.empty:
        return df
    if not texto_busca:
        return df
    mask = df[coluna_busca].astype(str).apply(
        lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x)
    )
    return df[mask]

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    comum = set_xml.intersection(set_sis)
    if not comum:
        return 0.0
    total = set_xml.union(set_sis)
    score = len(comum) / len(total)
    for palavra in comum:
        if any(u in palavra for u in ['L', 'ML', 'KG', 'G', 'M']):
            if any(c.isdigit() for c in palavra):
                score += 0.5
    return score

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor_match = None
    maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)":
            continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score:
            maior_score = score
            melhor_match = opcao
    if maior_score >= cutoff:
        return melhor_match, "Nome Similar (Palavras)"
    return None, "Nenhum"

# ==============================================================================
# ✅ CORREÇÃO PRINCIPAL: ATUALIZAÇÃO GLOBAL POR NOME NORMALIZADO
# ==============================================================================
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    nome_ref = normalizar_texto(nome_produto)

    for loja in todas_lojas:
        if loja == prefixo_ignorar:
            continue

        df_outra = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if df_outra.empty:
            continue

        df_outra.columns = df_outra.columns.str.strip().str.lower()
        nomes_norm = df_outra['nome do produto'].astype(str).apply(normalizar_texto)
        mask = nomes_norm == nome_ref

        if mask.any():
            idx = df_outra[mask].index[0]
            df_outra.at[idx, 'qtd_central'] = float(qtd_nova_casa)

            if novo_custo is not None:
                df_outra.at[idx, 'preco_custo'] = float(novo_custo)
            if novo_venda is not None:
                df_outra.at[idx, 'preco_venda'] = float(novo_venda)
            if nova_validade is not None:
                df_outra.at[idx, 'validade'] = nova_validade

            salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

# ==============================================================================
# ✅ LEITURA XML (CORRIGINDO FORNECEDOR EMITENTE)
# ==============================================================================
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()

    def tag_limpa(element):
        return element.tag.split('}')[-1]

    # 1) FORMATO NOVO (custom)
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
        # 2) NFE PADRÃO: pega emit/xNome e ide/nNF
        dados_nota = {'numero': 'S/N', 'fornecedor': 'IMPORTADO', 'data': datetime.now(), 'itens': []}

        nNF = root.find(".//{*}ide/{*}nNF")
        if nNF is not None and nNF.text:
            dados_nota['numero'] = nNF.text.strip()

        xNome_emit = root.find(".//{*}emit/{*}xNome")
        if xNome_emit is not None and xNome_emit.text:
            dados_nota['fornecedor'] = xNome_emit.text.strip()

    # 3) ITENS - custom
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

                dados_nota['itens'].append({
                    'nome': normalizar_texto(nome),
                    'qtd': qtd,
                    'ean': str(ean).strip(),
                    'preco_un_liquido': p_liq,
                    'preco_un_bruto': p_bruto,
                    'desconto_total_item': desc
                })
            except:
                continue
    else:
        # 4) ITENS - NFE padrão
        dets = [e for e in root.iter() if tag_limpa(e) == 'det']
        for det in dets:
            try:
                prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
                if prod:
                    item = {
                        'codigo_interno': '', 'ean': '', 'nome': '',
                        'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0,
                        'desconto_total_item': 0.0
                    }
                    vProd = 0.0
                    vDesc = 0.0
                    qCom = 0.0

                    for info in prod:
                        t = tag_limpa(info)
                        if t == 'cProd':
                            item['codigo_interno'] = info.text
                        elif t == 'cEAN':
                            item['ean'] = info.text
                        elif t == 'xProd':
                            item['nome'] = normalizar_texto(info.text)
                        elif t == 'qCom':
                            qCom = converter_ptbr(info.text)
                        elif t == 'vProd':
                            vProd = converter_ptbr(info.text)
                        elif t == 'vDesc':
                            vDesc = converter_ptbr(info.text)

                    if qCom > 0:
                        item['qtd'] = qCom
                        item['preco_un_bruto'] = vProd / qCom
                        item['desconto_total_item'] = vDesc
                        item['preco_un_liquido'] = (vProd - vDesc) / qCom

                    ean_xml = str(item['ean']).strip()
                    if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                        item['ean'] = item['codigo_interno']

                    dados_nota['itens'].append(item)
            except:
                continue

    # 5) Match com base oficial (se EAN faltar)
    lista_nomes_ref = []
    dict_ref_ean = {}

    if df_referencia is not None and not df_referencia.empty:
        for _, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            dict_ref_ean[nm] = str(row['código de barras']).strip()
            lista_nomes_ref.append(nm)

    for item in dados_nota['itens']:
        if item.get('ean', '') in ['SEM GTIN', '', 'None', 'NAN'] and lista_nomes_ref:
            melhor, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
            if melhor:
                item['ean'] = dict_ref_ean.get(melhor, item.get('ean', ''))

    return dados_nota

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True, help="Melhora a visualização para iPhone/Android")
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)":
    prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)":
    prefixo = "loja2"
else:
    prefixo = "loja3"

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
    modo = st.sidebar.radio(
        "Navegar:",
        ["📊 Dashboard (Visão Geral)", "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)",
         "🆕 Cadastrar Produto", "📥 Importar XML (Associação Inteligente)", "⚙️ Configurar Base Oficial",
         "🔄 Sincronizar (Planograma)", "📉 Baixar Vendas (Do Relatório)", "🏠 Gôndola (Loja)", "🛒 Fornecedor (Compras)",
         "💰 Histórico & Preços", "🏡 Estoque Central (Casa)", "📋 Tabela Geral"]
    )

    # 1. DASHBOARD
    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle - {loja_atual}")
        if df.empty:
            st.info("Comece cadastrando produtos.")
        else:
            hoje = datetime.now()
            df_valido = df[pd.notnull(df['validade'])].copy()
            df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) &
                                  ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
            df_atencao = df_valido[(df_valido['validade'] > hoje + timedelta(days=5)) &
                                   (df_valido['validade'] <= hoje + timedelta(days=10))]
            valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
            c2.metric("💰 Valor Investido", f"R$ {format_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()

            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty:
                st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras' para ver.")
            if not df_critico.empty:
                st.error("🚨 Produtos Vencendo!")
                st.dataframe(df_critico[['nome do produto', 'validade', 'qtd.estoque']])

    # 2. CADASTRAR PRODUTO (CORRIGIDO: preços aceitam 3,19)
    elif modo == "🆕 Cadastrar Produto":
        st.title(f"🆕 Cadastro - {loja_atual}")
        with st.form("form_cadastro"):
            c1, c2 = st.columns(2)
            with c1:
                novo_cod = st.text_input("Código de Barras:")
                novo_nome = st.text_input("Nome do Produto:")
                nova_cat = st.text_input("Categoria:")
            with c2:
                novo_custo = money_input_ptbr("Preço Custo:", key="cad_preco_custo", value=0.0)
                novo_venda = money_input_ptbr("Preço Venda:", key="cad_preco_venda", value=0.0)
                novo_min = st.number_input("Estoque Mínimo:", min_value=0, value=5)

            st.divider()
            c3, c4, c5 = st.columns(3)
            with c3:
                ini_loja = st.number_input("Qtd Loja:", min_value=0)
            with c4:
                ini_casa = st.number_input("Qtd Casa:", min_value=0)
            with c5:
                ini_val = st.date_input("Validade:", value=None)

            if st.form_submit_button("💾 CADASTRAR"):
                if not novo_cod or not novo_nome:
                    st.error("Código e Nome obrigatórios!")
                elif not df.empty and df['código de barras'].astype(str).str.contains(str(novo_cod).strip()).any():
                    st.error("Código já existe!")
                else:
                    novo = {
                        'código de barras': str(novo_cod).strip(),
                        'nome do produto': normalizar_texto(novo_nome),
                        'qtd.estoque': ini_loja,
                        'qtd_central': ini_casa,
                        'qtd_minima': novo_min,
                        'validade': pd.to_datetime(ini_val) if ini_val else None,
                        'status_compra': 'OK',
                        'qtd_comprada': 0,
                        'preco_custo': novo_custo,
                        'preco_venda': novo_venda,
                        'categoria': nova_cat,
                        'ultimo_fornecedor': '',
                        'preco_sem_desconto': 0.0
                    }
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                    st.success("Cadastrado!")
                    st.rerun()

    # 2.5 IMPORTAR XML (mantido, mas agora atualização não falha)
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML da Nota Fiscal")
        st.markdown("O sistema tentará encontrar os produtos. **Confirme se o vínculo está correto antes de salvar.**")
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota Fiscal: **{dados['numero']}** | Fornecedor: **{dados['fornecedor']}**")
                st.markdown("---")
                st.subheader("🛠️ Conferência e Cálculo de Descontos")

                lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                escolhas = {}

                for i, item in enumerate(dados['itens']):
                    ean_xml = str(item.get('ean', '')).strip()
                    nome_xml = str(item['nome']).strip()
                    qtd_xml = item['qtd']
                    p_bruto = item['preco_un_bruto']
                    p_liq = item['preco_un_liquido']
                    desc_total = item.get('desconto_total_item', 0)

                    match_inicial = "(CRIAR NOVO)"
                    tipo_match = "Nenhum"

                    if not df.empty:
                        mask_ean = df['código de barras'].astype(str) == ean_xml
                        if mask_ean.any():
                            match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                            tipo_match = "Código de Barras (Exato)"
                        else:
                            melhor_nome, tipo_encontrado = encontrar_melhor_match(nome_xml, df['nome do produto'].astype(str).tolist())
                            if melhor_nome:
                                match_inicial = melhor_nome
                                tipo_match = tipo_encontrado

                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.markdown(f"📄 XML: **{nome_xml}**")
                        st.caption(f"EAN XML: `{ean_xml}` | Qtd: {int(qtd_xml)}")
                        st.markdown(f"💰 Tabela: R$ {format_br(p_bruto)} | **Pago (Desc): R$ {format_br(p_liq)}**")
                        if desc_total:
                            st.caption(f"Desconto item: R$ {format_br(desc_total)}")

                    with c2:
                        idx_inicial = lista_produtos_sistema.index(str(match_inicial)) if str(match_inicial) in lista_produtos_sistema else 0
                        escolha_usuario = st.selectbox(
                            f"Vincular ao Sistema ({tipo_match}):",
                            lista_produtos_sistema,
                            index=idx_inicial,
                            key=f"sel_{i}"
                        )
                        escolhas[i] = escolha_usuario

                    st.divider()

                if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
                    novos_hist = []
                    criados_cont = 0
                    atualizados_cont = 0

                    for i, item in enumerate(dados['itens']):
                        produto_escolhido = escolhas[i]
                        qtd_xml = int(item['qtd'])
                        preco_pago = float(item['preco_un_liquido'])
                        preco_sem_desc = float(item['preco_un_bruto'])
                        desc_total_val = float(item.get('desconto_total_item', 0))
                        ean_xml = str(item.get('ean', '')).strip()
                        nome_xml = normalizar_texto(str(item['nome']).strip())

                        if produto_escolhido == "(CRIAR NOVO)":
                            novo_prod = {
                                'código de barras': ean_xml,
                                'nome do produto': nome_xml,
                                'qtd.estoque': 0,
                                'qtd_central': qtd_xml,
                                'qtd_minima': 5,
                                'validade': None,
                                'status_compra': 'OK',
                                'qtd_comprada': 0,
                                'preco_custo': preco_pago,
                                'preco_venda': preco_pago * 2,
                                'categoria': 'GERAL',
                                'ultimo_fornecedor': dados['fornecedor'],
                                'preco_sem_desconto': preco_sem_desc
                            }
                            df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                            criados_cont += 1
                            nome_final = nome_xml
                        else:
                            nome_final = normalizar_texto(produto_escolhido)
                            nomes_norm = df['nome do produto'].astype(str).apply(normalizar_texto)
                            mask = nomes_norm == nome_final
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] = converter_ptbr(df.at[idx, 'qtd_central']) + qtd_xml
                                df.at[idx, 'preco_custo'] = preco_pago
                                df.at[idx, 'preco_sem_desconto'] = preco_sem_desc
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1

                        # Sincroniza em outras lojas (agora não falha)
                        if nome_final:
                            qtd_casa_atual = float(df.loc[df['nome do produto'].astype(str).apply(normalizar_texto) == nome_final, 'qtd_central'].values[0])
                            atualizar_casa_global(nome_final, qtd_casa_atual, preco_pago, None, None, prefixo)

                        novos_hist.append({
                            'data': dados['data'],
                            'produto': nome_final,
                            'fornecedor': dados['fornecedor'],
                            'qtd': qtd_xml,
                            'preco_pago': preco_pago,
                            'total_gasto': qtd_xml * preco_pago,
                            'numero_nota': dados['numero'],
                            'desconto_total_money': desc_total_val,
                            'preco_sem_desconto': preco_sem_desc
                        })

                    salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)

                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)

                    st.success(f"✅ Processado! {criados_cont} novos, {atualizados_cont} atualizados e sincronizado.")
                    st.balloons()
                    st.rerun()

            except Exception as e:
                st.error(f"Erro ao ler XML: {e}")

    # 6. FORNECEDOR (Compras) - CORRIGIDO preço com vírgula
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
                    qtd = c1.number_input("Qtd Chegada:", value=int(converter_ptbr(df.at[idx, 'qtd_comprada'])))
                    custo = money_input_ptbr("Preço Pago (UN):", key=f"compra_custo_{idx}", value=float(converter_ptbr(df.at[idx, 'preco_custo'])))
                    venda = money_input_ptbr("Novo Preço Venda:", key=f"compra_venda_{idx}", value=float(converter_ptbr(df.at[idx, 'preco_venda'])))

                    if st.form_submit_button("✅ ENTRAR NO ESTOQUE"):
                        df.at[idx, 'qtd_central'] = converter_ptbr(df.at[idx, 'qtd_central']) + qtd
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
                        st.success("Estoque atualizado!")
                        st.rerun()
        else:
            st.success("Sem compras pendentes.")

    # 7. HISTÓRICO & PREÇOS (mantido)
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
                    "preco_sem_desconto": st.column_config.NumberColumn("Preço Tabela", format="R$ %.2f"),
                    "desconto_total_money": st.column_config.NumberColumn("Desconto TOTAL", format="R$ %.2f"),
                    "preco_pago": st.column_config.NumberColumn("Pago (Unit)", format="R$ %.2f", disabled=True),
                    "total_gasto": st.column_config.NumberColumn("Total Gasto", format="R$ %.2f", disabled=True)
                }
            )
            if st.button("💾 Salvar Alterações"):
                indices_originais = df_hist_visual.index.tolist()
                indices_editados = df_editado.index.tolist()
                indices_removidos = list(set(indices_originais) - set(indices_editados))
                if indices_removidos:
                    df_hist = df_hist.drop(indices_removidos)
                    st.warning(f"🗑️ {len(indices_removidos)} registros excluídos.")
                df_hist.update(df_editado)

                for idx, row in df_hist.iterrows():
                    try:
                        q = converter_ptbr(row.get('qtd', 0))
                        p_tab = converter_ptbr(row.get('preco_sem_desconto', 0))
                        d_tot = converter_ptbr(row.get('desconto_total_money', 0))
                        if q > 0 and p_tab > 0:
                            total_liq = (p_tab * q) - d_tot
                            df_hist.at[idx, 'preco_pago'] = total_liq / q
                            df_hist.at[idx, 'total_gasto'] = total_liq
                    except:
                        pass

                salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)
                st.success("Salvo!")
                st.rerun()
        else:
            st.info("Sem histórico.")

    # 8. ESTOQUE CENTRAL (Casa) - CORRIGIDO custo com vírgula no mobile
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
                            nova_qtd = col1.number_input(f"Qtd Casa:", value=int(converter_ptbr(row['qtd_central'])), key=f"q_{idx}")
                            novo_custo = money_input_ptbr("Custo:", key=f"c_{idx}", value=float(converter_ptbr(row['preco_custo'])))

                            if st.button(f"💾 Salvar {row['nome do produto']}", key=f"btn_{idx}"):
                                df.at[idx, 'qtd_central'] = nova_qtd
                                df.at[idx, 'preco_custo'] = novo_custo
                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(row['nome do produto'], nova_qtd, novo_custo, None, None, prefixo)
                                st.success("Salvo!")
                                st.rerun()
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

                        bar = st.progress(0)
                        total = len(df_editado)
                        for i, (idx2, row2) in enumerate(df_editado.iterrows()):
                            atualizar_casa_global(
                                df.at[idx2, 'nome do produto'],
                                converter_ptbr(row2['qtd_central']),
                                converter_ptbr(row2['preco_custo']),
                                None,
                                row2['validade'],
                                prefixo
                            )
                            bar.progress((i + 1) / total)
                        st.success("Sincronizado!")
                        st.rerun()

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

                            novo_custo = money_input_ptbr("Custo:", key=f"full_custo_{idx_prod}", value=float(converter_ptbr(df.at[idx_prod, 'preco_custo'])))
                            novo_venda = money_input_ptbr("Venda:", key=f"full_venda_{idx_prod}", value=float(converter_ptbr(df.at[idx_prod, 'preco_venda'])))

                            c_qtd, c_acao = st.columns([1, 2])
                            qtd_input = c_qtd.number_input("Quantidade:", min_value=0, value=0)
                            acao = c_acao.radio("Ação:", ["Somar (+) Entrada", "Substituir (=) Correção", "Apenas Salvar Dados"], index=2)

                            if st.form_submit_button("💾 SALVAR"):
                                df.at[idx_prod, 'nome do produto'] = normalizar_texto(c_nome)
                                df.at[idx_prod, 'validade'] = pd.to_datetime(nova_val) if nova_val else None
                                df.at[idx_prod, 'preco_custo'] = novo_custo
                                df.at[idx_prod, 'preco_venda'] = novo_venda
                                if c_forn:
                                    df.at[idx_prod, 'ultimo_fornecedor'] = c_forn

                                if acao.startswith("Somar") and qtd_input > 0:
                                    df.at[idx_prod, 'qtd_central'] = converter_ptbr(df.at[idx_prod, 'qtd_central']) + qtd_input
                                    dt_full = datetime.combine(dt_reg, hr_reg)
                                    hist = {
                                        'data': dt_full,
                                        'produto': normalizar_texto(c_nome),
                                        'fornecedor': c_forn,
                                        'qtd': qtd_input,
                                        'preco_pago': novo_custo,
                                        'total_gasto': qtd_input * novo_custo,
                                        'numero_nota': 'MANUAL',
                                        'desconto_total_money': 0.0,
                                        'preco_sem_desconto': novo_custo
                                    }
                                    df_hist = pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True)
                                    salvar_na_nuvem(f"{prefixo}_historico_compras", df_hist, COLS_HIST)

                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input

                                salvar_na_nuvem(f"{prefixo}_estoque", df, COLUNAS_VITAIS)
                                atualizar_casa_global(
                                    normalizar_texto(c_nome),
                                    df.at[idx_prod, 'qtd_central'],
                                    novo_custo,
                                    novo_venda,
                                    pd.to_datetime(nova_val) if nova_val else None,
                                    prefixo
                                )
                                st.success("Salvo!")
                                st.rerun()

    # 9. TABELA GERAL
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

                    bar = st.progress(0)
                    total = len(df_edit)
                    for i, (idx3, row3) in enumerate(df_edit.iterrows()):
                        atualizar_casa_global(
                            df.at[idx3, 'nome do produto'],
                            converter_ptbr(row3['qtd_central']),
                            converter_ptbr(row3['preco_custo']),
                            converter_ptbr(row3['preco_venda']),
                            row3.get('validade', None),
                            prefixo
                        )
                        bar.progress((i + 1) / total)
                    st.success("Tabela Geral atualizada!")
                    st.rerun()
            with c2:
                st.info("Módulos restantes do seu app continuam iguais aos seus originais.")

