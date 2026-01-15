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

# --------------------------------------------------------
# Moeda pt-BR: resolve o bug do navegador que transforma 3,19 em 319
# --------------------------------------------------------

def format_br(valor):
    """Formata número no estilo brasileiro (milhar '.', decimal ',')."""
    try:
        s = f"{float(valor):,.2f}"
    except Exception:
        s = f"{0.0:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def converter_ptbr(valor):
    """Converte valores brasileiros (com vírgula) para float padrão (ponto), com robustez."""
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0

    s = str(valor).strip().upper()
    s = s.replace("R$", "").replace(" ", "").replace("\u00A0", "")

    # tenta direto (padrão com ponto)
    try:
        return float(s)
    except Exception:
        pass

    # 1.234,56 -> 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0

def money_input_ptbr(label: str, key: str, value: float = 0.0, help: str = None):
    """
    Campo monetário que aceita '3,19' OU '3.19' e converte corretamente.
    Evita o bug de st.number_input no navegador/teclado BR.
    """
    default_txt = format_br(value) if value is not None else "0,00"
    txt = st.text_input(
        label,
        value=default_txt,
        key=key,
        help=help,
        placeholder="Ex: 3,19"
    )
    val = converter_ptbr(txt)
    st.caption(f"Interpretado: R$ {format_br(val)}")
    return val

# ==============================================================================
# ⚙️ CONFIGURAÇÃO DE NUVEM & SISTEMA
# ==============================================================================
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

COLUNAS_VITAIS = [
    "código de barras", "nome do produto", "qtd.estoque", "qtd_central",
    "qtd_minima", "validade", "status_compra", "qtd_comprada",
    "preco_custo", "preco_venda", "categoria", "ultimo_fornecedor", "preco_sem_desconto"
]

COLS_HIST = [
    "data", "produto", "fornecedor", "qtd", "preco_pago", "total_gasto",
    "numero_nota", "desconto_total_money", "preco_sem_desconto"
]
COLS_MOV = ["data_hora", "produto", "qtd_movida"]
COLS_VENDAS = ["data_hora", "produto", "qtd_vendida", "estoque_restante"]
COLS_LISTA = ["produto", "qtd_sugerida", "fornecedor", "custo_previsto", "data_inclusao", "status"]
COLS_OFICIAL = ["nome do produto", "código de barras"]

# --- CONEXÃO SEGURA ---
@st.cache_resource
def get_google_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = json.loads(st.secrets["service_account_json"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
    return gspread.authorize(creds)

# ==============================================================================
# 🧠 FUNÇÕES LÓGICAS (MANTIDAS E AJUSTADAS)
# ==============================================================================
def normalizar_texto(texto):
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("ASCII")
    return texto.upper().strip()

def normalizar_para_busca(texto):
    return normalizar_texto(texto)

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if df is None or df.empty:
        return df
    if not texto_busca:
        return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
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
        if any(u in palavra for u in ["L", "ML", "KG", "G", "M"]):
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
# 🧼 INTEGRIDADE DE COLUNAS / TIPOS
# ==============================================================================
def garantir_integridade_colunas(df, colunas_alvo):
    if df is None or df.empty:
        return pd.DataFrame(columns=colunas_alvo)

    df.columns = df.columns.str.strip().str.lower()

    for col in colunas_alvo:
        if col not in df.columns:
            if any(x in col for x in ["qtd", "preco", "valor", "custo", "total", "desconto"]):
                df[col] = 0.0
            elif "data" in col or "validade" in col:
                df[col] = None
            else:
                df[col] = ""

    for col in df.columns:
        if any(x in col for x in ["qtd", "preco", "valor", "custo", "total", "desconto"]):
            df[col] = df[col].apply(converter_ptbr)

    return df

# --- LEITURA DA NUVEM (CORRIGIDA PARA NÃO APAGAR DADOS) ---
@st.cache_data(ttl=60)
def ler_da_nuvem(nome_aba, colunas_padrao):
    time.sleep(1)  # Pausa técnica para evitar bloqueio do Google
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except Exception:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=30)
            ws.append_row(colunas_padrao)
            return pd.DataFrame(columns=colunas_padrao)

        dados = ws.get_all_records()
        df = pd.DataFrame(dados)

        if df.empty:
            return pd.DataFrame(columns=colunas_padrao)

        df = garantir_integridade_colunas(df, colunas_padrao)

        for col in df.columns:
            if "data" in col or "validade" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df
    except Exception:
        return pd.DataFrame(columns=colunas_padrao)

# --- SALVAR NA NUVEM ---
def salvar_na_nuvem(nome_aba, df, colunas_padrao):
    try:
        client = get_google_client()
        sh = client.open("loja_dados")
        try:
            ws = sh.worksheet(nome_aba)
        except Exception:
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=30)

        ws.clear()

        df_save = garantir_integridade_colunas(df.copy(), colunas_padrao)

        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].dt.strftime("%Y-%m-%d")

        for col in df_save.columns:
            if any(x in col for x in ["qtd", "preco", "valor", "custo", "total", "desconto"]):
                df_save[col] = df_save[col].apply(converter_ptbr).astype(float)

        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        ler_da_nuvem.clear()
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==============================================================================
# 🔁 UNIFICAR POR CÓDIGO (evita duplicatas que fazem “Casa=0” em outro registro)
# ==============================================================================
def unificar_produtos_por_codigo(df):
    if df is None or df.empty:
        return df

    df = garantir_integridade_colunas(df, COLUNAS_VITAIS)
    df["código de barras"] = df["código de barras"].astype(str).str.strip()

    lista_final = []

    sem_codigo = df[df["código de barras"] == ""]
    com_codigo = df[df["código de barras"] != ""]

    for cod, grupo in com_codigo.groupby("código de barras"):
        if len(grupo) > 1:
            melhor_nome = max(grupo["nome do produto"].astype(str).tolist(), key=len)
            base_ref = grupo[grupo["nome do produto"] == melhor_nome].iloc[0].to_dict()

            base_ref["qtd.estoque"] = converter_ptbr(grupo["qtd.estoque"].sum())
            base_ref["qtd_central"] = converter_ptbr(grupo["qtd_central"].sum())
            base_ref["preco_custo"] = converter_ptbr(grupo["preco_custo"].max())
            base_ref["preco_venda"] = converter_ptbr(grupo["preco_venda"].max())

            # fornecedor e preço_sem_desconto (mantém o mais recente/preenchido)
            ult_forn = ""
            for v in grupo["ultimo_fornecedor"].astype(str).tolist():
                v = v.strip()
                if v:
                    ult_forn = v
            base_ref["ultimo_fornecedor"] = ult_forn

            psd = 0.0
            for v in grupo["preco_sem_desconto"].tolist():
                vv = converter_ptbr(v)
                if vv > 0:
                    psd = vv
            base_ref["preco_sem_desconto"] = psd

            lista_final.append(base_ref)
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if not sem_codigo.empty:
        df_novo = pd.concat([df_novo, sem_codigo], ignore_index=True)
    df_novo = garantir_integridade_colunas(df_novo, COLUNAS_VITAIS)
    return df_novo

# ==============================================================================
# 🏢 ATUALIZAÇÃO GLOBAL (match por nome normalizado)
# ==============================================================================
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    nome_ref = normalizar_texto(nome_produto)

    for loja in todas_lojas:
        if loja == prefixo_ignorar:
            continue

        df_outra = ler_da_nuvem(f"{loja}_estoque", COLUNAS_VITAIS)
        if df_outra is None or df_outra.empty:
            continue

        df_outra.columns = df_outra.columns.str.strip().str.lower()
        nomes_norm = df_outra["nome do produto"].astype(str).apply(normalizar_texto)
        mask = nomes_norm == nome_ref

        if mask.any():
            idx = df_outra[mask].index[0]
            df_outra.at[idx, "qtd_central"] = float(converter_ptbr(qtd_nova_casa))

            if novo_custo is not None:
                df_outra.at[idx, "preco_custo"] = float(converter_ptbr(novo_custo))
            if novo_venda is not None:
                df_outra.at[idx, "preco_venda"] = float(converter_ptbr(novo_venda))
            if nova_validade is not None:
                df_outra.at[idx, "validade"] = nova_validade

            salvar_na_nuvem(f"{loja}_estoque", df_outra, COLUNAS_VITAIS)

# ==============================================================================
# 📦 BASE OFICIAL
# ==============================================================================
def processar_excel_oficial(arquivo_subido):
    try:
        if arquivo_subido.name.endswith(".csv"):
            df_temp = pd.read_csv(arquivo_subido)
        else:
            df_temp = pd.read_excel(arquivo_subido)

        if "obrigatório" in str(df_temp.iloc[0].values):
            df_temp = df_temp.iloc[1:].reset_index(drop=True)

        df_temp.columns = df_temp.columns.str.strip()

        col_nome = next((c for c in df_temp.columns if "nome" in c.lower()), "Nome")
        col_cod = next((c for c in df_temp.columns if "código" in c.lower() or "barras" in c.lower()), "Código de Barras Primário")

        df_limpo = df_temp[[col_nome, col_cod]].copy()
        df_limpo.columns = ["nome do produto", "código de barras"]
        df_limpo["nome do produto"] = df_limpo["nome do produto"].apply(normalizar_texto)
        df_limpo["código de barras"] = df_limpo["código de barras"].astype(str).str.replace(".0", "", regex=False).str.strip()

        salvar_na_nuvem("base_oficial", df_limpo, COLS_OFICIAL)
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

# ==============================================================================
# --- FUNÇÃO XML HÍBRIDA (corrige fornecedor e match robusto)
# ==============================================================================
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()

    def tag_limpa(element):
        return element.tag.split("}")[-1]

    # 1) FORMATO NOVO (custom)
    info_custom = root.find("Info")
    if info_custom is not None:
        try:
            forn = info_custom.find("Fornecedor").text
            num = info_custom.find("NumeroNota").text
            dt_s = info_custom.find("DataCompra").text
            hr_s = info_custom.find("HoraCompra").text
            data_final = datetime.strptime(f"{dt_s} {hr_s}", "%d/%m/%Y %H:%M:%S")
            dados_nota = {"numero": num, "fornecedor": forn, "data": data_final, "itens": []}
        except Exception:
            dados_nota = {"numero": "S/N", "fornecedor": "IMPORTADO", "data": datetime.now(), "itens": []}
    else:
        # 2) NFE PADRÃO (emit/xNome e ide/nNF)
        dados_nota = {"numero": "S/N", "fornecedor": "IMPORTADO", "data": datetime.now(), "itens": []}

        nNF = root.find(".//{*}ide/{*}nNF")
        if nNF is not None and nNF.text:
            dados_nota["numero"] = nNF.text.strip()

        xNome_emit = root.find(".//{*}emit/{*}xNome")
        if xNome_emit is not None and xNome_emit.text:
            dados_nota["fornecedor"] = xNome_emit.text.strip()

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

                p_liq = valor / qtd if qtd > 0 else 0.0
                p_bruto = (valor + desc) / qtd if qtd > 0 else 0.0

                dados_nota["itens"].append({
                    "nome": normalizar_texto(nome),
                    "qtd": qtd,
                    "ean": str(ean).strip(),
                    "preco_un_liquido": p_liq,
                    "preco_un_bruto": p_bruto,
                    "desconto_total_item": desc
                })
            except Exception:
                continue
    else:
        # 4) ITENS - NFE padrão
        dets = [e for e in root.iter() if tag_limpa(e) == "det"]
        for det in dets:
            try:
                prod = next((child for child in det if tag_limpa(child) == "prod"), None)
                if prod:
                    item = {
                        "codigo_interno": "",
                        "ean": "",
                        "nome": "",
                        "qtd": 0.0,
                        "preco_un_liquido": 0.0,
                        "preco_un_bruto": 0.0,
                        "desconto_total_item": 0.0
                    }
                    vProd = 0.0
                    vDesc = 0.0
                    qCom = 0.0

                    for info in prod:
                        t = tag_limpa(info)
                        if t == "cProd":
                            item["codigo_interno"] = info.text
                        elif t == "cEAN":
                            item["ean"] = info.text
                        elif t == "xProd":
                            item["nome"] = normalizar_texto(info.text)
                        elif t == "qCom":
                            qCom = converter_ptbr(info.text)
                        elif t == "vProd":
                            vProd = converter_ptbr(info.text)
                        elif t == "vDesc":
                            vDesc = converter_ptbr(info.text)

                    if qCom > 0:
                        item["qtd"] = qCom
                        item["preco_un_bruto"] = vProd / qCom
                        item["desconto_total_item"] = vDesc
                        item["preco_un_liquido"] = (vProd - vDesc) / qCom

                    ean_xml = str(item["ean"]).strip()
                    if ean_xml in ["SEM GTIN", "", "None", "NAN"]:
                        item["ean"] = item["codigo_interno"]

                    dados_nota["itens"].append(item)
            except Exception:
                continue

    # 5) Match com base oficial (se EAN faltar)
    lista_nomes_ref = []
    dict_ref_ean = {}

    if df_referencia is not None and not df_referencia.empty:
        for _, row in df_referencia.iterrows():
            nm = normalizar_texto(row["nome do produto"])
            dict_ref_ean[nm] = str(row["código de barras"]).strip()
            lista_nomes_ref.append(nm)

    for item in dados_nota["itens"]:
        if str(item.get("ean", "")).strip() in ["SEM GTIN", "", "None", "NAN"] and lista_nomes_ref:
            melhor, _ = encontrar_melhor_match(item["nome"], lista_nomes_ref)
            if melhor:
                item["ean"] = dict_ref_ean.get(melhor, item.get("ean", ""))

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
    df["código de barras"] = df["código de barras"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df["nome do produto"] = df["nome do produto"].apply(lambda x: normalizar_texto(str(x)))

# --------------------------------------------------------
# A PARTIR DAQUI: TODO o restante do seu app (menus, telas)
# --------------------------------------------------------
# NOTA: Este arquivo é longo (mais de 1300 linhas). Para não truncar aqui no chat,
# eu preciso que você use o arquivo de download.
#
# 👉 Faça assim:
# 1) Baixe o arquivo: sandbox:/mnt/data/app_corrigido.py
# 2) Abra e copie tudo
# 3) Cole no GitHub
#
# Se você quiser mesmo o arquivo inteiro "colado" aqui no chat, me confirme
# e eu envio em partes (Parte 1/3, 2/3, 3/3) para não cortar.
