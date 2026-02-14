import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
from io import BytesIO
import zipfile
import re
import plotly.express as px 
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# ==============================================================================
# 1. CONEXÃO E BANCO DE DADOS (GOOGLE SHEETS)
# ==============================================================================
@st.cache_resource
def conectar_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    return gspread.authorize(creds).open("Sistema_Estoque_Database")

@st.cache_data(ttl=60) 
def carregar_do_google(nome_aba):
    try:
        sh = conectar_google_sheets()
        try: worksheet = sh.worksheet(nome_aba)
        except gspread.WorksheetNotFound: return pd.DataFrame() 
        
        dados = worksheet.get_all_values()
        if not dados: return pd.DataFrame()
            
        headers = dados.pop(0)
        headers_unicos = []
        vistos = set()
        for i, col in enumerate(headers):
            nome_limpo = str(col).strip()
            if not nome_limpo: nome_limpo = f"col_extra_{i}"
            nome_final = nome_limpo
            contador = 1
            while nome_final in vistos:
                nome_final = f"{nome_limpo}_{contador}"
                contador += 1
            vistos.add(nome_final)
            headers_unicos.append(nome_final)

        return pd.DataFrame(dados, columns=headers_unicos)
    except: return pd.DataFrame()

def salvar_no_google(df, nome_aba, permitir_vazio=False):
    if df.empty and not permitir_vazio: return
    try:
        st.cache_data.clear() 
        client = conectar_google_sheets()
        try: worksheet = client.worksheet(nome_aba)
        except gspread.WorksheetNotFound: worksheet = client.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        colunas_proibidas = ['display_combo', 'produto_str', 'Selecionar', 'status_temp', 'nome_padrao', 'codigo_limpo']
        cols_para_salvar = [c for c in df.columns if c not in colunas_proibidas]
        df_limpo = df[cols_para_salvar].copy().fillna("")
        
        dados_lista = [df_limpo.columns.tolist()] + df_limpo.astype(str).values.tolist() if not df_limpo.empty else ([df.columns.tolist()] if not df.columns.empty else [])
        worksheet.clear()
        if dados_lista:
            worksheet.update(dados_lista)
            time.sleep(2)
    except Exception as e: st.error(f"ERRO AO SALVAR ({nome_aba}): {e}")

# ==============================================================================
# 2. FUNÇÕES AUXILIARES, LUPA E BUSCA
# ==============================================================================
def obter_hora_manaus(): return datetime.utcnow() - timedelta(hours=4)

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper().strip()

def normalizar_para_busca(texto): return normalizar_texto(texto)

def padronizar_codigo_barras(valor):
    if pd.isna(valor) or valor is None: return ""
    s = str(valor).strip().upper()
    if s.endswith('.0'): s = s[:-2]
    return re.sub(r'[^A-Z0-9]', '', s).lstrip('0')

def limpar_codigo_barras(valor):
    try: return re.sub(r"\D", "", "" if valor is None else str(valor))
    except: return ""

def parse_num_br(x, default=0.0):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""): return float(default)
        s = str(x).strip().replace("R$", "").replace("\u00a0", " ").strip()
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
        elif "," in s: s = s.replace(".", "").replace(",", ".")
        return float(re.sub(r"[^0-9\-\.]", "", s))
    except: return float(default)

def formatar_moeda_br(v):
    try: return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{v:.2f}"

def _to_float(valor):
    try:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)): return 0.0
        s = str(valor).strip().replace("R$", "").strip()
        if s == "" or s.lower() in {"nan", "none"}: return 0.0
        if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
        else: s = s.replace(",", ".")
        return float(s)
    except: return 0.0

def _to_int(valor):
    try: return int(round(_to_float(valor)))
    except: return 0

def calcular_pontuacao(nome_xml, nome_sistema):
    nome_xml_norm = normalizar_para_busca(nome_xml)
    nome_sis_norm = normalizar_para_busca(nome_sistema)
    nums_xml = set(re.findall(r'\d+', nome_xml_norm))
    nums_sis = set(re.findall(r'\d+', nome_sis_norm))
    if nums_xml and nums_sis and not nums_xml.intersection(nums_sis): return 0.0 
    set_xml = set(nome_xml_norm.split())
    set_sis = set(nome_sis_norm.split())
    common = set_xml.intersection(set_sis)
    if not common: return 0.0
    return len(common) / len(set_xml.union(set_sis))

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor_match, maior_score = None, 0.0
    for op in lista_opcoes:
        if op == "(CRIAR NOVO)": continue
        score = calcular_pontuacao(nome_buscado, op)
        if score > maior_score: maior_score, melhor_match = score, op
    return (melhor_match, "Match") if maior_score >= cutoff else (None, "Nenhum")

def filtrar_df_busca_robusta(df, query, cols_text=None, cols_barcode=None):
    try:
        if df is None or len(df) == 0: return df
        if not str(query).strip(): return df
        c_txt = [c for c in (cols_text or []) if c in df.columns]
        c_bar = [c for c in (cols_barcode or []) if c in df.columns]
        base_parts = [df[c].fillna("").astype(str).map(normalizar_para_busca) for c in c_txt]
        base = base_parts[0] if base_parts else pd.Series([""] * len(df), index=df.index)
        for s in base_parts[1:]: base = base + " | " + s
        barras = None
        if c_bar:
            bparts = [df[c].fillna("").astype(str).str.replace(r"\D", "", regex=True) for c in c_bar]
            barras = bparts[0]
            for b in bparts[1:]: barras = barras + " | " + b
        tokens = [t for t in re.split(r"\s+", normalizar_para_busca(str(query).strip())) if t]
        mask = pd.Series(True, index=df.index)
        for t in tokens:
            if t.isdigit() and len(t) >= 4:
                m_bar = barras.str.contains(t, na=False) if barras is not None else pd.Series(False, index=df.index)
                mask = mask & (m_bar | base.str.contains(t, na=False))
            else: mask = mask & base.str.contains(t, na=False)
        return df.loc[mask].copy()
    except: return df

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    try:
        if df is None or len(df) == 0: return df
        cols_bar = [c for c in ['código de barras','codigo de barras','ean','barcode'] if c in df.columns]
        cols_txt = [coluna_busca] if coluna_busca in df.columns else []
        for extra in ['produto', 'nome do produto', 'descricao']:
            if extra in df.columns and extra not in cols_txt: cols_txt.append(extra)
        return filtrar_df_busca_robusta(df, texto_busca, cols_text=cols_txt, cols_barcode=cols_bar)
    except: return df

def pick_col(colunas, *candidatos):
    if colunas is None: return None
    cols = list(colunas)
    norm_map = {c: normalizar_para_busca(str(c)) for c in cols}
    cand_norm = [normalizar_para_busca(str(x)) for x in candidatos if x is not None and str(x).strip()!=""]
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and nc == cn: return c
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and cn in nc: return c
    return None

def ler_excel_com_header_auto(file_obj, max_rows=25):
    try: file_obj.seek(0)
    except: pass
    amostra = pd.read_excel(file_obj, header=None, nrows=max_rows, dtype=str)
    chaves = ["PRODUTO", "ITEM", "DESCRICAO", "QTD", "QUANTIDADE", "TRANSACAO", "ID", "DATA", "LOJA"]
    melhor_linha, melhor_score = None, -1
    for r in range(min(len(amostra), max_rows)):
        row_norm = [normalizar_para_busca(str(x)) for x in amostra.iloc[r].tolist()]
        joined = " | ".join(row_norm)
        score = sum(1 for k in chaves if k in joined)
        if sum(1 for v in row_norm if v.strip() != "") >= 2 and score > melhor_score:
            melhor_score, melhor_linha = score, r
    header_row = melhor_linha if (melhor_linha is not None and melhor_score >= 2) else 0
    try: file_obj.seek(0)
    except: pass
    df = pd.read_excel(file_obj, header=header_row, dtype=str).dropna(axis=1, how="all")
    return df, header_row, list(df.columns)

# ==============================================================================
# 3. REGRAS DE NEGÓCIO E BLINDAGEM DE DADOS
# ==============================================================================
def garantir_colunas(df: pd.DataFrame, colunas_obrigatorias: list[str]) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame(columns=colunas_obrigatorias)
    for c in colunas_obrigatorias:
        if c not in df.columns: df[c] = ""
    return df

def blindar_estoque_df(df_estoque: pd.DataFrame) -> pd.DataFrame:
    if df_estoque is None: return pd.DataFrame()
    df = df_estoque.copy()
    df.columns = df.columns.astype(str).str.strip().str.lower()
    df = garantir_colunas(df, ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto', 'status'])
    for c in ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada']:
        df[c] = df[c].apply(_to_int)
        df.loc[df[c] < 0, c] = 0
    for c in ['preco_custo', 'preco_venda', 'preco_sem_desconto']:
        df[c] = df[c].apply(_to_float)
        df.loc[df[c] < 0, c] = 0.0
    df['código de barras'] = df['código de barras'].apply(lambda x: str(x).replace('.0', '').strip() if pd.notnull(x) else "")
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
    df['ultimo_fornecedor'] = df['ultimo_fornecedor'].fillna('').astype(str)
    df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')
    df['status'] = df['status'].replace('', 'Ativo').fillna('Ativo').apply(lambda x: 'Ativo' if str(x).strip().lower() in {'ativo', 'a'} else ('Inativo' if str(x).strip().lower() in {'inativo', 'i'} else str(x).strip().title()))
    return df

# === FUNÇÃO QUE FALTAVA (O MOTIVO DO ERRO) ===
def blindar_valor_estoque(df_estoque: pd.DataFrame) -> float:
    if df_estoque is None or df_estoque.empty: return 0.0
    df = df_estoque.copy()
    for c in ['qtd.estoque', 'qtd_central', 'preco_custo', 'status']:
        if c not in df.columns: df[c] = 0
    q_loja = pd.to_numeric(df['qtd.estoque'], errors='coerce').fillna(0).clip(lower=0)
    q_casa = pd.to_numeric(df['qtd_central'], errors='coerce').fillna(0).clip(lower=0)
    custo = pd.to_numeric(df['preco_custo'], errors='coerce').fillna(0).clip(lower=0)
    ativos = df['status'].astype(str) == 'Ativo'
    return float(((q_loja + q_casa) * custo)[ativos].sum())

def unificar_produtos_por_codigo(df):
    if df.empty: return df
    for col in ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    lista_final = []
    df['nome_padrao'] = df['nome do produto'].apply(normalizar_texto)

    for nome, grupo in df.groupby('nome_padrao'):
        if len(grupo) > 1:
            melhor_nome = max(grupo['nome do produto'].tolist(), key=len)
            codigos = [str(c).strip() for c in grupo['código de barras'].tolist() if padronizar_codigo_barras(c)]
            melhor_cod = max(codigos, key=len) if codigos else ""
            
            base_ref = grupo[grupo['nome do produto'] == melhor_nome].iloc[0].to_dict()
            base_ref['código de barras'] = melhor_cod
            base_ref['qtd.estoque'] = grupo['qtd.estoque'].sum()
            base_ref['qtd_central'] = grupo['qtd_central'].sum()
            base_ref['preco_custo'] = grupo['preco_custo'].max()
            base_ref['preco_venda'] = grupo['preco_venda'].max()
            base_ref['preco_sem_desconto'] = grupo['preco_sem_desconto'].max() if 'preco_sem_desconto' in grupo.columns else 0.0
            base_ref['status'] = 'Ativo' if 'Ativo' in grupo['status'].values else 'Inativo'
            lista_final.append(base_ref)
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if 'nome_padrao' in df_novo.columns: df_novo = df_novo.drop(columns=['nome_padrao'])
    return df_novo

# ==============================================================================
# 4. FUNÇÕES DE CARREGAMENTO E SALVAMENTO GERAIS (E AUDITORIA)
# ==============================================================================
def registrar_auditoria(prefixo, produto, qtd_antes, qtd_nova, acao, motivo="Manual"):
    try:
        aba = f"{prefixo}_log_auditoria"
        df_log = carregar_do_google(aba)
        novo_log = {'data_hora': str(obter_hora_manaus()), 'produto': produto, 'qtd_antes': qtd_antes, 'qtd_nova': qtd_nova, 'acao': acao, 'motivo': motivo}
        salvar_no_google(pd.concat([df_log, pd.DataFrame([novo_log])], ignore_index=True), aba)
    except: pass

def salvar_logs_em_lote(prefixo, lista_logs):
    if not lista_logs: return
    try:
        aba = f"{prefixo}_log_auditoria"
        salvar_no_google(pd.concat([carregar_do_google(aba), pd.DataFrame(lista_logs)], ignore_index=True), aba)
    except: pass

def salvar_estoque(df, prefixo):
    df_blindado = blindar_estoque_df(df)
    salvar_no_google(df_blindado, f"{prefixo}_estoque")
    st.session_state['df_ativo'] = df_blindado

def salvar_historico(df, prefixo): salvar_no_google(df, f"{prefixo}_historico_compras")
def salvar_movimentacoes(df, prefixo): salvar_no_google(df, f"{prefixo}_movimentacoes")
def salvar_vendas(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas")
def salvar_lista_compras(df, prefixo): salvar_no_google(df, f"{prefixo}_lista_compras", permitir_vazio=True)
def salvar_vendas_itens(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_itens", permitir_vazio=True)
def salvar_vendas_transacoes(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_transacoes", permitir_vazio=True)
def salvar_vendas_mensal_produto(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_mensal_produto", permitir_vazio=True)

def carregar_dados(prefixo_arq): return blindar_estoque_df(carregar_do_google(f"{prefixo_arq}_estoque"))
def carregar_movimentacoes(prefixo_arq): return carregar_do_google(f"{prefixo_arq}_movimentacoes")
def carregar_vendas(prefixo_arq): return carregar_do_google(f"{prefixo_arq}_vendas")
def carregar_lista_compras(prefixo_arq): return carregar_do_google(f"{prefixo_arq}_lista_compras")

def carregar_historico(prefixo_arq):
    try:
        df_h = carregar_do_google(f"{prefixo_arq}_historico_compras")
        if df_h.empty: return pd.DataFrame()
        df_h = df_h[[c for c in df_h.columns if c not in ['display_combo', 'produto_str', 'Selecionar', 'status_temp']]]
        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
        for c in ['qtd', 'preco_pago', 'total_gasto', 'desconto_total_money', 'preco_sem_desconto']:
             if c in df_h.columns: 
                 df_h[c] = pd.to_numeric(df_h[c].astype(str).str.replace(',', '.', regex=False), errors='coerce').fillna(0)
        for col in ['numero_nota', 'obs_importacao', 'data_emissao', 'código_barras']:
             if col not in df_h.columns: df_h[col] = ""
        if 'desconto_total_money' not in df_h.columns: df_h['desconto_total_money'] = df_h['desconto_obtido'] * df_h['qtd'] if 'desconto_obtido' in df_h.columns else 0.0
        if 'preco_sem_desconto' not in df_h.columns: df_h['preco_sem_desconto'] = 0.0
        mask_z = (df_h['preco_sem_desconto'] == 0) & (df_h['preco_pago'] > 0)
        df_h.loc[mask_z, 'preco_sem_desconto'] = df_h.loc[mask_z, 'preco_pago']
        return df_h
    except: return pd.DataFrame()

def carregar_vendas_itens(prefixo_arq):
    try:
        df_vi = carregar_do_google(f"{prefixo_arq}_vendas_itens")
        if df_vi.empty: return pd.DataFrame()
        df_vi.columns = df_vi.columns.str.strip().str.lower()
        if 'data_hora' in df_vi.columns: df_vi['data_hora'] = pd.to_datetime(df_vi['data_hora'], errors='coerce')
        for c in ['qtd_vendida', 'preco_unit', 'valor_total']:
            if c in df_vi.columns: df_vi[c] = df_vi[c].apply(parse_num_br)
        if 'código_barras' in df_vi.columns: df_vi['código_barras'] = df_vi['código_barras'].astype(str).str.replace('.0','',regex=False).str.strip()
        if 'produto' in df_vi.columns: df_vi['produto'] = df_vi['produto'].astype(str).apply(normalizar_texto)
        return df_vi
    except: return pd.DataFrame()

def carregar_vendas_transacoes(prefixo_arq):
    try:
        df_vt = carregar_do_google(f"{prefixo_arq}_vendas_transacoes")
        if df_vt.empty: return pd.DataFrame()
        df_vt.columns = df_vt.columns.str.strip().str.lower()
        if 'data_hora' in df_vt.columns: df_vt['data_hora'] = pd.to_datetime(df_vt['data_hora'], errors='coerce')
        for c in ['subtotal', 'descontos', 'taxas', 'total']:
            if c in df_vt.columns: df_vt[c] = df_vt[c].apply(parse_num_br)
        return df_vt
    except: return pd.DataFrame()

def inicializar_arquivos(prefixo):
    arquivos = {
        f"{prefixo}_estoque": ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto', 'status'],
        f"{prefixo}_historico_compras": ['data', 'data_emissao', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
        f"{prefixo}_movimentacoes": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],
        f"{prefixo}_vendas_itens": ['data_hora', 'mes_ref', 'transacao', 'código_barras', 'produto', 'qtd_vendida', 'preco_unit', 'valor_total', 'canal', 'obs_importacao'],
        f"{prefixo}_vendas_transacoes": ['data_hora', 'mes_ref', 'transacao', 'subtotal', 'descontos', 'taxas', 'total', 'forma_pagamento', 'obs_importacao'],
        f"{prefixo}_vendas_mensal_produto": ['mes_ref', 'código_barras', 'produto', 'qtd_vendida', 'valor_total', 'ultima_venda'],
        f"{prefixo}_lista_compras": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo'],
    }
    for aba, colunas in arquivos.items():
        if carregar_do_google(aba).empty: salvar_no_google(pd.DataFrame(columns=colunas), aba)

def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]

    dados_nota = {'numero': '', 'fornecedor': '', 'data_emissao': '', 'itens': []}
    lista_nomes_ref = [normalizar_texto(row['nome do produto']) for _, row in df_referencia.iterrows()] if not df_referencia.empty else []
    dict_ref_ean = {normalizar_texto(row['nome do produto']): str(row['código de barras']).strip() for _, row in df_referencia.iterrows()} if not df_referencia.empty else {}

    if tag_limpa(root) == 'NotaFiscal':
        info = root.find('Info')
        if info is not None:
            dados_nota['numero'] = info.find('NumeroNota').text if info.find('NumeroNota') is not None else ""
            dados_nota['fornecedor'] = info.find('Fornecedor').text if info.find('Fornecedor') is not None else ""
            try: dados_nota['data_emissao'] = info.find('DataCompra').text
            except: pass
        for item_xml in root.findall('.//Produtos/Item'):
            item = {'codigo_interno': '', 'ean': '', 'nome': normalizar_texto(item_xml.find('Nome').text), 'qtd': float(item_xml.find('Quantidade').text), 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': float(item_xml.find('ValorDesconto').text)}
            val_final = float(item_xml.find('ValorPagoFinal').text)
            item['ean'] = item_xml.find('CodigoBarras').text or ""
            item['codigo_interno'] = item['ean']
            if item['qtd'] > 0:
                item['preco_un_liquido'] = val_final / item['qtd']
                item['preco_un_bruto'] = (val_final + item['desconto_total_item']) / item['qtd']
            if str(item['ean']).strip() in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                if melhor_nome: item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            dados_nota['itens'].append(item)
        return dados_nota

    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == '': dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi' and elem.text:
            try: dados_nota['data_emissao'] = datetime.strptime(elem.text[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
            except: dados_nota['data_emissao'] = elem.text 

    for det in [e for e in root.iter() if tag_limpa(e) == 'det']:
        prod = next((child for child in det if tag_limpa(child) == 'prod'), None)
        if prod:
            item = {'codigo_interno': '', 'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
            vProd = vDesc = qCom = 0.0
            for info in prod:
                t = tag_limpa(info)
                if t == 'cProd': item['codigo_interno'] = info.text
                elif t == 'cEAN': item['ean'] = info.text
                elif t == 'xProd': item['nome'] = normalizar_texto(info.text)
                elif t == 'qCom': qCom = float(info.text)
                elif t == 'vProd': vProd = float(info.text) 
                elif t == 'vDesc': vDesc = float(info.text) 
            if qCom > 0:
                item['qtd'], item['preco_un_bruto'], item['desconto_total_item'], item['preco_un_liquido'] = qCom, vProd / qCom, vDesc, (vProd - vDesc) / qCom 
            if str(item['ean']).strip() in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                if melhor_nome: item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            dados_nota['itens'].append(item)
    return dados_nota

# ==============================================================================
# 5. ESTRUTURA DO APLICATIVO
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)
prefixo = "loja1" if loja_atual == "Loja 1 (Principal)" else ("loja2" if loja_atual == "Loja 2 (Filial)" else "loja3")

inicializar_arquivos(prefixo)
if 'df_ativo' not in st.session_state or st.session_state.get('loja_ativa_cache') != prefixo:
    st.session_state['df_ativo'] = carregar_dados(prefixo)
    st.session_state['loja_ativa_cache'] = prefixo

df = st.session_state['df_ativo']

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)", "📦 Inventário Rápido (Lote)", "⚖️ Conciliação (Shoppbud vs App)", 
        "🚚 Transferência em Massa (Picklist)", "📝 Lista de Compras (Planejamento)", "🆕 Cadastrar Produto", 
        "📥 Importar XML (Associação Inteligente)", "🔄 Sincronizar (Planograma)", "📈 Vendas (Importar & 80/20)", 
        "🔎 Raio-X do Estoque (Auditoria)", "🏠 Gôndola (Loja)", "💰 Inteligência de Compras (Histórico)", 
        "🏡 Estoque Central (Casa)", "📋 Tabela Geral", "🛠️ Ajuste & Limpeza (HOSPITAL)", "♻️ Restaurar Histórico"
    ])

    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle - {loja_atual}")
        if df.empty: st.info("Comece cadastrando produtos.")
        else:
            df_ativos = df[df['status'] == 'Ativo']
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Produtos Cadastrados", len(df_ativos))
            c2.metric("🏪 Itens Totais (Loja)", int(df_ativos['qtd.estoque'].sum()))
            c3.metric("🏡 Itens Totais (Casa)", int(df_ativos['qtd_central'].sum()))
            c4.metric("💰 Valor Investido", f"R$ {formatar_moeda_br(blindar_valor_estoque(df))}")
            st.divider()

    elif modo == "📦 Inventário Rápido (Lote)":
        st.title("📦 Inventário Rápido")
        st.info("Para usar esta função, pesquise o produto e altere a quantidade.")

    elif modo == "⚖️ Conciliação (Shoppbud vs App)":
        st.title("⚖️ Conciliação de Estoque")
        arq_planograma = st.file_uploader("📂 Carregar Planograma Shoppbud (.xlsx)", type=['xlsx', 'csv'])
        if arq_planograma:
            df_plan = pd.read_csv(arq_planograma, dtype=str) if arq_planograma.name.endswith('.csv') else pd.read_excel(arq_planograma, dtype=str)
            col_cod_plan = next((c for c in df_plan.columns if 'barras' in c.lower() or 'código' in c.lower() or 'codigo' in c.lower()), None)
            col_qtd_plan = next((c for c in df_plan.columns if 'estoque' in c.lower() or 'qtd' in c.lower()), None)
            if col_cod_plan and col_qtd_plan:
                df_plan[col_qtd_plan] = df_plan[col_qtd_plan].apply(parse_num_br)
                df_plan['código normalizado'] = df_plan[col_cod_plan].apply(padronizar_codigo_barras)
                df['código normalizado'] = df['código de barras'].apply(padronizar_codigo_barras)
                df_concilia = pd.merge(df[['código normalizado', 'nome do produto', 'qtd.estoque']], df_plan[[col_cod_plan, col_qtd_plan, 'código normalizado']], on='código normalizado', how='inner')
                df_divergente = df_concilia[df_concilia['qtd.estoque'] - df_concilia[col_qtd_plan] != 0].copy()
                if df_divergente.empty: st.success("✅ Estoque 100% batendo!")
                else: st.dataframe(df_divergente)
            else: st.error("Colunas não encontradas.")

    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa")
        st.info("Em breve.")

    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        df_lista = carregar_lista_compras(prefixo)
        if not df_lista.empty: st.dataframe(df_lista)
        else: st.info("Lista de compras vazia.")

    elif modo == "🆕 Cadastrar Produto":
        st.title(f"🆕 Cadastro - {loja_atual}")
        with st.form("form_cadastro"):
            c1, c2 = st.columns(2)
            novo_cod = c1.text_input("Código de Barras:")
            novo_nome = c1.text_input("Nome do Produto:")
            novo_custo = c2.number_input("Preço Custo:", min_value=0.0)
            novo_venda = c2.number_input("Preço Venda:", min_value=0.0)
            if st.form_submit_button("💾 CADASTRAR") and novo_nome:
                novo = {'código de barras': padronizar_codigo_barras(novo_cod), 'nome do produto': novo_nome.upper(), 'qtd.estoque': 0, 'qtd_central': 0, 'preco_custo': novo_custo, 'preco_venda': novo_venda, 'status': 'Ativo'}
                df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                salvar_estoque(df, prefixo)
                st.success("Cadastrado!")
                st.rerun()

    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML")
        df_hist = carregar_historico(prefixo)
        modo_import = st.radio("Modo:", ["📦 Atualizar Estoque da Casa", "📖 Apenas Histórico"], horizontal=True)
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            dados = ler_xml_nfe(arquivo_xml, pd.DataFrame())
            st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
            dt_lanc = st.date_input("Dia:", value=obter_hora_manaus().date())
            hr_lanc = st.time_input("Hora:", value=obter_hora_manaus().time())
            data_lancamento_final = datetime.combine(dt_lanc, hr_lanc)

            lista_sistema = ["(CRIAR NOVO PRODUTO)"] + [f"[SISTEMA] {r['código de barras']} - {r['nome do produto']}" for _, r in df.iterrows()]
            escolhas = {}
            for i, item in enumerate(dados['itens']):
                match_inicial = "(CRIAR NOVO PRODUTO)"
                if not df.empty:
                    cod_limpo = padronizar_codigo_barras(item['ean'])
                    m_cod = df['código de barras'].apply(padronizar_codigo_barras) == cod_limpo if cod_limpo else pd.Series(False, index=df.index)
                    if m_cod.any(): match_inicial = f"[SISTEMA] {df.loc[m_cod, 'código de barras'].values[0]} - {df.loc[m_cod, 'nome do produto'].values[0]}"
                    else:
                        melhor, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist(), cutoff=0.35)
                        if melhor: match_inicial = f"[SISTEMA] {df.loc[df['nome do produto']==melhor, 'código de barras'].values[0]} - {melhor}"

                st.divider()
                c1, c2 = st.columns([1, 1])
                c1.markdown(f"📦 **{item['nome']}** (XML)\n\nEAN: {item['ean']} | Qtd: {item['qtd']}")
                escolhas[i] = c2.selectbox("Qual é esse produto no sistema?", lista_sistema, index=lista_sistema.index(match_inicial) if match_inicial in lista_sistema else 0, key=f"x_{i}")
            
            if st.button("✅ CONFIRMAR ENTRADA NA CASA"):
                novos_hist = []
                for i, item in enumerate(dados['itens']):
                    esc = escolhas[i]
                    cod_hist = item['ean']
                    if "[SISTEMA]" in esc:
                         raw = esc.replace("[SISTEMA] ", "")
                         parts = raw.split(' - ', 1)
                         if len(parts) > 1: cod_hist, nome_final = parts[0].strip(), parts[1].strip()
                         else: nome_final = raw.strip()
                         
                         mask = df['nome do produto'] == nome_final
                         if mask.any():
                             idx = df[mask].index[0]
                             if "Atualizar" in modo_import: df.at[idx, 'qtd_central'] += item['qtd']
                             df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                    else:
                         nome_final = item['nome'].upper()
                         novo = {'código de barras': padronizar_codigo_barras(cod_hist), 'nome do produto': nome_final, 'qtd.estoque': 0, 'qtd_central': item['qtd'] if "Atualizar" in modo_import else 0, 'preco_custo': item['preco_un_liquido'], 'status': 'Ativo'}
                         df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    
                    novos_hist.append({'data': str(data_lancamento_final), 'código_barras': cod_hist, 'produto': nome_final, 'fornecedor': dados['fornecedor'], 'qtd': item['qtd'], 'preco_pago': item['preco_un_liquido'], 'total_gasto': item['qtd']*item['preco_un_liquido']})
                salvar_estoque(df, prefixo)
                if novos_hist: salvar_historico(pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True), prefixo)
                st.success("XML Importado com Sucesso!")
                st.rerun()

    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar (Planograma)")
        st.info("💡 A leitura agora é blindada contra bugs do Excel. Se o código de barras não for encontrado, ele busca pelo nome exato e salva o código na base.")
        arquivo = st.file_uploader("📂 Planograma (.csv ou .xlsx)", type=['xlsx', 'csv'])
        if arquivo:
            try:
                arquivo.seek(0)
                if arquivo.name.endswith('.csv'):
                    try: df_raw = pd.read_csv(arquivo, dtype=str)
                    except:
                        arquivo.seek(0)
                        df_raw = pd.read_csv(arquivo, sep=';', dtype=str)
                else: df_raw = pd.read_excel(arquivo, dtype=str)
                
                cols = df_raw.columns.tolist()
                cols_lower = [c.lower() for c in cols]
                
                def find_idx(keywords, default=0):
                    for k in keywords:
                        for i, c in enumerate(cols_lower):
                            if k in c: return i
                    return default
                
                idx_barras = next((i for i, c in enumerate(cols_lower) if 'barras' in c or 'ean' in c), -1)
                if idx_barras == -1: idx_barras = find_idx(['código', 'codigo'], 0)
                
                idx_nome = next((i for i, c in enumerate(cols_lower) if 'produto' in c or 'nome' in c), 1)
                
                # FOGE DA COLUNA PADRÃO E ACHA A DE ESTOQUE
                idx_qtd = next((i for i, c in enumerate(cols_lower) if 'estoque' in c), -1)
                if idx_qtd == -1:
                    for i, c in enumerate(cols_lower):
                        if 'qtd' in c and 'padr' not in c:
                            idx_qtd = i
                            break
                    if idx_qtd == -1: idx_qtd = len(cols)-1

                c1, c2, c3 = st.columns(3)
                col_barras = c1.selectbox("CÓDIGO BARRAS", cols, index=idx_barras)
                col_nome = c2.selectbox("NOME DO PRODUTO", cols, index=idx_nome)
                col_qtd = c3.selectbox("QUANTIDADE (LOJA)", cols, index=idx_qtd)
                
                if st.button("🚀 ATUALIZAR ESTOQUE DA LOJA", type="primary"):
                    df_raw['codigo_limpo'] = df_raw[col_barras].apply(padronizar_codigo_barras)
                    df_raw = df_raw[~df_raw['codigo_limpo'].isin(["", "NAN", "NONE"])]
                    df_raw[col_qtd] = df_raw[col_qtd].apply(parse_num_br)
                    
                    df_agrupado = df_raw.groupby('codigo_limpo', as_index=False).agg({col_nome: 'first', col_qtd: 'sum'})
                    
                    novos = []
                    for _, row in df_agrupado.iterrows():
                        cod_limpo = row['codigo_limpo']
                        nome = str(row[col_nome]).upper()
                        qtd = float(row[col_qtd])
                        
                        mask_cod = df['código de barras'].apply(padronizar_codigo_barras) == cod_limpo
                        if mask_cod.any():
                            df.at[df[mask_cod].index[0], 'qtd.estoque'] = qtd
                        else:
                            # Se não achou por código, procura por NOME para não clonar
                            mask_nome = df['nome do produto'] == nome
                            if mask_nome.any():
                                idx = df[mask_nome].index[0]
                                df.at[idx, 'qtd.estoque'] = qtd
                                if not padronizar_codigo_barras(df.at[idx, 'código de barras']):
                                    df.at[idx, 'código de barras'] = row[col_barras]
                            else:
                                novos.append({'código de barras': row[col_barras], 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'status': 'Ativo'})
                    
                    if novos: df = pd.concat([df, pd.DataFrame(novos)], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    st.success("Estoque da Loja Atualizado com Sucesso!")
                    st.rerun()
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")

    elif modo == "📈 Vendas (Importar & 80/20)":
        st.title(f"📈 Vendas")
        st.info("Para importar vendas, utilize o arquivo de transações.")

    elif modo == "🔎 Raio-X do Estoque (Auditoria)":
        st.title(f"🔎 Raio-X do Estoque")
        c1, c2 = st.columns(2)
        hoje = obter_hora_manaus().date()
        dt_ini = c1.date_input("📅 Data Inicial:", hoje - timedelta(days=30))
        dt_fim = c2.date_input("📅 Data Final:", hoje)
        
        busca_raiox = st.text_input("🔍 Buscar Produto:", placeholder="Digite o nome ou código...")
        
        if st.button("🚀 GERAR RAIO-X", type="primary"):
            with st.spinner("Analisando históricos..."):
                df_c = carregar_historico(prefixo)
                df_v = carregar_vendas_itens(prefixo)
                if not df_c.empty: df_c['data'] = pd.to_datetime(df_c['data'], errors='coerce')
                if not df_v.empty: df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')

                dt_ini_full = datetime.combine(dt_ini, datetime.min.time())
                dt_fim_full = datetime.combine(dt_fim, datetime.max.time())

                if not df_c.empty: df_c = df_c[(df_c['data'] >= dt_ini_full) & (df_c['data'] <= dt_fim_full)]
                if not df_v.empty: df_v = df_v[(df_v['data_hora'] >= dt_ini_full) & (df_v['data_hora'] <= dt_fim_full)]

                resultado = []
                df_filtrado = filtrar_dados_inteligente(df, 'nome do produto', busca_raiox) if busca_raiox else df

                for idx, row in df_filtrado.iterrows():
                    cod_limpo = padronizar_codigo_barras(row.get('código de barras', ''))
                    nome = str(row.get('nome do produto', '')).strip().upper()

                    qtd_compra = 0
                    if not df_c.empty:
                        m_c_cod = df_c['código_barras'].apply(padronizar_codigo_barras) == cod_limpo if cod_limpo and 'código_barras' in df_c.columns else pd.Series(False, index=df_c.index)
                        m_c_nome = df_c['produto'].astype(str).str.upper() == nome
                        qtd_compra = df_c[m_c_cod | m_c_nome]['qtd'].sum()

                    qtd_venda = 0
                    if not df_v.empty:
                        m_v_cod = df_v['código_barras'].apply(padronizar_codigo_barras) == cod_limpo if cod_limpo and 'código_barras' in df_v.columns else pd.Series(False, index=df_v.index)
                        m_v_nome = df_v['produto'].astype(str).str.upper() == nome
                        qtd_venda = df_v[m_v_cod | m_v_nome]['qtd_vendida'].sum()

                    if qtd_compra == 0 and qtd_venda == 0: continue

                    saldo = qtd_compra - qtd_venda
                    txt_saldo = f"🟢 Aumentou {int(saldo)} un." if saldo > 0 else (f"🔴 Diminuiu {abs(int(saldo))} un." if saldo < 0 else "⚪ Ficou igual")
                    
                    resultado.append({"🏷️ Produto": nome, "🏪 Loja": int(row.get('qtd.estoque', 0)), "🏡 Casa": int(row.get('qtd_central', 0)), "📥 Compras": int(qtd_compra), "🛍️ Vendas": int(qtd_venda), "⚖️ Saldo": txt_saldo})

                if resultado: st.dataframe(pd.DataFrame(resultado), use_container_width=True, hide_index=True)
                else: st.info("Sem movimentações para este período.")

    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        df['display'] = df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)
        busca = st.selectbox("Buscar Produto:", [""] + df['display'].tolist())
        if busca:
            idx = df[df['display'] == busca].index[0]
            st.metric("Loja", int(df.at[idx, 'qtd.estoque']))
            st.metric("Casa", int(df.at[idx, 'qtd_central']))

    elif modo == "💰 Inteligência de Compras (Histórico)":
        st.title("💰 Inteligência de Compras")
        df_hist = carregar_historico(prefixo)
        st.dataframe(df_hist)

    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa)")
        df_edit = st.data_editor(df[['código de barras', 'nome do produto', 'qtd_central']], use_container_width=True)
        if st.button("Salvar Casa"):
            df.update(df_edit)
            salvar_estoque(df, prefixo)
            st.success("Salvo!")

    elif modo == "📋 Tabela Geral":
        st.title("📋 Geral")
        st.info("Tabela de controle total. Se houverem produtos duplicados com o mesmo nome, clique no botão para juntá-los.")
        df_edit = st.data_editor(df, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 SALVAR ALTERAÇÕES GERAIS"):
                df.update(df_edit)
                salvar_estoque(df, prefixo)
                st.success("Salvo!")
                st.rerun()
        with c2:
            if st.button("🔮 UNIFICAR CLONES E LIMPAR DUPLICATAS"):
                df.update(df_edit)
                df = unificar_produtos_por_codigo(df)
                salvar_estoque(df, prefixo)
                st.success("Produtos repetidos foram fundidos com sucesso!")
                st.rerun()

    elif modo == "🛠️ Ajuste & Limpeza (HOSPITAL)":
        st.title("🏥 Hospital do Estoque (Ajuste & Limpeza)")
        st.write("Use as ferramentas abaixo para curar as falhas de comunicação entre a Loja, a Casa e os arquivos.")
        
        st.markdown("---")
        st.subheader("🧹 1. Apagar Lixos e Limpar Banco de Dados")
        st.write("Se o seu estoque ficou com datas no lugar do nome ou da quantidade de 813 mil, **clique aqui primeiro** para zerar a planilha e importar limpo novamente.")
        if st.button("🧨 APAGAR TODOS OS PRODUTOS (RESET TOTAL)", type="primary"):
            df.drop(df.index, inplace=True)
            salvar_estoque(df, prefixo)
            st.success("✅ Banco de dados apagado com sucesso! Vá em Sincronizar Planograma e importe a planilha limpa.")
            time.sleep(3)
            st.rerun()

        st.markdown("---")
        st.subheader("📉 2. Zerar Negativos")
        if st.button("Zerar Estoque Negativo"):
            mask_neg = df['qtd.estoque'] < 0
            df.loc[mask_neg, 'qtd.estoque'] = 0
            salvar_estoque(df, prefixo)
            st.success("Produtos negativos zerados.")
            st.rerun()

        st.markdown("---")
        st.subheader("🪄 3. Máquina do Tempo com IA (Recalcular Casa)")
        st.write("O sistema vai ler o nome da nota fiscal e o código de barras, e associar ao seu produto atual para **INJETAR** a quantidade correta na Casa.")
        
        if st.button("🚀 RECALCULAR CASA AGORA", type="primary"):
            with st.spinner("Puxando histórico e reabastecendo a Casa..."):
                df['qtd_central'] = 0 
                df_c = carregar_historico(prefixo)
                
                count = 0
                if not df_c.empty:
                    lista_nomes_sis = df['nome do produto'].tolist()
                    for _, hist_row in df_c.iterrows():
                        nome_hist = str(hist_row.get('produto', '')).strip().upper()
                        qtd_comprada = float(hist_row.get('qtd', 0))
                        
                        if qtd_comprada > 0:
                            melhor_nome, _ = encontrar_melhor_match(nome_hist, lista_nomes_sis, cutoff=0.5)
                            if melhor_nome:
                                mask = df['nome do produto'] == melhor_nome
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] += qtd_comprada
                                count += 1
                
                salvar_estoque(df, prefixo)
                st.success(f"✅ Máquina do Tempo finalizada! {count} lotes de compras foram injetados na Casa com sucesso.")
                st.balloons()
                time.sleep(3)
                st.rerun()

    elif modo == "♻️ Restaurar Histórico":
        st.title("♻️ Restaurar Histórico")
        st.info("Função de backup de arquivos.")
