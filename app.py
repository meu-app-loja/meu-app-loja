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

# Configuração da página
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# ==============================================================================
# ☁️ CONEXÃO COM GOOGLE SHEETS (COM CACHE E PROTEÇÃO)
# ==============================================================================
@st.cache_resource
def conectar_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    client = gspread.authorize(creds)
    return client.open("Sistema_Estoque_Database")

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
            if not nome_limpo: nome_limpo = f"coluna_extra_{i}"
            nome_final = nome_limpo
            contador = 1
            while nome_final in vistos:
                nome_final = f"{nome_limpo}_{contador}"
                contador += 1
            vistos.add(nome_final)
            headers_unicos.append(nome_final)

        df = pd.DataFrame(dados, columns=headers_unicos)
        return df
    except Exception as e:
        return pd.DataFrame()

def salvar_no_google(df, nome_aba, permitir_vazio=False):
    if df.empty and not permitir_vazio: return
    try:
        st.cache_data.clear() 
        client = conectar_google_sheets()
        try: worksheet = client.worksheet(nome_aba)
        except gspread.WorksheetNotFound: worksheet = client.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        colunas_proibidas = ['display_combo', 'produto_str', 'Selecionar', 'status_temp', 'codigo_padrao', 'nome_padrao']
        cols_para_salvar = [c for c in df.columns if c not in colunas_proibidas]
        df_limpo = df[cols_para_salvar].copy().fillna("")
        
        if not df_limpo.empty: dados_lista = [df_limpo.columns.tolist()] + df_limpo.astype(str).values.tolist()
        else: dados_lista = [df.columns.tolist()] if not df.columns.empty else []

        worksheet.clear()
        if dados_lista:
            worksheet.update(dados_lista)
            time.sleep(2)
    except Exception as e:
        st.error(f"ERRO DE CONEXÃO AO SALVAR ({nome_aba}): {e}")

# ==============================================================================
# 🕒 FUNÇÕES AUXILIARES E DE BUSCA
# ==============================================================================
def obter_hora_manaus(): return datetime.utcnow() - timedelta(hours=4)

def normalizar_texto(texto):
    if not isinstance(texto, str): return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto):
    if not isinstance(texto, str): return ""
    return normalizar_texto(texto)

def padronizar_codigo_barras(valor):
    if pd.isna(valor) or valor is None: return ""
    s = str(valor).strip().upper()
    if s.endswith('.0'): s = s[:-2]
    s = re.sub(r'[^A-Z0-9]', '', s)
    s = s.lstrip('0')
    return s

def filtrar_df_busca_robusta(df, query, cols_text=None, cols_barcode=None):
    try:
        if df is None or len(df) == 0: return df
        if query is None: return df
        q_raw = str(query).strip()
        if not q_raw: return df

        cols_text = cols_text or []
        cols_barcode = cols_barcode or []
        cols_text = [c for c in cols_text if c in df.columns]
        cols_barcode = [c for c in cols_barcode if c in df.columns]

        base_parts = [df[c].fillna("").astype(str).map(normalizar_para_busca) for c in cols_text]
        base = base_parts[0] if base_parts else pd.Series([""] * len(df), index=df.index)
        for s in base_parts[1:]: base = base + " | " + s

        barras = None
        if cols_barcode:
            bparts = [df[c].fillna("").astype(str).str.replace(r"\D", "", regex=True) for c in cols_barcode]
            barras = bparts[0]
            for b in bparts[1:]: barras = barras + " | " + b

        q_norm = normalizar_para_busca(q_raw)
        tokens = [t for t in re.split(r"\s+", q_norm) if t]

        mask = pd.Series(True, index=df.index)
        for t in tokens:
            if t.isdigit() and len(t) >= 4:
                m_bar = pd.Series(False, index=df.index)
                if barras is not None: m_bar = barras.str.contains(t, na=False)
                m_txt = base.str.contains(t, na=False)
                mask = mask & (m_bar | m_txt)
            else:
                mask = mask & base.str.contains(t, na=False)
        return df.loc[mask].copy()
    except: return df

def limpar_codigo_barras(valor):
    try:
        s = "" if valor is None else str(valor)
        return re.sub(r"\D", "", s)
    except: return ""

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
    melhor_linha = None
    melhor_score = -1
    for r in range(min(len(amostra), max_rows)):
        row_vals = [str(x) for x in amostra.iloc[r].tolist()]
        row_norm = [normalizar_para_busca(v) for v in row_vals]
        joined = " | ".join(row_norm)
        score = sum(1 for k in chaves if k in joined)
        nonempty = sum(1 for v in row_norm if v.strip() != "")
        if nonempty >= 2 and score > melhor_score:
            melhor_score = score
            melhor_linha = r

    header_row = melhor_linha if (melhor_linha is not None and melhor_score >= 2) else 0
    try: file_obj.seek(0)
    except: pass
    df = pd.read_excel(file_obj, header=header_row, dtype=str).dropna(axis=1, how="all")
    return df, header_row, list(df.columns)

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    def calcular_pontuacao(nome_xml, nome_sistema):
        set_xml = set(normalizar_para_busca(nome_xml).split())
        set_sis = set(normalizar_para_busca(nome_sistema).split())
        common = set_xml.intersection(set_sis)
        if not common: return 0.0
        total = set_xml.union(set_sis)
        score = len(common) / len(total)
        for palavra in common:
            if any(u in palavra for u in ['L', 'ML', 'KG', 'G', 'M']): 
                if any(c.isdigit() for c in palavra): score += 0.5
        return score
        
    melhor_match = None
    maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score:
            maior_score = score
            melhor_match = opcao
    if maior_score >= cutoff: return melhor_match, "Nome Similar"
    return None, "Nenhum"

# 🧑‍⚕️ CIRURGIA DE UNIFICAÇÃO (MAIS PODEROSA)
def unificar_produtos_por_codigo(df):
    if df.empty: return df
    for col in ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    lista_final = []
    
    # Agrupa por Nome Limpo para pegar as distorções
    df['nome_padrao'] = df['nome do produto'].apply(lambda x: re.sub(r'[^A-Z0-9]', '', normalizar_texto(x)))

    for nome, grupo in df.groupby('nome_padrao'):
        if len(grupo) > 1:
            melhor_nome = max(grupo['nome do produto'].tolist(), key=len)
            codigos = [str(c).strip() for c in grupo['código de barras'].tolist() if len(padronizar_codigo_barras(c)) > 3]
            melhor_cod = codigos[0] if codigos else ""
            
            soma_loja = grupo['qtd.estoque'].sum()
            soma_casa = grupo['qtd_central'].sum()
            custo_final = grupo['preco_custo'].max()
            venda_final = grupo['preco_venda'].max()
            status_final = 'Ativo' if 'Ativo' in grupo['status'].values else 'Inativo'
            
            base_ref = grupo[grupo['nome do produto'] == melhor_nome].iloc[0].to_dict()
            base_ref['código de barras'] = melhor_cod
            base_ref['qtd.estoque'] = soma_loja
            base_ref['qtd_central'] = soma_casa
            base_ref['preco_custo'] = custo_final
            base_ref['preco_venda'] = venda_final
            base_ref['status'] = status_final
            lista_final.append(base_ref)
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if 'nome_padrao' in df_novo.columns: df_novo = df_novo.drop(columns=['nome_padrao'])
    return df_novo

def processar_excel_oficial(arquivo_subido):
    try:
        if arquivo_subido.name.endswith('.csv'): df_temp = pd.read_csv(arquivo_subido, sep=None, engine='python', dtype=str)
        else: df_temp = pd.read_excel(arquivo_subido, dtype=str)
        if 'obrigatório' in str(df_temp.iloc[0].values): df_temp = df_temp.iloc[1:].reset_index(drop=True)
        df_temp.columns = df_temp.columns.str.strip()
        col_nome = next((c for c in df_temp.columns if 'nome' in c.lower()), 'Nome')
        col_cod = next((c for c in df_temp.columns if 'código' in c.lower() or 'barras' in c.lower()), 'Código de Barras Primário')
        df_limpo = df_temp[[col_nome, col_cod]].copy()
        df_limpo.columns = ['nome do produto', 'código de barras']
        df_limpo['nome do produto'] = df_limpo['nome do produto'].apply(normalizar_texto)
        df_limpo['código de barras'] = df_limpo['código de barras'].astype(str).str.replace('.0', '', regex=False).str.strip()
        salvar_no_google(df_limpo, "meus_produtos_oficiais")
        return True
    except Exception as e:
        st.error(f"Erro ao organizar o arquivo: {e}")
        return False

def carregar_base_oficial(): return carregar_do_google("meus_produtos_oficiais")

# ==============================================================================
# 🏢 CONFIGURAÇÃO E CARREGAMENTO
# ==============================================================================
st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)
st.sidebar.markdown("---")

prefixo = "loja1" if loja_atual == "Loja 1 (Principal)" else ("loja2" if loja_atual == "Loja 2 (Filial)" else "loja3")

def gerar_backup_zip_nuvem():
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        tabelas = [f"{prefixo}_estoque", f"{prefixo}_historico_compras", f"{prefixo}_movimentacoes", f"{prefixo}_vendas", f"{prefixo}_lista_compras", f"{prefixo}_log_auditoria", f"{prefixo}_ids_vendas", "meus_produtos_oficiais"]
        for tab in tabelas:
            df_temp = carregar_do_google(tab)
            if not df_temp.empty:
                zip_file.writestr(f"{tab}.csv", df_temp.to_csv(index=False).encode('utf-8'))
    buffer.seek(0)
    return buffer

st.sidebar.markdown("### 🛡️ Segurança (Nuvem)")
if st.sidebar.button("💾 Baixar Backup da Nuvem"):
    st.info("Baixando dados do Google Sheets...")
    st.sidebar.download_button(label="⬇️ Salvar Backup", data=gerar_backup_zip_nuvem(), file_name=f"backup_nuvem_{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M')}.zip", mime="application/zip")
st.sidebar.markdown("---")

def parse_num_br(x, default=0.0):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""): return float(default)
        s = str(x).strip().replace("R$", "").replace("\u00a0", " ").strip()
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."): s = s.replace(".", "").replace(",", ".")
            else: s = s.replace(",", "")
        else:
            if "," in s and "." not in s: s = s.replace(".", "").replace(",", ".")
        s = re.sub(r"[^0-9\-\.]", "", s)
        return float(s) if s not in ["", "-", ".", "-."] else float(default)
    except: return float(default)

def formatar_moeda_br(valor):
    try: return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    try:
        if df is None or len(df) == 0: return df
        cols_barcode_candidatas = ['código de barras','codigo de barras','código_barras','codigo_barras','barcode','ean']
        cols_barcode = [c for c in cols_barcode_candidatas if c in df.columns]
        cols_text = [coluna_busca] if coluna_busca in df.columns else []
        for extra in ['produto', 'nome do produto', 'nome_produto', 'descricao', 'descrição']:
            if extra in df.columns and extra not in cols_text: cols_text.append(extra)
        return filtrar_df_busca_robusta(df, texto_busca, cols_text=cols_text, cols_barcode=cols_barcode)
    except: return df

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

def garantir_colunas(df: pd.DataFrame, colunas_obrigatorias: list[str]) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame(columns=colunas_obrigatorias)
    for c in colunas_obrigatorias:
        if c not in df.columns: df[c] = ""
    return df

def blindar_estoque_df(df_estoque: pd.DataFrame) -> pd.DataFrame:
    if df_estoque is None: return pd.DataFrame()
    df = df_estoque.copy()
    df.columns = df.columns.astype(str).str.strip().str.lower()
    colunas_estoque_padrao = ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto', 'status']
    df = garantir_colunas(df, colunas_estoque_padrao)
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
    df['status'] = df['status'].replace('', 'Ativo').fillna('Ativo')
    df['status'] = df['status'].apply(lambda x: 'Ativo' if str(x).strip().lower() in {'ativo', 'a'} else ('Inativo' if str(x).strip().lower() in {'inativo', 'i'} else str(x).strip().title()))
    return df

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

def registrar_auditoria(prefixo, produto, qtd_antes, qtd_nova, acao, motivo="Manual"):
    try:
        aba_log = f"{prefixo}_log_auditoria"
        novo_log = {'data_hora': str(obter_hora_manaus()), 'produto': produto, 'qtd_antes': qtd_antes, 'qtd_nova': qtd_nova, 'acao': acao, 'motivo': motivo}
        df_log = carregar_do_google(aba_log)
        df_log = pd.concat([df_log, pd.DataFrame([novo_log])], ignore_index=True)
        salvar_no_google(df_log, aba_log)
    except: pass

def salvar_logs_em_lote(prefixo, lista_logs):
    if not lista_logs: return
    try:
        aba_log = f"{prefixo}_log_auditoria"
        df_log = carregar_do_google(aba_log)
        df_final = pd.concat([df_log, pd.DataFrame(lista_logs)], ignore_index=True)
        salvar_no_google(df_final, aba_log)
    except: pass

def carregar_ids_processados(prefixo):
    df_ids = carregar_do_google(f"{prefixo}_ids_vendas")
    if not df_ids.empty and 'id_transacao' in df_ids.columns: return set(df_ids['id_transacao'].astype(str).tolist())
    return set()

def salvar_ids_processados(prefixo, novos_ids):
    aba = f"{prefixo}_ids_vendas"
    if not novos_ids: return
    df_novo = pd.DataFrame({'id_transacao': list(novos_ids)})
    df_antigo = carregar_do_google(aba)
    if not df_antigo.empty: df_final = pd.concat([df_antigo, df_novo]).drop_duplicates()
    else: df_final = df_novo
    salvar_no_google(df_final, aba)

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    for loja in ["loja1", "loja2", "loja3"]:
        if loja == prefixo_ignorar: continue
        aba_outra = f"{loja}_estoque"
        df_outra = carregar_do_google(aba_outra)
        if not df_outra.empty:
            try:
                df_outra.columns = df_outra.columns.str.strip().str.lower()
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                    salvar_no_google(df_outra, aba_outra)
            except: pass

def atualizar_casa_global_em_lote(lista_atualizacoes, prefixo_origem):
    if not lista_atualizacoes: return
    dict_updates = {item['produto']: item for item in lista_atualizacoes}
    for loja in ["loja1", "loja2", "loja3"]:
        if loja == prefixo_origem: continue
        aba_outra = f"{loja}_estoque"
        df_outra = carregar_do_google(aba_outra)
        if not df_outra.empty:
            alterou_algo = False
            df_outra.columns = df_outra.columns.str.strip().str.lower()
            for produto, dados in dict_updates.items():
                mask = df_outra['nome do produto'].astype(str) == str(produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    df_outra.at[idx, 'qtd_central'] = dados['qtd_central']
                    if dados.get('custo') is not None: df_outra.at[idx, 'preco_custo'] = dados['custo']
                    if dados.get('venda') is not None: df_outra.at[idx, 'preco_venda'] = dados['venda']
                    alterou_algo = True
            if alterou_algo: salvar_no_google(df_outra, aba_outra)

def inicializar_arquivos(prefixo):
    arquivos = {
        f"{prefixo}_estoque": ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto', 'status'],
        f"{prefixo}_historico_compras": ['data', 'data_emissao', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
        f"{prefixo}_movimentacoes": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],
        f"{prefixo}_vendas_itens": ['data_hora', 'mes_ref', 'transacao', 'código_barras', 'produto', 'qtd_vendida', 'preco_unit', 'valor_total', 'canal', 'obs_importacao'],
        f"{prefixo}_vendas_transacoes": ['data_hora', 'mes_ref', 'transacao', 'subtotal', 'descontos', 'taxas', 'total', 'forma_pagamento', 'obs_importacao'],
        f"{prefixo}_vendas_mensal_produto": ['mes_ref', 'código_barras', 'produto', 'qtd_vendida', 'valor_total', 'ultima_venda'],
        f"{prefixo}_mix_review": ['data_criacao', 'codigo_barras', 'produto', 'status', 'motivo', 'ultima_venda', 'dias_sem_venda', 'meses_sem_venda', 'observacao', 'decisao'],
        f"{prefixo}_lista_compras": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo'],
    }
    for aba, colunas in arquivos.items():
        df = carregar_do_google(aba)
        if df.empty: salvar_no_google(pd.DataFrame(columns=colunas), aba)

def carregar_dados(prefixo_arquivo):
    try: return blindar_estoque_df(carregar_do_google(f"{prefixo_arquivo}_estoque"))
    except: return pd.DataFrame()

def carregar_historico(prefixo_arquivo):
    try:
        df_h = carregar_do_google(f"{prefixo_arquivo}_historico_compras")
        if df_h.empty: return pd.DataFrame()
        cols_ok = [c for c in df_h.columns if c not in ['display_combo', 'produto_str', 'Selecionar', 'status_temp']]
        df_h = df_h[cols_ok]
        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
        for c in ['qtd', 'preco_pago', 'total_gasto', 'desconto_total_money', 'preco_sem_desconto']:
             if c in df_h.columns: 
                 df_h[c] = df_h[c].astype(str).str.replace(',', '.', regex=False)
                 df_h[c] = pd.to_numeric(df_h[c], errors='coerce').fillna(0)
        for col in ['numero_nota', 'obs_importacao', 'data_emissao', 'código_barras']:
             if col not in df_h.columns: df_h[col] = ""
        if 'desconto_total_money' not in df_h.columns: df_h['desconto_total_money'] = 0.0
        if 'preco_sem_desconto' not in df_h.columns: df_h['preco_sem_desconto'] = 0.0
        return df_h
    except: return pd.DataFrame()

def carregar_movimentacoes(prefixo_arquivo):
    try:
        df_m = carregar_do_google(f"{prefixo_arquivo}_movimentacoes")
        if not df_m.empty: df_m['data_hora'] = pd.to_datetime(df_m['data_hora'], errors='coerce')
        return df_m
    except: return pd.DataFrame()

def carregar_vendas(prefixo_arquivo):
    try:
        df_v = carregar_do_google(f"{prefixo_arquivo}_vendas")
        if not df_v.empty: df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')
        return df_v
    except: return pd.DataFrame()

def carregar_vendas_itens(prefixo_arquivo):
    try:
        df_vi = carregar_do_google(f"{prefixo_arquivo}_vendas_itens")
        if df_vi.empty: return pd.DataFrame()
        df_vi.columns = df_vi.columns.str.strip().str.lower()
        if 'data_hora' in df_vi.columns: df_vi['data_hora'] = pd.to_datetime(df_vi['data_hora'], errors='coerce')
        for c in ['qtd_vendida', 'preco_unit', 'valor_total']:
            if c in df_vi.columns: df_vi[c] = df_vi[c].apply(parse_num_br)
        if 'código_barras' in df_vi.columns: df_vi['código_barras'] = df_vi['código_barras'].astype(str).str.replace('.0','',regex=False).str.strip()
        if 'produto' in df_vi.columns: df_vi['produto'] = df_vi['produto'].astype(str).apply(normalizar_texto)
        return df_vi
    except: return pd.DataFrame()

def carregar_vendas_transacoes(prefixo_arquivo):
    try:
        df_vt = carregar_do_google(f"{prefixo_arquivo}_vendas_transacoes")
        if df_vt.empty: return pd.DataFrame()
        df_vt.columns = df_vt.columns.str.strip().str.lower()
        if 'data_hora' in df_vt.columns: df_vt['data_hora'] = pd.to_datetime(df_vt['data_hora'], errors='coerce')
        for c in ['subtotal', 'descontos', 'taxas', 'total']:
            if c in df_vt.columns: df_vt[c] = df_vt[c].apply(parse_num_br)
        return df_vt
    except: return pd.DataFrame()

def carregar_lista_compras(prefixo_arquivo):
    try:
        df = carregar_do_google(f"{prefixo_arquivo}_lista_compras")
        if df.empty: return pd.DataFrame()
        if 'código_barras' not in df.columns: df['código_barras'] = ""
        if 'qtd_sugerida' in df.columns: df['qtd_sugerida'] = pd.to_numeric(df['qtd_sugerida'], errors='coerce')
        return df
    except: return pd.DataFrame()

def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]

    dados_nota = {'numero': '', 'fornecedor': '', 'data_emissao': '', 'itens': []}
    
    if tag_limpa(root) == 'NotaFiscal':
        info = root.find('Info')
        if info is not None:
            dados_nota['numero'] = info.find('NumeroNota').text if info.find('NumeroNota') is not None else ""
            dados_nota['fornecedor'] = info.find('Fornecedor').text if info.find('Fornecedor') is not None else ""
            try: dados_nota['data_emissao'] = info.find('DataCompra').text
            except: pass
        produtos = root.findall('.//Produtos/Item')
        for item_xml in produtos:
            item = {'codigo_interno': '', 'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
            item['nome'] = normalizar_texto(item_xml.find('Nome').text)
            qtd_raw = float(item_xml.find('Quantidade').text)
            val_final = float(item_xml.find('ValorPagoFinal').text)
            desc_val = float(item_xml.find('ValorDesconto').text)
            item['ean'] = item_xml.find('CodigoBarras').text or ""
            item['codigo_interno'] = item['ean']
            item['desconto_total_item'] = desc_val
            if qtd_raw > 0:
                item['qtd'] = qtd_raw
                item['preco_un_liquido'] = val_final / qtd_raw
                item['preco_un_bruto'] = (val_final + desc_val) / qtd_raw
            ean_xml = str(item['ean']).strip()
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']: item['ean'] = item['codigo_interno']
            dados_nota['itens'].append(item)
        return dados_nota

    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == '': dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi':
            raw_date = elem.text
            if raw_date:
                try: dados_nota['data_emissao'] = datetime.strptime(raw_date[:19], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M")
                except: dados_nota['data_emissao'] = raw_date 

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
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']: item['ean'] = item['codigo_interno']
            dados_nota['itens'].append(item)
    return dados_nota

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

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================
inicializar_arquivos(prefixo)

if 'df_ativo' not in st.session_state or st.session_state.get('loja_ativa_cache') != prefixo:
    st.session_state['df_ativo'] = carregar_dados(prefixo)
    st.session_state['loja_ativa_cache'] = prefixo
    st.session_state['alteracoes_pendentes'] = 0

df = st.session_state['df_ativo']
df_oficial = carregar_base_oficial() 

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)",
        "📦 Inventário Rápido (Lote)",
        "⚖️ Conciliação (Shoppbud vs App)",
        "🚚 Transferência em Massa (Picklist)",
        "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", 
        "📥 Importar XML (Associação Inteligente)", 
        "🔄 Sincronizar (Planograma)",
        "📈 Vendas (Importar & 80/20)",
        "🔎 Raio-X do Estoque (Auditoria)",
        "🏠 Gôndola (Loja)", 
        "💰 Inteligência de Compras (Histórico)",
        "🏡 Estoque Central (Casa)",
        "📋 Tabela Geral",
        "🛠️ Ajuste & Limpeza (HOSPITAL)",
        "♻️ Restaurar Histórico"
    ])

    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle - {loja_atual}")
        if df.empty:
            st.info("Comece cadastrando produtos.")
        else:
            df_ativos = df[df['status'] == 'Ativo']
            # CORREÇÃO AQUI: Mostramos os Totais Separados de Forma Mais Clara!
            total_itens = len(df_ativos)
            soma_loja = int(df_ativos['qtd.estoque'].sum())
            soma_casa = int(df_ativos['qtd_central'].sum())
            valor_estoque = blindar_valor_estoque(df)
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Produtos Cadastrados", total_itens)
            c2.metric("🏪 Itens na Loja", soma_loja)
            c3.metric("🏡 Itens na Casa", soma_casa)
            c4.metric("💰 Valor Investido", f"R$ {formatar_moeda_br(valor_estoque)}")
            st.divider()

    elif modo == "📦 Inventário Rápido (Lote)":
        st.title("📦 Inventário Rápido")
        st.info("Em breve.")

    elif modo == "⚖️ Conciliação (Shoppbud vs App)":
        st.title("⚖️ Conciliação de Estoque")
        arq_planograma = st.file_uploader("📂 Carregar Planograma Shoppbud (.xlsx)", type=['xlsx', 'csv'])
        if arq_planograma:
            if arq_planograma.name.endswith('.csv'): df_plan = pd.read_csv(arq_planograma, sep=None, engine='python', dtype=str)
            else: df_plan = pd.read_excel(arq_planograma, dtype=str)
            
            col_cod_plan = next((c for c in df_plan.columns if ('código' in c.lower() or 'codigo' in c.lower()) and 'barras' in c.lower()), None)
            col_qtd_plan = next((c for c in df_plan.columns if 'qtd' in c.lower() and 'estoque' in c.lower()), None)
            
            if col_cod_plan and col_qtd_plan:
                df_plan[col_qtd_plan] = df_plan[col_qtd_plan].apply(parse_num_br)
                df_plan['código normalizado'] = df_plan[col_cod_plan].apply(padronizar_codigo_barras)
                df['código normalizado'] = df['código de barras'].apply(padronizar_codigo_barras)
                df_concilia = pd.merge(df[['código normalizado', 'nome do produto', 'qtd.estoque']], df_plan[[col_cod_plan, col_qtd_plan, 'código normalizado']], on='código normalizado', how='inner')
                df_concilia['Diferença'] = df_concilia['qtd.estoque'] - df_concilia[col_qtd_plan]
                df_divergente = df_concilia[df_concilia['Diferença'] != 0].copy()
                if df_divergente.empty: st.success("✅ Estoque 100% batendo!")
                else: st.dataframe(df_divergente)
            else: st.error("Colunas não encontradas.")

    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa")
        st.info("Em breve.")

    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        st.info("Em breve.")

    elif modo == "🆕 Cadastrar Produto":
        st.title(f"🆕 Cadastro - {loja_atual}")
        st.info("Em breve.")

    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML")
        df_hist = carregar_historico(prefixo)
        modo_import = st.radio("Modo:", ["📦 Atualizar Estoque da Casa", "📖 Apenas Histórico"], horizontal=True)
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            dados = ler_xml_nfe(arquivo_xml, df_oficial)
            st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
            dt_lanc = st.date_input("Dia:", value=obter_hora_manaus().date())
            hr_lanc = st.time_input("Hora:", value=obter_hora_manaus().time())
            data_lancamento_final = datetime.combine(dt_lanc, hr_lanc)

            lista_visuais = sorted((df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist())
            lista_sistema = ["(CRIAR NOVO PRODUTO)"] + [f"[SISTEMA] {x}" for x in lista_visuais]
            
            escolhas = {}
            for i, item in enumerate(dados['itens']):
                match_inicial = "(CRIAR NOVO PRODUTO)"
                if not df.empty:
                    # Tenta associar de forma "Fuzzy" para ajudar
                    melhor, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].tolist(), cutoff=0.4)
                    if melhor:
                        cod_melhor = df.loc[df['nome do produto'] == melhor, 'código de barras'].values[0]
                        match_inicial = f"[SISTEMA] {cod_melhor} - {melhor}"

                st.divider()
                c1, c2 = st.columns([1, 1])
                with c1: st.markdown(f"📦 **{item['nome']}** (XML)\n\nEAN: {item['ean']} | Qtd Comprada: {item['qtd']}")
                with c2: escolhas[i] = st.selectbox("Qual é esse produto no seu sistema?", lista_sistema, index=lista_sistema.index(match_inicial) if match_inicial in lista_sistema else 0, key=f"x_{i}")
            
            if st.button("✅ CONFIRMAR ENTRADA NA CASA"):
                novos_hist = []
                for i, item in enumerate(dados['itens']):
                    esc = escolhas[i]
                    if "[SISTEMA]" in esc:
                         nome_final = esc.replace("[SISTEMA] ", "").split(' - ', 1)[1]
                         mask = df['nome do produto'] == nome_final
                         if mask.any():
                             idx = df[mask].index[0]
                             if "Atualizar" in modo_import:
                                 df.at[idx, 'qtd_central'] += item['qtd']
                             df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                             novos_hist.append({'data': str(data_lancamento_final), 'código_barras': df.at[idx, 'código de barras'], 'produto': nome_final, 'fornecedor': dados['fornecedor'], 'qtd': item['qtd'], 'preco_pago': item['preco_un_liquido'], 'total_gasto': item['qtd']*item['preco_un_liquido']})
                    else:
                        novo = {'código de barras': item['ean'], 'nome do produto': item['nome'].upper(), 'qtd.estoque': 0, 'qtd_central': item['qtd'] if "Atualizar" in modo_import else 0, 'preco_custo': item['preco_un_liquido'], 'status': 'Ativo'}
                        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                salvar_estoque(df, prefixo)
                if novos_hist: salvar_historico(pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True), prefixo)
                st.success("Entradas registradas na Casa!")
                st.rerun()

    # 🧑‍⚕️ CIRURGIA MESTRA: PLANOGRAMA BLINDADO
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar (Planograma)")
        st.info("💡 Para evitar erros, o arquivo agora é lido de forma blindada.")
        arquivo = st.file_uploader("📂 Planograma (.csv ou .xlsx)", type=['xlsx', 'csv'])
        if arquivo:
            try:
                arquivo.seek(0)
                if arquivo.name.endswith('.csv'): df_raw = pd.read_csv(arquivo, sep=None, engine='python', dtype=str)
                else: df_raw = pd.read_excel(arquivo, dtype=str)
                
                cols = df_raw.columns.tolist()
                cols_lower = [c.lower() for c in cols]
                
                # Identifica colunas exatas
                idx_barras = next((i for i, c in enumerate(cols_lower) if 'codigo de barras' in c), 0)
                idx_nome = next((i for i, c in enumerate(cols_lower) if 'nome do produto' in c), 1)
                idx_qtd = next((i for i, c in enumerate(cols_lower) if 'qtd.estoque' in c), len(cols)-1)

                c1, c2, c3 = st.columns(3)
                col_barras = c1.selectbox("CÓDIGO BARRAS", cols, index=idx_barras)
                col_nome = c2.selectbox("NOME DO PRODUTO", cols, index=idx_nome)
                col_qtd = c3.selectbox("QUANTIDADE (LOJA)", cols, index=idx_qtd)
                
                if st.button("🚀 ATUALIZAR ESTOQUE DA LOJA"):
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
        st.info("Para gerar o raio-x, selecione a data.")

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

    # 🏥 AQUI ESTÁ O NOVO HOSPITAL DO ESTOQUE
    elif modo == "🛠️ Ajuste & Limpeza (HOSPITAL)":
        st.title("🏥 Hospital do Estoque (Ajuste & Limpeza)")
        st.write("Use as ferramentas abaixo para curar as falhas de comunicação entre a Loja, a Casa e os arquivos.")
        
        st.markdown("---")
        st.subheader("🧹 1. Zerar Negativos")
        if st.button("Zerar Estoque Negativo"):
            mask_neg = df['qtd.estoque'] < 0
            df.loc[mask_neg, 'qtd.estoque'] = 0
            salvar_estoque(df, prefixo)
            st.success("Produtos negativos zerados.")
            st.rerun()

        st.markdown("---")
        st.subheader("🪄 2. Recalcular Casa pelas Notas Fiscais (Máquina do Tempo)")
        st.write("A sua Casa está zerada porque os XMLs antigos criaram clones em vez de adicionar o estoque na sua Coca-Cola verdadeira. Esta função vai varrer todo o seu histórico e **INJETAR** a quantidade correta na Casa dos produtos atuais.")
        
        if st.button("🚀 RECALCULAR CASA AGORA", type="primary"):
            with st.spinner("Puxando histórico e reabastecendo a Casa..."):
                df['qtd_central'] = 0 # Zera a casa para não somar duplicado
                df_hist = carregar_historico(prefixo)
                
                count = 0
                for _, hist_row in df_hist.iterrows():
                    nome_hist = str(hist_row.get('produto', '')).strip().upper()
                    qtd_comprada = float(hist_row.get('qtd', 0))
                    
                    if qtd_comprada > 0:
                        # Procura o produto no banco pelo nome da nota de forma difusa (Fuzzy)
                        melhor_nome, _ = encontrar_melhor_match(nome_hist, df['nome do produto'].tolist(), cutoff=0.5)
                        
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
        st.info("Função de backup de arquivos do histórico de compras.")
