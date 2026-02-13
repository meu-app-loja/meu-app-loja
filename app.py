import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
from io import BytesIO
import zipfile
import re

# --- NOVO: Biblioteca para gráficos bonitos e interativos ---
import plotly.express as px 

# --- BIBLIOTECAS DO GOOGLE SHEETS ---
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
    """Conecta ao Google Sheets usando as credenciais dos Secrets do Streamlit."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    client = gspread.authorize(creds)
    return client.open("Sistema_Estoque_Database")

# Cache de 60 segundos para evitar ler a mesma coisa toda hora (Economiza Cota)
# --- VERSÃO BLINDADA CONTRA ERRO DE COLUNAS DUPLICADAS/VAZIAS ---
@st.cache_data(ttl=60) 
def carregar_do_google(nome_aba):
    """Lê uma aba específica da planilha e transforma em DataFrame (Com Cache)."""
    try:
        sh = conectar_google_sheets()

        try:
            worksheet = sh.worksheet(nome_aba)
        except gspread.WorksheetNotFound:
            return pd.DataFrame() 
        
        dados = worksheet.get_all_values()
        if not dados:
            return pd.DataFrame()
            
        headers = dados.pop(0)
        
        # --- BLINDAGEM CIRÚRGICA (RESOLVE O ERRO DuplicateError) ---
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
        # -----------------------------------------------------------

        df = pd.DataFrame(dados, columns=headers_unicos)
        return df
    except Exception as e:
        return pd.DataFrame()

def salvar_no_google(df, nome_aba, permitir_vazio=False):
    """
    Salva o DataFrame na nuvem e limpa o cache.
    Inclui FILTRO DE LIMPEZA para não salvar colunas de rascunho (display_combo, etc).
    """
    if df.empty and not permitir_vazio: 
        return

    try:
        st.cache_data.clear() 
        client = conectar_google_sheets()
        sh = client
        try:
            worksheet = sh.worksheet(nome_aba)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        
        # --- FILTRO DE SEGURANÇA (LIMPEZA AUTOMÁTICA) ---
        # Antes de salvar, removemos colunas que o sistema cria apenas para visualização
        colunas_proibidas = ['display_combo', 'produto_str', 'Selecionar', 'status_temp']
        # Mantém apenas colunas que NÃO estão na lista de proibidas
        cols_para_salvar = [c for c in df.columns if c not in colunas_proibidas]
        df_limpo = df[cols_para_salvar].copy()
        
        df_limpo = df_limpo.fillna("")
        if not df_limpo.empty:
            dados_lista = [df_limpo.columns.tolist()] + df_limpo.astype(str).values.tolist()
        else:
            dados_lista = [df.columns.tolist()] if not df.columns.empty else []

        worksheet.clear()
        if dados_lista:
            worksheet.update(dados_lista)
            time.sleep(2)
        
    except Exception as e:
        st.error(f"ERRO DE CONEXÃO AO SALVAR ({nome_aba}): {e}. Tente novamente em alguns segundos.")

# ==============================================================================
# 🕒 AJUSTE DE FUSO HORÁRIO E FUNÇÕES
# ==============================================================================
def obter_hora_manaus():
    return datetime.utcnow() - timedelta(hours=4)

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return texto.upper().strip()

def normalizar_para_busca(texto):
    if not isinstance(texto, str): return ""
    return normalizar_texto(texto)


def filtrar_df_busca_robusta(df, query, cols_text=None, cols_barcode=None):
    """Filtro de busca robusto:
    - Aceita múltiplas palavras (todas precisam bater)
    - Normaliza acentos/caixa
    - Se o usuário digitar 4+ dígitos, também procura como substring em códigos de barras
    """
    try:
        if df is None or len(df) == 0:
            return df
        if query is None:
            return df
        q_raw = str(query).strip()
        if not q_raw:
            return df

        cols_text = cols_text or []
        cols_barcode = cols_barcode or []

        # garante colunas existentes
        cols_text = [c for c in cols_text if c in df.columns]
        cols_barcode = [c for c in cols_barcode if c in df.columns]

        # série base (texto concatenado)
        base_parts = []
        for c in cols_text:
            s = df[c].fillna("").astype(str).map(normalizar_para_busca)
            base_parts.append(s)
        if base_parts:
            base = base_parts[0]
            for s in base_parts[1:]:
                base = base + " | " + s
        else:
            base = pd.Series([""] * len(df), index=df.index)

        # prepara barras (somente dígitos)
        barras = None
        if cols_barcode:
            bparts = []
            for c in cols_barcode:
                b = df[c].fillna("").astype(str).str.replace(r"\D", "", regex=True)
                bparts.append(b)
            barras = bparts[0]
            for b in bparts[1:]:
                barras = barras + " | " + b

        # tokens normalizados
        q_norm = normalizar_para_busca(q_raw)
        tokens = [t for t in re.split(r"\s+", q_norm) if t]

        mask = pd.Series(True, index=df.index)
        for t in tokens:
            if t.isdigit() and len(t) >= 4:
                m_bar = pd.Series(False, index=df.index)
                if barras is not None:
                    m_bar = barras.str.contains(t, na=False)
                m_txt = base.str.contains(t, na=False)
                mask = mask & (m_bar | m_txt)
            else:
                mask = mask & base.str.contains(t, na=False)

        return df.loc[mask].copy()
    except Exception:
        # em caso de qualquer erro de parsing/busca, não quebra a tela: retorna df original
        return df



def limpar_codigo_barras(valor):
    """Converte código de barras para string apenas com dígitos."""
    try:
        s = "" if valor is None else str(valor)
        return re.sub(r"\D", "", s)
    except Exception:
        return ""

def pick_col(colunas, *candidatos):
    """Escolhe a coluna mais provável com base em normalização (acentos/pontuação) e match por contém."""
    if colunas is None:
        return None
    cols = list(colunas)
    norm_map = {c: normalizar_para_busca(str(c)) for c in cols}
    cand_norm = [normalizar_para_busca(str(x)) for x in candidatos if x is not None and str(x).strip()!=""]
    # 1) match exato
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and nc == cn:
                return c
    # 2) contém (token)
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and cn in nc:
                return c
    return None



def ler_excel_com_header_auto(file_obj, max_rows=25):
    """Lê Excel tentando detectar automaticamente a linha de cabeçalho.
    Retorna (df, header_row_detected, cols_detectadas)."""
    # Streamlit UploadedFile costuma ser BytesIO; precisamos resetar o ponteiro entre leituras
    try:
        file_obj.seek(0)
    except Exception:
        pass

    amostra = pd.read_excel(file_obj, header=None, nrows=max_rows)
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

    try:
        file_obj.seek(0)
    except Exception:
        pass

    df = pd.read_excel(file_obj, header=header_row).dropna(axis=1, how="all")
    cols = list(df.columns)
    return df, header_row, cols

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    common = set_xml.intersection(set_sis)
    if not common: return 0.0
    total = set_xml.union(set_sis)
    score = len(common) / len(total)
    for palavra in common:
        if any(u in palavra for u in ['L', 'ML', 'KG', 'G', 'M']): 
            if any(c.isdigit() for c in palavra):
                score += 0.5
    return score

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    melhor_match = None
    maior_score = 0.0
    for opcao in lista_opcoes:
        if opcao == "(CRIAR NOVO)": continue
        score = calcular_pontuacao(nome_buscado, opcao)
        if score > maior_score:
            maior_score = score
            melhor_match = opcao
    if maior_score >= cutoff:
        return melhor_match, "Nome Similar (Palavras)"
    return None, "Nenhum"

def unificar_produtos_por_codigo(df):
    if df.empty: return df
    cols_num = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']
    for col in cols_num:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    lista_final = []
    sem_codigo = df[df['código de barras'] == ""]
    com_codigo = df[df['código de barras'] != ""]

    for cod, grupo in com_codigo.groupby('código de barras'):
        if len(grupo) > 1:
            melhor_nome = max(grupo['nome do produto'].tolist(), key=len)
            soma_loja = grupo['qtd.estoque'].sum()
            soma_casa = grupo['qtd_central'].sum()
            custo_final = grupo['preco_custo'].max()
            venda_final = grupo['preco_venda'].max()
            sem_desc_final = grupo['preco_sem_desconto'].max() if 'preco_sem_desconto' in grupo.columns else 0.0
            status_final = 'Ativo' if 'Ativo' in grupo['status'].values else 'Inativo'
            
            base_ref = grupo[grupo['nome do produto'] == melhor_nome].iloc[0].to_dict()
            base_ref['qtd.estoque'] = soma_loja
            base_ref['qtd_central'] = soma_casa
            base_ref['preco_custo'] = custo_final
            base_ref['preco_venda'] = venda_final
            base_ref['preco_sem_desconto'] = sem_desc_final
            base_ref['status'] = status_final
            lista_final.append(base_ref)
        else:
            lista_final.append(grupo.iloc[0].to_dict())

    df_novo = pd.DataFrame(lista_final)
    if not sem_codigo.empty:
        df_novo = pd.concat([df_novo, sem_codigo], ignore_index=True)
    return df_novo

def processar_excel_oficial(arquivo_subido):
    try:
        if arquivo_subido.name.endswith('.csv'):
            df_temp = pd.read_csv(arquivo_subido)
        else:
            df_temp = pd.read_excel(arquivo_subido)
        if 'obrigatório' in str(df_temp.iloc[0].values):
            df_temp = df_temp.iloc[1:].reset_index(drop=True)
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

def carregar_base_oficial():
    return carregar_do_google("meus_produtos_oficiais")

# ==============================================================================
# 🏢 CONFIGURAÇÃO E CARREGAMENTO
# ==============================================================================

st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox("Gerenciar qual unidade?", ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"])
st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True)
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

def gerar_backup_zip_nuvem():
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        tabelas = [
            f"{prefixo}_estoque", f"{prefixo}_historico_compras", f"{prefixo}_movimentacoes",
            f"{prefixo}_vendas", f"{prefixo}_lista_compras", f"{prefixo}_log_auditoria",
            f"{prefixo}_ids_vendas", "meus_produtos_oficiais"
        ]
        for tab in tabelas:
            df_temp = carregar_do_google(tab)
            if not df_temp.empty:
                data = df_temp.to_csv(index=False).encode('utf-8')
                zip_file.writestr(f"{tab}.csv", data)
    buffer.seek(0)
    return buffer

st.sidebar.markdown("### 🛡️ Segurança (Nuvem)")
if st.sidebar.button("💾 Baixar Backup da Nuvem"):
    st.info("Baixando dados do Google Sheets...")
    zip_buffer = gerar_backup_zip_nuvem()
    st.sidebar.download_button(
        label="⬇️ Salvar Backup",
        data=zip_buffer,
        file_name=f"backup_nuvem_{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
        mime="application/zip"
    )
st.sidebar.markdown("---")

# --- FUNÇÕES AUXILIARES ---

def parse_num_br(x, default=0.0):
    """Converte números vindos de Excel/CSV (pt-BR ou en-US) para float, com blindagem."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
            return float(default)
        s = str(x).strip()
        # Remove moeda e espaços
        s = s.replace("R$", "").replace("\u00a0", " ").strip()
        # Se vier no formato 1.234,56 -> 1234.56
        # Estratégia: se tem ',' e '.' e a última vírgula está depois do último ponto, assume pt-BR.
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        else:
            # se só tem ',' assume decimal
            if "," in s and "." not in s:
                s = s.replace(".", "").replace(",", ".")
        # Remove qualquer coisa não numérica (exceto - e .)
        s = re.sub(r"[^0-9\-\.]", "", s)
        return float(s) if s not in ["", "-", ".", "-."] else float(default)
    except Exception:
        return float(default)

def formatar_moeda_br(valor):
    try: return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    """Mantido por compatibilidade: agora usa busca robusta.
    Procura por nome e (se existir) por código de barras com 4+ dígitos.
    """
    try:
        if df is None or len(df) == 0:
            return df
        cols_barcode_candidatas = [
            'código de barras','codigo de barras','código_barras','codigo_barras',
            'barcode','ean','ean13','gtin','gtin13','código barras','codigo barras'
        ]
        cols_barcode = [c for c in cols_barcode_candidatas if c in df.columns]
        cols_text = [coluna_busca] if coluna_busca in df.columns else []
        # também tenta incluir 'produto' ou 'nome do produto' se existirem
        for extra in ['produto', 'nome do produto', 'nome_produto', 'descricao', 'descrição']:
            if extra in df.columns and extra not in cols_text:
                cols_text.append(extra)
        return filtrar_df_busca_robusta(df, texto_busca, cols_text=cols_text, cols_barcode=cols_barcode)
    except Exception:
        return df

# ==============================================================================
# 🛡️ BLINDAGEM: conversão numérica pt-BR + clamp de negativos (regra de ouro)
# ==============================================================================
def _to_float(valor):
    """Converte valores variados (pt-BR, strings, NaN) para float."""
    try:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return 0.0
        s = str(valor).strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return 0.0
        # remove R$ e espaços
        s = s.replace("R$", "").strip()
        # padrão pt-BR: 1.234,56  -> 1234.56
        # se tiver ',' e '.', assume '.' milhar e ',' decimal
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            # se só tiver ',', assume decimal
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return 0.0

def _to_int(valor):
    try:
        return int(round(_to_float(valor)))
    except Exception:
        return 0

def garantir_colunas(df: pd.DataFrame, colunas_obrigatorias: list[str]) -> pd.DataFrame:
    """Garante que o DF tenha todas as colunas, sem quebrar o que já funciona."""
    if df is None or df.empty:
        return pd.DataFrame(columns=colunas_obrigatorias)
    for c in colunas_obrigatorias:
        if c not in df.columns:
            df[c] = ""
    return df

def blindar_estoque_df(df_estoque: pd.DataFrame) -> pd.DataFrame:
    """Normaliza colunas essenciais e impede persistir negativos."""
    if df_estoque is None:
        return pd.DataFrame()
    # padroniza nomes (o app trabalha em lower)
    df = df_estoque.copy()
    df.columns = df.columns.astype(str).str.strip().str.lower()

    colunas_estoque_padrao = [
        'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade',
        'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor',
        'preco_sem_desconto', 'status'
    ]
    df = garantir_colunas(df, colunas_estoque_padrao)

    # conversões numéricas + clamp
    cols_int = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada']
    cols_money = ['preco_custo', 'preco_venda', 'preco_sem_desconto']

    for c in cols_int:
        df[c] = df[c].apply(_to_int)
        df.loc[df[c] < 0, c] = 0  # nunca negativo

    for c in cols_money:
        df[c] = df[c].apply(_to_float)
        df.loc[df[c] < 0, c] = 0.0  # custo/preço negativo não faz sentido

    # textos
    df['código de barras'] = df['código de barras'].apply(lambda x: str(x).replace('.0', '').strip() if pd.notnull(x) else "")
    df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
    df['ultimo_fornecedor'] = df['ultimo_fornecedor'].fillna('').astype(str)

    # validade
    df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')

    # status
    df['status'] = df['status'].replace('', 'Ativo').fillna('Ativo')
    df['status'] = df['status'].apply(lambda x: 'Ativo' if str(x).strip().lower() in {'ativo', 'a'} else ('Inativo' if str(x).strip().lower() in {'inativo', 'i'} else str(x).strip().title()))
    return df

def blindar_valor_estoque(df_estoque: pd.DataFrame) -> float:
    """Cálculo financeiro seguro (sem NaN/negativos)."""
    if df_estoque is None or df_estoque.empty:
        return 0.0
    df = df_estoque.copy()
    # garante colunas
    for c in ['qtd.estoque', 'qtd_central', 'preco_custo', 'status']:
        if c not in df.columns: 
            df[c] = 0
    # clamp
    q_loja = pd.to_numeric(df['qtd.estoque'], errors='coerce').fillna(0).clip(lower=0)
    q_casa = pd.to_numeric(df['qtd_central'], errors='coerce').fillna(0).clip(lower=0)
    custo = pd.to_numeric(df['preco_custo'], errors='coerce').fillna(0).clip(lower=0)
    ativos = df['status'].astype(str) == 'Ativo'
    return float(((q_loja + q_casa) * custo)[ativos].sum())

# --- 🔐 LOG DE AUDITORIA EM LOTE ---
def registrar_auditoria(prefixo, produto, qtd_antes, qtd_nova, acao, motivo="Manual"):
    try:
        aba_log = f"{prefixo}_log_auditoria"
        novo_log = {
            'data_hora': str(obter_hora_manaus()), 'produto': produto,
            'qtd_antes': qtd_antes, 'qtd_nova': qtd_nova,
            'acao': acao, 'motivo': motivo
        }
        df_log = carregar_do_google(aba_log)
        df_log = pd.concat([df_log, pd.DataFrame([novo_log])], ignore_index=True)
        salvar_no_google(df_log, aba_log)
    except Exception as e: print(f"Erro log: {e}")

def salvar_logs_em_lote(prefixo, lista_logs):
    if not lista_logs: return
    try:
        aba_log = f"{prefixo}_log_auditoria"
        df_log = carregar_do_google(aba_log)
        df_novos = pd.DataFrame(lista_logs)
        df_final = pd.concat([df_log, df_novos], ignore_index=True)
        salvar_no_google(df_final, aba_log)
    except Exception as e: print(f"Erro log lote: {e}")

# --- 🔐 MEMÓRIA DE VENDAS PROCESSADAS ---
def carregar_ids_processados(prefixo):
    aba = f"{prefixo}_ids_vendas"
    df_ids = carregar_do_google(aba)
    if not df_ids.empty and 'id_transacao' in df_ids.columns:
        return set(df_ids['id_transacao'].astype(str).tolist())
    return set()

def salvar_ids_processados(prefixo, novos_ids):
    aba = f"{prefixo}_ids_vendas"
    if not novos_ids: return
    df_novo = pd.DataFrame({'id_transacao': list(novos_ids)})
    df_antigo = carregar_do_google(aba)
    if not df_antigo.empty:
        df_final = pd.concat([df_antigo, df_novo]).drop_duplicates()
    else: df_final = df_novo
    salvar_no_google(df_final, aba)

# --- 🏡 ATUALIZAÇÃO DE CASA GLOBAL (AGORA EM LOTE) ---
def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    """Atualiza 1 produto in todas as lojas (Modo Antigo)."""
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        aba_outra = f"{loja}_estoque"
        df_outra = carregar_do_google(aba_outra)
        if not df_outra.empty:
            try:
                df_outra.columns = df_outra.columns.str.strip().str.lower()
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    qtd_antiga = df_outra.at[idx, 'qtd_central']
                    df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                    salvar_no_google(df_outra, aba_outra)
                    registrar_auditoria(loja, nome_produto, qtd_antiga, qtd_nova_casa, "Sincronização Automática", f"Origem: {prefixo_ignorar}")
            except: pass

def atualizar_casa_global_em_lote(lista_atualizacoes, prefixo_origem):
    if not lista_atualizacoes: return
    
    todas_lojas = ["loja1", "loja2", "loja3"]
    dict_updates = {item['produto']: item for item in lista_atualizacoes}
    
    for loja in todas_lojas:
        if loja == prefixo_origem: continue
        
        aba_outra = f"{loja}_estoque"
        df_outra = carregar_do_google(aba_outra)
        
        if not df_outra.empty:
            alterou_algo = False
            logs_loja_outra = []
            
            df_outra.columns = df_outra.columns.str.strip().str.lower()
            
            for produto, dados in dict_updates.items():
                mask = df_outra['nome do produto'].astype(str) == str(produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    qtd_antiga = df_outra.at[idx, 'qtd_central']
                    
                    df_outra.at[idx, 'qtd_central'] = dados['qtd_central']
                    if dados.get('custo') is not None: df_outra.at[idx, 'preco_custo'] = dados['custo']
                    if dados.get('venda') is not None: df_outra.at[idx, 'preco_venda'] = dados['venda']
                    
                    alterou_algo = True
                    logs_loja_outra.append({
                        'data_hora': str(obter_hora_manaus()), 'produto': produto,
                        'qtd_antes': qtd_antiga, 'qtd_nova': dados['qtd_central'],
                        'acao': "Sincronização em Lote", 'motivo': f"Origem: {prefixo_origem}"
                    })
            
            if alterou_algo:
                salvar_no_google(df_outra, aba_outra)
                salvar_logs_em_lote(loja, logs_loja_outra)

# --- ARQUIVOS ---
def inicializar_arquivos(prefixo):
    arquivos = {
        f"{prefixo}_estoque": ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto', 'status'],
        f"{prefixo}_historico_compras": ['data', 'data_emissao', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
        f"{prefixo}_movimentacoes": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],

        f"{prefixo}_vendas_itens": ['data_hora', 'mes_ref', 'transacao', 'código_barras', 'produto', 'qtd_vendida', 'preco_unit', 'valor_total', 'canal', 'obs_importacao'],
        f"{prefixo}_vendas_transacoes": ['data_hora', 'mes_ref', 'transacao', 'subtotal', 'descontos', 'taxas', 'total', 'forma_pagamento', 'obs_importacao'],
        f"{prefixo}_vendas_mensal_produto": ['mes_ref', 'código_barras', 'produto', 'qtd_vendida', 'valor_total', 'ultima_venda'],
        f"{prefixo}_mix_review": ['produto', 'código_barras', 'status_giro', 'dias_sem_venda', 'qtd_estoque_total', 'acao_sugerida', 'decisao', 'observacoes', 'atualizado_em'],
        f"{prefixo}_lista_compras": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo'],
        f"{prefixo}_mix_review": ['data_criacao', 'codigo_barras', 'produto', 'status', 'motivo', 'ultima_venda', 'dias_sem_venda', 'meses_sem_venda', 'observacao', 'decisao'],
    }
    for aba, colunas in arquivos.items():
        df = carregar_do_google(aba)
        if df.empty: salvar_no_google(pd.DataFrame(columns=colunas), aba)

def carregar_dados(prefixo_arquivo):
    try:
        df_raw = carregar_do_google(f"{prefixo_arquivo}_estoque")
        if df_raw.empty:
            return pd.DataFrame()
        return blindar_estoque_df(df_raw)
    except Exception:
        return pd.DataFrame()

def carregar_historico(prefixo_arquivo):
    try:
        df_h = carregar_do_google(f"{prefixo_arquivo}_historico_compras")
        if df_h.empty: return pd.DataFrame()
        
        # Filtra colunas indesejadas (display_combo, etc) já na leitura para limpar visual
        cols_ok = [c for c in df_h.columns if c not in ['display_combo', 'produto_str', 'Selecionar', 'status_temp']]
        df_h = df_h[cols_ok]

        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
        cols_num = ['qtd', 'preco_pago', 'total_gasto', 'desconto_total_money', 'preco_sem_desconto']
        for c in cols_num:
             if c in df_h.columns: 
                 df_h[c] = df_h[c].astype(str).str.replace(',', '.', regex=False)
                 df_h[c] = pd.to_numeric(df_h[c], errors='coerce').fillna(0)
        if 'numero_nota' not in df_h.columns: df_h['numero_nota'] = ""
        if 'obs_importacao' not in df_h.columns: df_h['obs_importacao'] = ""
        if 'data_emissao' not in df_h.columns: df_h['data_emissao'] = ""
        
        if 'desconto_total_money' not in df_h.columns:
            if 'desconto_obtido' in df_h.columns: df_h['desconto_total_money'] = df_h['desconto_obtido'] * df_h['qtd']
            else: df_h['desconto_total_money'] = 0.0
        if 'preco_sem_desconto' not in df_h.columns: df_h['preco_sem_desconto'] = 0.0
        mask_zerado = (df_h['preco_sem_desconto'] == 0) & (df_h['preco_pago'] > 0)
        df_h.loc[mask_zerado, 'preco_sem_desconto'] = df_h.loc[mask_zerado, 'preco_pago']
        return df_h
    except: return pd.DataFrame()

def carregar_movimentacoes(prefixo_arquivo):
    try:
        df_m = carregar_do_google(f"{prefixo_arquivo}_movimentacoes")
        if df_m.empty: return pd.DataFrame()
        df_m['data_hora'] = pd.to_datetime(df_m['data_hora'], errors='coerce')
        return df_m
    except: return pd.DataFrame()

def carregar_vendas(prefixo_arquivo):
    try:
        df_v = carregar_do_google(f"{prefixo_arquivo}_vendas")
        if df_v.empty: return pd.DataFrame()
        df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')
        return df_v
    except: return pd.DataFrame()

def carregar_vendas_itens(prefixo_arquivo):
    try:
        df_vi = carregar_do_google(f"{prefixo_arquivo}_vendas_itens")
        if df_vi.empty: 
            return pd.DataFrame()
        df_vi.columns = df_vi.columns.str.strip().str.lower()
        if 'data_hora' in df_vi.columns:
            df_vi['data_hora'] = pd.to_datetime(df_vi['data_hora'], errors='coerce')
        for c in ['qtd_vendida', 'preco_unit', 'valor_total']:
            if c in df_vi.columns:
                df_vi[c] = df_vi[c].apply(parse_num_br)
        if 'código_barras' in df_vi.columns:
            df_vi['código_barras'] = df_vi['código_barras'].astype(str).str.replace('.0','',regex=False).str.strip()
        if 'produto' in df_vi.columns:
            df_vi['produto'] = df_vi['produto'].astype(str).apply(normalizar_texto)
        return df_vi
    except:
        return pd.DataFrame()

def carregar_vendas_transacoes(prefixo_arquivo):
    try:
        df_vt = carregar_do_google(f"{prefixo_arquivo}_vendas_transacoes")
        if df_vt.empty:
            return pd.DataFrame()
        df_vt.columns = df_vt.columns.str.strip().str.lower()
        if 'data_hora' in df_vt.columns:
            df_vt['data_hora'] = pd.to_datetime(df_vt['data_hora'], errors='coerce')
        for c in ['subtotal', 'descontos', 'taxas', 'total']:
            if c in df_vt.columns:
                df_vt[c] = df_vt[c].apply(parse_num_br)
        return df_vt
    except:
        return pd.DataFrame()

def carregar_lista_compras(prefixo_arquivo):
    try:
        df = carregar_do_google(f"{prefixo_arquivo}_lista_compras")
        if df.empty: return pd.DataFrame()
        if 'código_barras' not in df.columns: df['código_barras'] = ""
        if 'qtd_sugerida' in df.columns: df['qtd_sugerida'] = pd.to_numeric(df['qtd_sugerida'], errors='coerce')
        return df
    except: return pd.DataFrame()

def carregar_mix_review(prefixo_arquivo):
    try:
        df_mr = carregar_do_google(f"{prefixo_arquivo}_mix_review")
        if df_mr.empty:
            return pd.DataFrame(columns=['data_criacao','codigo_barras','produto','status','motivo','ultima_venda','dias_sem_venda','meses_sem_venda','observacao','decisao'])
        df_mr.columns = df_mr.columns.str.strip()
        return df_mr
    except:
        return pd.DataFrame(columns=['data_criacao','codigo_barras','produto','status','motivo','ultima_venda','dias_sem_venda','meses_sem_venda','observacao','decisao'])

# --- XML ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]

    dados_nota = {'numero': '', 'fornecedor': '', 'data_emissao': '', 'itens': []}
    lista_nomes_ref = []
    dict_ref_ean = {}
    if not df_referencia.empty:
        for idx, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            ean = str(row['código de barras']).strip()
            dict_ref_ean[nm] = ean
            lista_nomes_ref.append(nm)

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
            nome_raw = item_xml.find('Nome').text
            qtd_raw = float(item_xml.find('Quantidade').text)
            val_final = float(item_xml.find('ValorPagoFinal').text)
            desc_val = float(item_xml.find('ValorDesconto').text)
            cod_barras = item_xml.find('CodigoBarras').text
            item['nome'] = normalizar_texto(nome_raw)
            item['qtd'] = qtd_raw
            item['ean'] = cod_barras if cod_barras else ""
            item['codigo_interno'] = item['ean']
            item['desconto_total_item'] = desc_val
            if qtd_raw > 0:
                item['preco_un_liquido'] = val_final / qtd_raw
                item['preco_un_bruto'] = (val_final + desc_val) / qtd_raw
            
            ean_xml = str(item['ean']).strip()
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                if lista_nomes_ref:
                    melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                    if melhor_nome: item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            dados_nota['itens'].append(item)
        return dados_nota

    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == '': dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi':
            raw_date = elem.text
            if raw_date:
                try:
                    dt_obj = datetime.strptime(raw_date[:19], "%Y-%m-%dT%H:%M:%S")
                    dados_nota['data_emissao'] = dt_obj.strftime("%d/%m/%Y %H:%M")
                except:
                    dados_nota['data_emissao'] = raw_date 

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

# --- SALVAMENTO ---
def salvar_estoque(df, prefixo):
    df_blindado = blindar_estoque_df(df)
    salvar_no_google(df_blindado, f"{prefixo}_estoque")
    # mantém sessão sincronizada
    st.session_state['df_ativo'] = df_blindado
def salvar_historico(df, prefixo): salvar_no_google(df, f"{prefixo}_historico_compras")
def salvar_movimentacoes(df, prefixo): salvar_no_google(df, f"{prefixo}_movimentacoes")
def salvar_vendas(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas")
def salvar_lista_compras(df, prefixo): salvar_no_google(df, f"{prefixo}_lista_compras", permitir_vazio=True)
def salvar_vendas_itens(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_itens", permitir_vazio=True)
def salvar_vendas_transacoes(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_transacoes", permitir_vazio=True)
def salvar_vendas_mensal_produto(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas_mensal_produto", permitir_vazio=True)
def salvar_mix_review(df_mix, prefixo_arquivo): salvar_no_google(df_mix, f"{prefixo_arquivo}_mix_review", permitir_vazio=True)


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
ids_processados = carregar_ids_processados(prefixo)

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
        "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)",
        "📈 Vendas (Importar & 80/20)",
        "🔎 Raio-X do Estoque (Auditoria)",
        "🏠 Gôndola (Loja)", 
        "💰 Inteligência de Compras (Histórico)",
        "🏡 Estoque Central (Casa)",
        "📋 Tabela Geral",
        "🛠️ Ajuste & Limpeza",
        "♻️ Restaurar Histórico"
    ])

    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle (Nuvem) - {loja_atual}")
        df_lista_compras = carregar_lista_compras(prefixo)

        if df.empty:
            st.info("Comece cadastrando produtos.")
        else:
            hoje = obter_hora_manaus()
            df_valido = df[(pd.notnull(df['validade'])) & (df['status'] == 'Ativo')].copy()
            df_ativos = df[df['status'] == 'Ativo']
            
            df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
            df_atencao = df_valido[(df_valido['validade'] > hoje + timedelta(days=5)) & (df_valido['validade'] <= hoje + timedelta(days=10))]
            valor_estoque = blindar_valor_estoque(df)
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Itens (Ativos)", int(df_ativos['qtd.estoque'].sum()))
            c2.metric("💰 Valor Investido", f"R$ {formatar_moeda_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
            
            bajo_estoque = df_ativos[(df_ativos['qtd.estoque'] + df_ativos['qtd_central']) <= df_ativos['qtd_minima']]
            if not bajo_estoque.empty:
                st.warning(f"🚨 Existem {len(bajo_estoque)} produtos ATIVOS com estoque baixo! Vá em 'Lista de Compras'.")
            
            st.markdown("### 🚨 Gestão de Vencimentos")
            if not df_critico.empty:
                filtro_venc = st.text_input("🔍 Buscar produtos vencendo:", placeholder="Nome...")
                df_venc_show = filtrar_dados_inteligente(df_critico, 'nome do produto', filtro_venc)
                st.info("💡 Dica: Para remover o alerta, apague a data de validade (Delete) ou atualize-a.")
                df_venc_edit = st.data_editor(df_venc_show[['nome do produto', 'validade', 'qtd.estoque']], use_container_width=True, num_rows="dynamic", key="editor_vencimento_avancado")
                if st.button("💾 SALVAR CORREÇÕES DE VENCIMENTO"):
                    for i, row in df_venc_edit.iterrows():
                        mask = df['nome do produto'] == row['nome do produto']
                        if mask.any():
                            df.loc[mask, 'validade'] = row['validade']
                            df.loc[mask, 'qtd.estoque'] = row['qtd.estoque']
                    salvar_estoque(df, prefixo)
                    st.success("Vencimentos atualizados na Nuvem!")
                    st.rerun()
            else: st.success("Nenhum produto vencendo nos próximos 5 dias.")

    elif modo == "📦 Inventário Rápido (Lote)":
        st.title("📦 Inventário Rápido (Modo Offline)")
        st.info("💡 Conte tudo, adicione na lista abaixo e clique em 'PROCESSAR TUDO' apenas no final. Muito mais rápido!")

        if 'lote_inventario' not in st.session_state:
            st.session_state['lote_inventario'] = []

        if not df.empty:
            lista_nomes_codigos = sorted((df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist())
            
            st.markdown("### 1️⃣ Localizar Produto")
            produto_busca = st.selectbox("🔍 Digite Código ou Nome:", [""] + lista_nomes_codigos, key="busca_inv_lote")

            if produto_busca:
                parts = produto_busca.split(' - ', 1)
                cod_sel = parts[0]
                nome_sel = parts[1]
                
                mask = df['código de barras'] == cod_sel
                qtd_atual = 0
                val_atual = None
                if mask.any():
                    idx = df[mask].index[0]
                    qtd_atual = int(df.at[idx, 'qtd.estoque'])
                    val_atual = df.at[idx, 'validade']

                st.markdown(f"**Produto:** {nome_sel}")
                st.caption(f"Estoque Atual no Sistema: {qtd_atual}")

                c_qtd, c_val, c_btn = st.columns([1, 1, 1])
                with c_qtd:
                    qtd_contada = st.number_input("Nova Qtd Real:", min_value=0, value=qtd_atual, key="qtd_inv_input")
                with c_val:
                    val_nova = st.date_input("Nova Validade:", value=val_atual if pd.notnull(val_atual) else None, key="val_inv_input")
                with c_btn:
                    st.write("") 
                    st.write("") 
                    if st.button("➕ Adicionar à Lista Temporária", type="primary"):
                        novo_item = {
                            'Código': cod_sel,
                            'Produto': nome_sel,
                            'Nova Qtd Real': qtd_contada,
                            'Nova Validade': val_nova,
                            'Status': 'Pendente'
                        }
                        st.session_state['lote_inventario'].append(novo_item)
                        st.success(f"{nome_sel} adicionado à lista!")
                        st.rerun()

        st.divider()
        st.markdown("### 2️⃣ Revisar Lista Temporária (Antes de Salvar)")
        
        if st.session_state['lote_inventario']:
            df_lote = pd.DataFrame(st.session_state['lote_inventario'])
            
            df_lote_editavel = st.data_editor(
                df_lote, 
                use_container_width=True, 
                num_rows="dynamic",
                key="editor_lote_inv"
            )

            c_limpar, c_salvar = st.columns([1, 2])
            
            with c_limpar:
                if st.button("🗑️ Limpar Lista"):
                    st.session_state['lote_inventario'] = []
                    st.rerun()
            
            with c_salvar:
                if st.button("💾 PROCESSAR TUDO AGORA (Salvar na Nuvem)", type="primary"):
                    logs_inventario = []
                    alteracoes_feitas = 0
                    
                    bar = st.progress(0)
                    total = len(df_lote_editavel)

                    for i, row in df_lote_editavel.iterrows():
                        cod = row['Código']
                        nova_q = row['Nova Qtd Real']
                        nova_v = row['Nova Validade']
                        
                        mask = df['código de barras'] == cod
                        if mask.any():
                            idx = df[mask].index[0]
                            qtd_antiga = df.at[idx, 'qtd.estoque']
                            
                            df.at[idx, 'qtd.estoque'] = nova_q
                            df.at[idx, 'validade'] = pd.to_datetime(nova_v) if nova_v else None
                            
                            if qtd_antiga != nova_q:
                                logs_inventario.append({
                                    'data_hora': str(obter_hora_manaus()),
                                    'produto': row['Produto'],
                                    'qtd_antes': qtd_antiga,
                                    'qtd_nova': nova_q,
                                    'acao': 'Inventário Rápido',
                                    'motivo': 'Contagem Lote'
                                })
                            alteracoes_feitas += 1
                        bar.progress((i + 1) / total)

                    salvar_estoque(df, prefixo)
                    salvar_logs_em_lote(prefixo, logs_inventario)
                    
                    st.session_state['lote_inventario'] = [] 
                    st.balloons()
                    st.success(f"✅ Sucesso! {alteracoes_feitas} produtos atualizados de uma só vez.")
                    st.rerun()
        else:
            st.info("A lista temporária está vazia. Adicione produtos acima.")

    elif modo == "⚖️ Conciliação (Shoppbud vs App)":
        st.title("⚖️ Conciliação de Estoque")
        st.markdown("**Ferramenta de Auditoria:** Compare o estoque do seu App com o Planograma do Shoppbud.")
        arq_planograma = st.file_uploader("📂 Carregar Planograma Shoppbud (.xlsx)", type=['xlsx'])
        if arq_planograma:
            try:
                df_plan = pd.read_excel(arq_planograma)
                col_cod_plan = next((c for c in df_plan.columns if ('código' in c.lower() or 'codigo' in c.lower()) and 'barras' in c.lower()), None)
                col_qtd_plan = next((c for c in df_plan.columns if 'qtd' in c.lower() and 'estoque' in c.lower()), None)
                
                if col_cod_plan and col_qtd_plan:
                    df_plan['código normalizado'] = df_plan[col_cod_plan].astype(str).str.replace('.0', '').str.strip().str.lstrip('0')
                    df['código normalizado'] = df['código de barras'].astype(str).str.replace('.0', '').str.strip().str.lstrip('0')
                    df_concilia = pd.merge(df[['código normalizado', 'nome do produto', 'qtd.estoque']], df_plan[[col_cod_plan, col_qtd_plan, 'código normalizado']], on='código normalizado', how='inner')
                    df_concilia['Diferença'] = df_concilia['qtd.estoque'] - df_concilia[col_qtd_plan]
                    df_divergente = df_concilia[df_concilia['Diferença'] != 0].copy()
                    
                    if df_divergente.empty: st.success("✅ Parabéns! Seu estoque está 100% batendo!")
                    else:
                        st.warning(f"⚠️ Encontradas {len(df_divergente)} divergências.")
                        df_divergente['✅ Aceitar Qtd Shoppbud (Corrigir App)'] = False
                        df_editor_concilia = st.data_editor(df_divergente[['nome do produto', 'qtd.estoque', col_qtd_plan, 'Diferença', '✅ Aceitar Qtd Shoppbud (Corrigir App)']], column_config={"qtd.estoque": st.column_config.NumberColumn("Seu App", disabled=True), col_qtd_plan: st.column_config.NumberColumn("Shoppbud", disabled=True), "Diferença": st.column_config.NumberColumn("Diferença", disabled=True)}, use_container_width=True, hide_index=True)
                        
                        c_esq, c_dir = st.columns(2)
                        with c_esq:
                            if st.button("💾 ATUALIZAR MEU APP (Esquerda)", type="primary"):
                                itens_corrigidos = 0
                                logs_concilia = [] 
                                for idx, row in df_editor_concilia.iterrows():
                                    if row['✅ Aceitar Qtd Shoppbud (Corrigir App)']:
                                        mask = df['nome do produto'] == row['nome do produto']
                                        if mask.any():
                                            qtd_shopp = row[col_qtd_plan]
                                            qtd_antiga = df.loc[mask, 'qtd.estoque'].values[0]
                                            df.loc[mask, 'qtd.estoque'] = qtd_shopp
                                            logs_concilia.append({'data_hora': str(obter_hora_manaus()), 'produto': row['nome do produto'], 'qtd_antes': qtd_antiga, 'qtd_nova': qtd_shopp, 'acao': "Correção Conciliação", 'motivo': "Origem: Shoppbud"})
                                            itens_corrigidos += 1
                                salvar_estoque(df, prefixo)
                                salvar_logs_em_lote(prefixo, logs_concilia) 
                                st.success(f"✅ {itens_corrigidos} items corrigidos!")
                                st.rerun()
                        with c_dir:
                            df_export = df_divergente[~df_editor_concilia['✅ Aceitar Qtd Shoppbud (Corrigir App)']].copy()
                            if not df_export.empty:
                                buffer = BytesIO()
                                with pd.ExcelWriter(buffer) as writer:
                                    df_export_final = pd.DataFrame({'Código de Barras': df_export['código normalizado'], 'Quantidade': df_export['qtd.estoque']})
                                    df_export_final.to_excel(writer, index=False)
                                st.download_button(label="📥 BAIXAR EXCEL PARA SHOPPBUD", data=buffer.getvalue(), file_name=f"ajuste_shoppbud_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel")
                else: st.error(f"Não encontrei colunas corretas.")
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")

    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        df_mov = carregar_movimentacoes(prefixo)
        
        arquivos_pick = st.file_uploader("📂 Subir Picklist (.xlsx)", type=['xlsx', 'xls'], accept_multiple_files=True)
        if arquivos_pick:
            try:
                lista_dfs = []
                st.info(f"📂 {len(arquivos_pick)} arquivos carregados.")
                primeiro_arquivo = arquivos_pick[0]
                df_temp_raw = pd.read_excel(primeiro_arquivo, header=None)
                st.dataframe(df_temp_raw.head(5))
                linha_cabecalho = st.number_input("Em qual linha estão os títulos?", min_value=0, value=0)
                for arq in arquivos_pick:
                    arq.seek(0)
                    df_temp = pd.read_excel(arq, header=linha_cabecalho)
                    lista_dfs.append(df_temp)
                df_pick = pd.concat(lista_dfs, ignore_index=True)
                cols = df_pick.columns.tolist()
                st.markdown("---")
                c1, c2 = st.columns(2)
                col_barras = c1.selectbox("Selecione a coluna de CÓDIGO DE BARRAS:", cols)
                col_qtd = c2.selectbox("Selecione a coluna de QUANTIDADE:", cols)
                
                if st.button("🚀 PROCESSAR TRANSFERÊNCIA EM LOTE"):
                    movidos = 0
                    erros = 0
                    bar = st.progress(0)
                    total_linhas = len(df_pick)
                    log_movs = []
                    log_auditoria_buffer = []
                    atualizacoes_casa_global = [] 

                    for i, row in df_pick.iterrows():
                        cod_pick = str(row[col_barras]).replace('.0', '').strip()
                        qtd_pick = pd.to_numeric(row[col_qtd], errors='coerce')
                        if qtd_pick > 0:
                            mask = df['código de barras'] == cod_pick
                            if mask.any():
                                idx = df[mask].index[0]
                                nome_prod = df.at[idx, 'nome do produto']
                                qtd_antiga_loja = df.at[idx, 'qtd.estoque']
                                df.at[idx, 'qtd_central'] -= qtd_pick
                                df.at[idx, 'qtd.estoque'] += qtd_pick
                                log_movs.append({'data_hora': str(obter_hora_manaus()), 'produto': nome_prod, 'qtd_movida': qtd_pick})
                                
                                atualizacoes_casa_global.append({'produto': nome_prod, 'qtd_central': df.at[idx, 'qtd_central']})
                                log_auditoria_buffer.append({'data_hora': str(obter_hora_manaus()), 'produto': nome_prod, 'qtd_antes': qtd_antiga_loja, 'qtd_nova': df.at[idx, 'qtd.estoque'], 'acao': "Transferência Picklist", 'motivo': "Lote"})
                                movidos += 1
                            else: erros += 1
                        bar.progress((i+1)/total_linhas)
                    
                    salvar_estoque(df, prefixo)
                    if log_movs:
                        df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                        salvar_movimentacoes(df_mov, prefixo)
                    
                    salvar_logs_em_lote(prefixo, log_auditoria_buffer)
                    atualizar_casa_global_em_lote(atualizacoes_casa_global, prefixo)
                    
                    st.success(f"✅ {movidos} produtos transferidos!")
                    if erros > 0: st.warning(f"⚠️ {erros} produtos não encontrados.")
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")

    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        df_lista_compras = carregar_lista_compras(prefixo)
        
        tab_lista, tab_add = st.tabs(["📋 Ver Lista Atual (Editável)", "➕ Adicionar Itens"])
        with tab_lista:
            if not df_lista_compras.empty:
                df_lista_compras = df_lista_compras.reset_index(drop=True)
                
                busca_lista = st.text_input("🔍 Buscar na Lista:", placeholder="Ex: arroz...")
                df_lista_show = filtrar_dados_inteligente(df_lista_compras, 'produto', busca_lista)

                st.warning("⚠️ Atenção: Ao excluir ou editar itens na tabela, você DEVE clicar no botão 'SALVAR ALTERAÇÕES' abaixo para gravar.")
                
                df_edit_lista = st.data_editor(
                    df_lista_show,
                    use_container_width=True,
                    num_rows="dynamic",
                    key="editor_lista_compras",
                    column_config={
                        "status": st.column_config.SelectboxColumn("Status", options=["A Comprar", "Comprado", "Cancelado", "Manual"]),
                        "qtd_sugerida": st.column_config.NumberColumn("Qtd", min_value=0),
                        "código_barras": st.column_config.TextColumn("Código", disabled=True),
                    }
                )

                if st.button("💾 SALVAR ALTERAÇÕES DA LISTA"):
                    if not busca_lista:
                        df_lista_compras = df_edit_lista.copy()
                    else:
                        indices_originais = df_lista_show.index.tolist()
                        indices_editados = df_edit_lista.index.tolist()
                        removidos = list(set(indices_originais) - set(indices_editados))
                        
                        if removidos:
                            df_lista_compras = df_lista_compras.drop(removidos)
                        
                        df_lista_compras.update(df_edit_lista)
                    
                    salvar_lista_compras(df_lista_compras, prefixo)
                    st.success("Lista atualizada com sucesso!")
                    st.rerun()
            else:
                st.info("Sua lista de compras está vazia.")

        with tab_add:
            st.subheader("🤖 Gerador Automático (Somente Ativos)")
            if st.button("🚀 Gerar Lista Baseada no Estoque Baixo"):
                if df.empty: st.warning("Sem produtos.")
                else:
                    df_ativos = df[df['status'] == 'Ativo']
                    mask_baixo = (df_ativos['qtd.estoque'] + df_ativos['qtd_central']) <= df_ativos['qtd_minima']
                    produtos_baixo = df_ativos[mask_baixo]
                    
                    if produtos_baixo.empty: st.success("Tudo certo! Nenhum produto ativo com estoque baixo.")
                    else:
                        novos_itens = []
                        for _, row in produtos_baixo.iterrows():
                            ja_na_lista = False
                            if not df_lista_compras.empty: ja_na_lista = df_lista_compras['produto'].astype(str).str.contains(row['nome do produto'], regex=False).any()
                            if not ja_na_lista:
                                novos_itens.append({'produto': row['nome do produto'], 'código_barras': row['código de barras'], 'qtd_sugerida': row['qtd_minima'] * 3, 'fornecedor': row['ultimo_fornecedor'], 'custo_previsto': row['preco_custo'], 'data_inclusao': obter_hora_manaus().strftime("%d/%m/%Y %H:%M"), 'status': 'A Comprar'})
                        if novos_itens:
                            df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos_itens)], ignore_index=True)
                            salvar_lista_compras(df_lista_compras, prefixo)
                            st.success(f"{len(novos_itens)} itens adicionados!")
                            st.rerun()
                        else: st.warning("Itens já na lista.")
            st.divider()
            
            st.subheader("✋ Adicionar Manualmente")
            lista_visuais = sorted((df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist())
            prod_man_visual = st.selectbox("Produto:", [""] + lista_visuais, key="sel_prod_lista")
            
            if prod_man_visual:
                try:
                    parts = prod_man_visual.split(' - ', 1)
                    cod_sel = parts[0]
                    mask_sel = df['código de barras'] == cod_sel
                    if mask_sel.any():
                        q_loja = int(df.loc[mask_sel, 'qtd.estoque'].values[0])
                        q_casa = int(df.loc[mask_sel, 'qtd_central'].values[0])
                        st.info(f"ℹ️ Posição Atual: 📦 Loja: {q_loja} | 🏡 Casa: {q_casa}")
                except: pass

            with st.form("add_manual_lista"):
                c_qtd, c_forn = st.columns(2)
                qtd_man = c_qtd.number_input("Qtd a Comprar:", min_value=1, value=10)
                obs_man = c_forn.text_input("Fornecedor (Opcional):", placeholder="Ex: Atacadão")
                
                c_dt, c_hr = st.columns(2)
                if 'hora_lista_fixa' not in st.session_state:
                    st.session_state['hora_lista_fixa'] = obter_hora_manaus().time().replace(second=0, microsecond=0)
                
                dt_manual = c_dt.date_input("Data da Inclusão:", value=obter_hora_manaus().date())
                hr_manual = c_hr.time_input("Hora da Inclusão:", value=st.session_state['hora_lista_fixa'], step=60)
                
                if st.form_submit_button("Adicionar à Lista"):
                    if prod_man_visual:
                        try:
                            parts = prod_man_visual.split(' - ', 1)
                            cod = parts[0]; nome = parts[1]
                        except: cod = ""; nome = prod_man_visual
                        
                        data_final = datetime.combine(dt_manual, hr_manual).strftime("%d/%m/%Y %H:%M")
                        
                        novo_item = {
                            'produto': nome, 
                            'código_barras': cod, 
                            'qtd_sugerida': qtd_man, 
                            'fornecedor': obs_man, 
                            'custo_previsto': 0.0, 
                            'data_inclusao': data_final, 
                            'status': 'Manual'
                        }
                        df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame([novo_item])], ignore_index=True)
                        salvar_lista_compras(df_lista_compras, prefixo)
                        st.success("Adicionado!")
                        st.rerun()
                    else:
                        st.error("Selecione um produto.")

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
                    novo = {'código de barras': str(novo_cod).strip(), 'nome do produto': novo_nome.upper().strip(), 'qtd.estoque': ini_loja, 'qtd_central': ini_casa, 'qtd_minima': novo_min, 'validade': pd.to_datetime(ini_val) if ini_val else None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': novo_custo, 'preco_venda': novo_venda, 'categoria': nova_cat, 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0, 'status': 'Ativo'}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    registrar_auditoria(prefixo, novo_nome.upper().strip(), 0, ini_loja, "Novo Cadastro")
                    st.success("Cadastrado!")
                    st.rerun()

    # ==============================================================================
    # 🔄 XML IMPORTAÇÃO: CORREÇÃO DA CASA (QTD_CENTRAL)
    # ==============================================================================
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML")
        df_hist = carregar_historico(prefixo)
        
        modo_import = st.radio("Modo:", ["📦 Atualizar Estoque (Entrada)", "📖 Apenas Referência (Histórico)"], horizontal=True)
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
                
                st.markdown("### 🗓️ Datas da Operação")
                c_data_xml, c_data_sis = st.columns(2)
                
                data_xml_str = dados.get('data_emissao', 'Não encontrada no XML')
                c_data_xml.text_input("Data Emissão (Real da Nota - XML):", value=data_xml_str, disabled=True, key="view_data_xml")
                
                agora = obter_hora_manaus()
                with c_data_sis:
                    st.markdown("**Data de Lançamento no Sistema (Controle):**")
                    c_d, c_h = st.columns(2)
                    dt_lanc = c_d.date_input("Dia:", value=agora.date(), key="dt_lanc_xml")
                    hr_lanc = c_h.time_input("Hora:", value=agora.time(), step=60, key="hr_lanc_xml")
                
                data_lancamento_final = datetime.combine(dt_lanc, hr_lanc)

                lista_visuais = sorted((df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist())
                lista_sistema = ["(CRIAR NOVO)"] + [f"[SISTEMA] {x}" for x in lista_visuais]
                
                escolhas = {}
                for i, item in enumerate(dados['itens']):
                    match_inicial = "(CRIAR NOVO)"
                    if not df.empty:
                        # Limpa os zeros à esquerda para a busca do XML ser mais resiliente
                        cod_xml_limpo = str(item['ean']).strip().lstrip('0')
                        mask_ean = df['código de barras'].astype(str).str.strip().str.lstrip('0') == cod_xml_limpo
                        if mask_ean.any(): 
                            match_inicial = f"[SISTEMA] {df.loc[mask_ean, 'código de barras'].values[0]} - {df.loc[mask_ean, 'nome do produto'].values[0]}"
                        else:
                            melhor, _ = encontrar_melhor_match(item['nome'], df['nome do produto'].astype(str).tolist())
                            if melhor: 
                                cod_melhor = df.loc[df['nome do produto']==melhor, 'código de barras'].values[0]
                                match_inicial = f"[SISTEMA] {cod_melhor} - {melhor}"
                    
                    st.divider()
                    c1, c2 = st.columns([1, 1])
                    with c1: st.markdown(f"📦 **(XML) {item['nome']}**\n\n*EAN: {item['ean']}*")
                    with c2: escolhas[i] = st.selectbox("Vincular a:", lista_sistema, index=lista_sistema.index(match_inicial) if match_inicial in lista_sistema else 0, key=f"x_{i}")
                
                st.markdown("---")
                if st.button("✅ CONFIRMAR IMPORTAÇÃO"):
                    novos_hist = []; logs_xml = []; atualizacoes_casa_xml = [] 
                    for i, item in enumerate(dados['itens']):
                        esc = escolhas[i]
                        if "[SISTEMA]" in esc:
                             raw_sel = esc.replace("[SISTEMA] ", "")
                             nome_final = raw_sel.split(' - ', 1)[1]
                        else:
                             nome_final = item['nome'].upper()

                        if esc == "(CRIAR NOVO)":
                            # 🐞 CORREÇÃO DO INVENTÁRIO CASA AQUI 🐞
                            # A qtd nova vai para 'qtd_central' e NÃO para 'qtd.estoque'
                            novo = {
                                'código de barras': item['ean'], 
                                'nome do produto': nome_final, 
                                'qtd.estoque': 0, 
                                'qtd_central': item['qtd'] if "Atualizar" in modo_import else 0, 
                                'qtd_minima': 5, 
                                'validade': None, 
                                'status_compra': 'OK', 
                                'qtd_comprada': 0, 
                                'preco_custo': item['preco_un_liquido'], 
                                'preco_venda': item['preco_un_liquido']*2, 
                                'categoria': 'GERAL', 
                                'ultimo_fornecedor': dados['fornecedor'], 
                                'preco_sem_desconto': item['preco_un_bruto'], 
                                'status': 'Ativo'
                            }
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            if "Atualizar" in modo_import: logs_xml.append({'data_hora': str(data_lancamento_final), 'produto': nome_final, 'qtd_antes': 0, 'qtd_nova': item['qtd'], 'acao': "XML Novo", 'motivo': "Entrada Casa"})
                        else:
                            mask = df['nome do produto'].astype(str) == nome_final
                            if mask.any():
                                idx = df[mask].index[0]
                                if "Atualizar" in modo_import:
                                    df.at[idx, 'qtd_central'] += item['qtd']
                                    logs_xml.append({'data_hora': str(data_lancamento_final), 'produto': nome_final, 'qtd_antes': df.at[idx, 'qtd_central']-item['qtd'], 'qtd_nova': df.at[idx, 'qtd_central'], 'acao': "XML Entrada", 'motivo': "Entrada Casa"})
                                df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor'] 
                                df.at[idx, 'status'] = 'Ativo' 
                                atualizacoes_casa_xml.append({'produto': nome_final, 'qtd_central': df.at[idx, 'qtd_central'], 'custo': item['preco_un_liquido']})
                        
                        novos_hist.append({
                            'data': str(data_lancamento_final), 
                            'data_emissao': data_xml_str,        
                            'produto': nome_final, 
                            'fornecedor': dados['fornecedor'], 
                            'qtd': item['qtd'], 
                            'preco_pago': item['preco_un_liquido'], 
                            'preco_sem_desconto': item['preco_un_bruto'],    
                            'desconto_total_money': item['desconto_total_item'], 
                            'total_gasto': item['qtd']*item['preco_un_liquido']
                        })
                    
                    salvar_estoque(df, prefixo)
                    if novos_hist: salvar_historico(pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True), prefixo)
                    salvar_logs_em_lote(prefixo, logs_xml)
                    atualizar_casa_global_em_lote(atualizacoes_casa_xml, prefixo)
                    st.success("Processado com sucesso!")
                    st.rerun()
            except Exception as e: st.error(f"Erro: {e}")

    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base")
        arq = st.file_uploader("Arquivo", type=['xlsx', 'csv'])
        if arq and st.button("Processar"):
            if processar_excel_oficial(arq): st.success("Base atualizada!"); st.rerun()

    # ==============================================================================
    # 🔄 PLANOGRAMA: LIMPEZA DE ZEROS E AGRUPAMENTO DE LINHAS DUPLICADAS
    # ==============================================================================
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("📂 Planograma", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                arquivo.seek(0)
                if arquivo.name.endswith('.csv'):
                    try: 
                        df_raw = pd.read_csv(arquivo)
                    except:
                        arquivo.seek(0)
                        df_raw = pd.read_csv(arquivo, sep=';')
                else:
                    df_raw = pd.read_excel(arquivo)
            except Exception as e:
                st.error(f"Erro ao ler arquivo: {e}")
                df_raw = pd.DataFrame()
                
            if not df_raw.empty:
                df_raw.columns = df_raw.columns.astype(str).str.strip()
                cols = df_raw.columns.tolist()
                cols_lower = [c.lower() for c in cols]
                
                def find_idx(keywords, default=0):
                    for k in keywords:
                        for i, c in enumerate(cols_lower):
                            if k in c: return i
                    return default
                
                idx_barras = find_idx(['barras', 'ean', 'gtin', 'código', 'codigo'], 0)
                idx_nome = find_idx(['produto', 'nome', 'descrição'], 1 if len(cols)>1 else 0)
                idx_qtd = find_idx(['estoque', 'físico', 'quantidade real', 'qtd'], len(cols)-1)
                idx_preco = find_idx(['consumidor', 'venda', 'preço final', 'preco'], -1)

                st.info("🎯 **Mapeamento Automático:** Confirme abaixo se o sistema achou as colunas certas!")

                c1, c2, c3, c4 = st.columns(4)
                col_barras = c1.selectbox("CÓDIGO BARRAS", cols, index=idx_barras)
                col_nome = c2.selectbox("NOME", cols, index=idx_nome)
                col_qtd = c3.selectbox("QUANTIDADE", cols, index=idx_qtd)
                opcoes_preco = ["(Ignorar)"] + cols
                col_preco = c4.selectbox("PREÇO VENDA", opcoes_preco, index=(idx_preco + 1) if idx_preco != -1 else 0)
                
                if st.button("🚀 SINCRONIZAR TUDO", type="primary"):
                    df = carregar_dados(prefixo)
                    novos_prods = []
                    logs_plano = [] 
                    
                    # --- Limpeza de Código e Soma Inteligente das Prateleiras ---
                    df_raw[col_barras] = df_raw[col_barras].astype(str).str.replace('.0', '', regex=False).str.strip()
                    df_raw = df_raw[df_raw[col_barras] != ""]
                    df_raw = df_raw[df_raw[col_barras] != "nan"]
                    df_raw = df_raw[df_raw[col_barras] != "None"]
                    
                    df_raw[col_qtd] = df_raw[col_qtd].apply(parse_num_br)
                    
                    agg_dict = { col_nome: 'first', col_qtd: 'sum' }
                    if col_preco != "(Ignorar)":
                        df_raw[col_preco] = df_raw[col_preco].apply(parse_num_br)
                        agg_dict[col_preco] = 'first'
                        
                    df_agrupado = df_raw.groupby(col_barras).agg(agg_dict).reset_index()
                    
                    total_linhas = len(df_agrupado)
                    bar = st.progress(0)
                    
                    for i, row in df_agrupado.iterrows():
                        try:
                            cod = row[col_barras]
                            cod_limpo = cod.lstrip('0') # Limpador de Zeros!
                            nome = normalizar_texto(str(row[col_nome]))
                            qtd = row[col_qtd]
                            
                            if cod and nome:
                                # Compara ignorando os zeros do Shoppbud e do App
                                mask = df['código de barras'].astype(str).str.strip().str.lstrip('0') == cod_limpo
                                if mask.any():
                                    idx = df[mask].index[0]
                                    antigo = df.at[idx, 'qtd.estoque']
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if antigo != qtd: 
                                        logs_plano.append({'data_hora': str(obter_hora_manaus()), 'produto': nome, 'qtd_antes': antigo, 'qtd_nova': qtd, 'acao': "Sincronização", 'motivo': "Planograma"})
                                    if col_preco != "(Ignorar)":
                                        val = row[col_preco]
                                        if pd.notnull(val): df.loc[mask, 'preco_venda'] = val
                                else:
                                    val_p = 0.0
                                    if col_preco != "(Ignorar)": 
                                        val_p = row[col_preco] if pd.notnull(row[col_preco]) else 0.0
                                    novos_prods.append({'código de barras': cod, 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': val_p, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0, 'status': 'Ativo'})
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    
                    if novos_prods: df = pd.concat([df, pd.DataFrame(novos_prods)], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    salvar_logs_em_lote(prefixo, logs_plano) 
                    st.success("Sincronizado perfeitamente!")
                    st.rerun()

    # ==============================================================================
    # 📈 VENDAS (MUDANÇA DE BUSCA PARA IGNORAR ZEROS)
    # ==============================================================================
    elif modo == "📈 Vendas (Importar & 80/20)":

        st.title(f"📈 Vendas - Importar & 80/20 ({loja_atual})")
        st.markdown(
            "Importe as planilhas do Shoppbud e o sistema vai: **(1)** gravar o histórico na nuvem, "
            "**(2)** dar baixa no estoque com **blindagem contra negativos**, e **(3)** gerar análises 80/20 e alertas de 'sem giro'."
        )

        tab_imp, tab_8020, tab_alertas, tab_hist = st.tabs(["📂 Importar", "📊 80/20 & Comparar Meses", "🚦 Mix (Sem giro)", "📜 Histórico (Nuvem)"])

        with tab_imp:
            st.subheader("📂 Importar (Detecção Automática do Tipo de Relatório)")
            st.caption("Você pode subir **Sales (por produto/itens)** e/ou **Sales by Transaction**. O app detecta pelo cabeçalho.")
            arquivos = st.file_uploader("Arraste as planilhas aqui (.xlsx)", type=['xlsx', 'xls'], accept_multiple_files=True)

            colA, colB = st.columns(2)
            with colA:
                descontar_casa_se_faltar = st.checkbox("Se faltar na Loja, descontar o restante da Casa", value=True)
            with colB:
                bloquear_negativos = st.checkbox("Blindagem total (nunca deixar estoque negativo)", value=True)

            if arquivos and st.button("🚀 PROCESSAR IMPORTAÇÃO", type="primary"):
                df_vi = carregar_vendas_itens(prefixo)
                df_vt = carregar_vendas_transacoes(prefixo)

                total_linhas = 0
                registros_itens = []
                registros_trans = []
                logs_vendas = []
                faltas = []

                df_ref = df.copy()
                df_ref['nome_norm'] = df_ref['nome do produto'].astype(str).apply(normalizar_para_busca)
                # Dicionário blindado ignorando zeros a esquerda
                map_cod_to_idx = {str(r['código de barras']).strip().lstrip('0'): i for i, r in df_ref.reset_index().iterrows() if str(r['código de barras']).strip()}
                lista_nomes = df_ref['nome do produto'].astype(str).tolist()

                def detectar_tipo(cols, nome_arquivo: str = ""):
                    cols_norm = [normalizar_para_busca(c) for c in cols if c is not None]
                    joined = " | ".join(cols_norm)

                    nome_norm = normalizar_para_busca(nome_arquivo or "")
                    if "SALES-BY-TRANSACTION" in nome_norm or "BY-TRANSACTION" in nome_norm:
                        return "transacoes"
                    if nome_norm.startswith("SALES-") and "BY-TRANSACTION" not in nome_norm and "SALES BY TRANSACTION" not in nome_norm:
                        return "itens"

                    tem_produto = any(("PRODUTO" in c) or ("ITEM" == c) for c in cols_norm)
                    tem_qtd = any(("QTD" in c) or ("QUANT" in c) for c in cols_norm)
                    tem_id_trans = any(("ID" in c and "TRANS" in c) or ("TRANSACAO" in c) for c in cols_norm)

                    if tem_produto and tem_qtd: return "itens"
                    
                    tem_subtotal = any("SUBTOTAL" in c for c in cols_norm)
                    tem_total = any((c == "TOTAL") or ("VALOR TOTAL" in c) or ("TOTAL" in c and "VALOR" in c) for c in cols_norm)
                    tem_taxa = any(("TAXA" in c) or ("TAXAS" in c) for c in cols_norm)
                    tem_desc = any(("DESC" in c) or ("DESCONTO" in c) for c in cols_norm)
                    if tem_id_trans and (tem_subtotal or tem_total or tem_taxa or tem_desc): return "transacoes"

                    tem_data = any("DATA" == c or "DATA" in c for c in cols_norm)
                    if tem_id_trans and tem_data: return "transacoes"

                    return "desconhecido"

                def mes_ref_from_dt(dt_val):
                    try:
                        if pd.isna(dt_val): return ""
                        return pd.to_datetime(dt_val).strftime("%Y-%m")
                    except: return ""

                for arq in arquivos:
                    try:
                        df_raw, header_row_detected, cols_detectadas = ler_excel_com_header_auto(arq)
                        if df_raw.empty: continue
                        tipo = detectar_tipo(df_raw.columns)

                        if tipo == "itens":
                            col_cod = pick_col(df_raw.columns, 'código de barras', 'codigo de barras', 'ean', 'gtin', 'codbarras', 'código barras')
                            col_nome = pick_col(df_raw.columns, 'produto', 'nome do produto', 'item', 'descrição', 'descricao')
                            col_qtd = pick_col(df_raw.columns, 'qtd', 'qtd.', 'quantidade', 'qtd vendida', 'qtde', 'qte')
                            col_val_total = pick_col(df_raw.columns, 'valor total', 'total', 'valor', 'venda', 'valor venda')
                            col_data = pick_col(df_raw.columns, 'data', 'data da venda', 'data/hora', 'data hora', 'data_hora')
                            col_hora = pick_col(df_raw.columns, 'hora', 'hora da venda')
                            col_trans = pick_col(df_raw.columns, 'id da transação', 'id da transacao', 'transação', 'transacao', 'id transacao', 'id_transacao')
                            col_cat = pick_col(df_raw.columns, 'categoria', 'seção', 'secao', 'departamento')

                            for _, r in df_raw.iterrows():
                                qtd = parse_num_br(r.get(col_qtd, 0), default=0)
                                if qtd <= 0: continue

                                cod = str(r.get(col_cod, "") if col_cod else "").replace('.0', '').strip()
                                cod_limpo = cod.lstrip('0')
                                nome_raw = str(r.get(col_nome, "") if col_nome else "").strip()
                                nome_norm = normalizar_texto(nome_raw) if nome_raw else ""
                                
                                categoria_shoppbud = str(r.get(col_cat, "") if col_cat else "").strip()

                                dt_val = None
                                try:
                                    if col_data and col_hora and pd.notna(r.get(col_data)) and pd.notna(r.get(col_hora)):
                                        dt_val = pd.to_datetime(f"{r.get(col_data)} {r.get(col_hora)}", errors='coerce')
                                    elif col_data and pd.notna(r.get(col_data)):
                                        dt_val = pd.to_datetime(r.get(col_data), errors='coerce')
                                    elif col_hora and pd.notna(r.get(col_hora)):
                                        dt_val = pd.to_datetime(r.get(col_hora), errors='coerce')
                                except: dt_val = None
                                if dt_val is None or pd.isna(dt_val): dt_val = obter_hora_manaus()

                                mes_ref = mes_ref_from_dt(dt_val)
                                transacao = str(r.get(col_trans, "") if col_trans else "").strip()
                                val_total = parse_num_br(r.get(col_val_total, 0) if col_val_total else 0, default=0)
                                preco_unit = (val_total / qtd) if qtd > 0 else 0.0

                                idx_real = None
                                # Busca limpa ignorando zeros
                                if cod_limpo and cod_limpo in map_cod_to_idx:
                                    idx_real = map_cod_to_idx[cod_limpo]
                                elif nome_norm:
                                    melhor, _ = encontrar_melhor_match(nome_norm, lista_nomes, cutoff=0.25)
                                    if melhor:
                                        idx_real = df_ref.loc[df_ref['nome do produto'] == melhor].index[0]

                                if idx_real is None:
                                    registros_itens.append({
                                        'data_hora': str(dt_val), 'mes_ref': mes_ref, 'transacao': transacao,
                                        'código_barras': cod, 'produto': nome_norm or nome_raw,
                                        'qtd_vendida': qtd, 'preco_unit': preco_unit, 'valor_total': val_total,
                                        'canal': 'Shoppbud', 'obs_importacao': f"SEM MATCH | arquivo={arq.name}"
                                    })
                                    continue

                                nome_sis = df.at[idx_real, 'nome do produto']
                                qtd_loja = float(df.at[idx_real, 'qtd.estoque'])
                                qtd_casa = float(df.at[idx_real, 'qtd_central'])
                                qtd_pedir = float(qtd)

                                baixa_loja = min(qtd_loja, qtd_pedir)
                                resto = max(0.0, qtd_pedir - baixa_loja)

                                df.at[idx_real, 'qtd.estoque'] = qtd_loja - baixa_loja

                                baixa_casa = 0.0
                                if resto > 0 and descontar_casa_se_faltar:
                                    baixa_casa = min(qtd_casa, resto)
                                    df.at[idx_real, 'qtd_central'] = qtd_casa - baixa_casa
                                    resto = max(0.0, resto - baixa_casa)

                                if resto > 0:
                                    faltas.append({'produto': nome_sis, 'código_barras': df.at[idx_real, 'código de barras'], 'faltou': resto, 'arquivo': arq.name})
                                    if not bloquear_negativos:
                                        df.at[idx_real, 'qtd.estoque'] -= resto

                                if df.at[idx_real, 'status'] == 'Inativo':
                                    df.at[idx_real, 'status'] = 'Ativo'

                                if categoria_shoppbud and categoria_shoppbud.upper() != "GERAL":
                                    cat_atual = str(df.at[idx_real, 'categoria']).strip().upper()
                                    if cat_atual in ["", "GERAL", "NAN", "NONE"]:
                                        df.at[idx_real, 'categoria'] = categoria_shoppbud.upper()

                                registros_itens.append({
                                    'data_hora': str(dt_val), 'mes_ref': mes_ref, 'transacao': transacao,
                                    'código_barras': df.at[idx_real, 'código de barras'], 'produto': nome_sis,
                                    'qtd_vendida': qtd, 'preco_unit': preco_unit, 'valor_total': val_total,
                                    'canal': 'Shoppbud', 'obs_importacao': f"OK | arquivo={arq.name}"
                                })

                                logs_vendas.append({
                                    'data_hora': str(dt_val), 'produto': nome_sis,
                                    'qtd_antes': qtd_loja, 'qtd_nova': float(df.at[idx_real, 'qtd.estoque']),
                                    'acao': "Baixa por Venda", 'motivo': f"Shoppbud | {arq.name}"
                                })
                                total_linhas += 1

                        elif tipo == "transacoes":
                            cols = [str(c) for c in df_raw.columns]
                            cols_norm = {c: normalizar_para_busca(c) for c in cols}
                            
                            def pick(keywords):
                                for c in cols:
                                    cn = cols_norm.get(c, "")
                                    if any(k in cn for k in keywords): return c
                                return None
                            
                            col_trans = pick(["ID DA TRANSACAO", "ID TRANSACAO", "TRANSACAO", "ID DO PEDIDO", "PEDIDO"])
                            col_hora  = pick(["HORA DA VENDA", "DATA/HORA", "DATA HORA", "DATA"])
                            col_sub   = pick(["SUBTOTAL DE ITENS", "SUBTOTAL", "ITENS"])
                            col_desc  = pick(["DESCONTO", "DESCONTOS"])
                            col_tax   = pick(["TAXA", "TAXAS"])
                            col_tot   = pick(["TOTAL", "VALOR TOTAL"])

                            for _, r in df_raw.iterrows():
                                transacao = str(r.get(col_trans, "")).strip() if col_trans else ""
                                dt_val = pd.to_datetime(r.get(col_hora), errors='coerce') if col_hora else pd.NaT
                                if pd.isna(dt_val): dt_val = obter_hora_manaus()

                                mes_ref = mes_ref_from_dt(dt_val)
                                subtotal = parse_num_br(r.get(col_sub, 0) if col_sub else 0, default=0)
                                descontos = parse_num_br(r.get(col_desc, 0) if col_desc else 0, default=0)
                                taxas = parse_num_br(r.get(col_tax, 0) if col_tax else 0, default=0)
                                total = parse_num_br(r.get(col_tot, 0) if col_tot else 0, default=0)

                                registros_trans.append({
                                    'data_hora': str(dt_val), 'mes_ref': mes_ref, 'transacao': transacao,
                                    'subtotal': subtotal, 'descontos': descontos, 'taxas': taxas, 'total': total,
                                    'forma_pagamento': '', 'obs_importacao': f"OK | arquivo={arq.name}"
                                })
                                total_linhas += 1
                        else:
                            st.warning(f"⚠️ Não consegui identificar o tipo do arquivo: {arq.name}.")
                    except Exception as e:
                        st.error(f"Erro ao processar {arq.name}: {e}")

                salvar_estoque(df, prefixo)
                if logs_vendas: salvar_logs_em_lote(prefixo, logs_vendas)

                if registros_itens:
                    df_new = pd.DataFrame(registros_itens)
                    if not df_vi.empty: df_all = pd.concat([df_vi, df_new], ignore_index=True)
                    else: df_all = df_new
                    cols_dedup = [c for c in ['transacao','produto','data_hora','qtd_vendida','valor_total'] if c in df_all.columns]
                    if cols_dedup: df_all = df_all.drop_duplicates(subset=cols_dedup, keep='last')
                    salvar_vendas_itens(df_all, prefixo)

                if registros_trans:
                    df_new = pd.DataFrame(registros_trans)
                    if not df_vt.empty: df_all = pd.concat([df_vt, df_new], ignore_index=True)
                    else: df_all = df_new
                    cols_dedup = [c for c in ['transacao'] if c in df_all.columns]
                    if cols_dedup:
                        df_all['transacao'] = df_all['transacao'].astype(str)
                        df_all = df_all.drop_duplicates(subset=cols_dedup, keep='last')
                    salvar_vendas_transacoes(df_all, prefixo)

                df_vi2 = carregar_vendas_itens(prefixo)
                if not df_vi2.empty:
                    df_vi2['data_hora'] = pd.to_datetime(df_vi2['data_hora'], errors='coerce')
                    df_vi2['mes_ref'] = df_vi2['data_hora'].dt.strftime("%Y-%m")
                    agg = df_vi2.groupby(['mes_ref','código_barras','produto'], dropna=False).agg(
                        qtd_vendida=('qtd_vendida','sum'),
                        valor_total=('valor_total','sum'),
                        ultima_venda=('data_hora','max')
                    ).reset_index()
                    agg['ultima_venda'] = agg['ultima_venda'].astype(str)
                    salvar_vendas_mensal_produto(agg, prefixo)

                if faltas:
                    st.warning(f"⚠️ {len(faltas)} itens venderam mais do que o estoque registrado. O sistema **não** deixou negativo.")
                    st.dataframe(pd.DataFrame(faltas), use_container_width=True, hide_index=True)

                st.success(f"✅ Importação concluída. Linhas processadas: {total_linhas}.")
                st.rerun()

        with tab_8020:
            df_mensal = carregar_do_google(f"{prefixo}_vendas_mensal_produto")
            if df_mensal.empty:
                st.info("Sem dados de itens vendidos.")
            else:
                df_mensal.columns = df_mensal.columns.str.strip().str.lower()
                for c in ['qtd_vendida','valor_total']:
                    if c in df_mensal.columns:
                        df_mensal[c] = df_mensal[c].apply(parse_num_br)
                if 'ultima_venda' in df_mensal.columns:
                    df_mensal['ultima_venda'] = pd.to_datetime(df_mensal['ultima_venda'], errors='coerce')

                meses = sorted([m for m in df_mensal['mes_ref'].dropna().astype(str).unique().tolist() if m])
                if not meses:
                    st.info("Sem referência de mês nas vendas.")
                else:
                    c1, c2 = st.columns(2)
                    with c1: mes_ini = st.selectbox("Mês inicial", meses, index=max(0, len(meses)-1))
                    with c2: mes_fim = st.selectbox("Mês final", meses, index=max(0, len(meses)-1))

                    df_periodo = df_mensal[(df_mensal['mes_ref'] >= mes_ini) & (df_mensal['mes_ref'] <= mes_fim)].copy()
                    if df_periodo.empty: st.warning("Período sem vendas.")
                    else:
                        st.markdown("### 🧮 80/20 (Curva ABC)")
                        df_rank = df_periodo.groupby(['produto','código_barras'], dropna=False).agg(
                            qtd_vendida=('qtd_vendida','sum'), valor_total=('valor_total','sum'), ultima_venda=('ultima_venda','max')
                        ).reset_index().sort_values(by='valor_total', ascending=False)

                        total_val = df_rank['valor_total'].sum()
                        if total_val <= 0: st.warning("Valor total zerado.")
                        else:
                            df_rank['pct'] = df_rank['valor_total'] / total_val
                            df_rank['pct_acum'] = df_rank['pct'].cumsum()
                            df_rank['classe'] = df_rank['pct_acum'].apply(lambda x: 'A' if x <= 0.80 else ('B' if x <= 0.95 else 'C'))

                            topA = df_rank[df_rank['classe']=='A']
                            st.metric("Produtos Classe A (≈80% do valor)", len(topA))

                            busca_rank = st.text_input("Digite parte do nome ou 4+ dígitos do código de barras", key="busca_rank_8020")
                            df_rank_f = filtrar_df_busca_robusta(df_rank, busca_rank, cols_text=['produto'], cols_barcode=['código_barras'])

                            df_rank_f = df_rank_f.copy()
                            df_rank_f['código_barras'] = df_rank_f['código_barras'].map(limpar_codigo_barras)
                            df_rank_f['produto_label'] = df_rank_f.apply(
                                lambda r: f"{r.get('produto','')} [{r.get('código_barras','')}]" if r.get('código_barras','') else str(r.get('produto','')), axis=1
                            )

                            st.markdown("#### 📌 Top 15 por valor")
                            df_top = df_rank_f.head(15).copy()
                            fig_top = px.bar(df_top.sort_values('valor_total', ascending=True), x='valor_total', y='produto_label', orientation='h')
                            fig_top.update_layout(xaxis_title='R$ (valor total)', yaxis_title='Produto')
                            st.plotly_chart(fig_top, use_container_width=True)

                            st.markdown("#### 📋 Tabela (Top 30)")
                            df_show = df_rank_f.head(30).copy()
                            df_show['pct_fmt'] = df_show.get('pct', pd.Series()).map(lambda v: "—" if pd.isna(v) else f"{(v*100):.1f}%")
                            df_show['pct_acum_fmt'] = df_show.get('pct_acumulado', pd.Series()).map(lambda v: "—" if pd.isna(v) else f"{(v*100):.1f}%")
                            
                            cols = [c for c in ['classe','produto','código_barras','valor_total','qtd_vendida','pct_fmt','pct_acum_fmt'] if c in df_show.columns]
                            if 'valor_total' in df_show.columns:
                                df_show['valor_total_fmt'] = df_show['valor_total'].map(lambda v: f"R$ {formatar_moeda_br(v)}")
                                cols = [c if c!='valor_total' else 'valor_total_fmt' for c in cols]
                            st.dataframe(df_show[cols], use_container_width=True, hide_index=True)

        with tab_alertas:
            st.subheader("🚦 Mix: produtos sem giro / menos vendidos")
            df_vi = carregar_vendas_itens(prefixo)
            if df_vi.empty: st.info("Sem dados de vendas.")
            else:
                df_vi['data_hora'] = pd.to_datetime(df_vi['data_hora'], errors='coerce')
                ultima = df_vi.groupby(['produto','código_barras'], dropna=False)['data_hora'].max().reset_index()
                hoje = obter_hora_manaus()
                dias_padrao = st.number_input("Considerar 'sem giro' após quantos dias?", min_value=7, value=60)
                ultima['dias_sem_venda'] = (hoje - ultima['data_hora']).dt.days

                df_est = df[['nome do produto','código de barras','qtd.estoque','qtd_central','status']].copy()
                df_est.columns = ['produto','código_barras_est','qtd_loja','qtd_casa','status_prod']
                df_est['qtd_estoque_total'] = df_est['qtd_loja'].astype(float) + df_est['qtd_casa'].astype(float)

                df_mix = pd.merge(ultima, df_est, left_on=['produto','código_barras'], right_on=['produto','código_barras_est'], how='left')
                df_mix['status_giro'] = df_mix['dias_sem_venda'].apply(lambda d: 'Sem giro' if d >= dias_padrao else ('Atenção' if d >= int(dias_padrao*0.6) else 'OK'))
                df_mix = df_mix.sort_values(by=['status_giro','dias_sem_venda'], ascending=[True, False])

                st.dataframe(df_mix[['produto','código_barras','status_giro','dias_sem_venda','qtd_estoque_total','status_prod']].head(200), use_container_width=True, hide_index=True)

        with tab_hist:
            st.subheader("📜 Histórico de vendas salvo na nuvem")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Itens (por produto)**")
                df_vi = carregar_vendas_itens(prefixo)
                if not df_vi.empty:
                    busca = st.text_input("Buscar (itens):", key="busca_vi")
                    df_vi_show = filtrar_dados_inteligente(df_vi, 'produto', busca) if busca else df_vi
                    st.dataframe(df_vi_show.sort_values(by='data_hora', ascending=False).head(500), use_container_width=True, hide_index=True)
                else: st.info("Vazio.")
            with c2:
                st.markdown("**Transações (financeiro)**")
                df_vt = carregar_vendas_transacoes(prefixo)
                if not df_vt.empty: st.dataframe(df_vt.sort_values(by='data_hora', ascending=False).head(500), use_container_width=True, hide_index=True)
                else: st.info("Vazio.")

    elif modo == "🔎 Raio-X do Estoque (Auditoria)":
        st.title(f"🔎 Raio-X do Estoque - {loja_atual}")
        st.markdown(
            "Selecione um período para cruzar as **Entradas (Compras/XML)** com as **Saídas (Vendas/Shoppbud)** "
            "e descobrir exatamente o que deveria ter acontecido com o seu estoque."
        )

        c1, c2 = st.columns(2)
        hoje = obter_hora_manaus().date()
        dt_ini = c1.date_input("📅 Data Inicial:", hoje - timedelta(days=2))
        dt_fim = c2.date_input("📅 Data Final:", hoje)
        
        st.markdown("### 🎛️ Filtros da Auditoria")
        col_cat, col_busca = st.columns(2)
        
        if 'categoria' in df.columns:
            lista_categorias = sorted([c for c in df['categoria'].astype(str).unique() if c.strip()])
        else:
            lista_categorias = []
        
        cat_selecionada = col_cat.selectbox("🗂️ Filtrar por Categoria:", ["[ Todas ]"] + lista_categorias)

        df_opcoes = df.copy()
        if cat_selecionada != "[ Todas ]":
            df_opcoes = df_opcoes[df_opcoes['categoria'].astype(str).str.upper() == cat_selecionada.upper()]
        
        lista_prods_raiox = sorted((df_opcoes['código de barras'].astype(str) + " - " + df_opcoes['nome do produto'].astype(str)).unique().tolist())
        
        busca_raiox = col_busca.selectbox("🔍 Buscar Produto Específico (Menu Rápido):", ["[ Mostrar Todos ]"] + lista_prods_raiox)

        apenas_movimentados = st.checkbox("Mostrar apenas produtos que tiveram movimentação no período (Entrou ou Saiu)", value=True)

        if st.button("🚀 GERAR RAIO-X", type="primary"):
            with st.spinner("Analisando os históricos de Compras e Vendas..."):
                df_c = carregar_historico(prefixo)
                df_v = carregar_vendas_itens(prefixo)

                if not df_c.empty and 'data' in df_c.columns:
                    df_c['data'] = pd.to_datetime(df_c['data'], errors='coerce')
                if not df_v.empty and 'data_hora' in df_v.columns:
                    df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')

                dt_ini_full = datetime.combine(dt_ini, datetime.min.time())
                dt_fim_full = datetime.combine(dt_fim, datetime.max.time())

                if not df_c.empty:
                    df_c = df_c[(df_c['data'] >= dt_ini_full) & (df_c['data'] <= dt_fim_full)]
                if not df_v.empty:
                    df_v = df_v[(df_v['data_hora'] >= dt_ini_full) & (df_v['data_hora'] <= dt_fim_full)]

                resultado = []

                df_filtrado = df.copy()
                if cat_selecionada != "[ Todas ]":
                    df_filtrado = df_filtrado[df_filtrado['categoria'].astype(str).str.upper() == cat_selecionada.upper()]
                
                if busca_raiox != "[ Mostrar Todos ]":
                    cod_busca = busca_raiox.split(' - ', 1)[0]
                    df_filtrado = df_filtrado[df_filtrado['código de barras'].astype(str) == cod_busca]

                for idx, row in df_filtrado.iterrows():
                    cod = str(row.get('código de barras', '')).strip()
                    cod_limpo = cod.lstrip('0')
                    nome = str(row.get('nome do produto', '')).strip()

                    # 1. Contar Compras (XML)
                    qtd_compra = 0
                    if not df_c.empty:
                        mask_c = df_c['produto'].astype(str).str.upper() == nome.upper()
                        qtd_compra = df_c[mask_c]['qtd'].sum()

                    # 2. Contar Vendas (Shoppbud)
                    qtd_venda = 0
                    if not df_v.empty:
                        # Busca ignorando zeros à esquerda
                        mask_v_cod = df_v['código_barras'].astype(str).str.lstrip('0') == cod_limpo
                        mask_v_nome = df_v['produto'].astype(str).str.upper() == nome.upper()
                        mask_v = mask_v_cod | mask_v_nome if cod_limpo else mask_v_nome
                        qtd_venda = df_v[mask_v]['qtd_vendida'].sum()

                    if apenas_movimentados and qtd_compra == 0 and qtd_venda == 0:
                        continue

                    saldo = qtd_compra - qtd_venda

                    if saldo > 0: txt_saldo = f"🟢 Aumentou {int(saldo)} un."
                    elif saldo < 0: txt_saldo = f"🔴 Diminuiu {abs(int(saldo))} un."
                    else: txt_saldo = "⚪ Ficou igual"

                    nome_formatado = f"{cod} - {nome}" if cod else nome

                    resultado.append({
                        "🏷️ Código e Produto": nome_formatado,
                        "🏪 Loja": int(row.get('qtd.estoque', 0)),
                        "🏡 Casa": int(row.get('qtd_central', 0)),
                        "📥 Compras": int(qtd_compra),
                        "🛍️ Vendas": int(qtd_venda),
                        "⚖️ Saldo do Período": txt_saldo
                    })

                if resultado:
                    df_res = pd.DataFrame(resultado)
                    st.dataframe(df_res, use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum produto encontrado com os filtros selecionados ou não houve movimentação no período.")

    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
        reativar_auto = st.checkbox("☑️ Reativar automaticamente produtos contados? (Inventário Inteligente)", value=True)
        df_mov = carregar_movimentacoes(prefixo)
        
        if df.empty:
            st.warning("Cadastre produtos.")
        else:
            if usar_modo_mobile:
                st.info("📱 Modo Celular Ativado")
                termo_busca = st.text_input("🔍 Buscar Produto (Nome ou Código):", placeholder="Digite aqui...")
                df_show = filtrar_dados_inteligente(df, 'nome do produto', termo_busca)
                if df_show.empty:
                    st.warning("Nenhum produto encontrado.")
                else:
                    for idx, row in df_show.iterrows():
                        icon_status = "🟢" if row['status'] == 'Ativo' else "🔴"
                        with st.container(border=True):
                            st.subheader(f"{icon_status} 🆔 {row['código de barras']} | {row['nome do produto']}")
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
                                        if reativar_auto and df.at[idx, 'status'] == 'Inativo':
                                            df.at[idx, 'status'] = 'Ativo'
                                            st.toast(f"{row['nome do produto']} REATIVADO!")
                                        
                                        salvar_estoque(df, prefixo)
                                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                        registrar_auditoria(prefixo, row['nome do produto'], 0, q_tr, "Baixa Gôndola Mobile")
                                        st.success(f"Baixado {q_tr} un!")
                                        st.rerun()
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
                        st.caption(f"Status Atual: {'🟢 Ativo' if df.at[idx, 'status']=='Ativo' else '🔴 Inativo'}")
                        st.divider()
                        st.subheader("🚚 Transferência (Casa -> Loja)")
                        with st.form("form_transf_gondola"):
                            c_dt, c_hr, c_qtd = st.columns(3)
                            dt_transf = c_dt.date_input("Data da Transferência:", obter_hora_manaus().date())
                            hora_atual = obter_hora_manaus().time().replace(second=0, microsecond=0)
                            hr_transf = c_hr.time_input("Hora:", value=hora_atual, step=60)
                            
                            qtd_disponivel = int(df.at[idx, 'qtd_central'])
                            qtd_transf = c_qtd.number_input(f"Quantidade (Disp: {qtd_disponivel}):", min_value=0, max_value=qtd_disponivel, value=0)
                            
                            if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                if qtd_transf > 0:
                                    df.at[idx, 'qtd.estoque'] += qtd_transf
                                    df.at[idx, 'qtd_central'] -= qtd_transf
                                    if reativar_auto and df.at[idx, 'status'] == 'Inativo':
                                        df.at[idx, 'status'] = 'Ativo'
                                    
                                    salvar_estoque(df, prefixo)
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    data_final = datetime.combine(dt_transf, hr_transf)
                                    novo_mov = {'data_hora': str(data_final), 'produto': nome_prod, 'qtd_movida': qtd_transf}
                                    df_mov = pd.concat([df_mov, pd.DataFrame([novo_mov])], ignore_index=True)
                                    salvar_movimentacoes(df_mov, prefixo)
                                    registrar_auditoria(prefixo, nome_prod, 0, qtd_transf, "Transferência Gôndola Desktop")
                                    st.success(f"Sucesso! {qtd_transf} unid. transferidas.")
                                    st.rerun()
                                else: st.warning("Quantidade inválida.")
                        
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
                                qtd_antes_audit = df.at[idx, 'qtd.estoque']
                                df.at[idx, 'qtd.estoque'] = n_qtd_loja
                                df.at[idx, 'validade'] = pd.to_datetime(n_val) if n_val else None
                                salvar_estoque(df, prefixo)
                                registrar_auditoria(prefixo, c_nome, qtd_antes_audit, n_qtd_loja, "Ajuste Manual Gôndola")
                                st.success("Atualizado em todo o sistema!")
                                st.rerun()
                with tab_hist:
                    if not df_mov.empty:
                        busca_gondola_hist = st.text_input("🔍 Buscar no Histórico de Gôndola:", placeholder="Ex: oleo...", key="busca_gondola_hist")
                        df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_gondola_hist)
                        st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True, hide_index=True)

    elif modo == "💰 Inteligência de Compras (Histórico)":
        st.title("💰 Inteligência de Compras")
        df_hist = carregar_historico(prefixo)
        
        tab_graf, tab_dados = st.tabs(["📊 Análise & Gráficos", "📜 Histórico Completo (Editar)"])
        
        with tab_graf:
            if df_hist.empty:
                st.info("Sem histórico suficiente.")
            else:
                st.markdown("### 🔍 Análise Detalhada por Produto")
                df_hist['produto_str'] = df_hist['produto'].astype(str)
                if not df.empty:
                    mapa_codigos = dict(zip(df['nome do produto'], df['código de barras']))
                    df_hist['display_combo'] = df_hist['produto_str'].map(mapa_codigos).fillna('?') + " - " + df_hist['produto_str']
                else:
                    df_hist['display_combo'] = df_hist['produto_str']

                lista_prods_hist = sorted(df_hist['display_combo'].unique())
                prod_sel_graf_raw = st.selectbox("Selecione um Produto para analisar:", lista_prods_hist)
                
                if prod_sel_graf_raw:
                    if " - " in prod_sel_graf_raw:
                        nome_para_filtro = prod_sel_graf_raw.split(" - ", 1)[1]
                    else:
                        nome_para_filtro = prod_sel_graf_raw

                    df_prod = df_hist[df_hist['produto'] == nome_para_filtro].copy()
                    
                    if not df_prod.empty:
                        df_validos = df_prod[df_prod['preco_pago'] > 0.01]
                        if df_validos.empty: df_validos = df_prod 

                        menor_preco = df_validos['preco_pago'].min()
                        maior_preco = df_validos['preco_pago'].max()
                        media_preco = df_validos['preco_pago'].mean()
                        ultimo_preco = df_validos.sort_values(by='data', ascending=False).iloc[0]['preco_pago']
                        
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("💎 Menor Preço", f"R$ {menor_preco:.2f}")
                        c2.metric("💸 Maior Preço", f"R$ {maior_preco:.2f}")
                        c3.metric("📊 Média", f"R$ {media_preco:.2f}")
                        c4.metric("📅 Último Pago", f"R$ {ultimo_preco:.2f}", delta=f"{ultimo_preco - media_preco:.2f}")
                        st.divider()

                        st.markdown("### 🏆 Ranking: Onde comprar mais barato?")
                        df_ranking = df_validos.groupby('fornecedor')['preco_pago'].mean().reset_index().sort_values(by='preco_pago')
                        fig_bar = px.bar(
                            df_ranking, 
                            x='preco_pago', 
                            y='fornecedor', 
                            orientation='h', 
                            text_auto='.2f',
                            title="Preço Médio por Fornecedor (Quanto menor, melhor)",
                            color='preco_pago',
                            color_continuous_scale='RdYlGn_r' 
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)

                        st.markdown("### 📈 Evolução do Preço no Tempo")
                        df_evolucao = df_validos.sort_values(by='data')
                        fig_line = px.line(
                            df_evolucao, 
                            x='data', 
                            y='preco_pago', 
                            markers=True,
                            title="Histórico de Preço Pago",
                            hover_data={'fornecedor': True, 'preco_pago': ':.2f'}
                        )
                        st.plotly_chart(fig_line, use_container_width=True)
        
        with tab_dados:
            if not df_hist.empty:
                busca_hist_precos = st.text_input("🔍 Buscar no Histórico:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
                df_hist_visual = df_hist.copy()
                if busca_hist_precos:
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'produto', busca_hist_precos)
                    if df_hist_visual.empty: 
                        df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
                
                mapa_ean = dict(zip(df['nome do produto'], df['código de barras']))
                df_hist_visual['código_barras'] = df_hist_visual['produto'].map(mapa_ean)
                
                cols = ['data', 'data_emissao', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_sem_desconto', 'desconto_total_money', 'preco_pago', 'total_gasto', 'obs_importacao']
                cols = [c for c in cols if c in df_hist_visual.columns]
                df_hist_visual = df_hist_visual[cols]
                
                st.info("✅ Edite ou exclua (Delete) linhas.")
                
                estornar_estoque = st.checkbox("⚠️ Ao excluir uma linha, deseja ESTORNAR (Remover) a quantidade do Estoque Físico? (Cuidado!)", value=False)
                
                df_editado = st.data_editor(
                    df_hist_visual.sort_values(by='data', ascending=False), 
                    use_container_width=True, 
                    key="editor_historico_geral",
                    num_rows="dynamic", 
                    column_config={
                        "data": st.column_config.DatetimeColumn("Data Lançamento", format="DD/MM/YYYY HH:mm"),
                        "data_emissao": st.column_config.TextColumn("Data Nota (XML)", disabled=True),
                        "código_barras": st.column_config.TextColumn("Cód. Barras", disabled=True),
                        "preco_sem_desconto": st.column_config.NumberColumn("Preço Tabela", format="R$ %.2f"),
                        "desconto_total_money": st.column_config.NumberColumn("Desconto TOTAL", format="R$ %.2f"),
                        "preco_pago": st.column_config.NumberColumn("Pago (Unit)", format="R$ %.2f", disabled=True),
                        "total_gasto": st.column_config.NumberColumn("Total Gasto", format="R$ %.2f", disabled=True),
                    }
                )
                if st.button("💾 Salvar Alterações e Exclusões"):
                    indices_originais = df_hist_visual.index.tolist()
                    indices_editados = df_editado.index.tolist()
                    indices_removidos = list(set(indices_originais) - set(indices_editados))
                    
                    if indices_removidos:
                        if estornar_estoque:
                            for idx_rem in indices_removidos:
                                nome_prod = df_hist.loc[idx_rem, 'produto']
                                qtd_rem = float(df_hist.loc[idx_rem, 'qtd'])
                                
                                mask_est = df['nome do produto'] == nome_prod
                                if mask_est.any():
                                    idx_est = df[mask_est].index[0]
                                    df.at[idx_est, 'qtd_central'] -= qtd_rem 
                                    st.toast(f"Estornado {qtd_rem} de {nome_prod}")
                            salvar_estoque(df, prefixo)
                        
                        df_hist = df_hist.drop(indices_removidos)
                        st.warning(f"🗑️ {len(indices_removidos)} registros excluídos.")
                    
                    if 'código_barras' in df_editado.columns:
                        df_editado = df_editado.drop(columns=['código_barras'])
                    
                    df_hist.update(df_editado)
                    for idx, row in df_hist.iterrows():
                        try:
                            q = float(row.get('qtd', 0))
                            p_tab = float(row.get('preco_sem_desconto', 0))
                            d_tot = float(row.get('desconto_total_money', 0))
                            if q > 0 and p_tab > 0:
                                total_liq = (p_tab * q) - d_tot
                                df_hist.at[idx, 'preco_pago'] = total_liq / q
                                df_hist.at[idx, 'total_gasto'] = total_liq
                        except: pass
                    salvar_historico(df_hist, prefixo)
                    st.success("Histórico salvo!")
                    st.rerun()
            else: st.info("Sem histórico.")

    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
        tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
        with tab_ver:
            if not df.empty:
                if usar_modo_mobile:
                    st.info("📱 Modo Celular (Edição Rápida com FILA)")
                    
                    if st.session_state['alteracoes_pendentes'] > 0:
                        st.warning(f"⚠️ {st.session_state['alteracoes_pendentes']} alterações pendentes na memória.")
                        if st.button("☁️ SINCRONIZAR AGORA (Gravar no Google)"):
                            salvar_estoque(df, prefixo) 
                            st.session_state['alteracoes_pendentes'] = 0
                            st.success("Sincronizado com sucesso!")
                            st.rerun()
                    else:
                        st.success("✅ Tudo sincronizado.")
                    
                    st.markdown("---")
                    
                    busca_central = st.text_input("🔍 Buscar na Casa:", placeholder="Ex: arroz...")
                    df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_central)
                    for idx, row in df_show.iterrows():
                        with st.container(border=True):
                            st.write(f"📝 {row['código de barras']} | **{row['nome do produto']}**")
                            col1, col2 = st.columns(2)
                            nova_qtd = col1.number_input(f"Qtd Casa:", value=int(row['qtd_central']), key=f"q_{idx}")
                            novo_custo = col2.number_input(f"Custo:", value=float(row['preco_custo']), key=f"c_{idx}")
                            
                            if st.button(f"💾 Confirmar {row['nome do produto']} (Local)", key=f"btn_{idx}"):
                                df.at[idx, 'qtd_central'] = nova_qtd
                                df.at[idx, 'preco_custo'] = novo_custo
                                
                                st.session_state['alteracoes_pendentes'] += 1
                                st.toast(f"Salvo localmente! ({st.session_state['alteracoes_pendentes']} pendentes)")
                                st.rerun() 
                else:
                    st.info("✏️ Edição direta.")
                    busca_central = st.text_input("🔍 Buscar Produto na Casa:", placeholder="Ex: oleo concordia...", key="busca_central")
                    colunas_visiveis = ['código de barras', 'nome do produto', 'qtd_central', 'validade', 'preco_custo', 'ultimo_fornecedor']
                    df_visual = filtrar_dados_inteligente(df, 'nome do produto', busca_central)[colunas_visiveis]
                    df_editado = st.data_editor(df_visual, use_container_width=True, num_rows="dynamic", key="edit_casa")
                    if st.button("💾 SALVAR CORREÇÕES DA TABELA"):
                        indices_originais = df_visual.index.tolist()
                        indices_editados = df_editado.index.tolist()
                        indices_removidos = list(set(indices_originais) - set(indices_editados))
                        if indices_removidos:
                            df = df.drop(indices_removidos)
                            st.warning(f"{len(indices_removidos)} itens removidos.")
                        df.update(df_editado)
                        salvar_estoque(df, prefixo)
                        
                        bar = st.progress(0)
                        total = len(df_editado)
                        for i, (idx, row) in enumerate(df_editado.iterrows()):
                            nome = df.at[idx, 'nome do produto']
                            qtd = df.at[idx, 'qtd_central']
                            custo = df.at[idx, 'preco_custo']
                            venda = df.at[idx, 'preco_venda']
                            val = df.at[idx, 'validade']
                            atualizar_casa_global(nome, qtd, custo, None, val, prefixo)
                            bar.progress((i+1)/total)
                        registrar_auditoria(prefixo, "Vários", 0, 0, "Edição Tabela Casa")
                        st.success("Estoque atualizado!")
                        st.rerun()
        with tab_gerenciar:
            st.info("Adicione mercadoria manualmente.")
            df_hist = carregar_historico(prefixo)
            
            if not df.empty:
                lista_visuais = (df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist()
                lista_visuais = sorted(lista_visuais)
                prod_opcao = st.selectbox("Selecione o Produto:", lista_visuais)
                
                if prod_opcao:
                    mask = (df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)) == prod_opcao
                    if mask.any():
                        idx_prod = df[mask].index[0]
                        nome_atual = df.at[idx_prod, 'nome do produto']
                        val_atual = df.at[idx_prod, 'validade']
                        custo_atual = float(df.at[idx_prod, 'preco_custo'])
                        venda_atual = float(df.at[idx_prod, 'preco_venda'])
                        forn_atual = str(df.at[idx_prod, 'ultimo_fornecedor'])
                        with st.form("edit_estoque_casa_full"):
                            st.markdown(f"### Detalhes do Registro")
                            c_dt, c_hr = st.columns(2)
                            dt_reg = c_dt.date_input("Data da Entrada/Edição:", obter_hora_manaus().date())
                            hr_reg = c_hr.time_input("Hora:", value=obter_hora_manaus().time().replace(second=0, microsecond=0), step=60)
                            
                            c_forn = st.text_input("Fornecedor desta entrada:", value=forn_atual)
                            st.markdown("---")
                            c_nome = st.text_input("Nome do Produto (Editável):", value=nome_atual)
                            c_val, c_custo, c_venda = st.columns(3)
                            nova_val = c_val.date_input("Validade:", value=val_atual if pd.notnull(val_atual) else None)
                            novo_custo = c_custo.number_input("Preço Custo (UN):", value=custo_atual, format="%.2f")
                            novo_venda = c_venda.number_input("Preço Venda (UN):", value=venda_atual, format="%.2f")
                            st.markdown("---")
                            c_qtd, c_acao = st.columns([1, 2])
                            qtd_input = c_qtd.number_input("Quantidade:", min_value=0, value=0)
                            acao = c_acao.radio("Ação sobre o estoque:", ["Somar (+) Entrada de Mercadoria", "Substituir (=) Correção de Estoque", "Apenas Salvar Dados (Sem mudar qtd)"], index=2)
                            if st.form_submit_button("💾 SALVAR REGISTRO COMPLETO"):
                                df.at[idx_prod, 'nome do produto'] = c_nome.upper().strip()
                                df.at[idx_prod, 'validade'] = pd.to_datetime(nova_val) if nova_val else None
                                df.at[idx_prod, 'preco_custo'] = novo_custo
                                df.at[idx_prod, 'preco_venda'] = novo_venda
                                if c_forn: df.at[idx_prod, 'ultimo_fornecedor'] = c_forn
                                msg_acao = "Dados atualizados"
                                
                                qtd_antes_audit = df.at[idx_prod, 'qtd_central']
                                if acao.startswith("Somar") and qtd_input > 0:
                                    df.at[idx_prod, 'qtd_central'] += qtd_input
                                    msg_acao = f"Adicionado +{qtd_input}"
                                    dt_full = datetime.combine(dt_reg, hr_reg)
                                    hist = {'data': str(dt_full), 'produto': c_nome.upper().strip(), 'fornecedor': c_forn, 'qtd': qtd_input, 'preco_pago': novo_custo, 'total_gasto': qtd_input * novo_custo}
                                    salvar_historico(pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True), prefixo)
                                    registrar_auditoria(prefixo, c_nome, qtd_antes_audit, df.at[idx_prod, 'qtd_central'], "Entrada Manual Casa")
                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input
                                    msg_acao = f"Estoque corrigido para {qtd_input}"
                                    registrar_auditoria(prefixo, c_nome, qtd_antes_audit, qtd_input, "Correção Manual Casa")
                                
                                salvar_estoque(df, prefixo)
                                atualizar_casa_global(c_nome.upper().strip(), df.at[idx_prod, 'qtd_central'], novo_custo, novo_venda, pd.to_datetime(nova_val) if nova_val else None, prefixo)
                                st.success(f"✅ {msg_acao}!")
                                st.rerun()

    elif modo == "📋 Tabela Geral":
        st.title("📋 Geral")
        if not df.empty:
            st.info("💡 Botão 'CORRIGIR E UNIFICAR' abaixo ajuda a remover duplicados.")
            busca_geral = st.text_input("🔍 Buscar na Tabela Geral:", placeholder="Ex: oleo concordia...", key="busca_geral")
            df_visual_geral = filtrar_dados_inteligente(df, 'nome do produto', busca_geral)
            
            df_edit = st.data_editor(
                df_visual_geral, 
                use_container_width=True, 
                num_rows="dynamic", 
                key="geral_editor",
                column_config={
                    "status": st.column_config.SelectboxColumn("Status", options=["Ativo", "Inativo"], help="Defina se o produto está ativo para compras.")
                }
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("💾 SALVAR ALTERAÇÕES GERAIS"):
                    indices_originais = df_visual_geral.index.tolist()
                    indices_editados = df_edit.index.tolist()
                    indices_removidos = list(set(indices_originais) - set(indices_editados))
                    if indices_removidos:
                        df = df.drop(indices_removidos)
                        st.warning(f"🗑️ {len(indices_removidos)} produtos excluídos.")
                    df.update(df_edit)
                    salvar_estoque(df, prefixo)
                    
                    bar = st.progress(0)
                    total = len(df_edit)
                    for i, (idx, row) in enumerate(df_edit.iterrows()):
                        nome = df.at[idx, 'nome do produto']
                        qtd = df.at[idx, 'qtd_central']
                        custo = df.at[idx, 'preco_custo']
                        venda = df.at[idx, 'preco_venda']
                        val = df.at[idx, 'validade']
                        atualizar_casa_global(nome, qtd, custo, venda, val, prefixo)
                        bar.progress((i+1)/total)
                    registrar_auditoria(prefixo, "Vários", 0, 0, "Edição Tabela Geral")
                    st.success("Tabela Geral atualizada!")
                    st.rerun()
            with c2:
                if st.button("🔮 CORRIGIR NOMES E UNIFICAR (Pelo Código)"):
                    df.update(df_edit)
                    qtd_antes = len(df)
                    df = unificar_produtos_por_codigo(df)
                    qtd_depois = len(df)
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ Mágica feita! {qtd_antes - qtd_depois} duplicados unidos.")
                    st.balloons()
                    st.rerun()

    elif modo == "🛠️ Ajuste & Limpeza":
        st.title("🛠️ Ajuste & Limpeza de Estoque")
        st.info("Ferramentas para corrigir erros e limpar o cadastro.")
        
        c_z1, c_z2 = st.columns(2)
        with c_z1:
            st.markdown("### 📉 Zerar Negativos")
            st.write("Transforma todo estoque negativo em ZERO.")
            if st.button("ZERAR ESTOQUE NEGATIVO AGORA"):
                mask_neg = df['qtd.estoque'] < 0
                count_neg = mask_neg.sum()
                if count_neg > 0:
                    df.loc[mask_neg, 'qtd.estoque'] = 0
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ {count_neg} produtos negativos foram zerados!")
                    st.rerun()
                else:
                    st.info("Nenhum produto negativo encontrado.")
        
        st.divider()
        st.markdown("### 🧹 Inativar em Massa (Fantasmas)")
        st.write("Liste produtos com estoque ZERO (ou 1) para inativar rapidamente.")
        
        limite_f = st.number_input("Mostrar produtos com estoque MENOR ou IGUAL a:", value=0, min_value=0)
        
        df_fantasmas = df[(df['status'] == 'Ativo') & (df['qtd.estoque'] <= limite_f)].copy()
        
        if not df_fantasmas.empty:
            df_fantasmas['Selecionar'] = False
            df_fantasmas_edit = st.data_editor(
                df_fantasmas[['Selecionar', 'nome do produto', 'qtd.estoque', 'ultimo_fornecedor']], 
                hide_index=True, 
                use_container_width=True
            )
            
            if st.button("🔴 INATIVAR SELECIONADOS"):
                selecionados = df_fantasmas_edit[df_fantasmas_edit['Selecionar']]
                if not selecionados.empty:
                    count_inativados = 0
                    for _, row in selecionados.iterrows():
                        mask = df['nome do produto'] == row['nome do produto']
                        if mask.any():
                            df.loc[mask, 'status'] = 'Inativo'
                            count_inativados += 1
                    
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ {count_inativados} produtos inativados! Eles não aparecerão mais na Lista de Compras.")
                    st.rerun()
                else:
                    st.warning("Selecione algum produto na tabela acima.")
        else:
            st.success("Tudo limpo! Nenhum produto ativo com estoque baixo encontrado.")

    # ==============================================================================
    # ♻️ NOVA FERRAMENTA: RESTAURAR HISTÓRICO (AGORA COM SUBSTITUIÇÃO)
    # ==============================================================================
    elif modo == "♻️ Restaurar Histórico":
        st.title("♻️ Restaurador Inteligente de Histórico")
        st.info("Use para corrigir históricos corrompidos ou limpar dados duplicados.")

        df_atual = carregar_historico(prefixo)
        st.write(f"📊 Linhas atualmente no sistema (Nuvem): **{len(df_atual)}**")

        st.divider()
        st.markdown("### 1. Upload do Backup")
        st.markdown("Suba o arquivo Excel/CSV que você tem na Área de Trabalho.")
        arquivos_backup = st.file_uploader("📂 Arraste o arquivo aqui:", accept_multiple_files=True)
        
        st.markdown("### 2. Modo de Recuperação")
        modo_recup = st.radio("O que deseja fazer?", 
            ["🔗 UNIFICAR (Juntar Backup + Atual)", "☢️ SUBSTITUIÇÃO TOTAL (Apagar Atual e por Backup)"],
            index=0
        )
        
        if modo_recup == "☢️ SUBSTITUIÇÃO TOTAL (Apagar Atual e por Backup)":
            st.error("⚠️ CUIDADO: Isso vai apagar tudo que está no histórico hoje e colocar o conteúdo do arquivo no lugar. Use se o histórico atual estiver 'sujo' ou corrompido.")

        if arquivos_backup and st.button("🚀 EXECUTAR RECUPERAÇÃO"):
            if modo_recup.startswith("☢️"):
                lista_dfs = []
            else:
                lista_dfs = [df_atual]
            
            for arq in arquivos_backup:
                try:
                    if arq.name.endswith('.csv'):
                        try:
                            df_temp = pd.read_csv(arq)
                        except:
                            arq.seek(0)
                            df_temp = pd.read_csv(arq, sep=';')
                    else:
                        df_temp = pd.read_excel(arq)
                    
                    df_temp.columns = df_temp.columns.str.strip().str.lower()
                    
                    cols_ok = [c for c in df_temp.columns if c not in ['display_combo', 'produto_str', 'Selecionar', 'status_temp']]
                    df_temp = df_temp[cols_ok]
                    
                    lista_dfs.append(df_temp)
                    st.caption(f"✅ Lido: {arq.name} ({len(df_temp)} linhas)")
                except Exception as e:
                    st.error(f"Erro ao ler {arq.name}: {e}")

            if lista_dfs:
                df_gigante = pd.concat(lista_dfs, ignore_index=True)
                qtd_bruta = len(df_gigante)

                cols_chave = ['data', 'produto', 'qtd', 'total_gasto']
                cols_validas = [c for c in cols_chave if c in df_gigante.columns]
                
                if cols_validas:
                    df_limpo = df_gigante.drop_duplicates(subset=cols_validas, keep='first')
                    
                    if 'data' in df_limpo.columns:
                        df_limpo['data'] = pd.to_datetime(df_limpo['data'], errors='coerce')
                        df_limpo = df_limpo.sort_values(by='data', ascending=False)

                    qtd_limpa = len(df_limpo)
                    removidos = qtd_bruta - qtd_limpa

                    if not df_limpo.empty:
                        salvar_historico(df_limpo, prefixo)
                        st.success("✅ Histórico Restaurado e Salvo no Google Sheets!")
                        st.metric("Linhas Totais", qtd_bruta)
                        st.metric("Duplicatas Removidas", removidos, delta_color="inverse")
                        st.metric("Linhas Finais", qtd_limpa)
                        if removidos > 0: st.balloons()
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.warning("O arquivo resultante está vazio.")
            else:
                st.error("As planilhas não têm as colunas padrão (data, produto, qtd). Verifique os arquivos.")
