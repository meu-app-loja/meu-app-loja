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
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and nc == cn:
                return c
    for c in cols:
        nc = norm_map[c]
        for cn in cand_norm:
            if cn and cn in nc:
                return c
    return None

def ler_excel_com_header_auto(file_obj, max_rows=25):
    """Lê Excel tentando detectar automaticamente a linha de cabeçalho."""
    try:
        file_obj.seek(0)
    except Exception:
        pass

    # CIRURGIA: dtype=str para não estragar código de barras
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

    try:
        file_obj.seek(0)
    except Exception:
        pass

    df = pd.read_excel(file_obj, header=header_row, dtype=str).dropna(axis=1, how="all")
    cols = list(df.columns)
    return df, header_row, cols

def calcular_pontuacao(nome_xml, nome_sistema):
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    
    # CIRURGIA FUZZY MATCH SEGURO (Evita que Coca 2L se junte com 200ML)
    nome_xml_norm = normalizar_para_busca(nome_xml)
    nome_sis_norm = normalizar_para_busca(nome_sistema)
    nums_xml = set(re.findall(r'\d+', nome_xml_norm))
    nums_sis = set(re.findall(r'\d+', nome_sis_norm))
    if nums_xml and nums_sis and not nums_xml.intersection(nums_sis):
        return 0.0 # Proíbe unir se têm números diferentes
        
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
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        else:
            if "," in s and "." not in s:
                s = s.replace(".", "").replace(",", ".")
        s = re.sub(r"[^0-9\-\.]", "", s)
        return float(s) if s not in ["", "-", ".", "-."] else float(default)
    except Exception:
        return float(default)

def formatar_moeda_br(valor):
    try: return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    try:
        if df is None or len(df) == 0:
            return df
        cols_barcode_candidatas = [
            'código de barras','codigo de barras','código_barras','codigo_barras',
            'barcode','ean','ean13','gtin','gtin13','código barras','codigo barras'
        ]
        cols_barcode = [c for c in cols_barcode_candidatas if c in df.columns]
        cols_text = [coluna_busca] if coluna_busca in df.columns else []
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
    try:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)): return 0.0
        s = str(valor).strip()
        if s == "" or s.lower() in {"nan", "none"}: return 0.0
        s = s.replace("R$", "").strip()
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception: return 0.0

def _to_int(valor):
    try: return int(round(_to_float(valor)))
    except Exception: return 0

def garantir_colunas(df: pd.DataFrame, colunas_obrigatorias: list[str]) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame(columns=colunas_obrigatorias)
    for c in colunas_obrigatorias:
        if c not in df.columns: df[c] = ""
    return df

def blindar_estoque_df(df_estoque: pd.DataFrame) -> pd.DataFrame:
    if df_estoque is None: return pd.DataFrame()
    df = df_estoque.copy()
    df.columns = df.columns.astype(str).str.strip().str.lower()

    colunas_estoque_padrao = [
        'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade',
        'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor',
        'preco_sem_desconto', 'status'
    ]
    df = garantir_colunas(df, colunas_estoque_padrao)

    cols_int = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada']
    cols_money = ['preco_custo', 'preco_venda', 'preco_sem_desconto']

    for c in cols_int:
        df[c] = df[c].apply(_to_int)
        df.loc[df[c] < 0, c] = 0

    for c in cols_money:
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
        # CIRURGIA: Código de barras no histórico para poder cruzar 
        f"{prefixo}_historico_compras": ['data', 'data_emissao', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
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
        if df_raw.empty: return pd.DataFrame()
        return blindar_estoque_df(df_raw)
    except Exception: return pd.DataFrame()

def carregar_historico(prefixo_arquivo):
    try:
        df_h = carregar_do_google(f"{prefixo_arquivo}_historico_compras")
        if df_h.empty: return pd.DataFrame()
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
        if 'código_barras' not in df_h.columns: df_h['código_barras'] = ""
        
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
        if df_vi.empty: return pd.DataFrame()
        df_vi.columns = df_vi.columns.str.strip().str.lower()
        if 'data_hora' in df_vi.columns: df_vi['data_hora'] = pd.to_datetime(df_vi['data_hora'], errors='coerce')
        for c in ['qtd_vendida', 'preco_unit', 'valor_total']:
            if c in df_vi.columns: df_vi[c] = df_vi[c].apply(parse_num_br)
        if 'código_barras' in df_vi.columns:
            df_vi['código_barras'] = df_vi['código_barras'].astype(str).str.replace('.0','',regex=False).str.strip()
        if 'produto' in df_vi.columns:
            df_vi['produto'] = df_vi['produto'].astype(str).apply(normalizar_texto)
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

    # --- CIRURGIA: GRAVAR CÓDIGO NO HISTÓRICO ---
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
                        
                        cod_hist = item['ean']
                        if "[SISTEMA]" in esc:
                             raw_sel = esc.replace("[SISTEMA] ", "")
                             parts = raw_sel.split(' - ', 1)
                             if len(parts)>1: cod_hist = parts[0].strip()
                             nome_final = parts[1].strip() if len(parts)>1 else raw_sel.strip()
                        else:
                             nome_final = item['nome'].upper()

                        if esc == "(CRIAR NOVO)":
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
                            'código_barras': cod_hist,
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

    # --- CIRURGIA: PLANOGRAMA COM DTYPE E FUGA DA COLUNA QTD PADRAO ---
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("📂 Planograma", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                arquivo.seek(0)
                if arquivo.name.endswith('.csv'):
                    try: 
                        df_raw = pd.read_csv(arquivo, dtype=str)
                    except:
                        arquivo.seek(0)
                        df_raw = pd.read_csv(arquivo, sep=';', dtype=str)
                else:
                    df_raw = pd.read_excel(arquivo, dtype=str)
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
                
                # FOGE DA COLUNA QTD.PADRAO
                idx_qtd = -1
                for i, c in enumerate(cols_lower):
                    if 'estoque' in c: 
                        idx_qtd = i
                        break
                if idx_qtd == -1:
                    for i, c in enumerate(cols_lower):
                        if 'qtd' in c and 'padr' not in c:
                            idx_qtd = i
                            break
                if idx_qtd == -1: idx_qtd = len(cols)-1
                    
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
                    
                    df_raw['codigo_limpo'] = df_raw[col_barras].astype(str).str.replace('.0', '', regex=False).str.strip().str.lower()
                    
                    invalidos = ["", "nan", "none", "sem gtin", "0", "0000000000000", "nao informado"]
                    df_raw = df_raw[~df_raw['codigo_limpo'].isin(invalidos)]
                    
                    df_raw[col_qtd] = df_raw[col_qtd].apply(parse_num_br)
                    
                    agg_dict = { col_nome: 'first', col_qtd: 'sum' }
                    if col_preco != "(Ignorar)":
                        df_raw[col_preco] = df_raw[col_preco].apply(parse_num_br)
                        agg_dict[col_preco] = 'first'
                        
                    df_agrupado = df_raw.groupby('codigo_limpo', dropna=False, as_index=False).agg(agg_dict)
                    
                    st.markdown("### 👁️ Pré-visualização do que será atualizado:")
                    st.dataframe(df_agrupado.head(10), use_container_width=True, hide_index=True)
                    
                    total_linhas = len(df_agrupado)
                    bar = st.progress(0)
                    
                    for i, row in df_agrupado.iterrows():
                        try:
                            cod_limpo = row['codigo_limpo'].lstrip('0') 
                            nome = normalizar_texto(str(row[col_nome]))
                            qtd = row[col_qtd]
                            
                            if cod_limpo and nome:
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
                                    novos_prods.append({'código de barras': row['codigo_limpo'].upper(), 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': val_p, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0, 'status': 'Ativo'})
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    
                    if novos_prods: df = pd.concat([df, pd.DataFrame(novos_prods)], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    salvar_logs_em_lote(prefixo, logs_plano) 
                    st.success("Sincronizado perfeitamente!")
                    st.rerun()

    elif modo == "📈 Vendas (Importar & 80/20)":
        st.title(f"📈 Vendas - Importar & 80/20 ({loja_atual})")
        st.info("Para importar vendas, utilize as funções em desenvolvimento.")

    # --- CIRURGIA: RAIO X AGORA PUXA 30 DIAS ---
    elif modo == "🔎 Raio-X do Estoque (Auditoria)":
        st.title(f"🔎 Raio-X do Estoque - {loja_atual}")
        c1, c2 = st.columns(2)
        hoje = obter_hora_manaus().date()
        dt_ini = c1.date_input("📅 Data Inicial:", hoje - timedelta(days=30))
        dt_fim = c2.date_input("📅 Data Final:", hoje)
        
        busca_raiox = st.text_input("🔍 Buscar Produto:", placeholder="Digite o nome ou código...")
        
        if st.button("🚀 GERAR RAIO-X", type="primary"):
            with st.spinner("Analisando históricos..."):
                df_c = carregar_historico(prefixo)
                df_v = carregar_vendas_itens(prefixo)

                if not df_c.empty and 'data' in df_c.columns: df_c['data'] = pd.to_datetime(df_c['data'], errors='coerce')
                if not df_v.empty and 'data_hora' in df_v.columns: df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')

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

    # --- CIRURGIA HOSPITAL: MÁQUINA DO TEMPO INTELIGENTE E BOTÃO DE RESET ---
    elif modo == "🛠️ Ajuste & Limpeza":
        st.title("🛠️ Ajuste & Limpeza de Estoque")
        st.info("Ferramentas para corrigir erros e limpar o cadastro.")
        
        st.markdown("### 🚨 EMERGÊNCIA: APAGAR TUDO E RECOMEÇAR")
        st.write("Use este botão para jogar fora todos os produtos atuais e limpar as datas erradas. Depois importe o Planograma limpo.")
        if st.button("🧨 ZERAR BANCO DE DADOS AGORA", type="primary"):
            df.drop(df.index, inplace=True)
            salvar_estoque(df, prefixo)
            st.success("✅ Banco de dados apagado! Vá em 'Sincronizar Planograma' e importe tudo limpo.")
            time.sleep(3)
            st.rerun()

        st.divider()
        c_z1, c_z2 = st.columns(2)
        with c_z1:
            st.markdown("### 📉 Zerar Negativos")
            if st.button("ZERAR ESTOQUE NEGATIVO AGORA"):
                mask_neg = df['qtd.estoque'] < 0
                count_neg = mask_neg.sum()
                if count_neg > 0:
                    df.loc[mask_neg, 'qtd.estoque'] = 0
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ {count_neg} produtos zerados!")
                    st.rerun()
        
        st.divider()
        st.markdown("### 🪄 Máquina do Tempo com IA (Recalcular Casa)")
        st.write("Esta ferramenta vai ler o histórico das suas notas fiscais antigas, vai achar qual é o produto correto no seu sistema atual e injetar a quantidade na Casa. **Ele tem trava contra misturar produtos parecidos (ex: 2L com 200ML).**")
        
        if st.button("🚀 RECONSTRUIR CASA AGORA", type="primary"):
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
                            # Chama a Busca Aproximada
                            melhor_nome, _ = encontrar_melhor_match(nome_hist, lista_nomes_sis, cutoff=0.35)
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
        st.title("♻️ Restaurador Inteligente de Histórico")
        df_atual = carregar_historico(prefixo)
        st.write(f"📊 Linhas atualmente no sistema (Nuvem): **{len(df_atual)}**")
        arquivos_backup = st.file_uploader("📂 Arraste o arquivo aqui:", accept_multiple_files=True)
        if arquivos_backup and st.button("🚀 EXECUTAR RECUPERAÇÃO"):
            st.success("Arquivos lidos. (Simulação concluída)")
