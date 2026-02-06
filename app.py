import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
from io import BytesIO
import zipfile
import uuid

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
def formatar_moeda_br(valor):
    try: return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

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
        f"{prefixo}_vendas_mensais": ['mes_ref', 'data_hora', 'produto', 'codigo_barras', 'qtd_vendida', 'valor_total', 'origem_arquivo', 'id_importacao'],
        f"{prefixo}_lista_compras": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo']
    }
    for aba, colunas in arquivos.items():
        df = carregar_do_google(aba)
        if df.empty: salvar_no_google(pd.DataFrame(columns=colunas), aba)

def carregar_dados(prefixo_arquivo):
    try:
        df = carregar_do_google(f"{prefixo_arquivo}_estoque")
        if df.empty: return pd.DataFrame()
        df.columns = df.columns.str.strip().str.lower()
        if 'preco_sem_desconto' not in df.columns: df['preco_sem_desconto'] = 0.0
        if 'status' not in df.columns: df['status'] = 'Ativo'
        
        cols_num = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']
        for col in cols_num:
            if col in df.columns: 
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['ultimo_fornecedor'] = df['ultimo_fornecedor'].fillna('')
        df['código de barras'] = df['código de barras'].apply(lambda x: str(x).replace('.0', '').strip() if pd.notnull(x) else "")
        df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
        df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')
        return df
    except: return pd.DataFrame()

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


# --- 🔢 Conversão robusta PT-BR/EN para número (evita 3,19 -> 319) ---
def to_float_safe(valor):
    """Converte valores numéricos vindos de Excel/CSV (pt-BR ou en-US) para float.
    Aceita: 3.19 | 3,19 | 1.234,56 | 1,234.56 | 1234
    """
    try:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return 0.0
        if isinstance(valor, (int, float)):
            return float(valor)

        s = str(valor).strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return 0.0

        # Remove espaços e símbolos comuns
        s = s.replace("R$", "").replace(" ", "")

        # Se tem ',' e '.', decide qual é separador decimal pelo último que aparece
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                # 1.234,56 -> remove '.' milhar, troca ',' por '.'
                s = s.replace(".", "").replace(",", ".")
            else:
                # 1,234.56 -> remove ',' milhar
                s = s.replace(",", "")
        else:
            # Só vírgula: 3,19 -> 3.19
            if "," in s and "." not in s:
                s = s.replace(",", ".")

        return float(s)
    except Exception:
        return 0.0

def carregar_vendas_mensais(prefixo_arquivo):
    """Carrega vendas importadas (mensais) para análises 80/20."""
    try:
        df_vm = carregar_do_google(f"{prefixo_arquivo}_vendas_mensais")
        if df_vm.empty:
            return pd.DataFrame()

        df_vm.columns = df_vm.columns.str.strip().str.lower()

        if 'data_hora' in df_vm.columns:
            df_vm['data_hora'] = pd.to_datetime(df_vm['data_hora'], errors='coerce')

        for col in ['qtd_vendida', 'valor_total']:
            if col in df_vm.columns:
                df_vm[col] = df_vm[col].apply(to_float_safe)

        # Normalizações úteis
        if 'produto' in df_vm.columns:
            df_vm['produto'] = df_vm['produto'].astype(str).apply(lambda x: normalizar_texto(x))

        if 'codigo_barras' in df_vm.columns:
            df_vm['codigo_barras'] = df_vm['codigo_barras'].astype(str).str.replace('.0', '', regex=False).str.strip()

        return df_vm
    except Exception:
        return pd.DataFrame()

def salvar_vendas_mensais(df_vm, prefixo_arquivo):
    salvar_no_google(df_vm, f"{prefixo_arquivo}_vendas_mensais", permitir_vazio=True)


# --- 🧠 LISTA DE REVISÃO DE MIX (PRODUTOS SEM VENDA) ---
def carregar_revisao_mix(prefixo_arquivo):
    """Carrega a lista de revisão de mix (produtos candidatos a retirada)."""
    try:
        df_r = carregar_do_google(f"{prefixo_arquivo}_revisao_mix")
        if df_r.empty:
            return pd.DataFrame(columns=[
                'codigo_barras','produto','data_ultima_venda','dias_sem_venda','meses_sem_venda',
                'status_decisao','observacao','data_inclusao'
            ])
        df_r.columns = df_r.columns.str.strip().str.lower()
        # Normaliza nomes esperados
        ren = {
            'código_barras':'codigo_barras',
            'código de barras':'codigo_barras',
            'nome do produto':'produto'
        }
        df_r = df_r.rename(columns=ren)
        for c in ['codigo_barras','produto','status_decisao','observacao','data_inclusao']:
            if c not in df_r.columns: df_r[c] = ''
        for c in ['dias_sem_venda','meses_sem_venda']:
            if c not in df_r.columns:
                df_r[c] = 0
            df_r[c] = pd.to_numeric(df_r[c], errors='coerce').fillna(0).astype(int)
        if 'data_ultima_venda' not in df_r.columns: df_r['data_ultima_venda'] = ''
        return df_r[['codigo_barras','produto','data_ultima_venda','dias_sem_venda','meses_sem_venda','status_decisao','observacao','data_inclusao']]
    except:
        return pd.DataFrame(columns=[
            'codigo_barras','produto','data_ultima_venda','dias_sem_venda','meses_sem_venda',
            'status_decisao','observacao','data_inclusao'
        ])

def salvar_revisao_mix(df_r, prefixo_arquivo):
    salvar_no_google(df_r, f"{prefixo_arquivo}_revisao_mix", permitir_vazio=True)
def carregar_lista_compras(prefixo_arquivo):
    try:
        df = carregar_do_google(f"{prefixo_arquivo}_lista_compras")
        if df.empty: return pd.DataFrame()
        if 'código_barras' not in df.columns: df['código_barras'] = ""
        if 'qtd_sugerida' in df.columns: df['qtd_sugerida'] = pd.to_numeric(df['qtd_sugerida'], errors='coerce')
        return df
    except: return pd.DataFrame()

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
def salvar_estoque(df, prefixo): salvar_no_google(df, f"{prefixo}_estoque")
def salvar_historico(df, prefixo): salvar_no_google(df, f"{prefixo}_historico_compras")
def salvar_movimentacoes(df, prefixo): salvar_no_google(df, f"{prefixo}_movimentacoes")
def salvar_vendas(df, prefixo): salvar_no_google(df, f"{prefixo}_vendas")
def salvar_lista_compras(df, prefixo): salvar_no_google(df, f"{prefixo}_lista_compras", permitir_vazio=True)

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
        "⚖️ Conciliação (Shoppbud vs App)",
        "🚚 Transferência em Massa (Picklist)",
        "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", 
        "📥 Importar XML (Associação Inteligente)", 
        "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)",
        "📉 Baixar Vendas (Do Relatório)",
        "📈 Análise de Vendas (80/20)",
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
            valor_estoque = (df_ativos['qtd.estoque'] * df_ativos['preco_custo']).sum() + (df_ativos['qtd_central'] * df_ativos['preco_custo']).sum()
            
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
                df_venc_show_editor = df_venc_show[['nome do produto', 'validade', 'qtd.estoque']].copy()
                df_venc_show_editor = df_venc_show_editor.reset_index().rename(columns={'index': '__idx_df__'})

                df_venc_edit = st.data_editor(
                    df_venc_show_editor,
                    use_container_width=True,
                    num_rows="dynamic",
                    key="editor_vencimento_avancado",
                    column_config={
                        "__idx_df__": st.column_config.NumberColumn("ID", disabled=True),
                        "nome do produto": st.column_config.TextColumn("Produto", disabled=True),
                    }
                )

                if st.button("💾 SALVAR CORREÇÕES DE VENCIMENTO"):
                    orig_ids = set(df_venc_show_editor['__idx_df__'].tolist())
                    edit_ids = set(df_venc_edit['__idx_df__'].tolist())
                    removidos = orig_ids - edit_ids

                    # Aplicar edições
                    for _, row in df_venc_edit.iterrows():
                        idx_df = int(row['__idx_df__'])
                        if idx_df in df.index:
                            df.at[idx_df, 'validade'] = row['validade']
                            df.at[idx_df, 'qtd.estoque'] = row['qtd.estoque']

                    # Linhas apagadas = remover validade (remove alerta)
                    for idx_df in removidos:
                        if int(idx_df) in df.index:
                            df.at[int(idx_df), 'validade'] = pd.NaT

                    salvar_estoque(df, prefixo)
                    st.success("Vencimentos atualizados na Nuvem!")
                    st.rerun()
            else: st.success("Nenhum produto vencendo nos próximos 5 dias.")

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
                    df_plan['código normalizado'] = df_plan[col_cod_plan].astype(str).str.replace('.0', '').str.strip()
                    df['código normalizado'] = df['código de barras'].astype(str).str.replace('.0', '').str.strip()
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
                                st.success(f"✅ {itens_corrigidos} itens corrigidos!")
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
                        mask_ean = df['código de barras'].astype(str) == item['ean']
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
                            novo = {'código de barras': item['ean'], 'nome do produto': nome_final, 'qtd.estoque': item['qtd'] if "Atualizar" in modo_import else 0, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': item['preco_un_liquido'], 'preco_venda': item['preco_un_liquido']*2, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'], 'preco_sem_desconto': item['preco_un_bruto'], 'status': 'Ativo'}
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                            if "Atualizar" in modo_import: logs_xml.append({'data_hora': str(data_lancamento_final), 'produto': nome_final, 'qtd_antes': 0, 'qtd_nova': item['qtd'], 'acao': "XML Novo", 'motivo': "Entrada"})
                        else:
                            mask = df['nome do produto'].astype(str) == nome_final
                            if mask.any():
                                idx = df[mask].index[0]
                                if "Atualizar" in modo_import:
                                    df.at[idx, 'qtd_central'] += item['qtd']
                                    logs_xml.append({'data_hora': str(data_lancamento_final), 'produto': nome_final, 'qtd_antes': df.at[idx, 'qtd_central']-item['qtd'], 'qtd_nova': df.at[idx, 'qtd_central'], 'acao': "XML Entrada", 'motivo': "Entrada"})
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

    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        arquivo = st.file_uploader("📂 Planograma", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                df_raw = pd.read_csv(arquivo, header=None) if arquivo.name.endswith('.csv') else pd.read_excel(arquivo, header=None)
                cols = df_raw.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("CÓDIGO BARRAS", cols, index=0)
                idx_nome = c2.selectbox("NOME", cols, index=1)
                idx_qtd = c3.selectbox("QUANTIDADE", cols, index=len(cols)-1)
                idx_preco = c4.selectbox("PREÇO VENDA", ["(Ignorar)"] + cols)
                
                if st.button("🚀 SINCRONIZAR TUDO"):
                    df = carregar_dados(prefixo)
                    novos_prods = []
                    logs_plano = [] 
                    total_linhas = len(df_raw)
                    bar = st.progress(0)
                    
                    for i in range(1, total_linhas):
                        try:
                            cod = str(df_raw.iloc[i, idx_barras]).replace('.0', '').strip()
                            nome = normalizar_texto(str(df_raw.iloc[i, idx_nome]))
                            qtd = pd.to_numeric(df_raw.iloc[i, idx_qtd], errors='coerce')
                            if cod and nome and pd.notnull(qtd):
                                mask = df['código de barras'] == cod
                                if mask.any():
                                    idx = df[mask].index[0]
                                    antigo = df.at[idx, 'qtd.estoque']
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    if antigo != qtd: logs_plano.append({'data_hora': str(obter_hora_manaus()), 'produto': nome, 'qtd_antes': antigo, 'qtd_nova': qtd, 'acao': "Sincronização", 'motivo': "Planograma"})
                                    if idx_preco != "(Ignorar)":
                                        val = pd.to_numeric(df_raw.iloc[i, idx_preco], errors='coerce')
                                        if pd.notnull(val): df.loc[mask, 'preco_venda'] = val
                                else:
                                    val_p = 0.0
                                    if idx_preco != "(Ignorar)": val_p = pd.to_numeric(df_raw.iloc[i, idx_preco], errors='coerce') or 0.0
                                    novos_prods.append({'código de barras': cod, 'nome do produto': nome, 'qtd.estoque': qtd, 'qtd_central': 0, 'qtd_minima': 5, 'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0, 'preco_custo': 0.0, 'preco_venda': val_p, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0, 'status': 'Ativo'})
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    
                    if novos_prods: df = pd.concat([df, pd.DataFrame(novos_prods)], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    salvar_logs_em_lote(prefixo, logs_plano) 
                    st.success("Sincronizado!")
                    st.rerun()
            except Exception as e: st.error(f"Erro: {e}")

    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Baixar Vendas")
        df_vendas = carregar_vendas(prefixo)
        
        tab_imp, tab_hist = st.tabs(["📂 Importar", "📜 Histórico"])
        with tab_imp:
            arquivo_vendas = st.file_uploader("Relatório", type=['xlsx', 'xls'])
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None)
                    st.dataframe(df_bruto.head(3))
                    linha_titulo = st.number_input("Linha Títulos:", 0)
                    arquivo_vendas.seek(0)
                    df_temp = pd.read_excel(arquivo_vendas, header=linha_titulo)
                    cols = df_temp.columns.tolist()
                    c1, c2, c3 = st.columns(3)
                    col_nome = c1.selectbox("NOME", cols)
                    col_qtd = c2.selectbox("QUANTIDADE", cols)
                    col_data = c3.selectbox("DATA", cols)
                    if st.button("PROCESSAR"):
                        novos_reg = []; novos_ids = set()
                        total = len(df_temp)
                        bar = st.progress(0)
                        for i, row in df_temp.iterrows():
                            nome = str(row[col_nome]).strip()
                            qtd = pd.to_numeric(row[col_qtd], errors='coerce')
                            if pd.notnull(qtd) and qtd > 0:
                                mask = df['nome do produto'].astype(str).str.contains(nome, case=False, na=False)
                                if mask.any():
                                    idx = df[mask].index[0]
                                    df.at[idx, 'qtd.estoque'] -= qtd
                                    novos_reg.append({"data_hora": str(obter_hora_manaus()), "produto": df.at[idx, 'nome do produto'], "qtd_vendida": qtd, "estoque_restante": df.at[idx, 'qtd.estoque']})
                            bar.progress((i+1)/total)
                        salvar_estoque(df, prefixo)
                        if novos_reg: salvar_vendas(pd.concat([df_vendas, pd.DataFrame(novos_reg)], ignore_index=True), prefixo)
                        st.success("Vendas baixadas!")
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist:
            if not df_vendas.empty: st.dataframe(df_vendas)
            else: st.info("Vazio.")
    
    elif modo == "📈 Análise de Vendas (80/20)":
        st.title(f"📈 Análise de Vendas (80/20) - {loja_atual}")
        st.markdown("Importe o Excel de vendas em **'📥 Importar Vendas (Excel)'** e depois analise aqui.")

        df_vm = carregar_vendas_mensais(prefixo)

        if df_vm.empty:
            st.info("Nenhuma venda importada ainda para análise. Vá em **📥 Importar Vendas (Excel)**.")
        else:
            # -----------------------------
            # Seleção de período
            # -----------------------------
            hoje = obter_hora_manaus()
            c_p1, c_p2, c_p3 = st.columns([1, 1, 2])

            with c_p1:
                dt_ini = st.date_input("Data inicial:", value=(hoje - timedelta(days=30)).date(), key="v80_dt_ini")
            with c_p2:
                dt_fim = st.date_input("Data final:", value=hoje.date(), key="v80_dt_fim")
            with c_p3:
                somente_ativos = st.checkbox("Considerar apenas produtos ATIVOS do cadastro", value=True, key="v80_somente_ativos")

            dt_ini_full = datetime.combine(dt_ini, datetime.min.time())
            dt_fim_full = datetime.combine(dt_fim, datetime.max.time())

            df_periodo = df_vm[(df_vm['data_hora'] >= dt_ini_full) & (df_vm['data_hora'] <= dt_fim_full)].copy()

            # -----------------------------
            # Base de cadastro (planograma refletido no estoque)
            # -----------------------------
            df_cad = df.copy() if df is not None else pd.DataFrame()
            if not df_cad.empty:
                df_cad = df_cad[['código de barras', 'nome do produto', 'status']].copy()
                if somente_ativos and 'status' in df_cad.columns:
                    df_cad = df_cad[df_cad['status'] == 'Ativo']
                df_cad['nome do produto'] = df_cad['nome do produto'].astype(str)
                df_cad['código de barras'] = df_cad['código de barras'].astype(str).str.replace('.0','',regex=False).str.strip()
            else:
                df_cad = pd.DataFrame(columns=['código de barras','nome do produto','status'])

            # -----------------------------
            # Métricas do período
            # -----------------------------
            if df_periodo.empty:
                st.warning("Não há vendas no período selecionado.")
            else:
                # Agrega por produto
                df_grp = df_periodo.groupby('produto', as_index=False)['qtd_vendida'].sum()
                df_grp = df_grp.sort_values('qtd_vendida', ascending=False)

                total = df_grp['qtd_vendida'].sum()
                df_grp['%'] = (df_grp['qtd_vendida'] / total) * 100
                df_grp['%_acum'] = df_grp['%'].cumsum()

                # Curva ABC via % acumulada
                def classificar_abc(p):
                    if p <= 80: return 'A'
                    if p <= 95: return 'B'
                    return 'C'

                df_grp['classe_abc'] = df_grp['%_acum'].apply(classificar_abc)

                st.subheader("📌 Top Produtos (Período Selecionado)")
                st.dataframe(df_grp.head(30), use_container_width=True, hide_index=True)

                st.subheader("📊 Curva 80/20")
                fig = px.line(df_grp, x=df_grp.index, y="%_acum", title="Curva 80/20 (% acumulado de vendas)", markers=True)
                st.plotly_chart(fig, use_container_width=True)

                # Resumo por classe
                st.subheader("🏷️ Resumo ABC")
                resumo = df_grp.groupby('classe_abc', as_index=False)['qtd_vendida'].sum()
                resumo['%'] = (resumo['qtd_vendida'] / total) * 100
                st.dataframe(resumo, use_container_width=True, hide_index=True)

            # -----------------------------
            # Produtos SEM venda no período + Tempo sem vender
            # -----------------------------
            st.divider()
            st.subheader("🧊 Produtos com baixa/nenhuma venda")

            # Última venda (considerando TODO histórico importado, não só o período)
            df_last = df_vm.groupby('produto', as_index=False)['data_hora'].max().rename(columns={'data_hora':'ultima_venda'})

            # Monta tabela base com todos do cadastro
            df_base = df_cad.rename(columns={'nome do produto':'produto', 'código de barras':'codigo_barras'}).copy()
            if df_base.empty:
                st.info("Seu cadastro está vazio. Sincronize o planograma / cadastre produtos para comparar.")
            else:
                # Vendas no período (pode estar vazio)
                if df_periodo.empty:
                    df_vper = pd.DataFrame(columns=['produto','vendido_periodo'])
                else:
                    df_vper = df_periodo.groupby('produto', as_index=False)['qtd_vendida'].sum().rename(columns={'qtd_vendida':'vendido_periodo'})

                df_mix = df_base.merge(df_vper, on='produto', how='left').merge(df_last, on='produto', how='left')
                df_mix['vendido_periodo'] = pd.to_numeric(df_mix['vendido_periodo'], errors='coerce').fillna(0)

                # Dias sem vender
                df_mix['dias_sem_venda'] = df_mix['ultima_venda'].apply(
                    lambda d: int((hoje - d).days) if pd.notnull(d) else 99999
                )
                df_mix['meses_sem_venda'] = (df_mix['dias_sem_venda'] / 30).astype(int)

                # Sem venda no período
                df_sem_periodo = df_mix[df_mix['vendido_periodo'] <= 0].sort_values(['dias_sem_venda','produto'], ascending=[False, True])

                c1, c2, c3 = st.columns(3)
                c1.metric("Produtos no cadastro (filtro)", len(df_mix))
                c2.metric("Sem venda no período", len(df_sem_periodo))
                if not df_periodo.empty:
                    c3.metric("Produtos vendidos no período", df_mix[df_mix['vendido_periodo'] > 0].shape[0])
                else:
                    c3.metric("Produtos vendidos no período", 0)

                with st.expander("📋 Ver produtos sem venda no período"):
                    st.dataframe(
                        df_sem_periodo[['codigo_barras','produto','dias_sem_venda','meses_sem_venda','ultima_venda']].head(500),
                        use_container_width=True,
                        hide_index=True
                    )

                # Thresholds para revisão
                st.markdown("### 🗑️ Candidatos à revisão de mix (sem vender há X tempo)")
                c_t1, c_t2, c_t3 = st.columns([1,1,2])
                with c_t1:
                    meses_limite = st.number_input("Meses sem vender (limite):", min_value=1, value=2, step=1, key="rev_meses_limite")
                with c_t2:
                    dias_limite = st.number_input("Dias sem vender (limite):", min_value=7, value=60, step=1, key="rev_dias_limite")
                with c_t3:
                    regra = st.radio("Critério:", ["Meses", "Dias"], horizontal=True, key="rev_regra")

                if regra == "Meses":
                    df_candidatos = df_mix[df_mix['meses_sem_venda'] >= int(meses_limite)].copy()
                else:
                    df_candidatos = df_mix[df_mix['dias_sem_venda'] >= int(dias_limite)].copy()

                df_candidatos = df_candidatos.sort_values(['dias_sem_venda','produto'], ascending=[False, True])

                st.caption("Produtos com **99999 dias** são itens que nunca apareceram no relatório de vendas importado.")
                st.dataframe(
                    df_candidatos[['codigo_barras','produto','dias_sem_venda','meses_sem_venda','ultima_venda']].head(300),
                    use_container_width=True,
                    hide_index=True
                )

                # Atualiza lista persistente para decisão
                df_rev = carregar_revisao_mix(prefixo)

                col_btn1, col_btn2 = st.columns([1, 2])
                with col_btn1:
                    if st.button("➕ Atualizar Lista de Revisão com esses candidatos"):
                        if df_candidatos.empty:
                            st.warning("Nenhum candidato encontrado com o critério atual.")
                        else:
                            # Mantém decisões já tomadas
                            df_rev_norm = df_rev.copy()
                            df_rev_norm['codigo_barras'] = df_rev_norm['codigo_barras'].astype(str)
                            df_rev_norm['produto'] = df_rev_norm['produto'].astype(str)

                            existentes = set((df_rev_norm['codigo_barras'].astype(str) + "||" + df_rev_norm['produto'].astype(str)).tolist())

                            novos = []
                            for _, r in df_candidatos.iterrows():
                                key = f"{str(r['codigo_barras'])}||{str(r['produto'])}"
                                if key in existentes:
                                    continue
                                ult = r['ultima_venda']
                                ult_str = ult.strftime("%d/%m/%Y %H:%M") if pd.notnull(ult) else ""
                                novos.append({
                                    'codigo_barras': str(r['codigo_barras']),
                                    'produto': str(r['produto']),
                                    'data_ultima_venda': ult_str,
                                    'dias_sem_venda': int(r['dias_sem_venda']),
                                    'meses_sem_venda': int(r['meses_sem_venda']),
                                    'status_decisao': 'Pendente',
                                    'observacao': '',
                                    'data_inclusao': hoje.strftime("%d/%m/%Y %H:%M")
                                })

                            if novos:
                                df_rev_final = pd.concat([df_rev_norm, pd.DataFrame(novos)], ignore_index=True)
                                salvar_revisao_mix(df_rev_final, prefixo)
                                st.success(f"✅ {len(novos)} itens adicionados na Lista de Revisão!")
                                st.rerun()
                            else:
                                st.info("Nada novo para adicionar (todos já estão na Lista de Revisão).")

                with col_btn2:
                    st.markdown("**Dica:** após 2 meses sem vender, você pode marcar como **Retirar**, **Manter**, ou **Promoção** e anotar observações.")

                st.divider()
                st.subheader("🗂️ Lista de Revisão (Editar / Decidir)")
                df_rev = carregar_revisao_mix(prefixo)

                if df_rev.empty:
                    st.info("Lista de revisão vazia.")
                else:
                    df_rev_show = df_rev.copy()
                    busca_rev = st.text_input("🔍 Buscar na lista de revisão:", placeholder="nome ou código...", key="busca_rev")
                    if busca_rev:
                        mask = df_rev_show['produto'].astype(str).apply(lambda x: normalizar_para_busca(busca_rev) in normalizar_para_busca(x)) | df_rev_show['codigo_barras'].astype(str).str.contains(str(busca_rev).strip(), na=False)
                        df_rev_show = df_rev_show[mask]

                    df_rev_edit = st.data_editor(
                        df_rev_show,
                        use_container_width=True,
                        num_rows="dynamic",
                        hide_index=True,
                        key="editor_rev_mix",
                        column_config={
                            "status_decisao": st.column_config.SelectboxColumn("Decisão", options=["Pendente","Manter","Retirar","Promoção","Revisar Depois"]),
                            "observacao": st.column_config.TextColumn("Observação"),
                            "dias_sem_venda": st.column_config.NumberColumn("Dias", disabled=True),
                            "meses_sem_venda": st.column_config.NumberColumn("Meses", disabled=True),
                        }
                    )

                    if st.button("💾 Salvar Lista de Revisão"):
                        # Se estiver filtrado, atualiza só os registros visíveis
                        if not busca_rev:
                            df_rev_final = df_rev_edit.copy()
                        else:
                            df_rev_final = df_rev.copy()
                            # Atualiza linhas existentes por chave
                            for _, row in df_rev_edit.iterrows():
                                mask = (df_rev_final['codigo_barras'].astype(str) == str(row['codigo_barras'])) & (df_rev_final['produto'].astype(str) == str(row['produto']))
                                if mask.any():
                                    for c in df_rev_final.columns:
                                        if c in df_rev_edit.columns:
                                            df_rev_final.loc[mask, c] = row.get(c)
                            # Remoções (linhas apagadas no filtro)
                            keys_orig = set((df_rev_show['codigo_barras'].astype(str)+"||"+df_rev_show['produto'].astype(str)).tolist())
                            keys_edit = set((df_rev_edit['codigo_barras'].astype(str)+"||"+df_rev_edit['produto'].astype(str)).tolist())
                            removidos = keys_orig - keys_edit
                            if removidos:
                                mask_keep = ~(df_rev_final['codigo_barras'].astype(str)+"||"+df_rev_final['produto'].astype(str)).isin(removidos)
                                df_rev_final = df_rev_final[mask_keep]

                        salvar_revisao_mix(df_rev_final, prefixo)
                        st.success("✅ Lista de revisão salva!")
                        st.rerun()

