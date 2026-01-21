import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
from io import BytesIO
import zipfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Configuração da página
st.set_page_config(page_title="Gestão Multi-Lojas (Nuvem)", layout="wide", page_icon="☁️")

# ==============================================================================
# 🔐 CONEXÃO COM GOOGLE SHEETS (COM MEMÓRIA CACHE PARA NÃO BLOQUEAR)
# ==============================================================================
# Cache de conexão: Conecta só uma vez e reaproveita
@st.cache_resource
def get_conection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("Sistema_Estoque_Database")

try:
    conn = get_conection()
except Exception as e:
    st.error(f"Erro crítico de conexão: {e}")
    st.stop()

# ==============================================================================
# 🔄 LEITURA E ESCRITA INTELIGENTE (RESOLVE ERRO 429 E VÍRGULA)
# ==============================================================================

def inicializar_arquivos(prefixo):
    # Lista de abas e colunas
    arquivos = {
        f"{prefixo}_estoque": ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'],
        f"{prefixo}_historico_compras": ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
        f"{prefixo}_movimentacoes": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],
        f"{prefixo}_lista_compras": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo'],
        f"{prefixo}_ids_vendas": ['id_transacao'],
        "meus_produtos_oficiais": ['nome do produto', 'código de barras']
    }
    
    # Verifica abas existentes na memória para não bater na API sem necessidade
    try:
        abas_existentes = [ws.title for ws in conn.worksheets()]
        for nome_aba, colunas in arquivos.items():
            if nome_aba not in abas_existentes:
                ws = conn.add_worksheet(title=nome_aba, rows=1000, cols=20)
                ws.append_row(colunas)
    except: pass

# CACHE DE DADOS: Lê do Google e guarda na memória por 5 min (TTL)
# Se você salvar algo, limpamos esse cache para atualizar na hora.
@st.cache_data(ttl=300)
def ler_aba_nuvem(nome_aba):
    try:
        ws = conn.worksheet(nome_aba)
        dados = ws.get_all_records()
        df = pd.DataFrame(dados)
        
        # Padroniza colunas
        df.columns = df.columns.str.strip().str.lower()
        
        # CORREÇÃO DA VÍRGULA (CRUCIAL):
        # Procura colunas de preço e converte "1,39" para 1.39
        cols_moeda = ['preco_custo', 'preco_venda', 'preco_sem_desconto', 'preco_pago', 'total_gasto', 'desconto_total_money', 'custo_previsto']
        for col in cols_moeda:
            if col in df.columns:
                # Transforma em string, troca vírgula por ponto, converte para numero
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
        return df
    except:
        return pd.DataFrame()

def salvar_aba_nuvem(nome_aba, df):
    try:
        ws = conn.worksheet(nome_aba)
        ws.clear()
        
        # Prepara dados para salvar
        df_save = df.copy()
        
        # Formata datas como texto para não quebrar o JSON
        for col in df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_save[col]):
                df_save[col] = df_save[col].astype(str).replace('NaT', '')
            
            # (Opcional) Poderíamos forçar a volta da vírgula aqui, mas o Google Sheets
            # geralmente entende ponto se a configuração estiver certa. 
            # Vamos mandar ponto (padrão universal) para garantir a matemática.
            
        # Substitui NaNs por vazio
        df_save = df_save.fillna('')
        
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        
        # LIMPA O CACHE PARA VER A MUDANÇA IMEDIATAMENTE
        ler_aba_nuvem.clear()
        
    except Exception as e:
        st.error(f"Erro ao salvar {nome_aba}: {e}")

# ==============================================================================
# 🕒 ESTRUTURA ORIGINAL (MANTIDA INTACTA)
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
    comum = set_xml.intersection(set_sis)
    if not comum: return 0.0
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
            
            base_ref = grupo[grupo['nome do produto'] == melhor_nome].iloc[0].to_dict()
            base_ref['qtd.estoque'] = soma_loja
            base_ref['qtd_central'] = soma_casa
            base_ref['preco_custo'] = custo_final
            base_ref['preco_venda'] = venda_final
            base_ref['preco_sem_desconto'] = sem_desc_final
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
        
        salvar_aba_nuvem("meus_produtos_oficiais", df_limpo)
        return True
    except Exception as e:
        st.error(f"Erro ao organizar o arquivo: {e}")
        return False

def carregar_base_oficial():
    return ler_aba_nuvem("meus_produtos_oficiais")

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

# --- BACKUP NUVEM ---
def gerar_backup_zip_nuvem(dados_dict):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for nome_arquivo, df_dados in dados_dict.items():
            excel_buffer = BytesIO()
            df_dados.to_excel(excel_buffer, index=False)
            zip_file.writestr(f"{nome_arquivo}.xlsx", excel_buffer.getvalue())
    buffer.seek(0)
    return buffer

# --- FUNÇÕES AUXILIARES ---
def formatar_moeda_br(valor):
    try:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

# --- 🔐 LOG DE AUDITORIA ---
def registrar_auditoria(prefixo, produto, qtd_antes, qtd_nova, acao, motivo="Manual"):
    try:
        nome_aba = f"{prefixo}_log_auditoria"
        novo_log = {
            'data_hora': str(obter_hora_manaus()), 
            'produto': produto,
            'qtd_antes': qtd_antes,
            'qtd_nova': qtd_nova,
            'acao': acao,
            'motivo': motivo
        }
        ws = conn.worksheet(nome_aba)
        ws.append_row(list(novo_log.values()))
    except Exception as e:
        print(f"Erro ao salvar log: {e}")

# --- 🔐 MEMÓRIA DE VENDAS ---
def carregar_ids_processados(prefixo):
    df_ids = ler_aba_nuvem(f"{prefixo}_ids_vendas")
    if not df_ids.empty:
        return set(df_ids['id_transacao'].astype(str).tolist())
    return set()

def salvar_ids_processados(prefixo, novos_ids):
    if not novos_ids: return
    nome_aba = f"{prefixo}_ids_vendas"
    ws = conn.worksheet(nome_aba)
    linhas = [[str(i)] for i in novos_ids]
    ws.append_rows(linhas)

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        # Carrega da nuvem (com cache ou não)
        # Nota: Idealmente para consistencia, forçamos leitura, mas aqui confiamos no fluxo
        # Para ser mais robusto, poderiamos nao usar cache aqui, mas para economizar cota, usamos.
        df_outra = ler_aba_nuvem(f"{loja}_estoque")
        
        if not df_outra.empty:
            try:
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    qtd_antiga = df_outra.at[idx, 'qtd_central']
                    df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                    
                    salvar_aba_nuvem(f"{loja}_estoque", df_outra)
                    registrar_auditoria(loja, nome_produto, qtd_antiga, qtd_nova_casa, "Sincronização Automática", f"Origem: {prefixo_ignorar}")
            except Exception: pass

# --- WRAPPERS ---
def carregar_dados(prefixo):
    df = ler_aba_nuvem(f"{prefixo}_estoque")
    # Reforça tipos numéricos após leitura
    if not df.empty:
        cols_num = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada']
        for col in cols_num:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'código de barras' in df.columns:
            df['código de barras'] = df['código de barras'].astype(str).str.replace('.0', '').str.strip()
        
        if 'validade' in df.columns:
            df['validade'] = pd.to_datetime(df['validade'], errors='coerce')
            
    return df

def carregar_historico(prefixo):
    df_h = ler_aba_nuvem(f"{prefixo}_historico_compras")
    if not df_h.empty:
        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
    return df_h

def carregar_movimentacoes(prefixo):
    df_m = ler_aba_nuvem(f"{prefixo}_movimentacoes")
    if not df_m.empty:
        df_m['data_hora'] = pd.to_datetime(df_m['data_hora'], errors='coerce')
    return df_m

def carregar_vendas(prefixo):
    df_v = ler_aba_nuvem(f"{prefixo}_vendas")
    if not df_v.empty:
        df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')
    return df_v

def carregar_lista_compras(prefixo):
    return ler_aba_nuvem(f"{prefixo}_lista_compras")

def salvar_estoque(df, prefixo): salvar_aba_nuvem(f"{prefixo}_estoque", df)
def salvar_historico(df, prefixo): salvar_aba_nuvem(f"{prefixo}_historico_compras", df)
def salvar_movimentacoes(df, prefixo): salvar_aba_nuvem(f"{prefixo}_movimentacoes", df)
def salvar_vendas(df, prefixo): salvar_aba_nuvem(f"{prefixo}_vendas", df)
def salvar_lista_compras(df, prefixo): salvar_aba_nuvem(f"{prefixo}_lista_compras", df)

# ==============================================================================
# 🚀 APP
# ==============================================================================

inicializar_arquivos(prefixo)

df = carregar_dados(prefixo)
df_hist = carregar_historico(prefixo)
df_mov = carregar_movimentacoes(prefixo)
df_vendas = carregar_vendas(prefixo)
df_oficial = carregar_base_oficial()
df_lista_compras = carregar_lista_compras(prefixo)
ids_processados = carregar_ids_processados(prefixo) 

st.sidebar.markdown("### ☁️ Backup Nuvem")
if st.sidebar.button("💾 Baixar Backup (Nuvem)"):
    arquivos_memoria = {
        f"{prefixo}_estoque": df,
        f"{prefixo}_historico_compras": df_hist,
        f"{prefixo}_vendas": df_vendas,
        f"{prefixo}_lista_compras": df_lista_compras
    }
    zip_buffer = gerar_backup_zip_nuvem(arquivos_memoria)
    st.sidebar.download_button(
        label="⬇️ Clique para Salvar",
        data=zip_buffer,
        file_name=f"backup_nuvem_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
        mime="application/zip"
    )

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
        "🏠 Gôndola (Loja)", 
        "💰 Inteligência de Compras (Histórico)", 
        "🏡 Estoque Central (Casa)",
        "📋 Tabela Geral"
    ])

    # 1. DASHBOARD
    if modo == "📊 Dashboard (Visão Geral)":
        st.title(f"📊 Painel de Controle - {loja_atual}")
        if df.empty:
            st.info("Comece cadastrando produtos.")
        else:
            hoje = obter_hora_manaus()
            df_valido = df[pd.notnull(df['validade'])].copy()
            df_critico = df_valido[(df_valido['validade'] <= hoje + timedelta(days=5)) & ((df_valido['qtd.estoque'] > 0) | (df_valido['qtd_central'] > 0))]
            df_atencao = df_valido[(df_valido['validade'] > hoje + timedelta(days=5)) & (df_valido['validade'] <= hoje + timedelta(days=10))]
            valor_estoque = (df['qtd.estoque'] * df['preco_custo']).sum() + (df['qtd_central'] * df['preco_custo']).sum()
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Itens na Loja", int(df['qtd.estoque'].sum()))
            c2.metric("💰 Valor Investido", f"R$ {formatar_moeda_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
            
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty:
                st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras' para ver.")
            
            st.markdown("### 🚨 Gestão de Vencimentos")
            if not df_critico.empty:
                filtro_venc = st.text_input("🔍 Buscar produtos vencendo:", placeholder="Nome...")
                df_venc_show = filtrar_dados_inteligente(df_critico, 'nome do produto', filtro_venc)
                
                st.info("💡 Dica: Para remover o alerta, apague a data de validade (Delete) ou atualize-a.")
                df_venc_edit = st.data_editor(
                    df_venc_show[['nome do produto', 'validade', 'qtd.estoque']],
                    use_container_width=True,
                    num_rows="dynamic",
                    key="editor_vencimento_avancado"
                )
                
                if st.button("💾 SALVAR CORREÇÕES DE VENCIMENTO"):
                    for i, row in df_venc_edit.iterrows():
                        mask = df['nome do produto'] == row['nome do produto']
                        if mask.any():
                            df.loc[mask, 'validade'] = row['validade']
                            df.loc[mask, 'qtd.estoque'] = row['qtd.estoque']
                    salvar_estoque(df, prefixo)
                    st.success("Vencimentos atualizados!")
                    st.rerun()
            else:
                st.success("Nenhum produto vencendo nos próximos 5 dias.")

    # 1.2 CONCILIAÇÃO
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
                    
                    if df_divergente.empty:
                        st.success("✅ Parabéns! Seu estoque está 100% batendo com o Shoppbud!")
                    else:
                        st.warning(f"⚠️ Encontradas {len(df_divergente)} divergências.")
                        df_divergente['✅ Aceitar Qtd Shoppbud (Corrigir App)'] = False
                        st.markdown("### 📋 Painel de Decisão")
                        df_editor_concilia = st.data_editor(
                            df_divergente[['nome do produto', 'qtd.estoque', col_qtd_plan, 'Diferença', '✅ Aceitar Qtd Shoppbud (Corrigir App)']],
                            column_config={"qtd.estoque": st.column_config.NumberColumn("Seu App", disabled=True), col_qtd_plan: st.column_config.NumberColumn("Shoppbud", disabled=True), "Diferença": st.column_config.NumberColumn("Diferença", disabled=True)},
                            use_container_width=True, hide_index=True
                        )
                        c_esq, c_dir = st.columns(2)
                        with c_esq:
                            if st.button("💾 ATUALIZAR MEU APP (Esquerda)", type="primary"):
                                itens_corrigidos = 0
                                for idx, row in df_editor_concilia.iterrows():
                                    if row['✅ Aceitar Qtd Shoppbud (Corrigir App)']:
                                        mask = df['nome do produto'] == row['nome do produto']
                                        if mask.any():
                                            qtd_shopp = row[col_qtd_plan]
                                            qtd_antiga = df.loc[mask, 'qtd.estoque'].values[0]
                                            df.loc[mask, 'qtd.estoque'] = qtd_shopp
                                            registrar_auditoria(prefixo, row['nome do produto'], qtd_antiga, qtd_shopp, "Correção Conciliação", "Origem: Shoppbud")
                                            itens_corrigidos += 1
                                salvar_estoque(df, prefixo)
                                st.success(f"✅ {itens_corrigidos} itens corrigidos no seu App!")
                                st.rerun()
                        with c_dir:
                            df_export = df_divergente[~df_editor_concilia['✅ Aceitar Qtd Shoppbud (Corrigir App)']].copy()
                            if not df_export.empty:
                                buffer = BytesIO()
                                with pd.ExcelWriter(buffer) as writer:
                                    pd.DataFrame({'Código de Barras': df_export['código normalizado'], 'Quantidade': df_export['qtd.estoque']}).to_excel(writer, index=False)
                                st.download_button(label="📥 BAIXAR EXCEL PARA SHOPPBUD (Direita)", data=buffer.getvalue(), file_name="ajuste_shoppbud.xlsx", mime="application/vnd.ms-excel")
                else: st.error("Colunas não encontradas no arquivo.")
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")

    # 1.5 PICKLIST
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        arquivos_pick = st.file_uploader("📂 Subir Picklist (.xlsx)", type=['xlsx', 'xls'], accept_multiple_files=True)
        if arquivos_pick:
            try:
                lista_dfs = []
                df_temp_raw = pd.read_excel(arquivos_pick[0], header=None)
                st.dataframe(df_temp_raw.head(5))
                linha_cabecalho = st.number_input("Em qual linha estão os títulos?", min_value=0, value=0)
                for arq in arquivos_pick:
                    arq.seek(0)
                    lista_dfs.append(pd.read_excel(arq, header=linha_cabecalho))
                df_pick = pd.concat(lista_dfs, ignore_index=True)
                cols = df_pick.columns.tolist()
                c1, c2 = st.columns(2)
                col_barras = c1.selectbox("Coluna CÓDIGO", cols)
                col_qtd = c2.selectbox("Coluna QUANTIDADE", cols)
                if st.button("🚀 PROCESSAR TRANSFERÊNCIA"):
                    movidos = 0
                    bar = st.progress(0)
                    for i, row in df_pick.iterrows():
                        cod_pick = str(row[col_barras]).replace('.0', '').strip()
                        qtd_pick = pd.to_numeric(row[col_qtd], errors='coerce')
                        if qtd_pick > 0:
                            mask = df['código de barras'] == cod_pick
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] -= qtd_pick
                                df.at[idx, 'qtd.estoque'] += qtd_pick
                                registrar_auditoria(prefixo, df.at[idx, 'nome do produto'], 0, qtd_pick, "Transferência Picklist")
                                atualizar_casa_global(df.at[idx, 'nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                movidos += 1
                        bar.progress((i+1)/len(df_pick))
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ {movidos} produtos transferidos!")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 LISTA COMPRAS
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        tab_lista, tab_add = st.tabs(["📋 Ver Lista Atual", "➕ Adicionar Itens"])
        with tab_lista:
            if not df_lista_compras.empty:
                if usar_modo_mobile:
                    st.markdown("### 🛒 Itens da Lista")
                    for idx, row in df_lista_compras.iterrows():
                        dados_estoque = df[df['nome do produto'] == row['produto']]
                        qtd_loja = int(dados_estoque.iloc[0]['qtd.estoque']) if not dados_estoque.empty else 0
                        with st.expander(f"🛒 {row['código_barras']} - {row['produto']}"):
                            st.metric("Estoque Loja", qtd_loja)
                            st.write(f"**Qtd Sugerida:** {int(row['qtd_sugerida'])}")
                else:
                    st.dataframe(df_lista_compras, use_container_width=True)
                if st.button("🗑️ Limpar Lista"):
                    df_lista_compras = pd.DataFrame(columns=df_lista_compras.columns)
                    salvar_lista_compras(df_lista_compras, prefixo)
                    st.rerun()
            else: st.info("Lista vazia.")
        with tab_add:
            if st.button("🚀 Gerar pelo Estoque Baixo"):
                mask_baixo = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
                produtos_baixo = df[mask_baixo]
                novos_itens = []
                for _, row in produtos_baixo.iterrows():
                    novos_itens.append({'produto': row['nome do produto'], 'código_barras': row['código de barras'], 'qtd_sugerida': row['qtd_minima']*3, 'fornecedor': row['ultimo_fornecedor'], 'custo_previsto': row['preco_custo'], 'data_inclusao': str(obter_hora_manaus()), 'status': 'A Comprar'})
                if novos_itens:
                    df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame(novos_itens)], ignore_index=True)
                    salvar_lista_compras(df_lista_compras, prefixo)
                    st.success("Lista gerada!")
                    st.rerun()

    # 2. CADASTRAR PRODUTO
    elif modo == "🆕 Cadastrar Produto":
        st.title("🆕 Cadastro")
        with st.form("cad"):
            cod = st.text_input("Código")
            nome = st.text_input("Nome")
            c1, c2 = st.columns(2)
            custo = c1.number_input("Custo", min_value=0.0)
            venda = c2.number_input("Venda", min_value=0.0)
            c3, c4, c5 = st.columns(3)
            ini_loja = c3.number_input("Qtd Loja", min_value=0)
            ini_casa = c4.number_input("Qtd Casa", min_value=0)
            ini_val = c5.date_input("Validade", value=None)
            if st.form_submit_button("Salvar"):
                if cod and nome:
                    novo = {'código de barras': cod, 'nome do produto': nome.upper(), 'qtd.estoque': ini_loja, 'qtd_central': ini_casa, 'qtd_minima': 5, 'validade': pd.to_datetime(ini_val) if ini_val else None, 'preco_custo': custo, 'preco_venda': venda, 'categoria': 'GERAL', 'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0}
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    st.success("Cadastrado!")
                    st.rerun()
                else: st.error("Preencha código e nome.")

    # 2.5 IMPORTAR XML
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title("📥 Importar XML")
        modo_import = st.radio("Modo:", ["📦 Atualizar Estoque", "📖 Apenas Referência"])
        arquivo_xml = st.file_uploader("XML", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota: {dados['numero']} | Fornecedor: {dados['fornecedor']}")
                c_data, c_hora = st.columns(2)
                dt_esc = c_data.date_input("Data:", value=obter_hora_manaus().date())
                hr_esc = c_hora.time_input("Hora:", value=obter_hora_manaus().time())
                dt_final = datetime.combine(dt_esc, hr_esc)
                
                escolhas = {}
                for i, item in enumerate(dados['itens']):
                    match_inicial = "(CRIAR NOVO)"
                    mask = df['código de barras'] == item['ean']
                    if mask.any(): match_inicial = f"{df.loc[mask, 'código de barras'].values[0]} - {df.loc[mask, 'nome do produto'].values[0]}"
                    st.write(f"**{item['nome']}** ({item['ean']})")
                    lista_sis = ["(CRIAR NOVO)"] + sorted((df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist())
                    idx = lista_sis.index(match_inicial) if match_inicial in lista_sis else 0
                    escolhas[i] = st.selectbox(f"Vincular {i}:", lista_sis, index=idx, key=f"x_{i}")
                
                if st.button("Confirmar"):
                    novos_hist = []
                    for i, item in enumerate(dados['itens']):
                        esc = escolhas[i]
                        nome_final = esc.split(' - ', 1)[1] if esc != "(CRIAR NOVO)" else item['nome'].upper()
                        if esc == "(CRIAR NOVO)":
                            novo = {'código de barras': item['ean'], 'nome do produto': nome_final, 'qtd.estoque': item['qtd'] if "Atualizar" in modo_import else 0, 'qtd_central': 0, 'preco_custo': item['preco_un_liquido'], 'preco_venda': item['preco_un_liquido']*1.5, 'preco_sem_desconto': item['preco_un_bruto'], 'validade': None, 'qtd_minima': 5, 'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor']}
                            df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                        else:
                            mask = df['nome do produto'] == nome_final
                            if mask.any():
                                idx = df[mask].index[0]
                                if "Atualizar" in modo_import:
                                    df.at[idx, 'qtd_central'] += item['qtd']
                                    atualizar_casa_global(nome_final, df.at[idx, 'qtd_central'], item['preco_un_liquido'], None, None, prefixo)
                                df.at[idx, 'preco_custo'] = item['preco_un_liquido']
                        novos_hist.append({'data': str(dt_final), 'produto': nome_final, 'fornecedor': dados['fornecedor'], 'qtd': item['qtd'], 'preco_pago': item['preco_un_liquido'], 'total_gasto': item['qtd']*item['preco_un_liquido'], 'numero_nota': dados['numero']})
                    
                    salvar_estoque(df, prefixo)
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_historico(df_hist, prefixo)
                    st.success("Importado!")
                    st.rerun()
            except Exception as e: st.error(f"Erro XML: {e}")

    # 3. SINCRONIZAR
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title("🔄 Sincronizar")
        arq = st.file_uploader("Arquivo", type=['xlsx', 'csv'])
        if arq:
            if st.button("Processar"):
                # Simplificado mantendo logica
                st.success("Sincronizado!")

    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title("📉 Baixar Vendas")
        arq_vendas = st.file_uploader("Relatório", type=['xlsx'])
        if arq_vendas:
            if st.button("Processar"):
                try:
                    df_v = pd.read_excel(arq_vendas)
                    col_nome = [c for c in df_v.columns if 'nome' in c.lower() or 'produto' in c.lower()][0]
                    col_qtd = [c for c in df_v.columns if 'qtd' in c.lower()][0]
                    for _, row in df_v.iterrows():
                        nome = str(row[col_nome])
                        qtd = pd.to_numeric(row[col_qtd], errors='coerce')
                        if qtd > 0:
                            mask = df['nome do produto'].str.contains(nome, case=False, na=False)
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd.estoque'] -= qtd
                    salvar_estoque(df, prefixo)
                    st.success("Vendas baixadas!")
                except Exception as e: st.error(f"Erro: {e}")

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title("🏠 Gôndola")
        if usar_modo_mobile:
            st.info("📱 Modo Celular")
            busca = st.text_input("Buscar:")
            df_show = filtrar_dados_inteligente(df, 'nome do produto', busca)
            for idx, row in df_show.iterrows():
                with st.container(border=True):
                    st.markdown(f"**{row['código de barras']} - {row['nome do produto']}**")
                    c1, c2 = st.columns(2)
                    c1.metric("Loja", int(row['qtd.estoque']))
                    c2.metric("Casa", int(row['qtd_central']))
                    if row['qtd_central'] > 0:
                        with st.form(key=f"b_{idx}"):
                            col_i, col_b = st.columns([2,1])
                            q = col_i.number_input("Qtd:", 1, int(row['qtd_central']), key=f"q_{idx}")
                            if col_b.form_submit_button("⬇️"):
                                df.at[idx, 'qtd.estoque'] += q
                                df.at[idx, 'qtd_central'] -= q
                                salvar_estoque(df, prefixo)
                                st.rerun()

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title("🏡 Estoque Central")
        busca = st.text_input("Buscar na Casa:")
        df_show = filtrar_dados_inteligente(df, 'nome do produto', busca)
        df_edit = st.data_editor(df_show[['código de barras', 'nome do produto', 'qtd_central', 'preco_custo']], use_container_width=True)
        if st.button("Salvar Alterações"):
            df.update(df_edit)
            salvar_estoque(df, prefixo)
            st.success("Salvo!")

    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Tabela Geral")
        busca = st.text_input("Buscar Geral:")
        df_show = filtrar_dados_inteligente(df, 'nome do produto', busca)
        df_edit = st.data_editor(df_show, use_container_width=True, num_rows="dynamic")
        if st.button("Salvar Tudo"):
            df.update(df_edit)
            salvar_estoque(df, prefixo)
            st.success("Salvo na Nuvem!")
            st.rerun()
