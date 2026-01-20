import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
from io import BytesIO

# Configuração da página
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# ==============================================================================
# 🕒 AJUSTE DE FUSO HORÁRIO (MANAUS / AMAZONAS / CUIABÁ: UTC-4)
# ==============================================================================
def obter_hora_manaus():
    """Retorna a data e hora atual ajustada para o fuso de Manaus (-4h em relação ao UTC)."""
    return datetime.utcnow() - timedelta(hours=4)

# ==============================================================================
# 🆕 FUNÇÕES DE LIMPEZA E PADRONIZAÇÃO
# ==============================================================================
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
        df_limpo.to_excel("meus_produtos_oficiais.xlsx", index=False)
        return True
    except Exception as e:
        st.error(f"Erro ao organizar o arquivo: {e}")
        return False

def carregar_base_oficial():
    if os.path.exists("meus_produtos_oficiais.xlsx"):
        return pd.read_excel("meus_produtos_oficiais.xlsx")
    return pd.DataFrame()

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

# --- FUNÇÕES AUXILIARES ---
def formatar_moeda_br(valor):
    try:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

# --- 🔐 LOG DE AUDITORIA (NOVO) ---
def registrar_auditoria(prefixo, produto, qtd_antes, qtd_nova, acao, motivo="Manual"):
    """Grava um log invisível de alterações manuais."""
    try:
        arquivo_log = f"{prefixo}_log_auditoria.xlsx"
        novo_log = {
            'data_hora': obter_hora_manaus(),
            'produto': produto,
            'qtd_antes': qtd_antes,
            'qtd_nova': qtd_nova,
            'acao': acao,
            'motivo': motivo
        }
        if os.path.exists(arquivo_log):
            df_log = pd.read_excel(arquivo_log)
            df_log = pd.concat([df_log, pd.DataFrame([novo_log])], ignore_index=True)
        else:
            df_log = pd.DataFrame([novo_log])
        df_log.to_excel(arquivo_log, index=False)
    except Exception as e:
        print(f"Erro ao salvar log: {e}")

# --- 🔐 MEMÓRIA DE VENDAS PROCESSADAS (NOVO) ---
def carregar_ids_processados(prefixo):
    """Carrega lista de IDs de vendas já baixadas para evitar duplicidade."""
    arquivo = f"{prefixo}_ids_vendas.csv"
    if os.path.exists(arquivo):
        try:
            df_ids = pd.read_csv(arquivo)
            return set(df_ids['id_transacao'].astype(str).tolist())
        except: return set()
    return set()

def salvar_ids_processados(prefixo, novos_ids):
    """Salva novos IDs na lista de processados."""
    arquivo = f"{prefixo}_ids_vendas.csv"
    if not novos_ids: return
    df_novo = pd.DataFrame({'id_transacao': list(novos_ids)})
    if os.path.exists(arquivo):
        df_antigo = pd.read_csv(arquivo)
        df_final = pd.concat([df_antigo, df_novo]).drop_duplicates()
    else:
        df_final = df_novo
    df_final.to_csv(arquivo, index=False)

def atualizar_casa_global(nome_produto, qtd_nova_casa, novo_custo, novo_venda, nova_validade, prefixo_ignorar):
    todas_lojas = ["loja1", "loja2", "loja3"]
    for loja in todas_lojas:
        if loja == prefixo_ignorar: continue
        arquivo = f"{loja}_estoque.xlsx"
        if os.path.exists(arquivo):
            try:
                df_outra = pd.read_excel(arquivo)
                df_outra.columns = df_outra.columns.str.strip().str.lower()
                mask = df_outra['nome do produto'].astype(str) == str(nome_produto)
                if mask.any():
                    idx = df_outra[mask].index[0]
                    qtd_antiga = df_outra.at[idx, 'qtd_central'] # Para log
                    df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                    df_outra.to_excel(arquivo, index=False)
                    # Log na loja secundária também
                    registrar_auditoria(loja, nome_produto, qtd_antiga, qtd_nova_casa, "Sincronização Automática", f"Origem: {prefixo_ignorar}")
            except Exception: pass

# --- ARQUIVOS ---
def inicializar_arquivos(prefixo):
    arquivos = {
        f"{prefixo}_estoque.xlsx": ['código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'],
        f"{prefixo}_historico_compras.xlsx": ['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto', 'obs_importacao'],
        f"{prefixo}_movimentacoes.xlsx": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas.xlsx": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],
        f"{prefixo}_lista_compras.xlsx": ['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'],
        f"{prefixo}_log_auditoria.xlsx": ['data_hora', 'produto', 'qtd_antes', 'qtd_nova', 'acao', 'motivo']
    }
    for arquivo, colunas in arquivos.items():
        if not os.path.exists(arquivo):
            pd.DataFrame(columns=colunas).to_excel(arquivo, index=False)

def carregar_dados(prefixo_arquivo):
    try:
        df = pd.read_excel(f"{prefixo_arquivo}_estoque.xlsx")
        df.columns = df.columns.str.strip().str.lower()
        if 'preco_sem_desconto' not in df.columns: df['preco_sem_desconto'] = 0.0
        cols_num = ['qtd.estoque', 'qtd_central', 'qtd_minima', 'qtd_comprada', 'preco_custo', 'preco_venda', 'preco_sem_desconto']
        for col in cols_num:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['ultimo_fornecedor'] = df['ultimo_fornecedor'].fillna('')
        df['código de barras'] = df['código de barras'].apply(lambda x: str(x).replace('.0', '').strip() if pd.notnull(x) else "")
        df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
        df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')
        return df
    except: return pd.DataFrame()

def carregar_historico(prefixo_arquivo):
    try:
        df_h = pd.read_excel(f"{prefixo_arquivo}_historico_compras.xlsx")
        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
        if 'numero_nota' not in df_h.columns: df_h['numero_nota'] = ""
        if 'obs_importacao' not in df_h.columns: df_h['obs_importacao'] = ""
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
        df_m = pd.read_excel(f"{prefixo_arquivo}_movimentacoes.xlsx")
        df_m['data_hora'] = pd.to_datetime(df_m['data_hora'], errors='coerce')
        return df_m
    except: return pd.DataFrame()

def carregar_vendas(prefixo_arquivo):
    try:
        df_v = pd.read_excel(f"{prefixo_arquivo}_vendas.xlsx")
        df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')
        return df_v
    except: return pd.DataFrame()

def carregar_lista_compras(prefixo_arquivo):
    try:
        df = pd.read_excel(f"{prefixo_arquivo}_lista_compras.xlsx")
        # Garante que a coluna código de barras exista
        if 'código_barras' not in df.columns: df['código_barras'] = ""
        return df
    except: return pd.DataFrame()

# --- XML ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]

    dados_nota = {'numero': '', 'fornecedor': '', 'data': obter_hora_manaus(), 'itens': []}
    
    lista_nomes_ref = []
    dict_ref_ean = {}
    if not df_referencia.empty:
        for idx, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            ean = str(row['código de barras']).strip()
            dict_ref_ean[nm] = ean
            lista_nomes_ref.append(nm)

    # NotaFiscal Custom
    if tag_limpa(root) == 'NotaFiscal':
        info = root.find('Info')
        if info is not None:
            dados_nota['numero'] = info.find('NumeroNota').text if info.find('NumeroNota') is not None else ""
            dados_nota['fornecedor'] = info.find('Fornecedor').text if info.find('Fornecedor') is not None else ""
            try: dados_nota['data'] = datetime.strptime(info.find('DataCompra').text, '%d/%m/%Y')
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

    # NFe Padrão
    for elem in root.iter():
        tag = tag_limpa(elem)
        if tag == 'nNF': dados_nota['numero'] = elem.text
        elif tag == 'xNome' and dados_nota['fornecedor'] == '': dados_nota['fornecedor'] = elem.text
        elif tag == 'dhEmi':
            try: dados_nota['data'] = datetime.strptime(elem.text[:10], '%Y-%m-%d')
            except: pass

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
def salvar_estoque(df, prefixo): df.to_excel(f"{prefixo}_estoque.xlsx", index=False)
def salvar_historico(df, prefixo): df.to_excel(f"{prefixo}_historico_compras.xlsx", index=False)
def salvar_movimentacoes(df, prefixo): df.to_excel(f"{prefixo}_movimentacoes.xlsx", index=False)
def salvar_vendas(df, prefixo): df.to_excel(f"{prefixo}_vendas.xlsx", index=False)
def salvar_lista_compras(df, prefixo): df.to_excel(f"{prefixo}_lista_compras.xlsx", index=False)

# ==============================================================================
# 🚀 INÍCIO DO APP
# ==============================================================================

inicializar_arquivos(prefixo)
df = carregar_dados(prefixo)
df_hist = carregar_historico(prefixo)
df_mov = carregar_movimentacoes(prefixo)
df_vendas = carregar_vendas(prefixo)
df_oficial = carregar_base_oficial()
df_lista_compras = carregar_lista_compras(prefixo)
ids_processados = carregar_ids_processados(prefixo) # Carrega IDs já baixados

if df is not None:
    st.sidebar.title("🏪 Menu")
    # MENU ATUALIZADO: REMOVIDO "FORNECEDOR"
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)",
        "⚖️ Conciliação (Shoppbud vs App)", # NOVO MENU
        "🚚 Transferência em Massa (Picklist)",
        "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", 
        "📥 Importar XML (Associação Inteligente)", 
        "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)",
        "📉 Baixar Vendas (Do Relatório)",
        "🏠 Gôndola (Loja)", 
        # Menu "Fornecedor" removido conforme solicitado
        "💰 Inteligência de Compras (Histórico)", # NOME MELHORADO
        "🏡 Estoque Central (Casa)",
        "📋 Tabela Geral"
    ])

    # 1. DASHBOARD (COM PAINEL DE VENCIMENTO INTELIGENTE)
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
            
            # --- PAINEL VENCIMENTO MELHORADO ---
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
                    # Lógica para achar quem mudou no dataframe original
                    for i, row in df_venc_edit.iterrows():
                        # Busca no df original pelo nome (assumindo unico) ou idealmente ID/Codigo
                        mask = df['nome do produto'] == row['nome do produto']
                        if mask.any():
                            df.loc[mask, 'validade'] = row['validade']
                            df.loc[mask, 'qtd.estoque'] = row['qtd.estoque']
                    salvar_estoque(df, prefixo)
                    st.success("Vencimentos atualizados!")
                    st.rerun()
            else:
                st.success("Nenhum produto vencendo nos próximos 5 dias.")

    # 1.2 MÓDULO: CONCILIAÇÃO (NOVO E CORRIGIDO PARA NOMES DO EXCEL)
    elif modo == "⚖️ Conciliação (Shoppbud vs App)":
        st.title("⚖️ Conciliação de Estoque")
        st.markdown("""
        **Ferramenta de Auditoria:** Compare o estoque do seu App com o Planograma do Shoppbud.
        1. Importe suas vendas do dia (no menu 'Baixar Vendas') para atualizar seu App.
        2. Baixe o Planograma ATUAL do Shoppbud e carregue abaixo.
        """)
        
        arq_planograma = st.file_uploader("📂 Carregar Planograma Shoppbud (.xlsx)", type=['xlsx'])
        if arq_planograma:
            try:
                df_plan = pd.read_excel(arq_planograma)
                # --- CORREÇÃO: BUSCA INTELIGENTE DE COLUNAS (COM OU SEM ACENTO) ---
                col_cod_plan = next((c for c in df_plan.columns if ('código' in c.lower() or 'codigo' in c.lower()) and 'barras' in c.lower()), None)
                col_qtd_plan = next((c for c in df_plan.columns if 'qtd' in c.lower() and 'estoque' in c.lower()), None)
                # ------------------------------------------------------------------
                
                if col_cod_plan and col_qtd_plan:
                    # Preparar comparação
                    df_plan['código normalizado'] = df_plan[col_cod_plan].astype(str).str.replace('.0', '').str.strip()
                    df['código normalizado'] = df['código de barras'].astype(str).str.replace('.0', '').str.strip()
                    
                    # Merge (Cruzamento)
                    df_concilia = pd.merge(
                        df[['código normalizado', 'nome do produto', 'qtd.estoque']],
                        df_plan[[col_cod_plan, col_qtd_plan, 'código normalizado']],
                        on='código normalizado',
                        how='inner'
                    )
                    
                    # Calcular Diferença
                    df_concilia['Diferença'] = df_concilia['qtd.estoque'] - df_concilia[col_qtd_plan]
                    df_divergente = df_concilia[df_concilia['Diferença'] != 0].copy()
                    
                    if df_divergente.empty:
                        st.success("✅ Parabéns! Seu estoque está 100% batendo com o Shoppbud!")
                    else:
                        st.warning(f"⚠️ Encontradas {len(df_divergente)} divergências.")
                        
                        # Adicionar coluna de decisão para o usuario
                        df_divergente['✅ Aceitar Qtd Shoppbud (Corrigir App)'] = False
                        
                        # Mostrar tabela editável
                        st.markdown("### 📋 Painel de Decisão")
                        df_editor_concilia = st.data_editor(
                            df_divergente[['nome do produto', 'qtd.estoque', col_qtd_plan, 'Diferença', '✅ Aceitar Qtd Shoppbud (Corrigir App)']],
                            column_config={
                                "qtd.estoque": st.column_config.NumberColumn("Seu App", disabled=True),
                                col_qtd_plan: st.column_config.NumberColumn("Shoppbud", disabled=True),
                                "Diferença": st.column_config.NumberColumn("Diferença", disabled=True),
                            },
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        c_esq, c_dir = st.columns(2)
                        
                        # BOTÃO ESQUERDO: CORRIGIR APP
                        with c_esq:
                            if st.button("💾 ATUALIZAR MEU APP (Esquerda)", type="primary"):
                                itens_corrigidos = 0
                                for idx, row in df_editor_concilia.iterrows():
                                    if row['✅ Aceitar Qtd Shoppbud (Corrigir App)']:
                                        # Achar produto no DF principal e atualizar
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

                        # BOTÃO DIREITO: EXPORTAR PARA SHOPPBUD
                        with c_dir:
                            # Filtra o que NÃO foi marcado para aceitar (ou seja, o Shoppbud está errado)
                            df_export = df_divergente[~df_editor_concilia['✅ Aceitar Qtd Shoppbud (Corrigir App)']].copy()
                            if not df_export.empty:
                                buffer = BytesIO()
                                # --- CORREÇÃO: USAR ENGINE PADRÃO (SEM XLSXWRITER OBRIGATÓRIO) ---
                                with pd.ExcelWriter(buffer) as writer:
                                    # Formato simples para importar no Shoppbud
                                    df_export_final = pd.DataFrame({
                                        'Código de Barras': df_export['código normalizado'],
                                        'Quantidade': df_export['qtd.estoque'] # Manda a qtd do App (correta)
                                    })
                                    df_export_final.to_excel(writer, index=False)
                                
                                st.download_button(
                                    label="📥 BAIXAR EXCEL PARA SHOPPBUD (Direita)",
                                    data=buffer.getvalue(),
                                    file_name=f"ajuste_shoppbud_{datetime.now().strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.ms-excel"
                                )
                else:
                    st.error(f"Não encontrei colunas 'Código de Barras' ou 'Codigo de Barras' e 'Qtd Estoque' no arquivo. Colunas encontradas: {df_plan.columns.tolist()}")
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")


    # 1.5 MÓDULO: TRANSFERÊNCIA VIA PICKLIST
    elif modo == "🚚 Transferência em Massa (Picklist)":
        st.title(f"🚚 Transferência em Massa - {loja_atual}")
        st.markdown("**Sistema Shoppbud/Transferência:** Suba o Excel para mover estoque da Casa para a Loja.")
        arquivo_pick = st.file_uploader("📂 Subir Picklist (.xlsx)", type=['xlsx', 'xls'])
        
        if arquivo_pick:
            try:
                df_pick = pd.read_excel(arquivo_pick)
                df_pick.columns = df_pick.columns.str.strip().str.lower()
                col_barras = next((c for c in df_pick.columns if 'barras' in c), None)
                col_qtd = next((c for c in df_pick.columns if 'transferir' in c), None)
                
                if not col_barras or not col_qtd:
                    st.error("❌ Colunas 'Código de Barras' ou 'Transferir' não encontradas.")
                else:
                    if st.button("🚀 PROCESSAR TRANSFERÊNCIA"):
                        movidos = 0
                        erros = 0
                        bar = st.progress(0)
                        log_movs = []
                        total_linhas = len(df_pick)
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
                                    
                                    log_movs.append({'data_hora': obter_hora_manaus(), 'produto': nome_prod, 'qtd_movida': qtd_pick})
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    # Log Auditoria
                                    registrar_auditoria(prefixo, nome_prod, qtd_antiga_loja, df.at[idx, 'qtd.estoque'], "Transferência Picklist")
                                    movidos += 1
                                else:
                                    erros += 1
                            bar.progress((i+1)/total_linhas)
                        salvar_estoque(df, prefixo)
                        if log_movs:
                            df_mov = pd.concat([df_mov, pd.DataFrame(log_movs)], ignore_index=True)
                            salvar_movimentacoes(df_mov, prefixo)
                        st.success(f"✅ {movidos} produtos transferidos!")
                        if erros > 0: st.warning(f"⚠️ {erros} não encontrados.")
            except Exception as e: st.error(f"Erro: {e}")

    # 1.6 MÓDULO: LISTA DE COMPRAS
    elif modo == "📝 Lista de Compras (Planejamento)":
        st.title("📝 Planejamento de Compras")
        tab_lista, tab_add = st.tabs(["📋 Ver Lista Atual", "➕ Adicionar Itens"])
        with tab_lista:
            if not df_lista_compras.empty:
                st.info("💡 Esta é sua lista de compras. Quando for ao mercado, use esta tabela.")
                if usar_modo_mobile:
                    # --- MODO CELULAR COM VISUALIZAÇÃO DE ESTOQUE (RAIO-X) ---
                    st.markdown("### 🛒 Itens da Lista (Clique para ver Estoque)")
                    for idx, row in df_lista_compras.iterrows():
                        # Busca informações de estoque em tempo real
                        dados_estoque = df[df['nome do produto'] == row['produto']]
                        qtd_loja_atual = 0
                        qtd_casa_atual = 0
                        if not dados_estoque.empty:
                            qtd_loja_atual = int(dados_estoque.iloc[0]['qtd.estoque'])
                            qtd_casa_atual = int(dados_estoque.iloc[0]['qtd_central'])
                        
                        # Cria o expander clicável
                        with st.expander(f"🛒 {row['código_barras']} - {row['produto']}"):
                            c1, c2 = st.columns(2)
                            c1.metric("Estoque Loja", qtd_loja_atual)
                            c2.metric("Estoque Casa", qtd_casa_atual)
                            st.divider()
                            st.write(f"**Qtd Sugerida:** {int(row['qtd_sugerida'])}")
                            st.caption(f"Incluído em: {row['data_inclusao']}")
                            st.caption(f"Status: {row['status']}")
                else:
                    st.dataframe(df_lista_compras, use_container_width=True)
                
                c_del, c_pdf = st.columns(2)
                if c_del.button("🗑️ Limpar Lista Inteira (Após Comprar)"):
                    df_lista_compras = pd.DataFrame(columns=['produto', 'código_barras', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'])
                    salvar_lista_compras(df_lista_compras, prefixo)
                    st.success("Lista limpa!")
                    st.rerun()
            else:
                st.info("Sua lista de compras está vazia.")
        with tab_add:
            st.subheader("🤖 Gerador Automático")
            if st.button("🚀 Gerar Lista Baseada no Estoque Baixo"):
                if df.empty:
                    st.warning("Sem produtos cadastrados.")
                else:
                    mask_baixo = (df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']
                    produtos_baixo = df[mask_baixo]
                    if produtos_baixo.empty:
                        st.success("Tudo certo! Nada abaixo do mínimo.")
                    else:
                        novos_itens = []
                        for _, row in produtos_baixo.iterrows():
                            ja_na_lista = False
                            if not df_lista_compras.empty:
                                ja_na_lista = df_lista_compras['produto'].astype(str).str.contains(row['nome do produto'], regex=False).any()
                            if not ja_na_lista:
                                novos_itens.append({
                                    'produto': row['nome do produto'],
                                    'código_barras': row['código de barras'],
                                    'qtd_sugerida': row['qtd_minima'] * 3,
                                    'fornecedor': row['ultimo_fornecedor'],
                                    'custo_previsto': row['preco_custo'],
                                    'data_inclusao': obter_hora_manaus().strftime("%d/%m/%Y %H:%M"),
                                    'status': 'A Comprar'
                                })
                        if novos_itens:
                            df_novos = pd.DataFrame(novos_itens)
                            df_lista_compras = pd.concat([df_lista_compras, df_novos], ignore_index=True)
                            salvar_lista_compras(df_lista_compras, prefixo)
                            st.success(f"{len(novos_itens)} itens adicionados!")
                            st.rerun()
                        else: st.warning("Itens já estão na lista.")
            st.divider()
            st.subheader("✋ Adicionar Manualmente")
            with st.form("add_manual_lista"):
                # Lista com código para facilitar busca
                lista_visuais = (df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist()
                lista_visuais = sorted(lista_visuais)
                prod_man_visual = st.selectbox("Produto:", [""] + lista_visuais)
                
                c_qtd, c_forn = st.columns(2)
                qtd_man = c_qtd.number_input("Qtd a Comprar:", min_value=1, value=10)
                obs_man = c_forn.text_input("Fornecedor/Obs (Opcional):", placeholder="Ex: Atacadão")
                
                c_dt, c_hr = st.columns(2)
                
                # CORREÇÃO DEFINITIVA DO RELÓGIO (TRAVA DE MEMÓRIA PARA LISTA DE COMPRAS)
                if 'hora_lista_fixa' not in st.session_state:
                    st.session_state['hora_lista_fixa'] = obter_hora_manaus().time().replace(second=0, microsecond=0)
                
                hora_padrao_lista = st.session_state['hora_lista_fixa']
                
                dt_manual = c_dt.date_input("Data:", value=obter_hora_manaus().date())
                hr_manual = c_hr.time_input("Hora:", value=hora_padrao_lista, step=60)
                
                if st.form_submit_button("Adicionar à Lista"):
                    if prod_man_visual:
                        # Extrai código e nome
                        try:
                            parts = prod_man_visual.split(' - ', 1)
                            cod_barras_add = parts[0]
                            nome_prod_add = parts[1]
                        except:
                            cod_barras_add = ""
                            nome_prod_add = prod_man_visual
                        
                        preco_ref = 0.0
                        mask = df['nome do produto'] == nome_prod_add
                        if mask.any(): preco_ref = df.loc[mask, 'preco_custo'].values[0]
                        
                        # Correção de Salvamento: Usa a hora manual escolhida
                        data_formatada = datetime.combine(dt_manual, hr_manual).strftime("%d/%m/%Y %H:%M")
                        
                        novo_item = {
                            'produto': nome_prod_add, 
                            'código_barras': cod_barras_add,
                            'qtd_sugerida': qtd_man, 
                            'fornecedor': obs_man, 
                            'custo_previsto': preco_ref, 
                            'data_inclusao': data_formatada, 
                            'status': 'Manual'
                        }
                        df_lista_compras = pd.concat([df_lista_compras, pd.DataFrame([novo_item])], ignore_index=True)
                        salvar_lista_compras(df_lista_compras, prefixo)
                        st.success("Adicionado!")
                        st.rerun()
                    else: st.error("Selecione um produto.")

    # 2. CADASTRAR PRODUTO
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
                if not novo_cod or not novo_nome:
                    st.error("Código e Nome obrigatórios!")
                elif not df.empty and df['código de barras'].astype(str).str.contains(str(novo_cod).strip()).any():
                    st.error("Código já existe!")
                else:
                    novo = {
                        'código de barras': str(novo_cod).strip(), 'nome do produto': novo_nome.upper().strip(),
                        'qtd.estoque': ini_loja, 'qtd_central': ini_casa, 'qtd_minima': novo_min,
                        'validade': pd.to_datetime(ini_val) if ini_val else None, 
                        'status_compra': 'OK', 'qtd_comprada': 0,
                        'preco_custo': novo_custo, 'preco_venda': novo_venda, 'categoria': nova_cat,
                        'ultimo_fornecedor': '', 'preco_sem_desconto': 0.0
                    }
                    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
                    salvar_estoque(df, prefixo)
                    registrar_auditoria(prefixo, novo_nome.upper().strip(), 0, ini_loja, "Novo Cadastro")
                    st.success("Cadastrado!")
                    st.rerun()

    # 2.5 IMPORTAR XML (REFORMULADO)
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML da Nota Fiscal")
        st.markdown("O sistema tentará encontrar os produtos. **Confirme se o vínculo está correto antes de salvar.**")
        
        # --- SELETOR DE MODO DE IMPORTAÇÃO ---
        modo_import = st.radio("Modo de Importação:", ["📦 Atualizar Estoque (Entrada)", "📖 Apenas Referência (Histórico de Preços)"], horizontal=True)
        # -------------------------------------
        
        if df_oficial.empty:
            st.warning("⚠️ DICA: Vá em 'Configurar Base Oficial' e suba sua lista de produtos para melhorar a identificação automática.")
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota Fiscal: **{dados['numero']}** | Fornecedor: **{dados['fornecedor']}**")
                
                # --- DATA MANUAL DO XML (CORREÇÃO DE HORA COM TRAVA) ---
                c_data, c_hora = st.columns(2)
                
                # CORREÇÃO DEFINITIVA DO RELÓGIO (TRAVA DE MEMÓRIA)
                # Cria uma chave única baseada no nome do arquivo para não resetar o relógio
                chave_hora = f"hora_xml_{arquivo_xml.name}"
                if chave_hora not in st.session_state:
                    # Salva a hora APENAS na primeira vez que carrega o arquivo
                    st.session_state[chave_hora] = obter_hora_manaus().time().replace(second=0, microsecond=0)
                
                hora_padrao_congelada = st.session_state[chave_hora]
                
                data_xml_padrao = dados['data'].date() if dados['data'] else obter_hora_manaus().date()
                data_escolhida = c_data.date_input("📅 Data da Compra/Entrada (Histórico):", value=data_xml_padrao)
                
                # Ajuste: Usa a hora congelada no value
                hora_escolhida = c_hora.time_input("⏰ Hora:", value=hora_padrao_congelada, step=60)
                
                data_final_historico = datetime.combine(data_escolhida, hora_escolhida)
                # --------------------------

                st.markdown("---")
                st.subheader("🛠️ Conferência e Cálculo de Descontos")
                
                # Lista COMPLETA e NORMALIZADA para o dropdown
                # ALTERAÇÃO VISUAL: CODIGO - NOME
                lista_visuais = (df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist()
                lista_visuais = sorted(lista_visuais)
                lista_produtos_sistema = ["(CRIAR NOVO)"] + lista_visuais
                escolhas = {}
                
                for i, item in enumerate(dados['itens']):
                    ean_xml = str(item.get('ean', '')).strip()
                    nome_xml = str(item['nome']).strip()
                    qtd_xml = item['qtd']
                    p_bruto = item['preco_un_bruto']
                    p_liq = item['preco_un_liquido']
                    desc_total = item['desconto_total_item']
                    
                    match_inicial = "(CRIAR NOVO)"
                    tipo_match = "Nenhum"
                    
                    if not df.empty:
                        # 1. Tenta Código de Barras
                        mask_ean = df['código de barras'].astype(str) == ean_xml
                        if mask_ean.any():
                            nome_bd = df.loc[mask_ean, 'nome do produto'].values[0]
                            cod_bd = df.loc[mask_ean, 'código de barras'].values[0]
                            match_inicial = f"{cod_bd} - {nome_bd}"
                            tipo_match = "Código de Barras (Exato)"
                        else:
                            # 2. Tenta Nova Busca por Palavras
                            lista_nomes = df['nome do produto'].astype(str).tolist()
                            melhor_nome, tipo_encontrado = encontrar_melhor_match(nome_xml, lista_nomes)
                            if melhor_nome:
                                # Reconstrói o visual code - name
                                mask_nm = df['nome do produto'] == melhor_nome
                                if mask_nm.any():
                                    cod_bd = df.loc[mask_nm, 'código de barras'].values[0]
                                    match_inicial = f"{cod_bd} - {melhor_nome}"
                                    tipo_match = tipo_encontrado

                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.markdown(f"📄 XML: **{nome_xml}**")
                        st.caption(f"EAN XML: `{ean_xml}` | Qtd: {int(qtd_xml)}")
                        st.markdown(f"💰 Tabela: R$ {p_bruto:.2f} | **Pago (Desc): R$ {p_liq:.2f}**")
                        if desc_total > 0:
                            st.caption(f"📉 Desconto Total na nota: R$ {desc_total:.2f}")

                    with c2:
                        idx_inicial = 0
                        if match_inicial in lista_produtos_sistema:
                            idx_inicial = lista_produtos_sistema.index(match_inicial)
                        escolha_usuario = st.selectbox(
                            f"Vincular ao Sistema ({tipo_match}):", 
                            lista_produtos_sistema, 
                            index=idx_inicial,
                            key=f"sel_{i}"
                        )
                    escolhas[i] = escolha_usuario
                    st.divider()

                if st.button("✅ CONFIRMAR E SALVAR"):
                    novos_hist = []
                    criados_cont = 0
                    atualizados_cont = 0
                    
                    for i, item in enumerate(dados['itens']):
                        produto_escolhido = escolhas[i]
                        qtd_xml = int(item['qtd'])
                        preco_pago = item['preco_un_liquido']
                        preco_sem_desc = item['preco_un_bruto']
                        desc_total_val = item['desconto_total_item']
                        
                        ean_xml = str(item.get('ean', '')).strip()
                        nome_xml = str(item['nome']).strip()
                        
                        nome_final = ""
                        
                        # Extrai nome puro da escolha visual "COD - NOME"
                        if produto_escolhido != "(CRIAR NOVO)":
                            nome_final = produto_escolhido.split(' - ', 1)[1]
                        else:
                            nome_final = nome_xml.upper()

                        # LÓGICA DE CRIAÇÃO / ATUALIZAÇÃO
                        if produto_escolhido == "(CRIAR NOVO)":
                            # Cria mesmo se for referência (para ter o cadastro)
                            novo_prod = {
                                'código de barras': ean_xml, 
                                'nome do produto': nome_final,
                                'qtd.estoque': qtd_xml if "Atualizar Estoque" in modo_import else 0, # Só soma se não for Ref
                                'qtd_central': 0, 'qtd_minima': 5,
                                'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0,
                                'preco_custo': preco_pago, 'preco_venda': preco_pago * 2,
                                'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'],
                                'preco_sem_desconto': preco_sem_desc
                            }
                            df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                            criados_cont += 1
                            if "Atualizar Estoque" in modo_import:
                                registrar_auditoria(prefixo, nome_final, 0, qtd_xml, "XML - Cadastro Novo")
                        else:
                            mask = df['nome do produto'].astype(str) == nome_final
                            if mask.any():
                                idx = df[mask].index[0]
                                qtd_antiga = df.at[idx, 'qtd.estoque'] # Log
                                
                                # SOMA ESTOQUE APENAS SE MODO PADRÃO
                                if "Atualizar Estoque" in modo_import:
                                    # Se for estoque central ou loja? Padrão XML geralmente vai pra Central ou Loja?
                                    # O código original somava em 'qtd_central' no XML
                                    df.at[idx, 'qtd_central'] += qtd_xml 
                                    registrar_auditoria(prefixo, nome_final, df.at[idx, 'qtd_central']-qtd_xml, df.at[idx, 'qtd_central'], "XML - Entrada Estoque")
                                
                                # Atualiza preços sempre
                                df.at[idx, 'preco_custo'] = preco_pago
                                df.at[idx, 'preco_sem_desconto'] = preco_sem_desc
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1
                                
                                # Sincroniza Casa Global
                                atualizar_casa_global(nome_final, df.at[idx, 'qtd_central'], preco_pago, None, None, prefixo)
                        
                        # HISTÓRICO DE COMPRAS (Sempre Grava)
                        obs_tag = "(REF)" if "Referência" in modo_import else ""
                        novos_hist.append({
                            'data': data_final_historico, # Usa a data escolhida
                            'produto': nome_final, 'fornecedor': dados['fornecedor'], 
                            'qtd': qtd_xml, 'preco_pago': preco_pago, 'total_gasto': qtd_xml * preco_pago,
                            'numero_nota': dados['numero'], 'desconto_total_money': desc_total_val, 'preco_sem_desconto': preco_sem_desc,
                            'obs_importacao': obs_tag
                        })
                    
                    salvar_estoque(df, prefixo)
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_historico(df_hist, prefixo)
                    
                    st.success(f"✅ Processado! {criados_cont} novos, {atualizados_cont} atualizados.")
                    st.balloons()
                    st.rerun()
            except Exception as e: st.error(f"Erro ao ler XML: {e}")

    # 2.8 CONFIGURAR BASE OFICIAL
    elif modo == "⚙️ Configurar Base Oficial":
        st.title("⚙️ Configurar Base de Produtos Oficial")
        st.markdown("""
        Suba aqui o arquivo **product-2025...** exportado do sistema.
        Isso ajuda o app a preencher os códigos de barras automaticamente quando o XML não trouxer.
        """)
        arquivo_base = st.file_uploader("Suba o arquivo Excel/CSV aqui", type=['xlsx', 'csv'])
        if arquivo_base:
            if st.button("🚀 Processar e Salvar Base"):
                sucesso = processar_excel_oficial(arquivo_base)
                if sucesso:
                    st.success("Base Oficial atualizada com sucesso! Agora as importações de XML serão automáticas.")
                    st.rerun()

    # 3. SINCRONIZAR (CORRIGIDO: AGORA CRIA NOVOS PRODUTOS!)
    elif modo == "🔄 Sincronizar (Planograma)":
        st.title(f"🔄 Sincronizar - {loja_atual}")
        st.info("💡 Este módulo agora IMPORTA produtos novos da planilha e atualiza os existentes.")
        
        arquivo = st.file_uploader("📂 Arquivo Planograma (XLSX ou CSV)", type=['xlsx', 'xls', 'csv'])
        if arquivo:
            try:
                # Leitura robusta (CSV ou Excel)
                if arquivo.name.endswith('.csv'):
                    df_raw = pd.read_csv(arquivo, header=None)
                else:
                    df_raw = pd.read_excel(arquivo, header=None)
                
                st.write("Identifique as colunas:")
                st.dataframe(df_raw.head())
                cols = df_raw.columns.tolist()
                
                c1, c2, c3, c4 = st.columns(4)
                idx_barras = c1.selectbox("Coluna CÓDIGO BARRAS", cols, index=0)
                idx_nome = c2.selectbox("Coluna NOME DO PRODUTO", cols, index=1 if len(cols)>1 else 0)
                idx_qtd = c3.selectbox("Coluna QUANTIDADE", cols, index=len(cols)-1)
                opcoes_preco = ["(Não Atualizar Preço)"] + cols
                idx_preco = c4.selectbox("Coluna PREÇO VENDA", opcoes_preco)
                
                if st.button("🚀 SINCRONIZAR TUDO (Importar + Atualizar)"):
                    df = carregar_dados(prefixo)
                    
                    alt = 0
                    novos = 0
                    bar = st.progress(0)
                    total_linhas = len(df_raw)
                    novos_produtos = []

                    start_row = 1 
                    
                    for i in range(start_row, total_linhas):
                        try:
                            cod = str(df_raw.iloc[i, idx_barras]).replace('.0', '').strip()
                            nome_planilha = str(df_raw.iloc[i, idx_nome]).strip()
                            qtd = pd.to_numeric(df_raw.iloc[i, idx_qtd], errors='coerce')
                            
                            nome_norm = normalizar_texto(nome_planilha)

                            if cod and nome_norm and pd.notnull(qtd):
                                mask = df['código de barras'] == cod
                                
                                if mask.any():
                                    # ATUALIZA EXISTENTE
                                    idx = df[mask].index[0]
                                    qtd_antiga = df.at[idx, 'qtd.estoque']
                                    df.loc[mask, 'qtd.estoque'] = qtd
                                    # Log Auditoria
                                    if qtd_antiga != qtd:
                                        registrar_auditoria(prefixo, nome_norm, qtd_antiga, qtd, "Sincronização Planograma")

                                    if idx_preco != "(Não Atualizar Preço)":
                                        val_preco = pd.to_numeric(df_raw.iloc[i, idx_preco], errors='coerce')
                                        if pd.notnull(val_preco):
                                            df.loc[mask, 'preco_venda'] = val_preco
                                    alt += 1
                                else:
                                    # CRIA NOVO (CORREÇÃO CRÍTICA APLICADA)
                                    novo_preco_venda = 0.0
                                    if idx_preco != "(Não Atualizar Preço)":
                                        val_p = pd.to_numeric(df_raw.iloc[i, idx_preco], errors='coerce')
                                        if pd.notnull(val_p): novo_preco_venda = val_p
                                    
                                    novo_prod = {
                                        'código de barras': cod,
                                        'nome do produto': nome_norm,
                                        'qtd.estoque': qtd,
                                        'qtd_central': 0,
                                        'qtd_minima': 5,
                                        'validade': None,
                                        'status_compra': 'OK',
                                        'qtd_comprada': 0,
                                        'preco_custo': 0.0,
                                        'preco_venda': novo_preco_venda,
                                        'categoria': 'GERAL',
                                        'ultimo_fornecedor': '',
                                        'preco_sem_desconto': 0.0
                                    }
                                    novos_produtos.append(novo_prod)
                                    novos += 1
                        except: pass
                        bar.progress((i+1)/total_linhas)
                    
                    if novos_produtos:
                        df = pd.concat([df, pd.DataFrame(novos_produtos)], ignore_index=True)
                        registrar_auditoria(prefixo, "Vários", 0, len(novos_produtos), "Sincronização - Novos Produtos")
                    
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ Sucesso! {alt} produtos atualizados e {novos} NOVOS produtos cadastrados (incluindo a Coca Cola 2L!).")
                    if novos > 0:
                        st.balloons()
            except Exception as e: st.error(f"Erro: {e}")

    # 4. BAIXAR VENDAS (COM TRAVA DE DATA)
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Importar Vendas - {loja_atual}")
        tab_imp, tab_hist_vendas = st.tabs(["📂 Importar Arquivo", "📜 Histórico"])
        with tab_imp:
            st.info("💡 DICA: Use o filtro de data abaixo para ignorar vendas antigas que possam estar 'escondidas' no Excel.")
            
            # --- FILTRO DE DATA ---
            c_dt, c_hr = st.columns(2)
            # Pega data atual ajustada
            hora_padrao = obter_hora_manaus()
            data_corte = c_dt.date_input("🚫 Ignorar vendas ANTES do dia:", value=hora_padrao.date())
            # A hora padrão aqui já entra como "19:00" como sugestão, mas editável
            hora_corte = c_hr.time_input("⏰ E antes do horário:", value=datetime.strptime("19:00", "%H:%M").time(), step=60)
            data_hora_corte = datetime.combine(data_corte, hora_corte)
            st.warning(f"O sistema irá processar APENAS vendas feitas DEPOIS de: **{data_hora_corte.strftime('%d/%m/%Y %H:%M')}**")
            # ----------------------

            arquivo_vendas = st.file_uploader("📂 Relatório de Vendas", type=['xlsx', 'xls'], key="up_vendas")
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None)
                    st.dataframe(df_bruto.head(5), use_container_width=True)
                    linha_titulo = st.number_input("Número da linha dos TÍTULOS:", min_value=0, max_value=10, value=0)
                    arquivo_vendas.seek(0)
                    df_vendas_temp = pd.read_excel(arquivo_vendas, header=linha_titulo)
                    cols = df_vendas_temp.columns.tolist()
                    
                    c1, c2, c3, c4 = st.columns(4)
                    col_id = c1.selectbox("Coluna ID TRANSAÇÃO (Opcional)", ["(Ignorar)"] + cols)
                    col_nome = c2.selectbox("Coluna NOME?", cols)
                    col_qtd = c3.selectbox("Coluna QUANTIDADE?", cols)
                    col_data = c4.selectbox("Coluna DATA?", cols)
                    
                    if st.button("🚀 PROCESSAR VENDAS"):
                        if not df.empty:
                            atualizados = 0
                            ignorados_data = 0
                            ignorados_id = 0
                            novos_registros = []
                            novos_ids_processados = set()
                            bar = st.progress(0)
                            
                            # --- CORREÇÃO DE ORDEM CRONOLÓGICA ---
                            try:
                                df_vendas_temp[col_data] = pd.to_datetime(df_vendas_temp[col_data], dayfirst=True, errors='coerce')
                                df_vendas_temp = df_vendas_temp.sort_values(by=col_data, ascending=True)
                            except: pass
                            # -------------------------------------

                            total = len(df_vendas_temp)
                            for i, row in df_vendas_temp.iterrows():
                                # 1. Filtro de ID (Duplicidade)
                                if col_id != "(Ignorar)":
                                    id_venda = str(row[col_id]).strip()
                                    if id_venda in ids_processados or id_venda in novos_ids_processados:
                                        ignorados_id += 1
                                        continue
                                    
                                nome = str(row[col_nome]).strip()
                                qtd = pd.to_numeric(row[col_qtd], errors='coerce')
                                try:
                                    dt_v = pd.to_datetime(row[col_data], dayfirst=True)
                                    if pd.isna(dt_v): dt_v = obter_hora_manaus()
                                except: dt_v = obter_hora_manaus()

                                # 2. Filtro de Data (Segurança)
                                if dt_v < data_hora_corte:
                                    ignorados_data += 1
                                    continue # Pula essa linha

                                if pd.isna(qtd) or qtd <= 0: continue
                                
                                mask = (df['código de barras'].astype(str).str.contains(nome, na=False) |
                                        df['nome do produto'].astype(str).str.contains(nome, case=False, na=False))
                                if mask.any():
                                    idx = df[mask].index[0]
                                    antigo = df.at[idx, 'qtd.estoque']
                                    df.at[idx, 'qtd.estoque'] = antigo - qtd
                                    atualizados += 1
                                    novos_registros.append({
                                        "data_hora": dt_v, "produto": df.at[idx, 'nome do produto'],
                                        "qtd_vendida": qtd, "estoque_restante": df.at[idx, 'qtd.estoque']
                                    })
                                    if col_id != "(Ignorar)":
                                        novos_ids_processados.add(str(row[col_id]).strip())
                                        
                                bar.progress((i+1)/total)
                            
                            salvar_estoque(df, prefixo)
                            salvar_ids_processados(prefixo, novos_ids_processados) # Salva IDs
                            
                            if novos_registros:
                                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos_registros)], ignore_index=True)
                                salvar_vendas(df_vendas, prefixo)
                            
                            msg_final = f"✅ {atualizados} vendas baixadas com sucesso!"
                            if ignorados_data > 0:
                                msg_final += f"\n\n🛡️ {ignorados_data} vendas antigas ignoradas (Antes de {data_hora_corte.strftime('%H:%M')})."
                            if ignorados_id > 0:
                                msg_final += f"\n\n♻️ {ignorados_id} vendas duplicadas ignoradas."
                                
                            st.success(msg_final)
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist_vendas:
            # --- BOTÃO DE APAGAR HISTÓRICO ---
            if not df_vendas.empty:
                if st.button("🗑️ Apagar Histórico de Vendas", type="primary"):
                    df_vendas = pd.DataFrame(columns=['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'])
                    salvar_vendas(df_vendas, prefixo)
                    # Limpa IDs também
                    try: os.remove(f"{prefixo}_ids_vendas.csv")
                    except: pass
                    st.success("Histórico limpo com sucesso!")
                    st.rerun()
                st.divider()
                
                busca_vendas_hist = st.text_input("🔍 Buscar no Histórico de Vendas:", placeholder="Ex: oleo...", key="busca_vendas_hist")
                df_v_show = filtrar_dados_inteligente(df_vendas, 'produto', busca_vendas_hist)
                st.dataframe(df_v_show.sort_values(by="data_hora", ascending=False), use_container_width=True, hide_index=True)
            else:
                st.info("Histórico de vendas vazio.")

    # 5. GÔNDOLA
    elif modo == "🏠 Gôndola (Loja)":
        st.title(f"🏠 Gôndola - {loja_atual}")
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
                        cor_borda = "grey"
                        if row['qtd.estoque'] <= 0: cor_borda = "red"
                        elif row['qtd.estoque'] < row['qtd_minima']: cor_borda = "orange"
                        with st.container(border=True):
                            st.subheader(row['nome do produto'])
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
                                        salvar_estoque(df, prefixo)
                                        atualizar_casa_global(row['nome do produto'], df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                        registrar_auditoria(prefixo, row['nome do produto'], 0, q_tr, "Baixa Gôndola Mobile")
                                        st.success(f"Baixado {q_tr} un!")
                                        st.rerun()
                            else: st.warning("🚫 Casa Zerada (Sem estoque para baixar)")
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
                        st.divider()
                        
                        st.subheader("🚚 Transferência (Casa -> Loja)")
                        with st.form("form_transf_gondola"):
                            c_dt, c_hr, c_qtd = st.columns(3)
                            dt_transf = c_dt.date_input("Data da Transferência:", obter_hora_manaus().date())
                            # --- CORREÇÃO HORA ---
                            # Pega hora atual limpa
                            hora_atual = obter_hora_manaus().time().replace(second=0, microsecond=0)
                            hr_transf = c_hr.time_input("Hora:", value=hora_atual, step=60)
                            # ---------------------
                            
                            qtd_disponivel = int(df.at[idx, 'qtd_central'])
                            qtd_transf = c_qtd.number_input(f"Quantidade (Disp: {qtd_disponivel}):", min_value=0, max_value=qtd_disponivel, value=0)
                            
                            if st.form_submit_button("⬇️ CONFIRMAR TRANSFERÊNCIA"):
                                if qtd_transf > 0:
                                    df.at[idx, 'qtd.estoque'] += qtd_transf
                                    df.at[idx, 'qtd_central'] -= qtd_transf
                                    salvar_estoque(df, prefixo)
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
                                    data_final = datetime.combine(dt_transf, hr_transf)
                                    novo_mov = {'data_hora': data_final, 'produto': nome_prod, 'qtd_movida': qtd_transf}
                                    df_mov = pd.concat([df_mov, pd.DataFrame([novo_mov])], ignore_index=True)
                                    salvar_movimentacoes(df_mov, prefixo)
                                    registrar_auditoria(prefixo, nome_prod, 0, qtd_transf, "Transferência Gôndola Desktop")
                                    st.success(f"Sucesso! {qtd_transf} unid. transferidas em {data_final}. Casa sincronizada.")
                                    st.rerun()
                                else: st.warning("Quantidade inválida ou sem estoque na Casa.")
                        
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

    # 7. HISTÓRICO & PREÇOS (AGORA: INTELIGÊNCIA DE COMPRAS)
    elif modo == "💰 Inteligência de Compras (Histórico)":
        st.title("💰 Inteligência de Compras")
        
        tab_graf, tab_dados = st.tabs(["📊 Análise & Gráficos", "📜 Histórico Completo (Editar)"])
        
        with tab_graf:
            if df_hist.empty:
                st.info("Sem histórico suficiente para análises.")
            else:
                st.markdown("### 🏆 Ranking: Onde comprar mais barato?")
                
                # --- CRIA LISTA DE PRODUTOS COM CÓDIGO (INTELIGÊNCIA) ---
                # Garante que temos a coluna 'produto' como string
                df_hist['produto_str'] = df_hist['produto'].astype(str)
                
                # Se possível, vamos tentar recuperar o código de barras do produto atual
                # para exibir bonito no dropdown (COD - NOME)
                # Cruzamos com o DF atual para pegar o código
                if not df.empty:
                    # Cria um dicionário {Nome: Código}
                    mapa_codigos = dict(zip(df['nome do produto'], df['código de barras']))
                    # Cria uma coluna temporária no histórico para exibição
                    df_hist['display_combo'] = df_hist['produto_str'].map(mapa_codigos).fillna('?') + " - " + df_hist['produto_str']
                else:
                    df_hist['display_combo'] = df_hist['produto_str']

                lista_prods_hist = sorted(df_hist['display_combo'].unique())
                prod_sel_graf_raw = st.selectbox("Selecione um Produto para analisar:", lista_prods_hist)
                
                if prod_sel_graf_raw:
                    # Extrai o nome puro de volta para filtrar no histórico
                    # Se o formato for "COD - NOME", pegamos o NOME (parte depois do " - ")
                    if " - " in prod_sel_graf_raw:
                        nome_para_filtro = prod_sel_graf_raw.split(" - ", 1)[1]
                    else:
                        nome_para_filtro = prod_sel_graf_raw

                    # Filtra dados do produto usando o nome
                    df_prod = df_hist[df_hist['produto'] == nome_para_filtro].copy()
                    
                    # Gráfico 1: Preço Médio por Fornecedor
                    if not df_prod.empty:
                        df_ranking = df_prod.groupby('fornecedor')['preco_pago'].mean().sort_values()
                        st.bar_chart(df_ranking)
                        st.caption("Preço Médio pago por Fornecedor (Menor é melhor)")
                        
                        st.divider()
                        
                        # Gráfico 2: Evolução no Tempo
                        st.markdown("### 📈 Evolução do Preço no Tempo")
                        df_evolucao = df_prod.sort_values(by='data')
                        st.line_chart(df_evolucao, x='data', y='preco_pago')
        
        with tab_dados:
            if not df_hist.empty:
                busca_hist_precos = st.text_input("🔍 Buscar no Histórico:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
                df_hist_visual = df_hist.copy()
                if busca_hist_precos:
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'produto', busca_hist_precos)
                    if df_hist_visual.empty: 
                        df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
                
                # --- CRIAÇÃO DO MAPA DE CÓDIGOS PARA VISUALIZAÇÃO NA TABELA ---
                mapa_ean = dict(zip(df['nome do produto'], df['código de barras']))
                df_hist_visual['código_barras'] = df_hist_visual['produto'].map(mapa_ean)
                # Reorganiza colunas
                cols = ['data', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_sem_desconto', 'desconto_total_money', 'preco_pago', 'total_gasto', 'obs_importacao']
                cols = [c for c in cols if c in df_hist_visual.columns]
                df_hist_visual = df_hist_visual[cols]
                # ----------------------------------------------------

                st.info("✅ Você pode editar ou **excluir** linhas (selecione a linha e aperte Delete no teclado).")
                df_editado = st.data_editor(
                    df_hist_visual.sort_values(by='data', ascending=False), 
                    use_container_width=True, 
                    key="editor_historico_geral",
                    num_rows="dynamic", 
                    column_config={
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
                        df_hist = df_hist.drop(indices_removidos)
                        st.warning(f"🗑️ {len(indices_removidos)} registros excluídos permanentemente.")
                    
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
                    st.success("Histórico salvo e sincronizado!")
                    st.rerun()
            else: st.info("Sem histórico de compras.")

    # 8. ESTOQUE CENTRAL
    elif modo == "🏡 Estoque Central (Casa)":
        st.title(f"🏡 Estoque Central (Casa) - {loja_atual}")
        tab_ver, tab_gerenciar = st.tabs(["📋 Visualizar & Editar", "✍️ Gerenciar Entrada Manual"])
        with tab_ver:
            if not df.empty:
                if usar_modo_mobile:
                    st.info("📱 Modo Celular (Edição Rápida)")
                    busca_central = st.text_input("🔍 Buscar na Casa:", placeholder="Ex: arroz...")
                    df_show = filtrar_dados_inteligente(df, 'nome do produto', busca_central)
                    for idx, row in df_show.iterrows():
                        with st.container(border=True):
                            st.write(f"📝 {row['código de barras']} | **{row['nome do produto']}**")
                            col1, col2 = st.columns(2)
                            nova_qtd = col1.number_input(f"Qtd Casa:", value=int(row['qtd_central']), key=f"q_{idx}")
                            novo_custo = col2.number_input(f"Custo:", value=float(row['preco_custo']), key=f"c_{idx}")
                            if st.button(f"💾 Salvar {row['nome do produto']}", key=f"btn_{idx}"):
                                qtd_antiga = df.at[idx, 'qtd_central']
                                df.at[idx, 'qtd_central'] = nova_qtd
                                df.at[idx, 'preco_custo'] = novo_custo
                                salvar_estoque(df, prefixo)
                                atualizar_casa_global(row['nome do produto'], nova_qtd, novo_custo, None, None, prefixo)
                                registrar_auditoria(prefixo, row['nome do produto'], qtd_antiga, nova_qtd, "Edição Mobile Casa")
                                st.success("Salvo!")
                                st.rerun()
                else:
                    st.info("✏️ Se precisar corrigir o estoque, edite abaixo e clique em SALVAR.")
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
                            st.warning(f"{len(indices_removidos)} itens removidos permanentemente.")
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
                        st.success("Estoque atualizado e sincronizado em todas as lojas!")
                        st.rerun()
        with tab_gerenciar:
            st.info("Adicione mercadoria manualmente (ex: compra sem pedido) ou edite dados.")
            if not df.empty:
                # --- LISTA VISUAL: COD - NOME ---
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
                            # --- CORREÇÃO HORA ---
                            hr_reg = c_hr.time_input("Hora:", value=obter_hora_manaus().time().replace(second=0, microsecond=0), step=60) # STEP 60
                            # ---------------------
                            
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
                                    hist = {'data': dt_full, 'produto': c_nome.upper().strip(), 'fornecedor': c_forn, 'qtd': qtd_input, 'preco_pago': novo_custo, 'total_gasto': qtd_input * novo_custo}
                                    salvar_historico(pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True), prefixo)
                                    registrar_auditoria(prefixo, c_nome, qtd_antes_audit, df.at[idx_prod, 'qtd_central'], "Entrada Manual Casa")
                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input
                                    msg_acao = f"Estoque corrigido para {qtd_input}"
                                    registrar_auditoria(prefixo, c_nome, qtd_antes_audit, qtd_input, "Correção Manual Casa")
                                
                                salvar_estoque(df, prefixo)
                                atualizar_casa_global(c_nome.upper().strip(), df.at[idx_prod, 'qtd_central'], novo_custo, novo_venda, pd.to_datetime(nova_val) if nova_val else None, prefixo)
                                st.success(f"✅ {msg_acao} e sincronizado com outras lojas!")
                                st.rerun()

    # 9. GERAL
    elif modo == "📋 Tabela Geral":
        st.title("📋 Visão Geral (Editável)")
        if not df.empty:
            st.info("💡 DICA: Se um produto veio com nome errado e sem código: coloque o código correto nele aqui e clique no botão 'CORRIGIR E UNIFICAR' abaixo.")
            busca_geral = st.text_input("🔍 Buscar na Tabela Geral:", placeholder="Ex: oleo concordia...", key="busca_geral")
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
                        st.warning(f"🗑️ {len(indices_removidos)} produtos excluídos permanentemente.")
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
                    st.success("Tabela Geral atualizada e sincronizada!")
                    st.rerun()
            with c2:
                if st.button("🔮 CORRIGIR NOMES E UNIFICAR (Pelo Código)"):
                    df.update(df_edit)
                    qtd_antes = len(df)
                    df = unificar_produtos_por_codigo(df)
                    qtd_depois = len(df)
                    salvar_estoque(df, prefixo)
                    st.success(f"✅ Mágica feita! {qtd_antes - qtd_depois} produtos duplicados foram unidos e os nomes corrigidos.")
                    st.balloons()
                    st.rerun()
