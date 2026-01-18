import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import xml.etree.ElementTree as ET
import unicodedata
import difflib

# Configuração da página
st.set_page_config(page_title="Gestão Multi-Lojas", layout="wide", page_icon="🏪")

# ==============================================================================
# 🕒 AJUSTE DE FUSO HORÁRIO (MANAUS / AMAZONAS / CUIABÁ: UTC-4)
# ==============================================================================
def obter_hora_manaus():
    """Retorna a data e hora atual ajustada para o fuso de Manaus (-4h em relação ao UTC)."""
    return datetime.utcnow() - timedelta(hours=4)

# ==============================================================================
# 🆕 FUNÇÕES DE LIMPEZA E PADRONIZAÇÃO (CRUCIAIS PARA A BUSCA)
# ==============================================================================
def normalizar_texto(texto):
    """
    Padroniza o texto para salvar no banco:
    - Tudo Maiúsculo
    - Sem acentos
    - Sem espaços nas pontas (Trim)
    """
    if not isinstance(texto, str):
        return str(texto) if pd.notnull(texto) else ""
    # Remove acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    # Maiúsculo e sem espaços extras
    return texto.upper().strip()

def normalizar_para_busca(texto):
    """Limpa o texto mantendo apenas letras e números importantes para COMPARAÇÃO."""
    if not isinstance(texto, str): return ""
    return normalizar_texto(texto)

def calcular_pontuacao(nome_xml, nome_sistema):
    """
    Calcula pontuação baseada em palavras iguais.
    Evita que '2L' confunda com '200ML'.
    """
    # Separa "REGENTE GUARANA 2L" em {"REGENTE", "GUARANA", "2L"}
    set_xml = set(normalizar_para_busca(nome_xml).split())
    set_sis = set(normalizar_para_busca(nome_sistema).split())
    
    # Interseção: Palavras que existem nos dois
    comum = set_xml.intersection(set_sis)
    
    if not comum: return 0.0
    
    # Pontuação base: % de palavras encontradas
    total = set_xml.union(set_sis)
    score = len(comum) / len(total)
    
    # BÔNUS CRÍTICO: Se a unidade de medida (2L, 200ML, 1KG) for EXATAMENTE igual
    for palavra in comum:
        if any(u in palavra for u in ['L', 'ML', 'KG', 'G', 'M']): 
            if any(c.isdigit() for c in palavra): # Tem número junto? (ex: 2L)
                score += 0.5 # Bônus alto para garantir match de volume
                
    return score

def encontrar_melhor_match(nome_buscado, lista_opcoes, cutoff=0.3):
    """Encontra o melhor produto no sistema baseado em palavras inteiras."""
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
    """Agrupa produtos pelo código de barras."""
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
        # Detecta se é CSV ou Excel para ler corretamente
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
# 🏢 CONFIGURAÇÃO E CARREGAMENTO DE DADOS
# ==============================================================================

st.sidebar.title("🏢 Seleção da Loja")
loja_atual = st.sidebar.selectbox(
    "Gerenciar qual unidade?",
    ["Loja 1 (Principal)", "Loja 2 (Filial)", "Loja 3 (Extra)"]
)

st.sidebar.markdown("---")
usar_modo_mobile = st.sidebar.checkbox("📱 Modo Celular (Cartões)", value=True, help="Melhora a visualização para iPhone/Android")
st.sidebar.markdown("---")

if loja_atual == "Loja 1 (Principal)": prefixo = "loja1"
elif loja_atual == "Loja 2 (Filial)": prefixo = "loja2"
else: prefixo = "loja3"

# --- FUNÇÕES AUXILIARES ---
def formatar_moeda_br(valor):
    """Transforma 1031.28 em 1.031,28"""
    try:
        # Formata padrão americano (com vírgula no milhar)
        s = f"{valor:,.2f}"
        # Troca vírgula por X, ponto por vírgula, X por ponto
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return f"{valor:.2f}"

def filtrar_dados_inteligente(df, coluna_busca, texto_busca):
    if not texto_busca: return df
    # Busca simples (substring)
    mask = df[coluna_busca].astype(str).apply(lambda x: normalizar_para_busca(texto_busca) in normalizar_para_busca(x))
    return df[mask]

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
                    df_outra.at[idx, 'qtd_central'] = qtd_nova_casa
                    if novo_custo is not None: df_outra.at[idx, 'preco_custo'] = novo_custo
                    if novo_venda is not None: df_outra.at[idx, 'preco_venda'] = novo_venda
                    if nova_validade is not None: df_outra.at[idx, 'validade'] = nova_validade
                    df_outra.to_excel(arquivo, index=False)
            except Exception: pass

# --- FUNÇÕES DE CARREGAMENTO (AJUSTE: FORÇA NORMALIZAÇÃO NO LOAD) ---
def inicializar_arquivos(prefixo):
    arquivos = {
        f"{prefixo}_estoque.xlsx": [
            'código de barras', 'nome do produto', 'qtd.estoque', 'qtd_central', 
            'qtd_minima', 'validade', 'status_compra', 'qtd_comprada', 
            'preco_custo', 'preco_venda', 'categoria', 'ultimo_fornecedor', 'preco_sem_desconto'
        ],
        f"{prefixo}_historico_compras.xlsx": [
            'data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto'
        ],
        f"{prefixo}_movimentacoes.xlsx": ['data_hora', 'produto', 'qtd_movida'],
        f"{prefixo}_vendas.xlsx": ['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'],
        f"{prefixo}_lista_compras.xlsx": ['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status']
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
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['ultimo_fornecedor'] = df['ultimo_fornecedor'].fillna('')
        df['código de barras'] = df['código de barras'].apply(lambda x: str(x).replace('.0', '').strip() if pd.notnull(x) else "")
        
        # --- CORREÇÃO IMPORTANTE: LIMPEZA NO CARREGAMENTO ---
        # Garante que 'coca cola ' vire 'COCA COLA' sem espaços para aparecer na busca
        df['nome do produto'] = df['nome do produto'].apply(lambda x: normalizar_texto(str(x)))
        
        df['validade'] = pd.to_datetime(df['validade'], dayfirst=True, errors='coerce')
        return df
    except: return pd.DataFrame()

def carregar_historico(prefixo_arquivo):
    try:
        df_h = pd.read_excel(f"{prefixo_arquivo}_historico_compras.xlsx")
        df_h['data'] = pd.to_datetime(df_h['data'], errors='coerce')
        if 'numero_nota' not in df_h.columns: df_h['numero_nota'] = ""
        if 'desconto_total_money' not in df_h.columns:
            if 'desconto_obtido' in df_h.columns:
                df_h['desconto_total_money'] = df_h['desconto_obtido'] * df_h['qtd']
            else:
                df_h['desconto_total_money'] = 0.0
        if 'preco_sem_desconto' not in df_h.columns: df_h['preco_sem_desconto'] = 0.0
        mask_zerado = (df_h['preco_sem_desconto'] == 0) & (df_h['preco_pago'] > 0)
        df_h.loc[mask_zerado, 'preco_sem_desconto'] = df_h.loc[mask_zerado, 'preco_pago']
        return df_h
    except: return pd.DataFrame(columns=['data', 'produto', 'fornecedor', 'qtd', 'preco_pago', 'total_gasto', 'numero_nota', 'desconto_total_money', 'preco_sem_desconto'])

def carregar_movimentacoes(prefixo_arquivo):
    try:
        df_m = pd.read_excel(f"{prefixo_arquivo}_movimentacoes.xlsx")
        df_m['data_hora'] = pd.to_datetime(df_m['data_hora'], errors='coerce')
        return df_m
    except: return pd.DataFrame(columns=['data_hora', 'produto', 'qtd_movida'])

def carregar_vendas(prefixo_arquivo):
    try:
        df_v = pd.read_excel(f"{prefixo_arquivo}_vendas.xlsx")
        df_v['data_hora'] = pd.to_datetime(df_v['data_hora'], errors='coerce')
        return df_v
    except: return pd.DataFrame(columns=['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'])

def carregar_lista_compras(prefixo_arquivo):
    try: return pd.read_excel(f"{prefixo_arquivo}_lista_compras.xlsx")
    except: return pd.DataFrame(columns=['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'])

# --- FUNÇÃO XML INTELIGENTE (ADAPTADA PARA NOVO FORMATO) ---
def ler_xml_nfe(arquivo_xml, df_referencia):
    tree = ET.parse(arquivo_xml)
    root = tree.getroot()
    def tag_limpa(element): return element.tag.split('}')[-1]

    dados_nota = {'numero': '', 'fornecedor': '', 'data': obter_hora_manaus(), 'itens': []}

    # Prepara memória da base oficial para busca
    lista_nomes_ref = []
    dict_ref_ean = {}
    if not df_referencia.empty:
        for idx, row in df_referencia.iterrows():
            nm = normalizar_texto(row['nome do produto'])
            ean = str(row['código de barras']).strip()
            dict_ref_ean[nm] = ean
            lista_nomes_ref.append(nm)

    # === CASO 1: XML PERSONALIZADO (NotaFiscal) ===
    if tag_limpa(root) == 'NotaFiscal':
        info = root.find('Info')
        if info is not None:
            dados_nota['numero'] = info.find('NumeroNota').text if info.find('NumeroNota') is not None else ""
            dados_nota['fornecedor'] = info.find('Fornecedor').text if info.find('Fornecedor') is not None else ""
            try:
                dados_nota['data'] = datetime.strptime(info.find('DataCompra').text, '%d/%m/%Y')
            except: pass
        
        produtos = root.findall('.//Produtos/Item')
        for item_xml in produtos:
            item = {'codigo_interno': '', 'ean': '', 'nome': '', 'qtd': 0.0, 'preco_un_liquido': 0.0, 'preco_un_bruto': 0.0, 'desconto_total_item': 0.0}
            
            nome_raw = item_xml.find('Nome').text
            qtd_raw = float(item_xml.find('Quantidade').text)
            val_final = float(item_xml.find('ValorPagoFinal').text) # Total PAGO (Líquido)
            desc_val = float(item_xml.find('ValorDesconto').text) # Desconto Total
            cod_barras = item_xml.find('CodigoBarras').text

            item['nome'] = normalizar_texto(nome_raw)
            item['qtd'] = qtd_raw
            item['ean'] = cod_barras if cod_barras else ""
            item['codigo_interno'] = item['ean']
            item['desconto_total_item'] = desc_val

            if qtd_raw > 0:
                # Reverte cálculo para achar o bruto (compatibilidade)
                item['preco_un_liquido'] = val_final / qtd_raw
                item['preco_un_bruto'] = (val_final + desc_val) / qtd_raw
            
            # Lógica de Busca Inteligente (Igual ao original)
            ean_xml = str(item['ean']).strip()
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                if lista_nomes_ref:
                    melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                    if melhor_nome:
                        item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            
            dados_nota['itens'].append(item)
            
        return dados_nota

    # === CASO 2: XML PADRÃO NFE (CÓDIGO ORIGINAL) ===
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
            vProd = 0.0
            vDesc = 0.0
            qCom = 0.0
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
            
            # Lógica de Busca XML (EAN)
            ean_xml = str(item['ean']).strip()
            if ean_xml in ['SEM GTIN', '', 'None', 'NAN']:
                item['ean'] = item['codigo_interno']
                if lista_nomes_ref:
                    # Usa a busca inteligente também aqui
                    melhor_nome, _ = encontrar_melhor_match(item['nome'], lista_nomes_ref)
                    if melhor_nome:
                        item['ean'] = dict_ref_ean.get(melhor_nome, item['codigo_interno'])
            
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

if df is not None:
    st.sidebar.title("🏪 Menu")
    modo = st.sidebar.radio("Navegar:", [
        "📊 Dashboard (Visão Geral)",
        "🚚 Transferência em Massa (Picklist)",
        "📝 Lista de Compras (Planejamento)",
        "🆕 Cadastrar Produto", 
        "📥 Importar XML (Associação Inteligente)", 
        "⚙️ Configurar Base Oficial",
        "🔄 Sincronizar (Planograma)",
        "📉 Baixar Vendas (Do Relatório)",
        "🏠 Gôndola (Loja)", 
        "🛒 Fornecedor (Compras)", 
        "💰 Histórico & Preços",
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
            # AQUI ESTÁ A CORREÇÃO DA FORMATAÇÃO DO DINHEIRO
            c2.metric("💰 Valor Investido", f"R$ {formatar_moeda_br(valor_estoque)}")
            c3.metric("🚨 Vencendo (5 dias)", len(df_critico))
            c4.metric("⚠️ Atenção (10 dias)", len(df_atencao))
            st.divider()
            
            baixo_estoque = df[(df['qtd.estoque'] + df['qtd_central']) <= df['qtd_minima']]
            if not baixo_estoque.empty:
                st.warning(f"🚨 Existem {len(baixo_estoque)} produtos com estoque baixo! Vá em 'Lista de Compras' para ver.")
                
            if not df_critico.empty:
                st.error("🚨 Produtos Vencendo! (Edite a data abaixo para corrigir)")
                # AQUI ESTÁ A CORREÇÃO PARA EDITAR A DATA E REMOVER DO ALERTA
                df_critico_edit = st.data_editor(
                    df_critico[['nome do produto', 'validade', 'qtd.estoque']],
                    use_container_width=True,
                    key="editor_vencimento"
                )
                if st.button("💾 ATUALIZAR DATAS DE VENCIMENTO"):
                    df.update(df_critico_edit)
                    salvar_estoque(df, prefixo)
                    st.success("Datas atualizadas com sucesso!")
                    st.rerun()

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
                                    df.at[idx, 'qtd_central'] -= qtd_pick
                                    df.at[idx, 'qtd.estoque'] += qtd_pick
                                    log_movs.append({'data_hora': obter_hora_manaus(), 'produto': nome_prod, 'qtd_movida': qtd_pick})
                                    atualizar_casa_global(nome_prod, df.at[idx, 'qtd_central'], None, None, None, prefixo)
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
                    for idx, row in df_lista_compras.iterrows():
                        with st.container(border=True):
                            st.write(f"**{row['produto']}**")
                            c1, c2 = st.columns(2)
                            c1.caption(f"Qtd: {int(row['qtd_sugerida'])}")
                            c2.caption(f"Status: {row['status']}")
                else:
                    st.dataframe(df_lista_compras, use_container_width=True)
                c_del, c_pdf = st.columns(2)
                if c_del.button("🗑️ Limpar Lista Inteira (Após Comprar)"):
                    df_lista_compras = pd.DataFrame(columns=['produto', 'qtd_sugerida', 'fornecedor', 'custo_previsto', 'data_inclusao', 'status'])
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
                                    'qtd_sugerida': row['qtd_minima'] * 3,
                                    'fornecedor': row['ultimo_fornecedor'],
                                    'custo_previsto': row['preco_custo'],
                                    'data_inclusao': obter_hora_manaus().strftime("%d/%m/%Y"),
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
                lista_prods = [""] + sorted(df['nome do produto'].astype(str).unique().tolist())
                prod_man = st.selectbox("Produto:", lista_prods)
                qtd_man = st.number_input("Qtd a Comprar:", min_value=1, value=10)
                obs_man = st.text_input("Fornecedor/Obs:", placeholder="Ex: Atacadão")
                if st.form_submit_button("Adicionar à Lista"):
                    if prod_man:
                        preco_ref = 0.0
                        mask = df['nome do produto'] == prod_man
                        if mask.any(): preco_ref = df.loc[mask, 'preco_custo'].values[0]
                        novo_item = {'produto': prod_man, 'qtd_sugerida': qtd_man, 'fornecedor': obs_man, 'custo_previsto': preco_ref, 'data_inclusao': obter_hora_manaus().strftime("%d/%m/%Y"), 'status': 'Manual'}
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
                    st.success("Cadastrado!")
                    st.rerun()

    # 2.5 IMPORTAR XML (COM NOVA LÓGICA DE MATCH E REAÇÃO EM TEMPO REAL)
    elif modo == "📥 Importar XML (Associação Inteligente)":
        st.title(f"📥 Importar XML da Nota Fiscal")
        st.markdown("O sistema tentará encontrar os produtos. **Confirme se o vínculo está correto antes de salvar.**")
        if df_oficial.empty:
            st.warning("⚠️ DICA: Vá em 'Configurar Base Oficial' e suba sua lista de produtos para melhorar a identificação automática.")
        arquivo_xml = st.file_uploader("Arraste o XML aqui", type=['xml'])
        if arquivo_xml:
            try:
                dados = ler_xml_nfe(arquivo_xml, df_oficial)
                st.success(f"Nota Fiscal: **{dados['numero']}** | Fornecedor: **{dados['fornecedor']}**")
                st.markdown("---")
                st.subheader("🛠️ Conferência e Cálculo de Descontos")
                
                # Lista COMPLETA e NORMALIZADA para o dropdown
                lista_produtos_sistema = ["(CRIAR NOVO)"] + sorted(df['nome do produto'].astype(str).unique().tolist())
                escolhas = {}
                
                for i, item in enumerate(dados['itens']):
                    ean_xml = str(item.get('ean', '')).strip()
                    nome_xml = str(item['nome']).strip()
                    qtd_xml = item['qtd']
                    
                    # DADOS FINANCEIROS
                    p_bruto = item['preco_un_bruto']
                    p_liq = item['preco_un_liquido']
                    desc_total = item['desconto_total_item']
                    
                    match_inicial = "(CRIAR NOVO)"
                    tipo_match = "Nenhum"
                    ean_sistema = ""
                    
                    if not df.empty:
                        # 1. Tenta Código de Barras
                        mask_ean = df['código de barras'].astype(str) == ean_xml
                        if mask_ean.any():
                            match_inicial = df.loc[mask_ean, 'nome do produto'].values[0]
                            ean_sistema = df.loc[mask_ean, 'código de barras'].values[0]
                            tipo_match = "Código de Barras (Exato)"
                        else:
                            # 2. Tenta Nova Busca por Palavras (Smart Match)
                            lista_nomes = df['nome do produto'].astype(str).tolist()
                            melhor_nome, tipo_encontrado = encontrar_melhor_match(nome_xml, lista_nomes)
                            
                            if melhor_nome:
                                match_inicial = melhor_nome
                                tipo_match = tipo_encontrado
                                mask_nome = df['nome do produto'].astype(str) == match_inicial
                                if mask_nome.any():
                                    ean_sistema = df.loc[mask_nome, 'código de barras'].values[0]

                    c1, c2 = st.columns([1, 1])
                    with c1:
                        st.markdown(f"📄 XML: **{nome_xml}**")
                        st.caption(f"EAN XML: `{ean_xml}` | Qtd: {int(qtd_xml)}")
                        st.markdown(f"💰 Tabela: R$ {p_bruto:.2f} | **Pago (Desc): R$ {p_liq:.2f}**")
                        if desc_total > 0:
                            st.caption(f"📉 Desconto Total na nota: R$ {desc_total:.2f}")

                    with c2:
                        idx_inicial = 0
                        match_inicial_str = str(match_inicial)
                        if match_inicial_str in lista_produtos_sistema:
                            idx_inicial = lista_produtos_sistema.index(match_inicial_str)
                            
                        escolha_usuario = st.selectbox(
                            f"Vincular ao Sistema ({tipo_match}):", 
                            lista_produtos_sistema, 
                            index=idx_inicial,
                            key=f"sel_{i}"
                        )
                        
                        # --- CORREÇÃO DE VISUALIZAÇÃO EM TEMPO REAL ---
                        # Busca o código de barras do item selecionado na hora
                        ean_visual_sistema = "---"
                        if escolha_usuario != "(CRIAR NOVO)":
                            row_sistema = df[df['nome do produto'] == escolha_usuario]
                            if not row_sistema.empty:
                                ean_visual_sistema = row_sistema.iloc[0]['código de barras']
                            st.info(f"🆔 EAN no Sistema: **{ean_visual_sistema}**")

                    escolhas[i] = escolha_usuario
                    st.divider()

                if st.button("✅ CONFIRMAR E SALVAR ESTOQUE"):
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
                        qtd_central_final = 0
                        
                        if produto_escolhido == "(CRIAR NOVO)":
                            novo_prod = {
                                'código de barras': ean_xml, 
                                'nome do produto': nome_xml.upper(),
                                'qtd.estoque': 0, 'qtd_central': qtd_xml, 'qtd_minima': 5,
                                'validade': None, 'status_compra': 'OK', 'qtd_comprada': 0,
                                'preco_custo': preco_pago, 'preco_venda': preco_pago * 2,
                                'categoria': 'GERAL', 'ultimo_fornecedor': dados['fornecedor'],
                                'preco_sem_desconto': preco_sem_desc
                            }
                            df = pd.concat([df, pd.DataFrame([novo_prod])], ignore_index=True)
                            criados_cont += 1
                            nome_final = nome_xml.upper()
                            qtd_central_final = qtd_xml
                        else:
                            mask = df['nome do produto'].astype(str) == str(produto_escolhido)
                            if mask.any():
                                idx = df[mask].index[0]
                                df.at[idx, 'qtd_central'] += qtd_xml
                                df.at[idx, 'preco_custo'] = preco_pago
                                df.at[idx, 'preco_sem_desconto'] = preco_sem_desc
                                df.at[idx, 'ultimo_fornecedor'] = dados['fornecedor']
                                atualizados_cont += 1
                                nome_final = produto_escolhido
                                qtd_central_final = df.at[idx, 'qtd_central']
                        
                        atualizar_casa_global(nome_final, qtd_central_final, preco_pago, None, None, prefixo)
                        
                        novos_hist.append({
                            'data': obter_hora_manaus(), 'produto': nome_final, 'fornecedor': dados['fornecedor'], 
                            'qtd': qtd_xml, 'preco_pago': preco_pago, 'total_gasto': qtd_xml * preco_pago,
                            'numero_nota': dados['numero'], 'desconto_total_money': desc_total_val, 'preco_sem_desconto': preco_sem_desc
                        })
                    
                    salvar_estoque(df, prefixo)
                    if novos_hist:
                        df_hist = pd.concat([df_hist, pd.DataFrame(novos_hist)], ignore_index=True)
                        salvar_historico(df_hist, prefixo)
                    
                    st.success(f"✅ Processado! {criados_cont} novos, {atualizados_cont} atualizados e CASA sincronizada.")
                    st.balloons()
                    st.rerun()
            except Exception as e: st.error(f"Erro ao ler XML: {e}")

    # 4. BAIXAR VENDAS
    elif modo == "📉 Baixar Vendas (Do Relatório)":
        st.title(f"📉 Importar Vendas - {loja_atual}")
        tab_imp, tab_hist_vendas = st.tabs(["📂 Importar Arquivo", "📜 Histórico"])
        with tab_imp:
            arquivo_vendas = st.file_uploader("📂 Relatório de Vendas", type=['xlsx', 'xls'], key="up_vendas")
            if arquivo_vendas:
                try:
                    df_bruto = pd.read_excel(arquivo_vendas, header=None)
                    st.dataframe(df_bruto.head(5), use_container_width=True)
                    linha_titulo = st.number_input("Número da linha dos TÍTULOS:", min_value=0, max_value=10, value=0)
                    arquivo_vendas.seek(0)
                    df_vendas_temp = pd.read_excel(arquivo_vendas, header=linha_titulo)
                    cols = df_vendas_temp.columns.tolist()
                    c1, c2, c3 = st.columns(3)
                    col_nome = c1.selectbox("Coluna NOME?", cols)
                    col_qtd = c2.selectbox("Coluna QUANTIDADE?", cols)
                    col_data = c3.selectbox("Coluna DATA?", cols)
                    if st.button("🚀 PROCESSAR VENDAS"):
                        if not df.empty:
                            atualizados = 0
                            novos_registros = []
                            bar = st.progress(0)
                            
                            # --- CORREÇÃO DE ORDEM CRONOLÓGICA ---
                            try:
                                df_vendas_temp[col_data] = pd.to_datetime(df_vendas_temp[col_data], dayfirst=True, errors='coerce')
                                df_vendas_temp = df_vendas_temp.sort_values(by=col_data, ascending=True)
                            except: pass
                            # -------------------------------------

                            total = len(df_vendas_temp)
                            for i, row in df_vendas_temp.iterrows():
                                nome = str(row[col_nome]).strip()
                                qtd = pd.to_numeric(row[col_qtd], errors='coerce')
                                try:
                                    dt_v = pd.to_datetime(row[col_data], dayfirst=True)
                                    if pd.isna(dt_v): dt_v = obter_hora_manaus()
                                except: dt_v = obter_hora_manaus()
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
                                bar.progress((i+1)/total)
                            salvar_estoque(df, prefixo)
                            if novos_registros:
                                df_vendas = pd.concat([df_vendas, pd.DataFrame(novos_registros)], ignore_index=True)
                                salvar_vendas(df_vendas, prefixo)
                            st.success(f"✅ {atualizados} vendas baixadas!")
                except Exception as e: st.error(f"Erro: {e}")
        with tab_hist_vendas:
            # --- BOTÃO DE APAGAR HISTÓRICO (NOVO) ---
            if not df_vendas.empty:
                if st.button("🗑️ Apagar Histórico de Vendas", type="primary"):
                    df_vendas = pd.DataFrame(columns=['data_hora', 'produto', 'qtd_vendida', 'estoque_restante'])
                    salvar_vendas(df_vendas, prefixo)
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
                        if df.at[idx, 'qtd_central'] > 0:
                            st.subheader("🚚 Transferência (Casa -> Loja)")
                            with st.form("form_transf_gondola"):
                                c_dt, c_hr, c_qtd = st.columns(3)
                                dt_transf = c_dt.date_input("Data da Transferência:", obter_hora_manaus().date())
                                hr_transf = c_hr.time_input("Hora:", obter_hora_manaus().time())
                                qtd_transf = c_qtd.number_input(f"Quantidade (Máx: {int(df.at[idx, 'qtd_central'])}):", min_value=0, max_value=int(df.at[idx, 'qtd_central']), value=0)
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
                                        st.success(f"Sucesso! {qtd_transf} unid. transferidas em {data_final}. Casa sincronizada.")
                                        st.rerun()
                                    else: st.info("Sem estoque na Casa para transferir.")
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
                                df.at[idx, 'qtd.estoque'] = n_qtd_loja
                                df.at[idx, 'validade'] = pd.to_datetime(n_val) if n_val else None
                                salvar_estoque(df, prefixo)
                                st.success("Atualizado em todo o sistema!")
                                st.rerun()
                with tab_hist:
                    if not df_mov.empty:
                        busca_gondola_hist = st.text_input("🔍 Buscar no Histórico de Gôndola:", placeholder="Ex: oleo...", key="busca_gondola_hist")
                        df_mov_show = filtrar_dados_inteligente(df_mov, 'produto', busca_gondola_hist)
                        st.dataframe(df_mov_show.sort_values(by='data_hora', ascending=False), use_container_width=True, hide_index=True)

    # 6. FORNECEDOR
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
                    dt_compra = c_dt.date_input("Data da Compra:", obter_hora_manaus().date())
                    hr_compra = c_hr.time_input("Hora da Compra:", obter_hora_manaus().time())
                    forn_compra = st.text_input("Fornecedor desta compra:", value=df.at[idx, 'ultimo_fornecedor'])
                    c1, c2, c3 = st.columns(3)
                    qtd = c1.number_input("Qtd Chegada:", value=int(df.at[idx, 'qtd_comprada']))
                    custo = c2.number_input("Preço Pago (UN):", value=float(df.at[idx, 'preco_custo']), format="%.2f")
                    venda = c3.number_input("Novo Preço Venda:", value=float(df.at[idx, 'preco_venda']), format="%.2f")
                    if st.form_submit_button("✅ ENTRAR NO ESTOQUE"):
                        df.at[idx, 'qtd_central'] += qtd
                        df.at[idx, 'preco_custo'] = custo
                        df.at[idx, 'preco_venda'] = venda
                        df.at[idx, 'status_compra'] = 'OK'
                        df.at[idx, 'qtd_comprada'] = 0
                        df.at[idx, 'ultimo_fornecedor'] = forn_compra 
                        salvar_estoque(df, prefixo)
                        atualizar_casa_global(item, df.at[idx, 'qtd_central'], custo, venda, None, prefixo)
                        dt_full = datetime.combine(dt_compra, hr_compra)
                        hist = {'data': dt_full, 'produto': item, 'fornecedor': forn_compra, 'qtd': qtd, 'preco_pago': custo, 'total_gasto': qtd*custo}
                        salvar_historico(pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True), prefixo)
                        st.success("Estoque atualizado e Casa sincronizada!")
                        st.rerun()
        else: st.success("Sem compras pendentes.")

    # 7. HISTÓRICO & PREÇOS
    elif modo == "💰 Histórico & Preços":
        st.title("💰 Histórico & Preços")
        if not df_hist.empty:
            busca_hist_precos = st.text_input("🔍 Buscar no Histórico de Compras:", placeholder="Digite o nome, fornecedor...", key="busca_hist_precos")
            df_hist_visual = df_hist
            if busca_hist_precos:
                df_hist_visual = filtrar_dados_inteligente(df_hist, 'produto', busca_hist_precos)
                if df_hist_visual.empty: 
                    df_hist_visual = filtrar_dados_inteligente(df_hist, 'fornecedor', busca_hist_precos)
            
            # --- CRIAÇÃO DO MAPA DE CÓDIGOS PARA VISUALIZAÇÃO ---
            mapa_ean = dict(zip(df['nome do produto'], df['código de barras']))
            df_hist_visual['código_barras'] = df_hist_visual['produto'].map(mapa_ean)
            # Reorganiza colunas para o código aparecer no começo
            cols = ['data', 'código_barras', 'produto', 'fornecedor', 'qtd', 'preco_sem_desconto', 'desconto_total_money', 'preco_pago', 'total_gasto']
            cols = [c for c in cols if c in df_hist_visual.columns]
            df_hist_visual = df_hist_visual[cols]
            # ----------------------------------------------------

            st.info("✅ Você pode editar ou **excluir** linhas (selecione a linha e aperte Delete).")
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
                
                # Remove a coluna temporária antes de salvar para não sujar o original
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
                            # --- AJUSTE VISUAL: CÓDIGO NO CARD ---
                            st.write(f"📝 {row['código de barras']} | **{row['nome do produto']}**")
                            col1, col2 = st.columns(2)
                            nova_qtd = col1.number_input(f"Qtd Casa:", value=int(row['qtd_central']), key=f"q_{idx}")
                            novo_custo = col2.number_input(f"Custo:", value=float(row['preco_custo']), key=f"c_{idx}")
                            if st.button(f"💾 Salvar {row['nome do produto']}", key=f"btn_{idx}"):
                                df.at[idx, 'qtd_central'] = nova_qtd
                                df.at[idx, 'preco_custo'] = novo_custo
                                salvar_estoque(df, prefixo)
                                atualizar_casa_global(row['nome do produto'], nova_qtd, novo_custo, None, None, prefixo)
                                st.success("Salvo!")
                                st.rerun()
                else:
                    st.info("✏️ Se precisar corrigir o estoque, edite abaixo e clique em SALVAR.")
                    busca_central = st.text_input("🔍 Buscar Produto na Casa:", placeholder="Ex: oleo concordia...", key="busca_central")
                    # --- AJUSTE VISUAL: COLUNA CÓDIGO ---
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
                        st.success("Estoque atualizado e sincronizado em todas as lojas!")
                        st.rerun()
        with tab_gerenciar:
            st.info("Adicione mercadoria manualmente (ex: compra sem pedido) ou edite dados.")
            if not df.empty:
                # --- ALTERAÇÃO AQUI: LISTA COM CÓDIGO DE BARRAS PARA VISUALIZAÇÃO ---
                lista_visuais = (df['código de barras'].astype(str) + " - " + df['nome do produto'].astype(str)).unique().tolist()
                lista_visuais = sorted(lista_visuais)
                
                prod_opcao = st.selectbox("Selecione o Produto:", lista_visuais)
                
                if prod_opcao:
                    # Recria a lógica de busca baseada na string composta
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
                            # --- AJUSTE DO RELÓGIO (STEP=60) ---
                            hr_reg = c_hr.time_input("Hora:", obter_hora_manaus().time(), step=60)
                            
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
                                if acao.startswith("Somar") and qtd_input > 0:
                                    df.at[idx_prod, 'qtd_central'] += qtd_input
                                    msg_acao = f"Adicionado +{qtd_input}"
                                    dt_full = datetime.combine(dt_reg, hr_reg)
                                    hist = {'data': dt_full, 'produto': c_nome.upper().strip(), 'fornecedor': c_forn, 'qtd': qtd_input, 'preco_pago': novo_custo, 'total_gasto': qtd_input * novo_custo}
                                    salvar_historico(pd.concat([df_hist, pd.DataFrame([hist])], ignore_index=True), prefixo)
                                elif acao.startswith("Substituir"):
                                    df.at[idx_prod, 'qtd_central'] = qtd_input
                                    msg_acao = f"Estoque corrigido para {qtd_input}"
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
