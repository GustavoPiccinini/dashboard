import io
import gc
import streamlit as st
import pandas as pd
import plotly.express as px

# ══════════════════════════════════════════════
# CONFIGURAÇÃO DA PÁGINA
# ══════════════════════════════════════════════
st.set_page_config(
    page_title="Dashboard de Atendimentos",
    page_icon="📋",
    layout="wide"
)

st.markdown("""
<style>
    div[data-testid="metric-container"] {
        background: #f0f2f6; border-radius: 10px; padding: 12px;
    }
    .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# AUTENTICAÇÃO
# ══════════════════════════════════════════════
SENHA_LOCAL = "assistencia"

try:
    senha_correta = st.secrets["SENHA_DASHBOARD"]
except Exception:
    senha_correta = SENHA_LOCAL

st.sidebar.title("🔒 Acesso")
senha_digitada = st.sidebar.text_input("Senha de acesso", type="password")

if senha_digitada != senha_correta:
    st.sidebar.warning("Digite a senha para acessar.")
    st.title("📋 Dashboard de Atendimentos")
    st.info("🔒 Insira a senha na barra lateral para acessar o dashboard.")
    st.stop()

st.sidebar.success("✅ Acesso liberado!")

# ══════════════════════════════════════════════
# MAPEAMENTO DE COLUNAS
# ══════════════════════════════════════════════
COL_MAP = {
    "codigo":     "Codigo_do_grupo",
    "cpf":        "CPF",
    "nis":        "NIS",
    "nascimento": "DATA_DE_NASCIMENTO",
    "nome":       "Nome_referencia",
    "data":       "DATA",
    "servico":    "SERVICO",
    "quantia":    "QUANTIA",
    "unidade":    "UNIDADE_DE_ATENDIMENTO",
    "login":      "login",
    "categoria":  "Categoria",
}

# ══════════════════════════════════════════════
# CARREGAMENTO OTIMIZADO
# Usa ttl para liberar cache após 1 hora
# Converte colunas de texto para 'category' (usa até 10x menos memória)
# ══════════════════════════════════════════════
@st.cache_data(ttl=3600, max_entries=1)
def load_data(raw_bytes, filename):
    if filename.endswith(".csv"):
        df = None
        for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, sep=None, engine="python")
                break
            except (UnicodeDecodeError, Exception):
                continue
        if df is None:
            raise ValueError("Não foi possível detectar o encoding do CSV.")
    else:
        df = pd.read_excel(io.BytesIO(raw_bytes))

    # Otimizar memória: converte colunas de texto repetitivo para category
    colunas_categoricas = [
        "SERVICO", "UNIDADE_DE_ATENDIMENTO", "login", "Categoria", "Nome_referencia"
    ]
    for col in colunas_categoricas:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Converte data
    if "DATA" in df.columns:
        df["DATA"] = pd.to_datetime(df["DATA"], dayfirst=True, errors="coerce")

    # Libera memória explicitamente
    gc.collect()
    return df

@st.cache_data(ttl=3600, max_entries=1)
def load_sample():
    return pd.DataFrame([
        {"Codigo_do_grupo": 144991, "CPF": "000.000.029-46", "NIS": "NULL",        "DATA_DE_NASCIMENTO": "02/03/1980", "Nome_referencia": "Maria de Fatima Cardoso",  "DATA": "28/11/2025 00:00", "SERVICO": "CADASTRO ÚNICO - FOLHA RESUMO",    "QUANTIA": 1, "UNIDADE_DE_ATENDIMENTO": "Cras - Aeroporto",    "login": "Sara Morais Alcântara",  "Categoria": "Procedimento"},
        {"Codigo_do_grupo": 93199,  "CPF": "021.765.619-61", "NIS": "12589385538", "DATA_DE_NASCIMENTO": "09/09/1976", "Nome_referencia": "Mariana Ferreira Krempel", "DATA": "27/11/2025 17:00", "SERVICO": "ATIVIDADE EM GRUPO",               "QUANTIA": 1, "UNIDADE_DE_ATENDIMENTO": "Centro da Juventude", "login": "Josimar Gabriel da Paz", "Categoria": "Procedimento"},
        {"Codigo_do_grupo": 93200,  "CPF": "007.466.329-03", "NIS": "20358812647", "DATA_DE_NASCIMENTO": "04/12/1979", "Nome_referencia": "Luziele Aparecida Santos", "DATA": "27/11/2025 17:00", "SERVICO": "ATIVIDADE EM GRUPO",               "QUANTIA": 1, "UNIDADE_DE_ATENDIMENTO": "Centro da Juventude", "login": "Josimar Gabriel da Paz", "Categoria": "Procedimento"},
        {"Codigo_do_grupo": 93201,  "CPF": "005.650.619-28", "NIS": "12642302584", "DATA_DE_NASCIMENTO": "29/01/1978", "Nome_referencia": "Vani Silva Souza",         "DATA": "27/11/2025 17:00", "SERVICO": "ATIVIDADE EM GRUPO",               "QUANTIA": 1, "UNIDADE_DE_ATENDIMENTO": "Centro da Juventude", "login": "Josimar Gabriel da Paz", "Categoria": "Procedimento"},
    ])

# ══════════════════════════════════════════════
# SIDEBAR — upload e filtros
# ══════════════════════════════════════════════
st.sidebar.markdown("---")
st.sidebar.title("⚙️ Configurações")
st.sidebar.markdown("---")

uploaded_file = st.sidebar.file_uploader(
    "📂 Carregar arquivo de dados",
    type=["xlsx", "xls", "csv"],
    help="Excel ou CSV com os dados de atendimento"
)

if uploaded_file:
    try:
        raw = uploaded_file.read()
        df = load_data(raw, uploaded_file.name)
        st.sidebar.success(f"✅ {len(df):,} registros carregados!")
        del raw  # libera memória do arquivo bruto
        gc.collect()
    except Exception as e:
        st.sidebar.error(f"Erro ao carregar: {e}")
        df = load_sample()
else:
    df = load_sample()
    st.sidebar.info("💡 Usando dados de exemplo.")

# Atalhos de coluna
C = {k: v for k, v in COL_MAP.items() if v in df.columns}
missing = [v for k, v in COL_MAP.items() if v not in df.columns]
if missing:
    st.sidebar.warning(f"Colunas não encontradas: {missing}")

col_cpf       = C.get("cpf",        "CPF")
col_nome      = C.get("nome",       "Nome_referencia")
col_unidade   = C.get("unidade",    "UNIDADE_DE_ATENDIMENTO")
col_servico   = C.get("servico",    "SERVICO")
col_categoria = C.get("categoria",  "Categoria")
col_login     = C.get("login",      "login")
col_data      = C.get("data",       "DATA")
col_nis       = C.get("nis",        "NIS")
col_nasc      = C.get("nascimento", "DATA_DE_NASCIMENTO")
col_codigo    = C.get("codigo",     "Codigo_do_grupo")
col_quantia   = C.get("quantia",    "QUANTIA")

st.sidebar.markdown("### 🔍 Filtros")
search = st.sidebar.text_input("Buscar por nome ou CPF", placeholder="Digite aqui...")

def opts(col, label_all):
    if col not in df.columns:
        return [label_all]
    valores = df[col].unique().tolist()
    valores = [str(v) for v in valores if pd.notna(v)]
    return [label_all] + sorted(valores)

filtro_unidade   = st.sidebar.selectbox("Unidade de atendimento", opts(col_unidade,  "Todas"))
filtro_servico   = st.sidebar.selectbox("Serviço",                opts(col_servico,  "Todos"))
filtro_categoria = st.sidebar.selectbox("Categoria",              opts(col_categoria,"Todas"))
filtro_login     = st.sidebar.selectbox("Atendente (login)",      opts(col_login,    "Todos"))

if col_data in df.columns:
    dmin = df[col_data].min().date()
    dmax = df[col_data].max().date()
    filtro_data = st.sidebar.date_input("Período", value=(dmin, dmax), min_value=dmin, max_value=dmax)
else:
    filtro_data = None

# ══════════════════════════════════════════════
# APLICAR FILTROS — opera sobre cópia mínima
# ══════════════════════════════════════════════
mask = pd.Series(True, index=df.index)

if search:
    m = pd.Series(False, index=df.index)
    if col_nome in df.columns:
        m |= df[col_nome].astype(str).str.lower().str.contains(search.lower(), na=False)
    if col_cpf in df.columns:
        m |= df[col_cpf].astype(str).str.contains(search, na=False)
    mask &= m

if filtro_unidade   != "Todas" and col_unidade   in df.columns: mask &= df[col_unidade].astype(str)   == filtro_unidade
if filtro_servico   != "Todos" and col_servico   in df.columns: mask &= df[col_servico].astype(str)   == filtro_servico
if filtro_categoria != "Todas" and col_categoria in df.columns: mask &= df[col_categoria].astype(str) == filtro_categoria
if filtro_login     != "Todos" and col_login     in df.columns: mask &= df[col_login].astype(str)     == filtro_login

if filtro_data and col_data in df.columns and len(filtro_data) == 2:
    mask &= (df[col_data] >= pd.Timestamp(filtro_data[0])) & (df[col_data] <= pd.Timestamp(filtro_data[1]))

df_f = df[mask]

# ══════════════════════════════════════════════
# TÍTULO E MÉTRICAS
# ══════════════════════════════════════════════
st.title("📋 Dashboard de Atendimentos")
st.caption("Clique em qualquer linha da tabela para ver o perfil completo do cidadão ou do atendente.")
st.markdown("---")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total de atendimentos", f"{len(df_f):,}")
m2.metric("CPFs distintos",        f"{df_f[col_cpf].nunique():,}"     if col_cpf     in df_f.columns else "—")
m3.metric("Unidades ativas",       f"{df_f[col_unidade].nunique():,}" if col_unidade in df_f.columns else "—")
m4.metric("Tipos de serviço",      f"{df_f[col_servico].nunique():,}" if col_servico in df_f.columns else "—")
m5.metric("Atendentes ativos",     f"{df_f[col_login].nunique():,}"   if col_login   in df_f.columns else "—")

st.markdown("---")

# ══════════════════════════════════════════════
# ABAS
# ══════════════════════════════════════════════
aba_registros, aba_graficos, aba_atendentes, aba_exportar = st.tabs([
    "📄 Registros", "📊 Gráficos", "👥 Atendentes", "📥 Exportar"
])

# ─────────────────────────────────────
# ABA 1 — REGISTROS
# ─────────────────────────────────────
with aba_registros:
    st.subheader("Registros de atendimento")

    colunas_tabela = [v for v in [col_codigo, col_nome, col_cpf, col_unidade,
                                   col_servico, col_data, col_login, col_categoria]
                      if v in df_f.columns]

    # Exibe no máximo 500 linhas na tabela para não sobrecarregar
    df_exib = df_f[colunas_tabela].head(500).reset_index(drop=True)
    if len(df_f) > 500:
        st.caption(f"⚠️ Exibindo 500 de {len(df_f):,} registros. Use os filtros para refinar.")

    evento = st.dataframe(
        df_exib,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    selected_rows = evento.selection.rows if hasattr(evento, "selection") else []

    if selected_rows:
        idx = selected_rows[0]
        row = df_exib.iloc[idx]
        cpf_sel = row.get(col_cpf)

        if cpf_sel:
            registros_cpf = df[df[col_cpf].astype(str) == str(cpf_sel)]
            st.markdown("---")
            st.subheader(f"👤 Perfil: {row.get(col_nome, 'Cidadão')}")

            pa, pb, pc, pd_ = st.columns(4)
            pa.metric("CPF",           str(cpf_sel))
            pb.metric("NIS",           str(row.get(col_nis,  "—")))
            pc.metric("Nascimento",    str(row.get(col_nasc, "—")))
            pd_.metric("Atendimentos", len(registros_cpf))

            st.markdown("##### Histórico completo de serviços")
            cols_hist = [v for v in [col_data, col_servico, col_unidade,
                                      col_quantia, col_login, col_categoria]
                         if v in registros_cpf.columns]
            st.dataframe(registros_cpf[cols_hist].reset_index(drop=True),
                         use_container_width=True, hide_index=True)

            if col_servico in registros_cpf.columns:
                svc_count = registros_cpf[col_servico].astype(str).value_counts().reset_index()
                svc_count.columns = ["Serviço", "Qtd"]
                fig = px.bar(svc_count, x="Qtd", y="Serviço", orientation="h",
                             title="Serviços recebidos por este cidadão",
                             color="Qtd", color_continuous_scale="Blues", text="Qtd")
                fig.update_layout(showlegend=False, coloraxis_showscale=False,
                                  yaxis_title=None, xaxis_title="Quantidade")
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────
# ABA 2 — GRÁFICOS
# ─────────────────────────────────────
with aba_graficos:
    if df_f.empty:
        st.warning("Nenhum dado para exibir com os filtros atuais.")
    else:
        g1, g2 = st.columns(2)

        with g1:
            if col_servico in df_f.columns:
                dados_svc = df_f[col_servico].astype(str).value_counts().reset_index()
                dados_svc.columns = ["Serviço", "Qtd"]
                fig1 = px.bar(dados_svc, x="Qtd", y="Serviço", orientation="h",
                              title="Atendimentos por tipo de serviço",
                              color="Qtd", color_continuous_scale="Teal", text="Qtd")
                fig1.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)

        with g2:
            if col_unidade in df_f.columns:
                dados_uni = df_f[col_unidade].astype(str).value_counts().reset_index()
                dados_uni.columns = ["Unidade", "Qtd"]
                fig2 = px.bar(dados_uni, x="Qtd", y="Unidade", orientation="h",
                              title="Atendimentos por unidade",
                              color="Qtd", color_continuous_scale="Purples", text="Qtd")
                fig2.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig2.update_traces(textposition="outside")
                st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)

        with g3:
            if col_login in df_f.columns:
                dados_login = df_f[col_login].astype(str).value_counts().reset_index()
                dados_login.columns = ["Atendente", "Qtd"]
                fig3 = px.bar(dados_login, x="Qtd", y="Atendente", orientation="h",
                              title="Atendimentos por atendente",
                              color="Qtd", color_continuous_scale="Oranges", text="Qtd")
                fig3.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig3.update_traces(textposition="outside")
                st.plotly_chart(fig3, use_container_width=True)

        with g4:
            if col_data in df_f.columns:
                df_tempo = df_f[[col_data]].dropna().copy()
                df_tempo["Mes"] = df_tempo[col_data].dt.to_period("M").astype(str)
                dados_tempo = df_tempo.groupby("Mes").size().reset_index(name="Qtd")
                del df_tempo
                fig4 = px.line(dados_tempo, x="Mes", y="Qtd",
                               title="Evolução dos atendimentos ao longo do tempo",
                               markers=True)
                fig4.update_layout(xaxis_title="Mês", yaxis_title="Atendimentos")
                st.plotly_chart(fig4, use_container_width=True)

        if col_unidade in df_f.columns and col_servico in df_f.columns:
            st.markdown("##### Mapa de calor — Serviços por unidade")
            heat = df_f.groupby(
                [df_f[col_unidade].astype(str), df_f[col_servico].astype(str)]
            ).size().reset_index(name="Qtd")
            heat.columns = ["Unidade", "Serviço", "Qtd"]
            heat_pivot = heat.pivot(index="Unidade", columns="Serviço", values="Qtd").fillna(0)
            fig5 = px.imshow(heat_pivot, text_auto=True, color_continuous_scale="Blues",
                             title="Quantidade de atendimentos por unidade e tipo de serviço")
            fig5.update_layout(xaxis_title="Serviço", yaxis_title="Unidade")
            st.plotly_chart(fig5, use_container_width=True)

# ─────────────────────────────────────
# ABA 3 — ATENDENTES
# ─────────────────────────────────────
with aba_atendentes:
    st.subheader("Perfil por atendente")

    if col_login not in df_f.columns:
        st.warning("Coluna de login/atendente não encontrada.")
    else:
        atendentes = sorted(df_f[col_login].astype(str).dropna().unique().tolist())
        atendente_sel = st.selectbox("Selecione o atendente", atendentes)

        if atendente_sel:
            df_at = df_f[df_f[col_login].astype(str) == atendente_sel]

            a1, a2, a3 = st.columns(3)
            a1.metric("Total de atendimentos", len(df_at))
            a2.metric("CPFs atendidos",        df_at[col_cpf].nunique()     if col_cpf     in df_at.columns else "—")
            a3.metric("Unidades de atuação",   df_at[col_unidade].nunique() if col_unidade in df_at.columns else "—")

            col_esq, col_dir = st.columns(2)

            with col_esq:
                if col_servico in df_at.columns:
                    svc_at = df_at[col_servico].astype(str).value_counts().reset_index()
                    svc_at.columns = ["Serviço", "Qtd"]
                    fig_a1 = px.bar(svc_at, x="Qtd", y="Serviço", orientation="h",
                                    title="Serviços realizados", color="Qtd",
                                    color_continuous_scale="Teal", text="Qtd")
                    fig_a1.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    fig_a1.update_traces(textposition="outside")
                    st.plotly_chart(fig_a1, use_container_width=True)

            with col_dir:
                if col_unidade in df_at.columns:
                    uni_at = df_at[col_unidade].astype(str).value_counts().reset_index()
                    uni_at.columns = ["Unidade", "Qtd"]
                    fig_a2 = px.pie(uni_at, names="Unidade", values="Qtd",
                                    title="Distribuição por unidade")
                    st.plotly_chart(fig_a2, use_container_width=True)

            st.markdown("##### Cidadãos atendidos")
            cols_at = [v for v in [col_nome, col_cpf, col_servico, col_data, col_unidade]
                       if v in df_at.columns]
            st.dataframe(df_at[cols_at].reset_index(drop=True),
                         use_container_width=True, hide_index=True)

# ─────────────────────────────────────
# ABA 4 — EXPORTAR
# ─────────────────────────────────────
with aba_exportar:
    st.subheader("Exportar dados filtrados")
    st.caption(f"{len(df_f):,} registros com os filtros atuais.")

    col_x1, col_x2 = st.columns(2)

    with col_x1:
        csv_bytes = df_f.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            label="⬇️ Baixar como CSV",
            data=csv_bytes,
            file_name="atendimentos_filtrado.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col_x2:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_f.to_excel(writer, index=False, sheet_name="Atendimentos")
            if col_login in df_f.columns:
                df_f.groupby(col_login).size().reset_index(name="Total").to_excel(
                    writer, index=False, sheet_name="Por Atendente")
            if col_unidade in df_f.columns:
                df_f.groupby(col_unidade).size().reset_index(name="Total").to_excel(
                    writer, index=False, sheet_name="Por Unidade")
            if col_servico in df_f.columns:
                df_f.groupby(col_servico).size().reset_index(name="Total").to_excel(
                    writer, index=False, sheet_name="Por Serviço")
        st.download_button(
            label="⬇️ Baixar Excel com abas de resumo",
            data=output.getvalue(),
            file_name="atendimentos_filtrado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
