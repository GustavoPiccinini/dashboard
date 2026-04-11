import io
import os
import tempfile
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Relatorio de Atendimentos", page_icon="📋", layout="wide")

st.markdown("""
<style>
    div[data-testid="metric-container"] { background:#f0f2f6; border-radius:10px; padding:12px; }
    .block-container { padding-top:1.5rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# MAPEAMENTO DE COLUNAS
# ══════════════════════════════════════════════
COL = {
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
# CONEXÃO DUCKDB — reconecta automaticamente em reruns
# ══════════════════════════════════════════════
def criar_conexao(filepath: str) -> duckdb.DuckDBPyConnection:
    # Converte qualquer arquivo para CSV UTF-8 limpo via pandas
    ext = os.path.splitext(filepath)[1].lower()
    csv_path = filepath + "_clean.csv"
    parquet_path = filepath + ".parquet"

    if not os.path.exists(csv_path):
        df_tmp = None

        if ext == ".csv":
            for enc in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
                for sep in [",", ";", "\t"]:
                    try:
                        df_tmp = pd.read_csv(filepath, encoding=enc, sep=sep, engine="python")
                        if df_tmp.shape[1] > 1:
                            break
                    except Exception:
                        continue
                if df_tmp is not None and df_tmp.shape[1] > 1:
                    break

        elif ext in [".xlsx", ".xls"]:
            df_tmp = pd.read_excel(filepath)

        elif ext == ".parquet":
            df_tmp = pd.read_parquet(filepath)

        else:
            raise ValueError(f"Formato de arquivo não suportado: {ext}")

        # validação
        if df_tmp is None or df_tmp.shape[1] <= 1:
            raise ValueError("Não foi possível ler o arquivo.")

        # Corrigir coluna DATA
        col_data = next((c for c in df_tmp.columns if c.strip().upper() == "DATA"), None)
        if col_data:
            df_tmp[col_data] = pd.to_datetime(
                df_tmp[col_data].astype(str).str.strip(),
                dayfirst=True,
                errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        # salvar CSV e Parquet
        df_tmp.to_csv(csv_path, index=False, encoding="utf-8")
        df_tmp.to_parquet(parquet_path, index=False)

        # salvar caminhos
        st.session_state["tmp_csv"] = csv_path
        st.session_state["tmp_parquet"] = parquet_path

        del df_tmp

    con = duckdb.connect()

    parquet_path = st.session_state.get("tmp_parquet")

    if parquet_path and os.path.exists(parquet_path):
        con.execute(f"""
            CREATE OR REPLACE VIEW dados AS
            SELECT * FROM '{parquet_path}'
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE VIEW dados AS
            SELECT * FROM read_csv_auto('{csv_path}',
                header=true,
                delim=',',
                ignore_errors=true,
                auto_detect=true
            )
        """)

    con.execute("SELECT COUNT(*) FROM dados").fetchone()
    return con


def get_con() -> duckdb.DuckDBPyConnection:
    """Retorna conexão válida, recriando se necessário."""
    tmp_path = st.session_state.get("tmp_path")

    if not tmp_path:
        st.error("Faça upload do arquivo.")
        st.stop()

    con = st.session_state.get("con")

    try:
        if con:
            con.execute("SELECT COUNT(*) FROM dados").fetchone()
            return con
    except Exception:
        pass

    # Reconecta
    con = criar_conexao(tmp_path)
    st.session_state["con"] = con
    return con


def run(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).df()


def run_val(sql: str):
    return get_con().execute(sql).fetchone()[0]


def col_exists(col: str) -> bool:
    try:
        get_con().execute(f'SELECT "{col}" FROM dados LIMIT 1')
        return True
    except Exception:
        return False


def safe_col(key: str):
    c = COL.get(key, key)
    return c if col_exists(c) else None

# ══════════════════════════════════════════════
# AUTENTICAÇÃO
# ══════════════════════════════════════════════
try:
    senha_correta = st.secrets["SENHA_DASHBOARD"]
except Exception:
    senha_correta = None  # sem senha local — configure nos Secrets do Streamlit Cloud

st.sidebar.title("🔒 Acesso")
senha_digitada = st.sidebar.text_input("Senha de acesso", type="password")
if senha_digitada != senha_correta:
    st.sidebar.warning("Digite a senha para acessar.")
    st.title("📋 Relatorio de Atendimentos")
    st.info("🔒 Insira a senha na barra lateral.")
    st.stop()
st.sidebar.success("✅ Acesso liberado!")

# ══════════════════════════════════════════════
# UPLOAD
# ══════════════════════════════════════════════
st.sidebar.markdown("---")
st.sidebar.title("⚙️ Configurações")
st.sidebar.markdown("---")

uploaded = st.sidebar.file_uploader(
    "📂 Carregar arquivo", type=["csv", "xlsx", "xls","parquet"],
    help="CSV ou Excel. Processado via DuckDB."
)

if uploaded:
    if st.session_state.get("last_file") != uploaded.name:
        suffix = os.path.splitext(uploaded.name)[1]
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(uploaded.read())
        tmp.flush()
        tmp.close()
        st.session_state["tmp_path"] = tmp.name
        st.session_state["last_file"] = uploaded.name
        st.session_state.pop("con", None)  # força reconexão
    try:
        total_geral = run_val("SELECT COUNT(*) FROM dados")
        st.session_state["total_geral"] = total_geral
        st.sidebar.success(f"✅ {total_geral:,} registros carregados!")
    except Exception as e:
        st.sidebar.error(f"Erro: {e}")
        st.stop()
else:
    st.sidebar.info("💡 Faça upload do arquivo para começar.")
    st.title("📋 Relatorio de Atendimentos")
    st.info("📂 Faça upload do seu arquivo CSV ou Excel na barra lateral.")
    st.stop()

# ══════════════════════════════════════════════
# COLUNAS DISPONÍVEIS
# ══════════════════════════════════════════════
c_cpf      = safe_col("cpf")
c_nome     = safe_col("nome")
c_unidade  = safe_col("unidade")
c_servico  = safe_col("servico")
c_categoria= safe_col("categoria")
c_login    = safe_col("login")
c_data     = safe_col("data")
c_nis      = safe_col("nis")
c_nasc     = safe_col("nascimento")
c_codigo   = safe_col("codigo")
c_quantia  = safe_col("quantia")

def esc(v: str) -> str:
    """Escapa aspas simples para uso seguro em SQL."""
    return str(v).replace("'", "''")

@st.cache_data(ttl=300, show_spinner=False)
def opts_db_cached(col, label_all, _cache_key):
    if not col:
        return [label_all]
    vals = run(f'SELECT DISTINCT "{col}" FROM dados WHERE "{col}" IS NOT NULL ORDER BY "{col}" LIMIT 500')[col].tolist()
    return [label_all] + [str(v) for v in vals]

def opts_db(col, label_all):
    cache_key = st.session_state.get("last_file", "")
    return opts_db_cached(col, label_all, cache_key)

# ══════════════════════════════════════════════
# FILTROS
# ══════════════════════════════════════════════
st.sidebar.markdown("### 🔍 Filtros")
search      = st.sidebar.text_input("Buscar por nome ou CPF", placeholder="Digite aqui...")
f_unidade   = st.sidebar.selectbox("Unidade",   opts_db(c_unidade,  "Todas"))
f_servico   = st.sidebar.selectbox("Serviço",   opts_db(c_servico,  "Todos"))
f_categoria = st.sidebar.selectbox("Categoria", opts_db(c_categoria,"Todas"))
f_login     = st.sidebar.selectbox("Atendente", opts_db(c_login,    "Todos"))

f_data = None
if c_data:
    try:
        dmin = run(f'SELECT MIN(CAST("{c_data}" AS DATE)) FROM dados').iloc[0, 0]
        dmax = run(f'SELECT MAX(CAST("{c_data}" AS DATE)) FROM dados').iloc[0, 0]
        if dmin and dmax:
            f_data = st.sidebar.date_input("Período", value=(dmin, dmax), min_value=dmin, max_value=dmax)
    except Exception:
        pass

# ══════════════════════════════════════════════
# WHERE CLAUSE
# ══════════════════════════════════════════════
wheres = []
if search:
    parts = []
    if c_nome: parts.append(f"LOWER(CAST(\"{c_nome}\" AS VARCHAR)) LIKE '%{search.lower()}%'")
    if c_cpf:  parts.append(f"CAST(\"{c_cpf}\" AS VARCHAR) LIKE '%{search}%'")
    if parts:  wheres.append(f"({' OR '.join(parts)})")
if f_unidade   != "Todas" and c_unidade:   wheres.append(f'"{c_unidade}" = \'{esc(f_unidade)}\'')
if f_servico   != "Todos" and c_servico:   wheres.append(f'"{c_servico}" = \'{esc(f_servico)}\'')
if f_categoria != "Todas" and c_categoria: wheres.append(f'"{c_categoria}" = \'{esc(f_categoria)}\'')
if f_login     != "Todos" and c_login:     wheres.append(f'"{c_login}" = \'{esc(f_login)}\'')
if f_data and len(f_data) == 2 and c_data:
    wheres.append(f"CAST(\"{c_data}\" AS DATE) BETWEEN '{f_data[0]}' AND '{f_data[1]}'")

where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""

# ══════════════════════════════════════════════
# MÉTRICAS
# ══════════════════════════════════════════════
st.title("📋 Relatorio de Atendimentos")
st.caption("Clique em qualquer linha para ver o perfil completo.")
st.markdown("---")

total_f  = run_val(f"SELECT COUNT(*) FROM dados {where_sql}")
cpfs_f   = run_val(f'SELECT COUNT(DISTINCT "{c_cpf}") FROM dados {where_sql}')     if c_cpf     else 0
uni_f    = run_val(f'SELECT COUNT(DISTINCT "{c_unidade}") FROM dados {where_sql}') if c_unidade else 0
svc_f    = run_val(f'SELECT COUNT(DISTINCT "{c_servico}") FROM dados {where_sql}') if c_servico else 0
login_f  = run_val(f'SELECT COUNT(DISTINCT "{c_login}") FROM dados {where_sql}')   if c_login   else 0

# Taxa de retorno: CPFs com mais de 1 atendimento
taxa_retorno = 0
if c_cpf:
    try:
        cpfs_multi = run_val(f'SELECT COUNT(*) FROM (SELECT "{c_cpf}", COUNT(*) AS n FROM dados {where_sql} GROUP BY "{c_cpf}" HAVING n > 1)')
        taxa_retorno = round((cpfs_multi / cpfs_f * 100), 1) if cpfs_f > 0 else 0
    except Exception:
        pass

# Comparativo mês atual vs anterior
delta_txt = ""
if c_data:
    try:
        mes_atual = run_val(f'''SELECT COUNT(*) FROM dados {where_sql} {"AND" if where_sql else "WHERE"} CAST("{c_data}" AS DATE) >= DATE_TRUNC('month', CURRENT_DATE)''')
        mes_ant   = run_val(f'''SELECT COUNT(*) FROM dados {where_sql} {"AND" if where_sql else "WHERE"} CAST("{c_data}" AS DATE) >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH AND CAST("{c_data}" AS DATE) < DATE_TRUNC('month', CURRENT_DATE)''')
        if mes_ant > 0:
            delta_pct = round(((mes_atual - mes_ant) / mes_ant) * 100, 1)
            delta_txt = f"{delta_pct:+.1f}% vs mês anterior"
    except Exception:
        pass

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total atendimentos", f"{total_f:,}", delta_txt if delta_txt else None)
m2.metric("CPFs distintos",     f"{cpfs_f:,}")
m3.metric("Unidades",    f"{uni_f:,}")
m4.metric("Tipos de serviço",   f"{svc_f:,}")
m5.metric("Atendentes ",  f"{login_f:,}")
m6.metric("Taxa de retorno",    f"{taxa_retorno}%", help="CPFs com mais de 1 atendimento")
st.markdown("---")

# ══════════════════════════════════════════════
# ABAS
# ══════════════════════════════════════════════
aba_reg, aba_graf, aba_at, aba_exp = st.tabs(["📄 Registros", "📊 Gráficos", "👥 Atendentes", "📥 Exportar"])

# ─────────────────────────────────────
# ABA 1 — REGISTROS
# ─────────────────────────────────────
with aba_reg:
    st.subheader("Registros de atendimento")
    cols_tab = [c for c in [c_codigo, c_nome, c_cpf, c_unidade, c_servico, c_data, c_login, c_categoria] if c]
    cols_sel = ", ".join([f'"{c}"' for c in cols_tab])

    PAGE_SIZE = 100
    total_pages = max(1, (total_f + PAGE_SIZE - 1) // PAGE_SIZE)
    pg_col, _ = st.columns([1, 3])
    with pg_col:
        page = st.number_input("Página", min_value=1, max_value=total_pages, value=1, step=1)
    offset = (page - 1) * PAGE_SIZE
    st.caption(f"Página {page} de {total_pages} — {total_f:,} registros no total")

    df_tab = run(f"SELECT {cols_sel} FROM dados {where_sql} LIMIT {PAGE_SIZE} OFFSET {offset}")

    evento = st.dataframe(df_tab, use_container_width=True, hide_index=True,
                          on_select="rerun", selection_mode="single-row")

    sel = evento.selection.rows if hasattr(evento, "selection") else []
    if sel and c_cpf:
        row = df_tab.iloc[sel[0]]
        cpf_sel = str(row.get(c_cpf, "")).replace("'", "''")
        if cpf_sel:
            st.markdown("---")
            st.subheader(f"👤 Perfil: {row.get(c_nome, 'Cidadão')}")
            total_cpf = run_val(f'SELECT COUNT(*) FROM dados WHERE "{c_cpf}" = \'{cpf_sel}\'')
            pa, pb, pc, pd_ = st.columns(4)
            pa.metric("CPF",           cpf_sel)
            pb.metric("NIS",           str(row.get(c_nis,  "—")))
            pc.metric("Nascimento",    str(row.get(c_nasc, "—")))
            pd_.metric("Atendimentos", total_cpf)

            cols_h = [c for c in [c_data, c_servico, c_unidade, c_quantia, c_login, c_categoria] if c]
            df_hist = run(f'''SELECT {", ".join([f"{chr(34)}{c}{chr(34)}" for c in cols_h])} FROM dados WHERE "{c_cpf}" = '{cpf_sel}'LIMIT 500''') 
            if len(df_hist) == 500:
                st.caption(f"⚠️ Exibindo 500 de {total_cpf:,} atendimentos para este CPF.")    
            st.markdown("##### Histórico de serviços")
            st.dataframe(df_hist, use_container_width=True, hide_index=True)

            if c_servico:
                svc_c = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Qtd FROM dados WHERE "{c_cpf}" = \'{cpf_sel}\' GROUP BY "{c_servico}" ORDER BY Qtd DESC')
                fig = px.bar(svc_c, x="Qtd", y="Servico", orientation="h",
                             title="Serviços recebidos", color="Qtd",
                             color_continuous_scale="Blues", text="Qtd")
                fig.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────
# ABA 2 — GRÁFICOS
# ─────────────────────────────────────
with aba_graf:
    if total_f == 0:
        st.warning("Nenhum dado com os filtros atuais.")
    else:
        g1, g2 = st.columns(2)
        with g1:
            if c_servico:
                d = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_servico}" ORDER BY Qtd DESC')
                fig1 = px.bar(d, x="Qtd", y="Servico", orientation="h", title="Por tipo de serviço",
                              color="Qtd", color_continuous_scale="Teal", text="Qtd")
                fig1.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
        with g2:
            if c_unidade:
                d = run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_unidade}" ORDER BY Qtd DESC')
                fig2 = px.bar(d, x="Qtd", y="Unidade", orientation="h", title="Por unidade",
                              color="Qtd", color_continuous_scale="Purples", text="Qtd")
                fig2.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig2.update_traces(textposition="outside")
                st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)
        with g3:
            if c_login:
                try:
                    d = run(f'SELECT "{c_login}" AS Atendente, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_login}" ORDER BY Qtd DESC LIMIT 10')
                    fig3 = px.bar(d, x="Qtd", y="Atendente", orientation="h", title="Top 10 atendentes",
                                  color="Qtd", color_continuous_scale="Oranges", text="Qtd")
                    fig3.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                    fig3.update_traces(textposition="outside")
                    st.plotly_chart(fig3, use_container_width=True)
                except Exception:
                    st.caption("Gráfico de atendentes indisponível.")
        with g4:
            if c_data:
                try:
                    w_data = f"{where_sql} AND" if where_sql.strip() else "WHERE"
                    d = run(f'''
                        SELECT STRFTIME(CAST("{c_data}" AS DATE), '%Y-%m') AS Mes, COUNT(*) AS Qtd
                        FROM dados {w_data} "{c_data}" IS NOT NULL
                        GROUP BY Mes ORDER BY Mes
                    ''')
                    if not d.empty:
                        # Destacar últimos 12 meses
                        d = d.tail(24)
                        fig4 = px.line(d, x="Mes", y="Qtd", title="Evolução mensal (últimos 24 meses)", markers=True)
                        fig4.update_layout(xaxis_title="Mês", yaxis_title="Atendimentos",
                                           xaxis_tickangle=-45)
                        fig4.update_traces(line_color="#1f77b4", line_width=2)
                        st.plotly_chart(fig4, use_container_width=True)
                except Exception:
                    st.caption("Gráfico de evolução indisponível.")

        if c_unidade and c_servico:
            st.markdown("##### Mapa de calor — Unidade × Serviço")
            d = run(f'SELECT "{c_unidade}" AS Unidade, "{c_servico}" AS Servico, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_unidade}", "{c_servico}"')
            pivot = d.pivot(index="Unidade", columns="Servico", values="Qtd").fillna(0)
            fig5 = px.imshow(pivot, text_auto=True, color_continuous_scale="Blues",
                             title="Atendimentos por unidade e serviço")
            st.plotly_chart(fig5, use_container_width=True)

        # Ranking top serviços
        if c_servico and c_cpf:
            st.markdown("##### 🏆 Ranking — Serviços mais procurados")
            rank_cols = st.columns(2)
            with rank_cols[0]:
                top_svc = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Total, COUNT(DISTINCT "{c_cpf}") AS CPFs_unicos FROM dados {where_sql} GROUP BY "{c_servico}" ORDER BY Total DESC LIMIT 10')
                st.dataframe(top_svc, use_container_width=True, hide_index=True)
            with rank_cols[1]:
                # CPFs com mais atendimentos
                if c_nome:
                    top_cpf = run(f'SELECT "{c_nome}" AS Nome, "{c_cpf}" AS CPF, COUNT(*) AS Atendimentos FROM dados {where_sql} GROUP BY "{c_cpf}", "{c_nome}" ORDER BY Atendimentos DESC LIMIT 10')
                else:
                    top_cpf = run(f'SELECT "{c_cpf}" AS CPF, COUNT(*) AS Atendimentos FROM dados {where_sql} GROUP BY "{c_cpf}" ORDER BY Atendimentos DESC LIMIT 10')
                st.markdown("**Top 10 CPFs com mais atendimentos**")
                st.dataframe(top_cpf, use_container_width=True, hide_index=True)

# ─────────────────────────────────────
# ABA 3 — ATENDENTES
# ─────────────────────────────────────
with aba_at:
    st.subheader("Perfil por atendente")
    if not c_login:
        st.warning("Coluna de login não encontrada.")
    else:
        if where_sql.strip():
            atendentes = run(f'SELECT DISTINCT "{c_login}" FROM dados {where_sql} AND "{c_login}" IS NOT NULL ORDER BY "{c_login}"')[c_login].tolist()
        else:
            atendentes = run(f'SELECT DISTINCT "{c_login}" FROM dados WHERE "{c_login}" IS NOT NULL ORDER BY "{c_login}"')[c_login].tolist()
        at_sel = st.selectbox("Selecione o atendente", atendentes)
        if at_sel:
            at_safe = at_sel.replace("'", "''")
            w_at = f'WHERE "{c_login}" = \'{at_safe}\''
            a1, a2, a3 = st.columns(3)
            a1.metric("Total atendimentos", run_val(f'SELECT COUNT(*) FROM dados {w_at}'))
            a2.metric("CPFs atendidos",     run_val(f'SELECT COUNT(DISTINCT "{c_cpf}") FROM dados {w_at}')     if c_cpf     else "—")
            a3.metric("Unidades",           run_val(f'SELECT COUNT(DISTINCT "{c_unidade}") FROM dados {w_at}') if c_unidade else "—")

            cl, cr = st.columns(2)
            with cl:
                if c_servico:
                    d = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Qtd FROM dados {w_at} GROUP BY "{c_servico}" ORDER BY Qtd DESC')
                    fig_a1 = px.bar(d, x="Qtd", y="Servico", orientation="h", title="Serviços realizados",
                                    color="Qtd", color_continuous_scale="Teal", text="Qtd")
                    fig_a1.update_layout(coloraxis_showscale=False, yaxis_title=None)
                    fig_a1.update_traces(textposition="outside")
                    st.plotly_chart(fig_a1, use_container_width=True)
            with cr:
                if c_unidade:
                    d = run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Qtd FROM dados {w_at} GROUP BY "{c_unidade}"')
                    fig_a2 = px.pie(d, names="Unidade", values="Qtd", title="Por unidade")
                    st.plotly_chart(fig_a2, use_container_width=True)

            cols_at = [c for c in [c_nome, c_cpf, c_servico, c_data, c_unidade] if c]
            df_at = run(f'''SELECT {", ".join([f"{chr(34)}{c}{chr(34)}" for c in cols_at])}FROM dados {w_at} LIMIT 500''')
            st.markdown("##### Cidadãos atendidos")
            st.dataframe(df_at, use_container_width=True, hide_index=True)

# ─────────────────────────────────────
# ABA 4 — EXPORTAR
# ─────────────────────────────────────
with aba_exp:
    st.subheader("Exportar dados filtrados")
    st.caption(f"{total_f:,} registros com os filtros atuais.")
    EXPORT_LIMIT = 50_000
    st.info(f"Exportação limitada a {EXPORT_LIMIT:,} registros por vez.")
    df_exp = run(f"SELECT * FROM dados {where_sql} LIMIT {EXPORT_LIMIT}")
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df_exp.to_excel(writer, index=False, sheet_name="Atendimentos")
        if c_login:
            run(f'SELECT "{c_login}" AS Atendente, COUNT(*) AS Total FROM dados {where_sql} GROUP BY "{c_login}" ORDER BY Total DESC').to_excel(writer, index=False, sheet_name="Por Atendente")
        if c_unidade:
            run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Total FROM dados {where_sql} GROUP BY "{c_unidade}" ORDER BY Total DESC').to_excel(writer, index=False, sheet_name="Por Unidade")
        if c_servico:
            run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Total FROM dados {where_sql} GROUP BY "{c_servico}" ORDER BY Total DESC').to_excel(writer, index=False, sheet_name="Por Serviço")
    st.download_button("⬇️ Baixar Excel com resumos", out.getvalue(), "atendimentos.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True)