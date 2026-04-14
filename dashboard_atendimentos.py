import io
import os
import tempfile
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Dashboard de Atendimentos", page_icon="📋", layout="wide")

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
            con.execute("SELECT 1").fetchone()  # health check leve
            return con
    except Exception:
        pass
    con = criar_conexao(tmp_path)
    st.session_state["con"] = con
    return con


def run(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).df()


def run_val(sql: str):
    return get_con().execute(sql).fetchone()[0]


@st.cache_data(ttl=3600, show_spinner=False)
def get_colunas(_cache_key: str) -> list:
    """Retorna lista de colunas disponíveis — executa só uma vez por arquivo."""
    try:
        df_cols = get_con().execute("SELECT * FROM dados LIMIT 0").df()
        return list(df_cols.columns)
    except Exception:
        return []

def safe_col(key: str):
    cache_key = st.session_state.get("last_file", "")
    colunas = get_colunas(cache_key)
    c = COL.get(key, key)
    return c if c in colunas else None

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
    st.title("📋 Dashboard de Atendimentos")
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
    st.title("📋 Dashboard de Atendimentos")
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

def altura_grafico(n_itens: int, min_h: int = 200, por_item: int = 40) -> int:
    """Calcula altura proporcional ao número de itens."""
    return max(min_h, min(n_itens * por_item + 80, 600))

@st.cache_data(ttl=300, show_spinner=False)
def opts_db_cached(col, label_all, _cache_key):
    if not col:
        return [label_all]
    try:
        con = get_con()
        vals = con.execute(f'SELECT DISTINCT "{col}" FROM dados WHERE "{col}" IS NOT NULL ORDER BY "{col}" LIMIT 500').df()[col].tolist()
        return [label_all] + [str(v) for v in vals]
    except Exception:
        return [label_all]

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
st.title("📋 Dashboard de Atendimentos")
st.caption("Clique em qualquer linha para ver o perfil completo.")
st.markdown("---")

@st.cache_data(ttl=120, show_spinner=False)
def calc_metricas(where: str, _ck: str):
    """Calcula todas as métricas de uma vez — cache de 2 min."""
    con = get_con()
    def qv(sql):
        try: return con.execute(sql).fetchone()[0]
        except: return 0
    tf   = qv(f"SELECT COUNT(*) FROM dados {where}")
    cf   = qv(f'SELECT COUNT(DISTINCT "{c_cpf}") FROM dados {where}')     if c_cpf     else 0
    uf   = qv(f'SELECT COUNT(DISTINCT "{c_unidade}") FROM dados {where}') if c_unidade else 0
    sf   = qv(f'SELECT COUNT(DISTINCT "{c_servico}") FROM dados {where}') if c_servico else 0
    lf   = qv(f'SELECT COUNT(DISTINCT "{c_login}") FROM dados {where}')   if c_login   else 0
    taxa = 0
    if c_cpf and cf > 0:
        multi = qv(f'SELECT COUNT(*) FROM (SELECT "{c_cpf}" FROM dados {where} GROUP BY "{c_cpf}" HAVING COUNT(*) > 1)')
        taxa = round(multi / cf * 100, 1)
    delta = ""
    if c_data:
        ao = "AND" if where else "WHERE"
        ma = qv(f'SELECT COUNT(*) FROM dados {where} {ao} CAST("{c_data}" AS DATE) >= DATE_TRUNC(\'month\', CURRENT_DATE)')
        mp = qv(f'SELECT COUNT(*) FROM dados {where} {ao} CAST("{c_data}" AS DATE) >= DATE_TRUNC(\'month\', CURRENT_DATE) - INTERVAL 1 MONTH AND CAST("{c_data}" AS DATE) < DATE_TRUNC(\'month\', CURRENT_DATE)')
        if mp > 0:
            delta = f"{round(((ma-mp)/mp)*100,1):+.1f}% vs mês anterior"
    return tf, cf, uf, sf, lf, taxa, delta

_ck = st.session_state.get("last_file", "")
total_f, cpfs_f, uni_f, svc_f, login_f, taxa_retorno, delta_txt = calc_metricas(where_sql, _ck)

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total atendimentos", f"{total_f:,}", delta_txt if delta_txt else None)
m2.metric("CPFs distintos",     f"{cpfs_f:,}")
m3.metric("Unidades ativas",    f"{uni_f:,}")
m4.metric("Tipos de serviço",   f"{svc_f:,}")
m5.metric("Atendentes ativos",  f"{login_f:,}")
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
            total_cpf = run_val("SELECT COUNT(*) FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_sel + "'")
            pa, pb, pc, pd_ = st.columns(4)
            # Busca dados completos do cidadão diretamente no banco
            cols_perfil = [c for c in [c_cpf, c_nis, c_nasc] if c]
            df_perfil = get_con().execute(
                "SELECT " + ", ".join([chr(34)+c+chr(34) for c in cols_perfil]) +
                " FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_sel + "' LIMIT 1"
            ).df()
            nis_val  = str(df_perfil.iloc[0][c_nis])  if c_nis  and not df_perfil.empty else "—"
            nasc_val = str(df_perfil.iloc[0][c_nasc]) if c_nasc and not df_perfil.empty else "—"
            pa.metric("CPF",           cpf_sel)
            pb.metric("NIS",           nis_val)
            pc.metric("Nascimento",    nasc_val)
            pd_.metric("Atendimentos", total_cpf)

            cols_h = [c for c in [c_data, c_servico, c_unidade, c_quantia, c_login, c_categoria] if c]
            df_hist = run("SELECT " + ", ".join([chr(34)+c+chr(34) for c in cols_h]) + " FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_sel + "' LIMIT 500") 
            if len(df_hist) == 500:
                st.caption(f"⚠️ Exibindo 500 de {total_cpf:,} atendimentos para este CPF.")    
            st.markdown("##### Histórico de serviços")
            st.dataframe(df_hist, use_container_width=True, hide_index=True)

            if c_servico:
                svc_c = run("SELECT " + chr(34) + c_servico + chr(34) + " AS Servico, COUNT(*) AS Qtd FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_sel + "' GROUP BY " + chr(34) + c_servico + chr(34) + " ORDER BY Qtd DESC")
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
                              color="Qtd", color_continuous_scale="Teal", text="Qtd",
                              height=altura_grafico(len(d)))
                fig1.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig1.update_traces(textposition="outside")
                st.plotly_chart(fig1, use_container_width=True)
        with g2:
            if c_unidade:
                d = run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_unidade}" ORDER BY Qtd DESC')
                fig2 = px.bar(d, x="Qtd", y="Unidade", orientation="h", title="Por unidade",
                              color="Qtd", color_continuous_scale="Purples", text="Qtd",
                              height=altura_grafico(len(d)))
                fig2.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                fig2.update_traces(textposition="outside")
                st.plotly_chart(fig2, use_container_width=True)

        g3, g4 = st.columns(2)
        with g3:
            if c_login:
                try:
                    d = run(f'SELECT "{c_login}" AS Atendente, COUNT(*) AS Qtd FROM dados {where_sql} GROUP BY "{c_login}" ORDER BY Qtd DESC LIMIT 10')
                    fig3 = px.bar(d, x="Qtd", y="Atendente", orientation="h", title="Top 10 atendentes",
                                  color="Qtd", color_continuous_scale="Oranges", text="Qtd",
                                  height=altura_grafico(len(d)))
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

        # Ranking clicável — Serviços e CPFs
        if c_servico and c_cpf:
            st.markdown("##### 🏆 Ranking — Serviços mais procurados")
            top_svc = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Total, COUNT(DISTINCT "{c_cpf}") AS CPFs_unicos FROM dados {where_sql} GROUP BY "{c_servico}" ORDER BY Total DESC LIMIT 10')
            ev_svc = st.dataframe(top_svc, use_container_width=True, hide_index=True,
                                  on_select="rerun", selection_mode="single-row")
            svc_sel_rows = ev_svc.selection.rows if hasattr(ev_svc, "selection") else []
            if svc_sel_rows:
                svc_nome = top_svc.iloc[svc_sel_rows[0]]["Servico"]
                svc_safe = esc(svc_nome)
                st.markdown(f"**Registros para: {svc_nome}**")
                cols_svc = [c for c in [c_nome, c_cpf, c_unidade, c_data, c_login] if c]
                and_or = "AND" if where_sql else "WHERE"
                df_svc = run("SELECT " + ", ".join([chr(34)+c+chr(34) for c in cols_svc]) + " FROM dados " + where_sql + " " + and_or + " " + chr(34) + c_servico + chr(34) + " = '" + svc_safe + "' LIMIT 200")
                st.dataframe(df_svc, use_container_width=True, hide_index=True)

            st.markdown("##### 👤 Top 10 CPFs com mais atendimentos")
            if c_nome:
                top_cpf = run(f'SELECT "{c_nome}" AS Nome, "{c_cpf}" AS CPF, COUNT(*) AS Atendimentos FROM dados {where_sql} GROUP BY "{c_cpf}", "{c_nome}" ORDER BY Atendimentos DESC LIMIT 10')
            else:
                top_cpf = run(f'SELECT "{c_cpf}" AS CPF, COUNT(*) AS Atendimentos FROM dados {where_sql} GROUP BY "{c_cpf}" ORDER BY Atendimentos DESC LIMIT 10')
            ev_cpf = st.dataframe(top_cpf, use_container_width=True, hide_index=True,
                                  on_select="rerun", selection_mode="single-row")
            cpf_sel_rows = ev_cpf.selection.rows if hasattr(ev_cpf, "selection") else []
            if cpf_sel_rows:
                cpf_click = top_cpf.iloc[cpf_sel_rows[0]]["CPF"]
                cpf_safe = esc(str(cpf_click))
                nome_click = top_cpf.iloc[cpf_sel_rows[0]].get("Nome", cpf_click)
                total_click = run_val("SELECT COUNT(*) FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_safe + "'")
                st.markdown(f"**Histórico: {nome_click} — {total_click:,} atendimentos**")
                cols_h = [c for c in [c_data, c_servico, c_unidade, c_login, c_categoria] if c]
                df_click = run("SELECT " + ", ".join([chr(34)+c+chr(34) for c in cols_h]) + " FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_safe + "' ORDER BY " + chr(34) + str(c_data or "") + chr(34) + " DESC LIMIT 200")
                st.dataframe(df_click, use_container_width=True, hide_index=True)

# ─────────────────────────────────────
# ABA 3 — ATENDENTES E UNIDADES
# ─────────────────────────────────────
with aba_at:
    st.subheader("Perfil por atendente e unidade")
    w_base = f"{where_sql} AND" if where_sql.strip() else "WHERE"

    col_fat, col_funi = st.columns(2)
    with col_fat:
        if c_login:
            atendentes = run(f'SELECT DISTINCT "{c_login}" FROM dados {w_base} "{c_login}" IS NOT NULL ORDER BY "{c_login}"')[c_login].tolist()
            at_sels = st.multiselect("Selecione atendentes", atendentes)
        else:
            at_sels = []
    with col_funi:
        if c_unidade:
            unidades_at = run(f'SELECT DISTINCT "{c_unidade}" FROM dados {w_base} "{c_unidade}" IS NOT NULL ORDER BY "{c_unidade}"')[c_unidade].tolist()
            uni_sels = st.multiselect("Selecione unidades", unidades_at)
        else:
            uni_sels = []

    # Monta WHERE combinado
    if at_sels or uni_sels:
        wheres_at = []
        if at_sels and c_login:
            at_lista = ", ".join(["'" + esc(a) + "'" for a in at_sels])
            wheres_at.append(f'"{c_login}" IN ({at_lista})')
        if uni_sels and c_unidade:
            uni_lista = ", ".join(["'" + esc(u) + "'" for u in uni_sels])
            wheres_at.append(f'"{c_unidade}" IN ({uni_lista})')
        w_at = "WHERE " + " AND ".join(wheres_at)

        # ── Métricas ──
        tot_at  = run_val(f"SELECT COUNT(*) FROM dados {w_at}")
        q_cpf   = f'SELECT COUNT(DISTINCT "{c_cpf}") FROM dados {w_at}'
        q_uni   = f'SELECT COUNT(DISTINCT "{c_unidade}") FROM dados {w_at}'
        q_svc   = f'SELECT COUNT(DISTINCT "{c_servico}") FROM dados {w_at}'
        cpf_at  = run_val(q_cpf)  if c_cpf     else None
        uni_cnt = run_val(q_uni)  if c_unidade else None
        svc_cnt = run_val(q_svc)  if c_servico else None

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total atendimentos", f"{tot_at:,}")
        m2.metric("CPFs atendidos",     f"{cpf_at:,}"  if cpf_at  is not None else "—")
        m3.metric("Unidades",           f"{uni_cnt:,}" if uni_cnt is not None else "—")
        m4.metric("Tipos de serviço",   f"{svc_cnt:,}" if svc_cnt is not None else "—")

        # ── Comparativo atendentes (se mais de 1 selecionado) ──
        if c_login and len(at_sels) > 1:
            st.markdown("##### Comparativo entre atendentes")
            d_comp = run(f'SELECT "{c_login}" AS Atendente, COUNT(*) AS Total FROM dados {w_at} GROUP BY "{c_login}" ORDER BY Total DESC')
            fig_comp = px.bar(d_comp, x="Atendente", y="Total", color="Atendente",
                              text="Total", title="Total por atendente",
                              height=altura_grafico(len(d_comp), por_item=60))
            fig_comp.update_layout(showlegend=False, xaxis_title=None)
            fig_comp.update_traces(textposition="outside")
            st.plotly_chart(fig_comp, use_container_width=True)

        # ── Comparativo unidades (se mais de 1 selecionada) ──
        if c_unidade and len(uni_sels) > 1:
            st.markdown("##### Comparativo entre unidades")
            d_uni_comp = run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Total FROM dados {w_at} GROUP BY "{c_unidade}" ORDER BY Total DESC')
            fig_uni = px.bar(d_uni_comp, x="Unidade", y="Total", color="Unidade",
                             text="Total", title="Total por unidade",
                             height=altura_grafico(len(d_uni_comp), por_item=60))
            fig_uni.update_layout(showlegend=False, xaxis_title=None)
            fig_uni.update_traces(textposition="outside")
            st.plotly_chart(fig_uni, use_container_width=True)

        # ── Gráficos ──
        gl, gr = st.columns(2)
        with gl:
            if c_servico:
                d_svc = run(f'SELECT "{c_servico}" AS Servico, COUNT(*) AS Qtd FROM dados {w_at} GROUP BY "{c_servico}" ORDER BY Qtd DESC LIMIT 10')
                fig_s = px.bar(d_svc, x="Qtd", y="Servico", orientation="h",
                               title="Serviços realizados (top 10)",
                               color="Qtd", color_continuous_scale="Teal", text="Qtd",
                               height=altura_grafico(len(d_svc)))
                fig_s.update_layout(coloraxis_showscale=False, yaxis_title=None)
                fig_s.update_traces(textposition="outside")
                st.plotly_chart(fig_s, use_container_width=True)
        with gr:
            if c_unidade and not uni_sels:
                # Só mostra pizza de unidade se não filtrou por unidade específica
                d_uni = run(f'SELECT "{c_unidade}" AS Unidade, COUNT(*) AS Qtd FROM dados {w_at} GROUP BY "{c_unidade}"')
                fig_u = px.pie(d_uni, names="Unidade", values="Qtd", title="Distribuição por unidade")
                st.plotly_chart(fig_u, use_container_width=True)
            elif c_login and not at_sels:
                d_log = run(f'SELECT "{c_login}" AS Atendente, COUNT(*) AS Qtd FROM dados {w_at} GROUP BY "{c_login}" ORDER BY Qtd DESC LIMIT 10')
                fig_l = px.pie(d_log, names="Atendente", values="Qtd", title="Distribuição por atendente")
                st.plotly_chart(fig_l, use_container_width=True)

        # ── Tabela clicável ──
        cols_at = [c for c in [c_nome, c_cpf, c_login, c_servico, c_data, c_unidade] if c]
        col_sel = ", ".join([f'"{c}"' for c in cols_at])
        df_at = run(f"SELECT {col_sel} FROM dados {w_at} LIMIT 500")
        st.markdown("##### Registros — clique em uma linha para ver o perfil do cidadão")
        ev_at = st.dataframe(df_at, use_container_width=True, hide_index=True,
                             on_select="rerun", selection_mode="single-row")

        sel_at = ev_at.selection.rows if hasattr(ev_at, "selection") else []
        if sel_at and c_cpf:
            row_at = df_at.iloc[sel_at[0]]
            cpf_at_sel = str(row_at.get(c_cpf, "")).replace("'", "''")
            if cpf_at_sel:
                st.markdown("---")
                nome_at_sel = row_at.get(c_nome, "Cidadão")
                st.subheader(f"👤 Perfil: {nome_at_sel}")

                total_cpf_at = run_val("SELECT COUNT(*) FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_at_sel + "'")
                pa, pb, pc, pd_ = st.columns(4)
                cols_perfil_at = [c for c in [c_cpf, c_nis, c_nasc] if c]
                df_perfil_at = get_con().execute(
                    "SELECT " + ", ".join([chr(34)+c+chr(34) for c in cols_perfil_at]) +
                    " FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_at_sel + "' LIMIT 1"
                ).df()
                nis_at  = str(df_perfil_at.iloc[0][c_nis])  if c_nis  and not df_perfil_at.empty else "—"
                nasc_at = str(df_perfil_at.iloc[0][c_nasc]) if c_nasc and not df_perfil_at.empty else "—"
                pa.metric("CPF",           cpf_at_sel)
                pb.metric("NIS",           nis_at)
                pc.metric("Nascimento",    nasc_at)
                pd_.metric("Atendimentos", f"{total_cpf_at:,}")

                cols_h = [c for c in [c_data, c_servico, c_unidade, c_quantia, c_login, c_categoria] if c]
                col_h_sel = ", ".join([f'"{c}"' for c in cols_h])
                df_hist_at = run("SELECT " + col_h_sel + " FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_at_sel + "' ORDER BY " + chr(34) + str(c_data or "") + chr(34) + " DESC LIMIT 500")
                st.markdown("##### Histórico completo de serviços")
                st.dataframe(df_hist_at, use_container_width=True, hide_index=True)

                if c_servico and not df_hist_at.empty:
                    svc_at_c = run("SELECT " + chr(34) + c_servico + chr(34) + " AS Servico, COUNT(*) AS Qtd FROM dados WHERE " + chr(34) + c_cpf + chr(34) + " = '" + cpf_at_sel + "' GROUP BY " + chr(34) + c_servico + chr(34) + " ORDER BY Qtd DESC")
                    fig_h = px.bar(svc_at_c, x="Qtd", y="Servico", orientation="h",
                                   title="Serviços recebidos", color="Qtd",
                                   color_continuous_scale="Blues", text="Qtd")
                    fig_h.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Quantidade")
                    fig_h.update_traces(textposition="outside")
                    st.plotly_chart(fig_h, use_container_width=True)
        # ── Exportar dados da seleção atual ──
        st.markdown("---")
        st.markdown("##### 📥 Exportar dados desta seleção")
        EXPORT_LIMIT_AT = 50_000
        st.caption(f"Exporta os registros do filtro atual (atendentes/unidades selecionados) — limite {EXPORT_LIMIT_AT:,} linhas.")

        col_at_exp = [c for c in [c_nome, c_cpf, c_nis, c_nasc, c_login, c_servico, c_data, c_unidade, c_categoria] if c]
        df_at_exp = run("SELECT " + ", ".join([chr(34)+c+chr(34) for c in col_at_exp]) + " FROM dados " + w_at + " LIMIT " + str(EXPORT_LIMIT_AT))

        out_at = io.BytesIO()
        with pd.ExcelWriter(out_at, engine="openpyxl") as writer:
            df_at_exp.to_excel(writer, index=False, sheet_name="Atendimentos")
            if c_login:
                run("SELECT " + chr(34) + c_login + chr(34) + " AS Atendente, COUNT(*) AS Total FROM dados " + w_at + " GROUP BY " + chr(34) + c_login + chr(34) + " ORDER BY Total DESC").to_excel(writer, index=False, sheet_name="Por Atendente")
            if c_servico:
                run("SELECT " + chr(34) + c_servico + chr(34) + " AS Servico, COUNT(*) AS Total FROM dados " + w_at + " GROUP BY " + chr(34) + c_servico + chr(34) + " ORDER BY Total DESC").to_excel(writer, index=False, sheet_name="Por Serviço")
            if c_unidade:
                run("SELECT " + chr(34) + c_unidade + chr(34) + " AS Unidade, COUNT(*) AS Total FROM dados " + w_at + " GROUP BY " + chr(34) + c_unidade + chr(34) + " ORDER BY Total DESC").to_excel(writer, index=False, sheet_name="Por Unidade")

        st.download_button(
            "⬇️ Baixar Excel desta seleção",
            out_at.getvalue(),
            "atendimentos_selecao.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    else:
        st.info("Selecione ao menos um atendente ou uma unidade para ver os dados.")

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